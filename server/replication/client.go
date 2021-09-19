package replication

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/YuriyNasretdinov/chukcha/client"
	"github.com/YuriyNasretdinov/chukcha/protocol"
)

const defaultClientTimeout = 10 * time.Second
const pollInterval = 50 * time.Millisecond
const retryTimeout = 1 * time.Second

const batchSize = 4 * 1024 * 1024 // 4 MiB

var errNotFound = errors.New("chunk not found")
var errIncomplete = errors.New("chunk is not complete")

// Client describes the client-side state of replication and continiously downloads
// new chunks from other servers.
type Client struct {
	state        *State
	wr           DirectWriter
	instanceName string
	httpCl       *http.Client
	s            *client.Simple

	// mu protects perCategory
	mu          sync.Mutex
	perCategory map[string]*CategoryDownloader
}

type CategoryDownloader struct {
	eventsCh chan Chunk

	state        *State
	wr           DirectWriter
	instanceName string
	httpCl       *http.Client
	s            *client.Simple

	// Information about the current chunk being downloaded.
	curMu          sync.Mutex
	curChunk       Chunk
	curChunkCancel context.CancelFunc
	curChunkDone   chan bool
}

// DirectWriter writes to underlying storage directly for replication purposes.
type DirectWriter interface {
	Stat(category string, fileName string) (size int64, exists bool, err error)
	WriteDirect(category string, fileName string, contents []byte) error
	AckDirect(ctx context.Context, category string, chunk string) error
}

// NewClient initialises the replication client.
func NewClient(st *State, wr DirectWriter, instanceName string) *Client {
	return &Client{
		state:        st,
		wr:           wr,
		instanceName: instanceName,
		httpCl: &http.Client{
			Timeout: defaultClientTimeout,
		},
		s:           client.NewSimple(nil),
		perCategory: make(map[string]*CategoryDownloader),
	}
}

func (c *Client) Loop(ctx context.Context) {
	go c.acknowledgeLoop(ctx)
	c.replicationLoop(ctx)
}

func (c *Client) acknowledgeLoop(ctx context.Context) {
	for ch := range c.state.WatchAcknowledgeQueue(ctx, c.instanceName) {
		log.Printf("acknowledging chunk %+v", ch)

		c.ensureChunkIsNotBeingDownloaded(ch)

		// TODO: handle errors better
		if err := c.wr.AckDirect(ctx, ch.Category, ch.FileName); err != nil {
			log.Printf("Could not ack chunk %+v from the acknowledge queue: %v", ch, err)
		}

		if err := c.state.DeleteChunkFromAcknowledgeQueue(ctx, c.instanceName, ch); err != nil {
			log.Printf("Could not delete chunk %+v from the acknowledge queue: %v", ch, err)
		}
	}
}

// We only care about the situation when the chunk is being downloaded
// while we are deleting it from local disk, because:
//
// 1. If we finished downloading chunk before ack, it is ok, no race condition here.
// 2. If we try to download chunk after ack is done, the download request will fail
//    because the chunk already does not exist at the source (because it was acknowledged).
func (c *Client) ensureChunkIsNotBeingDownloaded(ch Chunk) {
	c.mu.Lock()
	downloader, ok := c.perCategory[ch.Category]
	c.mu.Unlock()

	if !ok {
		return
	}

	downloader.curMu.Lock()
	downloadedChunk := downloader.curChunk
	cancelFunc := downloader.curChunkCancel
	doneCh := downloader.curChunkDone
	downloader.curMu.Unlock()

	if downloadedChunk.Category != ch.Category || downloadedChunk.FileName != ch.FileName || downloadedChunk.Owner != ch.Owner {
		return
	}

	// cancel func does nothing if context is already finished and cancelled
	cancelFunc()

	// write to doneCh is guaranteed after chunk finished downloading
	<-doneCh
}

func (c *Client) replicationLoop(ctx context.Context) {
	for ch := range c.state.WatchReplicationQueue(ctx, c.instanceName) {
		downloader, ok := c.perCategory[ch.Category]
		if !ok {
			downloader = &CategoryDownloader{
				eventsCh:     make(chan Chunk, 3),
				state:        c.state,
				wr:           c.wr,
				instanceName: c.instanceName,
				httpCl:       c.httpCl,
				s:            c.s,
			}
			go downloader.Loop(ctx)

			c.mu.Lock()
			c.perCategory[ch.Category] = downloader
			c.mu.Unlock()
		}

		downloader.eventsCh <- ch
	}
}

func (c *CategoryDownloader) Loop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ch := <-c.eventsCh:
			c.downloadChunk(ctx, ch)

			// TODO: handle errors
			if err := c.state.DeleteChunkFromReplicationQueue(ctx, c.instanceName, ch); err != nil {
				log.Printf("Could not delete chunk %+v from the replication queue: %v", ch, err)
			}
		}
	}
}

func (c *CategoryDownloader) downloadChunk(parentCtx context.Context, ch Chunk) {
	log.Printf("downloading chunk %+v", ch)
	defer log.Printf("finished downloading chunk %+v", ch)

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	c.curMu.Lock()
	c.curChunk = ch
	c.curChunkCancel = cancel
	c.curChunkDone = make(chan bool)
	c.curMu.Unlock()

	defer func() {
		// Let everyone how is interested know that we no longer processing
		// this chunk.
		close(c.curChunkDone)

		c.curMu.Lock()
		c.curChunk = Chunk{}
		c.curChunkCancel = nil
		c.curChunkDone = nil
		c.curMu.Unlock()
	}()

	for {
		err := c.downloadChunkIteration(ctx, ch)
		if errors.Is(err, errNotFound) {
			log.Printf("got a not found error while downloading chunk %+v, skipping chunk", ch)
			return
		} else if errors.Is(err, errIncomplete) {
			time.Sleep(pollInterval)
			continue
		} else if err != nil {
			log.Printf("got an error while downloading chunk %+v: %v", ch, err)

			select {
			case <-ctx.Done():
				return
			default:
			}

			// TODO: exponential backoff
			time.Sleep(retryTimeout)
			continue
		}

		return
	}
}

func (c *CategoryDownloader) downloadChunkIteration(ctx context.Context, ch Chunk) error {
	size, _, err := c.wr.Stat(ch.Category, ch.FileName)
	if err != nil {
		return fmt.Errorf("getting file stat: %v", err)
	}

	addr, err := c.listenAddrForChunk(ctx, ch)
	if err != nil {
		return fmt.Errorf("getting listen address: %v", err)
	}

	info, err := c.getChunkInfo(ctx, addr, ch)
	if err == errNotFound {
		log.Printf("chunk not found at %q", addr)
		return nil
	} else if err != nil {
		return err
	}

	if uint64(size) >= info.Size {
		if !info.Complete {
			return errIncomplete
		}
		return nil
	}

	buf, err := c.downloadPart(ctx, addr, ch, size)
	if err != nil {
		return fmt.Errorf("downloading chunk: %w", err)
	}

	if err := c.wr.WriteDirect(ch.Category, ch.FileName, buf); err != nil {
		return fmt.Errorf("writing chunk: %v", err)
	}

	if !info.Complete {
		return errIncomplete
	}

	return nil
}

func (c *CategoryDownloader) listenAddrForChunk(ctx context.Context, ch Chunk) (string, error) {
	peers, err := c.state.ListPeers(ctx)
	if err != nil {
		return "", err
	}

	var addr string
	for _, p := range peers {
		if p.InstanceName == ch.Owner {
			addr = p.ListenAddr
			break
		}
	}

	if addr == "" {
		return "", fmt.Errorf("could not find peer %q", ch.Owner)
	}

	return "http://" + addr, nil
}

func (c *CategoryDownloader) getChunkInfo(ctx context.Context, addr string, curCh Chunk) (protocol.Chunk, error) {
	chunks, err := c.s.ListChunks(ctx, curCh.Category, addr, true)
	if err != nil {
		return protocol.Chunk{}, err
	}

	for _, ch := range chunks {
		if ch.Name == curCh.FileName {
			return ch, nil
		}
	}

	return protocol.Chunk{}, errNotFound
}

func (c *CategoryDownloader) downloadPart(ctx context.Context, addr string, ch Chunk, off int64) ([]byte, error) {
	u := url.Values{}
	u.Add("off", strconv.Itoa(int(off)))
	u.Add("maxSize", strconv.Itoa(batchSize))
	u.Add("chunk", ch.FileName)
	u.Add("category", ch.Category)
	u.Add("from_replication", "1")

	readURL := fmt.Sprintf("%s/read?%s", addr, u.Encode())

	req, err := http.NewRequestWithContext(ctx, "GET", readURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating http request: %w", err)
	}

	resp, err := c.httpCl.Do(req)
	if err != nil {
		return nil, fmt.Errorf("read %q: %v", readURL, err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		defer io.Copy(io.Discard, resp.Body)

		if resp.StatusCode == http.StatusNotFound {
			return nil, errNotFound
		}

		return nil, fmt.Errorf("http status code %d", resp.StatusCode)
	}

	var b bytes.Buffer
	_, err = io.Copy(&b, resp.Body)

	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

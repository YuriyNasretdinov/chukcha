package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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

type categoryAndReplica struct {
	category string
	replica  string
}

// Client describes the client-side state of replication and continiously downloads
// new chunks from other servers.
type Client struct {
	logger       *log.Logger
	dirname      string
	peers        []Peer
	wr           DirectWriter
	instanceName string
	httpCl       *http.Client
	r            *client.Raw

	// mu protects everything below
	mu                     sync.Mutex
	perCategoryPerReplica  map[categoryAndReplica]*CategoryDownloader
	peersAlreadyStarted    map[string]bool
	ackPeersAlreadyStarted map[string]bool
}

type CategoryDownloader struct {
	logger   *log.Logger
	eventsCh chan Chunk

	peers        []Peer
	wr           DirectWriter
	instanceName string
	httpCl       *http.Client
	r            *client.Raw

	// Information about the current chunk being downloaded.
	curMu          sync.Mutex
	curChunk       Chunk
	curChunkCancel context.CancelFunc
	curChunkDone   chan bool
}

// DirectWriter writes to underlying storage directly for replication purposes.
type DirectWriter interface {
	Stat(category string, fileName string) (size int64, exists bool, deleted bool, err error)
	WriteDirect(category string, fileName string, contents []byte) error
	AckDirect(ctx context.Context, category string, chunk string) error

	SetReplicationDisabled(category string, v bool) error
	Write(ctx context.Context, category string, msgs []byte) (chunkName string, off int64, err error)
}

// NewClient initialises the replication client.
func NewClient(logger *log.Logger, dirname string, wr DirectWriter, peers []Peer, instanceName string) *Client {
	httpCl := &http.Client{
		Timeout: defaultClientTimeout,
	}

	raw := client.NewRaw(httpCl)
	raw.Logger = logger

	return &Client{
		logger:                 logger,
		dirname:                dirname,
		wr:                     wr,
		instanceName:           instanceName,
		httpCl:                 httpCl,
		r:                      raw,
		peers:                  peers,
		perCategoryPerReplica:  make(map[categoryAndReplica]*CategoryDownloader),
		peersAlreadyStarted:    make(map[string]bool),
		ackPeersAlreadyStarted: make(map[string]bool),
	}
}

func (c *Client) Loop(ctx context.Context, disableAcknowledge bool) {
	if !disableAcknowledge {
		go c.startPerPeerLoops(ctx, systemAck, c.peers, c.ackPeersAlreadyStarted, c.onNewChunkInAckQueue)
	}

	c.startPerPeerLoops(ctx, systemReplication, c.peers, c.peersAlreadyStarted, c.onNewChunkInReplicationQueue)
}

// We only care about the situation when the chunk is being downloaded
// while we are deleting it from local disk, because:
//
// 1. If we finished downloading chunk before ack, it is ok, no race condition here.
// 2. If we try to download chunk after ack is done, the download request will fail
//    because the chunk already does not exist at the source (because it was acknowledged).
func (c *Client) ensureChunkIsNotBeingDownloaded(ch Chunk) {
	c.mu.Lock()
	downloader, ok := c.perCategoryPerReplica[categoryAndReplica{
		category: ch.Category,
		replica:  ch.Owner,
	}]
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

func (c *Client) onNewChunkInAckQueue(ctx context.Context, ch Chunk) {
	c.logger.Printf("acknowledging chunk %+v", ch)

	c.ensureChunkIsNotBeingDownloaded(ch)

	// TODO: handle errors better
	if err := c.wr.AckDirect(ctx, ch.Category, ch.FileName); err != nil {
		c.logger.Printf("Could not ack chunk %+v from the acknowledge queue: %v", ch, err)
	}
}

func (c *Client) onNewChunkInReplicationQueue(ctx context.Context, ch Chunk) {
	key := categoryAndReplica{
		category: ch.Category,
		replica:  ch.Owner,
	}

	downloader, ok := c.perCategoryPerReplica[key]
	if !ok {
		downloader = &CategoryDownloader{
			logger:       c.logger,
			eventsCh:     make(chan Chunk, 3),
			peers:        c.peers,
			wr:           c.wr,
			instanceName: c.instanceName,
			httpCl:       c.httpCl,
			r:            c.r,
		}
		go downloader.Loop(ctx)

		c.mu.Lock()
		c.perCategoryPerReplica[key] = downloader
		c.mu.Unlock()
	}

	downloader.eventsCh <- ch
}

func (c *Client) startPerPeerLoops(ctx context.Context, category string, peers []Peer, alreadyStarted map[string]bool, onChunk func(context.Context, Chunk)) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, peer := range peers {
		if peer.InstanceName == c.instanceName {
			continue
		} else if alreadyStarted[peer.InstanceName] {
			continue
		}

		alreadyStarted[peer.InstanceName] = true
		go c.perPeerLoop(ctx, category, peer, onChunk)
	}
}

func (c *Client) perPeerLoop(ctx context.Context, category string, p Peer, onChunk func(context.Context, Chunk)) {
	if category == systemReplication {
		go c.downloadExistingCategories(ctx, p, onChunk)
	}

	// TODO: handle change of address for peer
	// TODO: in the client, allow to start reading from the end.
	cl := client.NewSimple([]string{"http://" + p.ListenAddr})
	cl.SetAcknowledge(false)

	stateFilePath := filepath.Join(c.dirname, category+"-"+p.InstanceName+"-state.json")
	stateContents, err := ioutil.ReadFile(stateFilePath)
	if err == nil {
		cl.RestoreSavedState(stateContents)
	} else if !errors.Is(err, os.ErrNotExist) {
		c.logger.Printf("Could not read state file %s: %v", stateFilePath, err)
	}

	scratch := make([]byte, 256*1024)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		err := cl.Process(ctx, category, scratch, func(b []byte) error {
			d := json.NewDecoder(bytes.NewReader(b))

			for {
				var ch Chunk
				err := d.Decode(&ch)
				if errors.Is(err, io.EOF) {
					return nil
				} else if err != nil {
					c.logger.Printf("Failed to decode chunk from category %q: %v", category, err)
					continue
				}

				onChunk(ctx, ch)
			}
		})

		if err != nil {
			log.Printf("Failed to get events from category %q: %v", category, err)
			time.Sleep(10 * time.Second)
			continue
		}

		stateContents, err := cl.MarshalState()
		if err != nil {
			c.logger.Printf("Could not marshal client state: %v", err)
			continue
		}

		if err := ioutil.WriteFile(stateFilePath+".tmp", stateContents, 0666); err != nil {
			c.logger.Printf("Could not write state file %q: %v", stateFilePath+".tmp", err)
		}

		if err := os.Rename(stateFilePath+".tmp", stateFilePath); err != nil {
			c.logger.Printf("Could not rename state file %q: %v", stateFilePath, err)
		}
	}
}

func (c *Client) downloadExistingCategories(ctx context.Context, p Peer, onChunk func(context.Context, Chunk)) {
	for {
		err := c.tryDownloadExistingCategories(ctx, p, onChunk)
		if err == nil {
			return
		}

		c.logger.Printf("Failed to download existing categories from peer %+v: %v", p, err)
		time.Sleep(retryTimeout)
	}
}

func (c *Client) tryDownloadExistingCategories(ctx context.Context, p Peer, onChunk func(context.Context, Chunk)) error {
	c.logger.Printf("Downloading all categories from peer %+v", p)

	cl := client.NewRaw(http.DefaultClient)
	cats, err := cl.ListCategories(ctx, "http://"+p.ListenAddr)
	if err != nil {
		return fmt.Errorf("listing categories: %w", err)
	}

	for _, cat := range cats {
		if strings.HasPrefix(cat, systemCategoryPrefix) {
			continue
		}

		c.logger.Printf("Downloading list of chunks for category %q from peer %+v", cat, p)

		chunks, err := cl.ListChunks(ctx, "http://"+p.ListenAddr, cat, true)
		if err != nil {
			return fmt.Errorf("listing chunks for category %q: %w", cat, err)
		}

		if len(chunks) == 0 {
			continue
		}

		sort.Slice(chunks, func(i, j int) bool {
			return chunks[i].Name < chunks[j].Name
		})

		latestCh := chunks[len(chunks)-1]
		onChunk(ctx, Chunk{
			Owner:    p.InstanceName,
			Category: cat,
			FileName: latestCh.Name,
		})
	}

	return nil
}

func (c *CategoryDownloader) Loop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ch := <-c.eventsCh:
			c.downloadAllChunksUpTo(ctx, ch)
		}
	}
}

func (c *CategoryDownloader) downloadAllChunksUpTo(ctx context.Context, toReplicate Chunk) {
	for {
		err := c.downloadAllChunksUpToIteration(ctx, toReplicate)
		if err != nil {
			c.logger.Printf("got an error while doing downloadAllChunksUpToIteration for chunk %+v: %v", toReplicate, err)

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

// downloadAllChunksUpTo downloads all chunks from the owner of the requested
// chunk that are either the requested chunk, or the chunks that are older
// than the chunk requested.
//
// This is needed because we can receive chunk replicate requests out of order,
// but replication must be in order because readers rely on the fact that if
// they read chunk X then there is no need to even examine chunks X-1, X-2, etc.
func (c *CategoryDownloader) downloadAllChunksUpToIteration(ctx context.Context, toReplicate Chunk) error {
	addr, err := c.listenAddrForChunk(ctx, toReplicate)
	if err != nil {
		return fmt.Errorf("getting listen address: %v", err)
	}

	chunks, err := c.r.ListChunks(ctx, addr, toReplicate.Category, true)
	if err != nil {
		return fmt.Errorf("list chunks from %q: %v", addr, err)
	}

	var chunksToReplicate []protocol.Chunk
	for _, ch := range chunks {
		instance, _ := protocol.ParseChunkFileName(ch.Name)

		if instance == toReplicate.Owner && ch.Name <= toReplicate.FileName {
			chunksToReplicate = append(chunksToReplicate, ch)
		}
	}

	sort.Slice(chunksToReplicate, func(i, j int) bool {
		return chunksToReplicate[i].Name < chunksToReplicate[j].Name
	})

	for _, ch := range chunksToReplicate {
		size, exists, deleted, err := c.wr.Stat(toReplicate.Category, ch.Name)
		if err != nil {
			return fmt.Errorf("getting file stat: %v", err)
		}

		// Do not redownload chunks that were already acknowledged.
		if deleted {
			continue
		}

		// TODO: test downloading empty chunks
		if !exists || ch.Size > uint64(size) || !ch.Complete || (ch.Complete && ch.Size == 0) {
			c.downloadChunk(ctx, Chunk{
				Owner:    toReplicate.Owner,
				Category: toReplicate.Category,
				FileName: ch.Name,
			})
		}
	}

	return nil
}

func (c *CategoryDownloader) downloadChunk(parentCtx context.Context, ch Chunk) {
	c.logger.Printf("downloading chunk %+v", ch)
	defer c.logger.Printf("finished downloading chunk %+v", ch)

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
			c.logger.Printf("got a not found error while downloading chunk %+v, skipping chunk", ch)
			return
		} else if errors.Is(err, errIncomplete) {
			time.Sleep(pollInterval)
			continue
		} else if err != nil {
			c.logger.Printf("got an error while downloading chunk %+v: %v", ch, err)

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
	chunkReadOff, exists, _, err := c.wr.Stat(ch.Category, ch.FileName)
	if err != nil {
		return fmt.Errorf("getting file stat: %v", err)
	}

	addr, err := c.listenAddrForChunk(ctx, ch)
	if err != nil {
		return fmt.Errorf("getting listen address: %v", err)
	}

	info, err := c.getChunkInfo(ctx, addr, ch)
	if err == errNotFound {
		c.logger.Printf("chunk %+v not found at %q", ch, addr)
		return nil
	} else if err != nil {
		return err
	}

	if exists && uint64(chunkReadOff) >= info.Size {
		if !info.Complete {
			return errIncomplete
		}
		return nil
	}

	buf, err := c.downloadPart(ctx, addr, ch, chunkReadOff)
	if err != nil {
		return fmt.Errorf("downloading chunk: %w", err)
	}

	if err := c.wr.WriteDirect(ch.Category, ch.FileName, buf); err != nil {
		return fmt.Errorf("writing chunk: %v", err)
	}

	size, _, _, err := c.wr.Stat(ch.Category, ch.FileName)
	if err != nil {
		return fmt.Errorf("getting file stat: %v", err)
	}

	// Because writing to disk on the client side of the replication can fail
	// we must have a separate request to tell the owner of the chunk
	// that we did successfully download and write locally the downloaded data.
	//
	// Note: this request can fail, and it won't be retried by this loop because
	// the local chunk size is already downloaded to the required size.
	// However, client wait timeout will solve this issue because the client
	// should eventually retry the write and they will receive the noticication.
	// Alternatively, if some other clients write something to this category
	// in the meantime, replication client will also send ackDownload request
	// for the new range, and because we only send the final size, it should
	// notify the client that was waiting for the previous write.
	if err := c.ackDownload(ctx, addr, ch, uint64(size)); err != nil {
		return fmt.Errorf("replication ack: %v", err)
	}

	if uint64(size) < info.Size || !info.Complete {
		return errIncomplete
	}

	return nil
}

func (c *CategoryDownloader) listenAddrForChunk(ctx context.Context, ch Chunk) (string, error) {
	var addr string
	for _, p := range c.peers {
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
	chunks, err := c.r.ListChunks(ctx, addr, curCh.Category, true)
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

func (c *CategoryDownloader) ackDownload(ctx context.Context, addr string, ch Chunk, size uint64) error {
	u := url.Values{}
	u.Add("size", strconv.FormatUint(size, 10))
	u.Add("chunk", ch.FileName)
	u.Add("category", ch.Category)
	u.Add("instance", c.instanceName)

	ackURL := fmt.Sprintf("%s/replication/ack?%s", addr, u.Encode())

	req, err := http.NewRequestWithContext(ctx, "GET", ackURL, nil)
	if err != nil {
		return fmt.Errorf("creating http request: %w", err)
	}

	resp, err := c.httpCl.Do(req)
	if err != nil {
		return fmt.Errorf("replication ack %q: %v", ackURL, err)
	}

	defer resp.Body.Close()

	var b bytes.Buffer
	_, err = io.Copy(&b, resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http status code %d, error message %s", resp.StatusCode, b.Bytes())
	}

	return err
}

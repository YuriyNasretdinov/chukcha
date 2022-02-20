package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/YuriyNasretdinov/chukcha/generics/heap"
	"github.com/YuriyNasretdinov/chukcha/protocol"
)

var errBufTooSmall = errors.New("the buffer is too small to contain a single message")

const DeletedSuffix = ".deleted"

type StorageHooks interface {
	AfterCreatingChunk(ctx context.Context, category string, fileName string) error
	AfterAcknowledgeChunk(ctx context.Context, category string, fileName string) error
}

type downloadNotification struct {
	chunk string
	size  uint64
}

type replicationSub struct {
	chunk string
	size  uint64
	ch    chan bool
}

func (r replicationSub) Less(v replicationSub) bool {
	if r.chunk == v.chunk {
		return r.size < v.size
	}

	return r.chunk < v.chunk
}

// OnDisk stores all the data on disk.
type OnDisk struct {
	logger       *log.Logger
	dirname      string
	category     string
	instanceName string
	maxChunkSize uint64

	repl StorageHooks

	// downloadNoficationMu protects downloadNotifications
	downloadNoficationMu     sync.RWMutex
	downloadNotificationSubs heap.Min[replicationSub]
	downloadNotifications    map[string]*downloadNotification

	// writeMu protects lastChunk* entries
	writeMu                     sync.Mutex
	lastChunkFp                 *os.File
	lastChunk                   string
	lastChunkSize               uint64
	lastChunkIdx                uint64
	lastChunkAddedToReplication bool
}

// NewOnDisk creates a server that stores all it's data on disk.
func NewOnDisk(logger *log.Logger, dirname, category, instanceName string, maxChunkSize uint64, rotateChunkInterval time.Duration, repl StorageHooks) (*OnDisk, error) {
	s := &OnDisk{
		logger:                   logger,
		dirname:                  dirname,
		category:                 category,
		instanceName:             instanceName,
		repl:                     repl,
		maxChunkSize:             maxChunkSize,
		downloadNotifications:    make(map[string]*downloadNotification),
		downloadNotificationSubs: heap.NewMin[replicationSub](),
	}

	if err := s.initLastChunkIdx(dirname); err != nil {
		return nil, err
	}

	go s.createNextChunkThread(rotateChunkInterval)

	return s, nil
}

func (s *OnDisk) initLastChunkIdx(dirname string) error {
	files, err := os.ReadDir(dirname)
	if err != nil {
		return fmt.Errorf("readdir(%q): %v", dirname, err)
	}

	for _, fi := range files {
		instance, chunkIdx := protocol.ParseChunkFileName(fi.Name())
		if chunkIdx < 0 || instance != s.instanceName {
			continue
		}

		if uint64(chunkIdx)+1 >= s.lastChunkIdx {
			s.lastChunkIdx = uint64(chunkIdx) + 1
		}
	}

	return nil
}

// WriteDirect writes directly to the chunk files to avoid circular dependency with
// replication.
// THE METHOD IS NOT THREAD-SAFE.
func (s *OnDisk) WriteDirect(chunk string, contents []byte) error {
	fl := os.O_CREATE | os.O_WRONLY | os.O_APPEND

	filename := filepath.Join(s.dirname, chunk)
	fp, err := os.OpenFile(filename, fl, 0666)
	if err != nil {
		return err
	}
	defer fp.Close()

	_, err = fp.Write(contents)
	return err
}

func (s *OnDisk) createNextChunkThread(interval time.Duration) {
	for {
		time.Sleep(interval)

		if err := s.tryCreateNextEmptyChunkIfNeeded(); err != nil {
			s.logger.Printf("Creating next empty chunk in background failed: %v", err)
		}
	}
}

func (s *OnDisk) tryCreateNextEmptyChunkIfNeeded() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.lastChunkSize == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.logger.Printf("Creating empty chunk from timer")

	return s.createNextChunk(ctx)
}

// createNextChunk creates a new chunk and closes
// the previous file descriptor if needed.
//
// This method is meant to be used either from a timer
// to close the active chunk or to create a new chunk
// when the max chunk is exceeded.
func (s *OnDisk) createNextChunk(ctx context.Context) error {
	if s.lastChunkFp != nil {
		s.lastChunkFp.Close()
		s.lastChunkFp = nil
	}

	newChunk := fmt.Sprintf("%s-chunk%09d", s.instanceName, s.lastChunkIdx)

	fp, err := os.OpenFile(filepath.Join(s.dirname, newChunk), os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer fp.Close()

	s.lastChunk = newChunk
	s.lastChunkSize = 0
	s.lastChunkIdx++
	s.lastChunkAddedToReplication = false

	if err := s.repl.AfterCreatingChunk(ctx, s.category, s.lastChunk); err != nil {
		return fmt.Errorf("after creating new chunk: %w", err)
	}

	s.lastChunkAddedToReplication = true

	return nil
}

func (s *OnDisk) getLastChunkFp() (*os.File, error) {
	if s.lastChunkFp != nil {
		return s.lastChunkFp, nil
	}

	fp, err := os.OpenFile(filepath.Join(s.dirname, s.lastChunk), os.O_WRONLY, 0666)
	if err != nil {
		return nil, fmt.Errorf("open chunk for writing: %v", err)
	}
	s.lastChunkFp = fp

	return fp, nil
}

// Write accepts the messages from the clients and stores them.
func (s *OnDisk) Write(ctx context.Context, msgs []byte) (chunkName string, off int64, err error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	willExceedMaxChunkSize := s.lastChunkSize+uint64(len(msgs)) > s.maxChunkSize

	if s.lastChunk == "" || (s.lastChunkSize > 0 && willExceedMaxChunkSize) {
		if err := s.createNextChunk(ctx); err != nil {
			return "", 0, fmt.Errorf("creating next chunk %v", err)
		}
	}

	if !s.lastChunkAddedToReplication {
		if err := s.repl.AfterCreatingChunk(ctx, s.category, s.lastChunk); err != nil {
			return "", 0, fmt.Errorf("after creating new chunk: %w", err)
		}
	}

	fp, err := s.getLastChunkFp()
	if err != nil {
		return "", 0, err
	}

	n, err := fp.Write(msgs)
	if err != nil {
		if truncateErr := fp.Truncate(int64(s.lastChunkSize)); truncateErr != nil {
			s.logger.Printf("Failed to truncate %s: %v", fp.Name(), err)
		}

		return "", 0, err
	}

	s.lastChunkSize += uint64(n)
	return s.lastChunk, int64(s.lastChunkSize), nil
}

// Read copies the data from the in-memory store and writes
// the data read to the provided Writer, starting with the
// offset provided.
func (s *OnDisk) Read(chunk string, off uint64, maxSize uint64, w io.Writer) error {
	chunk = filepath.Clean(chunk)
	_, err := os.Stat(filepath.Join(s.dirname, chunk))
	if err != nil {
		return fmt.Errorf("stat %q: %w", chunk, err)
	}

	fp, err := os.Open(filepath.Join(s.dirname, chunk))
	if err != nil {
		return fmt.Errorf("Open(%q): %w", chunk, err)
	}
	defer fp.Close()

	buf := make([]byte, maxSize)
	n, err := fp.ReadAt(buf, int64(off))

	// ReadAt returns EOF even when it did read some data.
	if n == 0 {
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
	}

	// Read until the last message.
	// Do not send the incomplete part of the last
	// message if it is cut in half.
	truncated, _, err := cutToLastMessage(buf[0:n])
	if err != nil {
		return err
	}

	if _, err := w.Write(truncated); err != nil {
		return err
	}

	return nil
}

func (s *OnDisk) isLastChunk(chunk string) bool {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	return chunk == s.lastChunk
}

// Ack marks the current chunk as done and deletes it's contents.
func (s *OnDisk) Ack(ctx context.Context, chunk string, size uint64) error {
	if s.isLastChunk(chunk) {
		return fmt.Errorf("could not delete incomplete chunk %q", chunk)
	}

	chunkFilename := filepath.Join(s.dirname, chunk)

	fi, err := os.Stat(chunkFilename)
	if err != nil {
		return fmt.Errorf("stat %q: %w", chunk, err)
	}

	if uint64(fi.Size()) > size {
		return fmt.Errorf("file was not fully processed: the supplied processed size %d is smaller than the chunk file size %d", size, fi.Size())
	}

	if err := s.doAckChunk(chunk); err != nil {
		return fmt.Errorf("ack %q: %v", chunk, err)
	}

	// We ignore the error here so that we can continue reading chunks when etcd is down.
	if err := s.repl.AfterAcknowledgeChunk(ctx, s.category, chunk); err != nil {
		s.logger.Printf("Failed to replicate ack request: %v", err)
	}

	return nil
}

func (s *OnDisk) doAckChunk(chunk string) error {
	chunkFilename := filepath.Join(s.dirname, chunk)

	fp, err := os.OpenFile(chunkFilename, os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("failed to get file descriptor for ack operation for chunk %q: %v", chunk, err)
	}
	defer fp.Close()

	if err := fp.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate file %q: %v", chunk, err)
	}

	if err := os.Rename(chunkFilename, chunkFilename+DeletedSuffix); err != nil {
		return fmt.Errorf("failed to rename file %q to the deleted form: %v", chunk, err)
	}

	return nil
}

// AckDirect is a method that is called from replication to replay acknowledge
// requests on the replica side.
func (s *OnDisk) AckDirect(chunk string) error {
	if err := s.doAckChunk(chunk); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("removing %q: %v", chunk, err)
	}

	return nil
}

// ReplicationAck lets writers that are waiting for min_sync_replicas to know
// that their writes were successful.
func (s *OnDisk) ReplicationAck(ctx context.Context, chunk, instance string, size uint64) error {
	s.downloadNoficationMu.Lock()
	defer s.downloadNoficationMu.Unlock()

	s.downloadNotifications[instance] = &downloadNotification{
		chunk: chunk,
		size:  size,
	}

	for s.downloadNotificationSubs.Len() > 0 {
		r := s.downloadNotificationSubs.Pop()

		if (chunk == r.chunk && size >= r.size) || chunk > r.chunk {
			select {
			case r.ch <- true:
			default:
			}
		} else {
			s.downloadNotificationSubs.Push(r)
			break
		}
	}

	return nil
}

// Wait waits until the minSyncReplicas report back that they successfully downloaded
// the respective chunk from us.
func (s *OnDisk) Wait(ctx context.Context, chunkName string, off uint64, minSyncReplicas uint) error {
	r := replicationSub{
		chunk: chunkName,
		size:  off,
		ch:    make(chan bool, 2),
	}

	s.downloadNoficationMu.Lock()
	s.downloadNotificationSubs.Push(r)
	s.downloadNoficationMu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.ch:
		}

		var replicatedCount uint

		s.downloadNoficationMu.RLock()
		for _, n := range s.downloadNotifications {
			if (n.chunk > chunkName) || (n.chunk == chunkName && n.size >= off) {
				replicatedCount++
			}
		}
		s.downloadNoficationMu.RUnlock()

		if replicatedCount >= minSyncReplicas {
			return nil
		}
	}
}

// ListChunks returns the list of current chunks.
func (s *OnDisk) ListChunks() ([]protocol.Chunk, error) {
	var res []protocol.Chunk

	dis, err := os.ReadDir(s.dirname)
	if err != nil {
		return nil, err
	}

	for idx, di := range dis {
		fi, err := di.Info()
		if errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, fmt.Errorf("reading directory: %v", err)
		}

		if strings.HasSuffix(di.Name(), DeletedSuffix) {
			continue
		}

		instanceName, _ := protocol.ParseChunkFileName(di.Name())

		c := protocol.Chunk{
			Name:     di.Name(),
			Complete: true,
			Size:     uint64(fi.Size()),
		}

		// The last chunk for every instance is incomplete, so either
		// we are at the end of the list, or the next filename belongs
		// to a different instance. Files are sorted by name already so
		// we can rely on it.
		if idx == len(dis)-1 {
			c.Complete = false
		} else if nextInstance, _ := protocol.ParseChunkFileName(dis[idx+1].Name()); nextInstance != instanceName {
			c.Complete = false
		}

		res = append(res, c)
	}

	return res, nil
}

func cutToLastMessage(res []byte) (truncated []byte, rest []byte, err error) {
	n := len(res)

	if n == 0 {
		return res, nil, nil
	}

	if res[n-1] == '\n' {
		return res, nil, nil
	}

	lastPos := bytes.LastIndexByte(res, '\n')
	if lastPos < 0 {
		return nil, nil, errBufTooSmall
	}

	return res[0 : lastPos+1], res[lastPos+1:], nil
}

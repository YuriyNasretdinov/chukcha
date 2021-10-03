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
	"sync"

	"github.com/YuriyNasretdinov/chukcha/protocol"
)

// TODO: limit the max message size too.
const maxFileChunkSize = 20 * 1024 * 1024 // bytes

var errBufTooSmall = errors.New("the buffer is too small to contain a single message")

type StorageHooks interface {
	BeforeCreatingChunk(ctx context.Context, category string, fileName string) error
	BeforeAcknowledgeChunk(ctx context.Context, category string, fileName string) error
}

// OnDisk stores all the data on disk.
type OnDisk struct {
	dirname      string
	category     string
	instanceName string

	repl StorageHooks

	writeMu       sync.Mutex
	lastChunk     string
	lastChunkSize uint64
	lastChunkIdx  uint64

	fpsMu sync.Mutex
	fps   map[string]*os.File
}

// NewOnDisk creates a server that stores all it's data on disk.
func NewOnDisk(dirname, category, instanceName string, repl StorageHooks) (*OnDisk, error) {
	s := &OnDisk{
		dirname:      dirname,
		category:     category,
		instanceName: instanceName,
		repl:         repl,
		fps:          make(map[string]*os.File),
	}

	if err := s.initLastChunkIdx(dirname); err != nil {
		return nil, err
	}

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

// Write accepts the messages from the clients and stores them.
func (s *OnDisk) Write(ctx context.Context, msgs []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.lastChunk == "" || (s.lastChunkSize+uint64(len(msgs)) > maxFileChunkSize) {
		s.lastChunk = fmt.Sprintf("%s-chunk%09d", s.instanceName, s.lastChunkIdx)
		s.lastChunkSize = 0
		s.lastChunkIdx++

		if err := s.repl.BeforeCreatingChunk(ctx, s.category, s.lastChunk); err != nil {
			return fmt.Errorf("before creating new chunk: %w", err)
		}
	}

	fp, err := s.getFileDescriptor(s.lastChunk, true)
	if err != nil {
		return err
	}

	_, err = fp.Write(msgs)
	s.lastChunkSize += uint64(len(msgs))
	return err
}

func (s *OnDisk) getFileDescriptor(chunk string, write bool) (*os.File, error) {
	s.fpsMu.Lock()
	defer s.fpsMu.Unlock()

	fp, ok := s.fps[chunk]
	if ok {
		return fp, nil
	}

	fl := os.O_RDONLY
	if write {
		fl = os.O_CREATE | os.O_RDWR | os.O_EXCL
	}

	filename := filepath.Join(s.dirname, chunk)
	fp, err := os.OpenFile(filename, fl, 0666)
	if err != nil {
		return nil, fmt.Errorf("create file %q: %s", filename, err)
	}

	s.fps[chunk] = fp
	return fp, nil
}

func (s *OnDisk) forgetFileDescriptor(chunk string) {
	s.fpsMu.Lock()
	defer s.fpsMu.Unlock()

	fp, ok := s.fps[chunk]
	if !ok {
		return
	}

	fp.Close()
	delete(s.fps, chunk)
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

	fp, err := s.getFileDescriptor(chunk, false)
	if err != nil {
		return fmt.Errorf("getFileDescriptor(%q): %v", chunk, err)
	}

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

	// We ignore the error here so that we can continue reading chunks when etcd is down.
	if err := s.repl.BeforeAcknowledgeChunk(ctx, s.category, chunk); err != nil {
		// TODO: remember the ack request still
		log.Printf("Failed to replicate ack request: %v", err)
	}

	if err := os.Remove(chunkFilename); err != nil {
		return fmt.Errorf("removing %q: %v", chunk, err)
	}

	s.forgetFileDescriptor(chunk)
	return nil
}

// AckDirect is a method that is called from replication to replay acknowledge
// requests on the replica side.
func (s *OnDisk) AckDirect(chunk string) error {
	chunkFilename := filepath.Join(s.dirname, chunk)

	if err := os.Remove(chunkFilename); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("removing %q: %v", chunk, err)
	}

	s.forgetFileDescriptor(chunk)
	return nil
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

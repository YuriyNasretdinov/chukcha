package server

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
)

const maxInMemoryChunkSize = 10 * 1024 * 1024 // bytes

var (
	errBufTooSmall = errors.New("buffer is too small to fit a single message")
)

// InMemory stores all the data in memory.
type InMemory struct {
	sync.RWMutex
	lastChunk     string
	lastChunkSize uint64
	lastChunkIdx  uint64
	bufs          map[string][]byte
}

// Write accepts the messages from the clients and stores them.
func (s *InMemory) Write(msgs []byte) error {
	s.Lock()
	defer s.Unlock()

	if s.lastChunk == "" || (s.lastChunkSize+uint64(len(msgs)) > maxInMemoryChunkSize) {
		s.lastChunk = fmt.Sprintf("chunk%d", s.lastChunkIdx)
		s.lastChunkSize = 0
		s.lastChunkIdx++
	}

	if s.bufs == nil {
		s.bufs = make(map[string][]byte)
	}

	s.bufs[s.lastChunk] = append(s.bufs[s.lastChunk], msgs...)
	s.lastChunkSize += uint64(len(msgs))
	return nil
}

// Read copies the data from the in-memory store and writes
// the data read to the provided Writer, starting with the
// offset provided.
func (s *InMemory) Read(chunk string, off uint64, maxSize uint64, w io.Writer) error {
	s.RLock()
	defer s.RUnlock()

	buf, ok := s.bufs[chunk]
	if !ok {
		return fmt.Errorf("chunk %q does not exist", chunk)
	}

	maxOff := uint64(len(buf))

	if off >= maxOff {
		return nil
	} else if off+maxSize >= maxOff {
		w.Write(buf[off:])
		return nil
	}

	// Read until the last message.
	// Do not send the incomplete part of the last
	// message if it is cut in half.
	truncated, _, err := cutToLastMessage(buf[off : off+maxSize])
	if err != nil {
		return err
	}

	if _, err := w.Write(truncated); err != nil {
		return err
	}

	return nil
}

// Ack marks the current chunk as done and deletes it's contents.
func (s *InMemory) Ack(chunk string) error {
	s.Lock()
	defer s.Unlock()

	_, ok := s.bufs[chunk]
	if !ok {
		return fmt.Errorf("chunk %q does not exist", chunk)
	}

	if chunk == s.lastChunk {
		return fmt.Errorf("chunk %q is currently being written into and can't be acknowledged", chunk)
	}

	delete(s.bufs, chunk)
	return nil
}

// ListChunks returns the list of chunks that are present in the system.
func (s *InMemory) ListChunks() ([]Chunk, error) {
	s.RLock()
	defer s.RUnlock()

	res := make([]Chunk, 0, len(s.bufs))
	for chunk := range s.bufs {
		var c Chunk
		c.Complete = (s.lastChunk != chunk)
		c.Name = chunk

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

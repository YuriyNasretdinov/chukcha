package server

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
)

// TODO: limit the max message size too.
const maxFileChunkSize = 20 * 1024 * 1024 // bytes

var errBufTooSmall = errors.New("the buffer is too small to contain a single message")
var filenameRegexp = regexp.MustCompile("^chunk([0-9]+)$")

// OnDisk stores all the data on disk.
type OnDisk struct {
	dirname string

	sync.RWMutex
	lastChunk     string
	lastChunkSize uint64
	lastChunkIdx  uint64
	fps           map[string]*os.File
}

// NewOnDisk creates a server that stores all it's data on disk.
func NewOnDisk(dirname string) (*OnDisk, error) {
	s := &OnDisk{
		dirname: dirname,
		fps:     make(map[string]*os.File),
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
		res := filenameRegexp.FindStringSubmatch(fi.Name())
		if res == nil {
			continue
		}

		chunkIdx, err := strconv.Atoi(res[1])
		if err != nil {
			return fmt.Errorf("unexpected error parsing filename %q: %v", fi.Name(), err)
		}

		if uint64(chunkIdx)+1 >= s.lastChunkIdx {
			s.lastChunkIdx = uint64(chunkIdx) + 1
		}
	}

	return nil
}

// Write accepts the messages from the clients and stores them.
func (s *OnDisk) Write(msgs []byte) error {
	s.Lock()
	defer s.Unlock()

	if s.lastChunk == "" || (s.lastChunkSize+uint64(len(msgs)) > maxFileChunkSize) {
		s.lastChunk = fmt.Sprintf("chunk%d", s.lastChunkIdx)
		s.lastChunkSize = 0
		s.lastChunkIdx++
	}

	fp, err := s.getFileDescriptor(s.lastChunk)
	if err != nil {
		return err
	}

	_, err = fp.Write(msgs)
	s.lastChunkSize += uint64(len(msgs))
	return err
}

func (s *OnDisk) getFileDescriptor(chunk string) (*os.File, error) {
	fp, ok := s.fps[chunk]
	if ok {
		return fp, nil
	}

	fp, err := os.OpenFile(filepath.Join(s.dirname, chunk), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("create file %q: %s", fp.Name(), err)
	}

	s.fps[chunk] = fp
	return fp, nil
}

// Read copies the data from the in-memory store and writes
// the data read to the provided Writer, starting with the
// offset provided.
func (s *OnDisk) Read(chunk string, off uint64, maxSize uint64, w io.Writer) error {
	s.Lock()
	defer s.Unlock()

	chunk = filepath.Clean(chunk)
	_, err := os.Stat(filepath.Join(s.dirname, chunk))
	if err != nil {
		return fmt.Errorf("stat %q: %w", chunk, err)
	}

	fp, err := s.getFileDescriptor(chunk)
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

// Ack marks the current chunk as done and deletes it's contents.
func (s *OnDisk) Ack(chunk string) error {
	s.Lock()
	defer s.Unlock()

	if chunk == s.lastChunk {
		return fmt.Errorf("could not delete incomplete chunk %q", chunk)
	}

	chunkFilename := filepath.Join(s.dirname, chunk)

	_, err := os.Stat(chunkFilename)
	if err != nil {
		return fmt.Errorf("stat %q: %w", chunk, err)
	}

	if err := os.Remove(chunkFilename); err != nil {
		return fmt.Errorf("removing %q: %v", chunk, err)
	}

	fp, ok := s.fps[chunk]
	if ok {
		fp.Close()
	}
	delete(s.fps, chunk)
	return nil
}

// ListChunks returns the list of current chunks.
func (s *OnDisk) ListChunks() ([]Chunk, error) {
	var res []Chunk

	dis, err := os.ReadDir(s.dirname)
	if err != nil {
		return nil, err
	}

	for _, di := range dis {
		c := Chunk{
			Name:     di.Name(),
			Complete: (di.Name() != s.lastChunk),
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

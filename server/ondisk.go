package server

import (
	"io"
	"os"
)

// TODO: limit the max message size too.
const readBlockSize = 1024 * 1024

// OnDisk stores all the data on disk.
type OnDisk struct {
	fp *os.File
}

// NewOnDisk creates a server that stores all it's data on disk.
func NewOnDisk(fp *os.File) *OnDisk {
	return &OnDisk{fp: fp}
}

// Write accepts the messages from the clients and stores them.
func (s *OnDisk) Write(msgs []byte) error {
	_, err := s.fp.Write(msgs)
	return err
}

// Read copies the data from the in-memory store and writes
// the data read to the provided Writer, starting with the
// offset provided.
func (s *OnDisk) Read(off uint64, maxSize uint64, w io.Writer) error {
	buf := make([]byte, maxSize)
	n, err := s.fp.ReadAt(buf, int64(off))

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
func (s *OnDisk) Ack() error {
	var err error

	err = s.fp.Truncate(0)
	if err != nil {
		return err
	}

	// Writes would continue from the previous place otherwise.
	_, err = s.fp.Seek(0, os.SEEK_SET)
	return err
}

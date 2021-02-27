package server

import (
	"bytes"
	"errors"
	"io"
)

var (
	errBufTooSmall = errors.New("buffer is too small to fit a single message")
)

// InMemory stores all the data in memory.
type InMemory struct {
	buf []byte
}

// Write accepts the messages from the clients and stores them.
func (s *InMemory) Write(msgs []byte) error {
	s.buf = append(s.buf, msgs...)
	return nil
}

// Read copies the data from the in-memory store and writes
// the data read to the provided Writer, starting with the
// offset provided.
func (s *InMemory) Read(off uint64, maxSize uint64, w io.Writer) error {
	maxOff := uint64(len(s.buf))

	if off >= maxOff {
		return nil
	} else if off+maxSize >= maxOff {
		w.Write(s.buf[off:])
		return nil
	}

	// Read until the last message.
	// Do not send the incomplete part of the last
	// message if it is cut in half.
	truncated, _, err := cutToLastMessage(s.buf[off : off+maxSize])
	if err != nil {
		return err
	}

	if _, err := w.Write(truncated); err != nil {
		return err
	}

	return nil
}

// Ack marks the current chunk as done and deletes it's contents.
func (s *InMemory) Ack() error {
	s.buf = s.buf[0:0]
	return nil
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

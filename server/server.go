package server

import (
	"bytes"
	"errors"
)

var errBufTooSmall = errors.New("buffer is too small to fit a single message")

// InMemory stores all the data in memory.
type InMemory struct {
	buf     bytes.Buffer
	restBuf bytes.Buffer
}

// Send sends the messages to the Chukcha servers.
func (s *InMemory) Send(msgs []byte) error {
	_, err := s.buf.Write(msgs)
	return err
}

// Receive will either wait for new messages or return an
// error in case something goes wrong.
// The scratch buffer can be used to read the data.
func (s *InMemory) Receive(scratch []byte) ([]byte, error) {
	startOff := 0

	if s.restBuf.Len() > 0 {
		if s.restBuf.Len() >= len(scratch) {
			return nil, errBufTooSmall
		}

		n, err := s.restBuf.Read(scratch)
		if err != nil {
			return nil, err
		}

		s.restBuf.Reset()
		startOff += n
	}

	n, err := s.buf.Read(scratch[startOff:])
	if err != nil {
		return nil, err
	}

	truncated, rest, err := cutToLastMessage(scratch[0 : n+startOff])
	if err != nil {
		return nil, err
	}

	s.restBuf.Reset()
	s.restBuf.Write(rest)

	return truncated, nil
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

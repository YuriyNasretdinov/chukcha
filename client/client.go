package client

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
)

var errBufTooSmall = errors.New("buffer is too small to fit a single message")

const defaultScratchSize = 64 * 1024

// Simple represents an instance of client connected to a set of Chukcha servers.
type Simple struct {
	addrs []string
	cl    *http.Client
	off   uint64
}

// NewSimple creates a new client for the Chukcha server.
func NewSimple(addrs []string) *Simple {
	return &Simple{
		addrs: addrs,
		cl:    &http.Client{},
	}
}

// Send sends the messages to the Chukcha servers.
func (s *Simple) Send(msgs []byte) error {
	resp, err := s.cl.Post(s.addrs[0]+"/write", "application/octet-stream", bytes.NewReader(msgs))
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, resp.Body)
		return fmt.Errorf("http code %d, %s", resp.StatusCode, b.String())
	}

	io.Copy(ioutil.Discard, resp.Body)
	return nil
}

// Receive will either wait for new messages or return an
// error in case something goes wrong.
// The scratch buffer can be used to read the data.
func (s *Simple) Receive(scratch []byte) ([]byte, error) {
	if scratch == nil {
		scratch = make([]byte, defaultScratchSize)
	}

	addrIdx := rand.Intn(len(s.addrs))
	addr := s.addrs[addrIdx]
	readURL := fmt.Sprintf("%s/read?off=%d&maxSize=%d", addr, s.off, len(scratch))

	resp, err := s.cl.Get(readURL)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, resp.Body)
		return nil, fmt.Errorf("http code %d, %s", resp.StatusCode, b.String())
	}

	b := bytes.NewBuffer(scratch[0:0])
	_, err = io.Copy(b, resp.Body)
	if err != nil {
		return nil, err
	}

	// 0 bytes read but no errors means the end of file by convention.
	if b.Len() == 0 {
		if err := s.ackCurrentChunk(addr); err != nil {
			return nil, err
		}

		return nil, io.EOF
	}

	s.off += uint64(b.Len())
	return b.Bytes(), nil
}

func (s *Simple) ackCurrentChunk(addr string) error {
	resp, err := s.cl.Get(addr + "/ack")
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, resp.Body)
		return fmt.Errorf("http code %d, %s", resp.StatusCode, b.String())
	}

	io.Copy(ioutil.Discard, resp.Body)
	return nil
}

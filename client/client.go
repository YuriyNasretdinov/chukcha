package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"

	"github.com/YuriyNasretdinov/chukcha/protocol"
)

const defaultScratchSize = 64 * 1024

// Simple represents an instance of client connected to a set of Chukcha servers.
type Simple struct {
	addrs    []string
	cl       *http.Client
	curChunk protocol.Chunk
	off      uint64
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

var errRetry = errors.New("please retry the request")

// Receive will either wait for new messages or return an
// error in case something goes wrong.
// The scratch buffer can be used to read the data.
func (s *Simple) Receive(scratch []byte) ([]byte, error) {
	if scratch == nil {
		scratch = make([]byte, defaultScratchSize)
	}

	for {
		res, err := s.receive(scratch)
		if err == errRetry {
			continue
		}
		return res, err
	}
}

func (s *Simple) receive(scratch []byte) ([]byte, error) {
	addrIdx := rand.Intn(len(s.addrs))
	addr := s.addrs[addrIdx]

	if err := s.updateCurrentChunk(addr); err != nil {
		return nil, fmt.Errorf("updateCurrentChunk: %w", err)
	}

	readURL := fmt.Sprintf("%s/read?off=%d&maxSize=%d&chunk=%s", addr, s.off, len(scratch), s.curChunk.Name)

	resp, err := s.cl.Get(readURL)
	if err != nil {
		return nil, fmt.Errorf("read %q: %v", readURL, err)
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
		return nil, fmt.Errorf("writing response: %v", err)
	}

	// 0 bytes read but no errors means the end of file by convention.
	if b.Len() == 0 {
		if !s.curChunk.Complete {
			if err := s.updateCurrentChunkCompleteStatus(addr); err != nil {
				return nil, fmt.Errorf("updateCurrentChunkCompleteStatus: %v", err)
			}

			if !s.curChunk.Complete {
				// We actually did read until the end and no new data appeared
				// in between requests.
				if s.off >= s.curChunk.Size {
					return nil, io.EOF
				}

				// New data appeared in between us sending the read request and
				// the chunk becoming complete.
				return nil, errRetry
			}
		}

		// The chunk has been marked complete. However, new data appeared
		// in between us sending the read request and the chunk becoming complete.
		if s.off < s.curChunk.Size {
			return nil, errRetry
		}

		if err := s.ackCurrentChunk(addr); err != nil {
			return nil, fmt.Errorf("ack current chunk: %v", err)
		}

		// need to read the next chunk so that we do not return empty
		// response
		s.curChunk = protocol.Chunk{}
		s.off = 0
		return nil, errRetry
	}

	s.off += uint64(b.Len())
	return b.Bytes(), nil
}

func (s *Simple) updateCurrentChunk(addr string) error {
	if s.curChunk.Name != "" {
		return nil
	}

	chunks, err := s.listChunks(addr)
	if err != nil {
		return fmt.Errorf("listChunks failed: %v", err)
	}

	if len(chunks) == 0 {
		return io.EOF
	}

	// We need to prioritise the chunks that are complete
	// so that we ack them.
	for _, c := range chunks {
		if c.Complete {
			s.curChunk = c
			return nil
		}
	}

	s.curChunk = chunks[0]
	return nil
}

func (s *Simple) updateCurrentChunkCompleteStatus(addr string) error {
	chunks, err := s.listChunks(addr)
	if err != nil {
		return fmt.Errorf("listChunks failed: %v", err)
	}

	// We need to prioritise the chunks that are complete
	// so that we ack them.
	for _, c := range chunks {
		if c.Name == s.curChunk.Name {
			s.curChunk = c
			return nil
		}
	}

	return nil
}

func (s *Simple) listChunks(addr string) ([]protocol.Chunk, error) {
	listURL := fmt.Sprintf("%s/listChunks", addr)

	resp, err := s.cl.Get(listURL)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		return nil, fmt.Errorf("listChunks error: %s", body)
	}

	var res []protocol.Chunk
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}

	return res, nil
}

func (s *Simple) ackCurrentChunk(addr string) error {
	resp, err := s.cl.Get(fmt.Sprintf(addr+"/ack?chunk=%s&size=%d", s.curChunk.Name, s.off))
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

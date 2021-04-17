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
	"net/url"
	"strconv"

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
func (s *Simple) Send(category string, msgs []byte) error {
	u := url.Values{}
	u.Add("category", category)
	resp, err := s.cl.Post(s.addrs[0]+"/write?"+u.Encode(), "application/octet-stream", bytes.NewReader(msgs))
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

// Process will either wait for new messages or return an
// error in case something goes wrong.
// The scratch buffer can be used to read the data.
// The read offset will advance only if processFn()
// returns no errors for the data being processed.
func (s *Simple) Process(category string, scratch []byte, processFn func([]byte) error) error {
	if scratch == nil {
		scratch = make([]byte, defaultScratchSize)
	}

	for {
		err := s.process(category, scratch, processFn)
		if err == errRetry {
			continue
		}
		return err
	}
}

func (s *Simple) process(category string, scratch []byte, processFn func([]byte) error) error {
	addrIdx := rand.Intn(len(s.addrs))
	addr := s.addrs[addrIdx]

	if err := s.updateCurrentChunk(category, addr); err != nil {
		return fmt.Errorf("updateCurrentChunk: %w", err)
	}

	u := url.Values{}
	u.Add("off", strconv.Itoa(int(s.off)))
	u.Add("maxSize", strconv.Itoa(len(scratch)))
	u.Add("chunk", s.curChunk.Name)
	u.Add("category", category)

	readURL := fmt.Sprintf("%s/read?%s", addr, u.Encode())

	resp, err := s.cl.Get(readURL)
	if err != nil {
		return fmt.Errorf("read %q: %v", readURL, err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, resp.Body)
		return fmt.Errorf("http code %d, %s", resp.StatusCode, b.String())
	}

	b := bytes.NewBuffer(scratch[0:0])
	_, err = io.Copy(b, resp.Body)
	if err != nil {
		return fmt.Errorf("writing response: %v", err)
	}

	// 0 bytes read but no errors means the end of file by convention.
	if b.Len() == 0 {
		if !s.curChunk.Complete {
			if err := s.updateCurrentChunkCompleteStatus(category, addr); err != nil {
				return fmt.Errorf("updateCurrentChunkCompleteStatus: %v", err)
			}

			if !s.curChunk.Complete {
				// We actually did read until the end and no new data appeared
				// in between requests.
				if s.off >= s.curChunk.Size {
					return io.EOF
				}

				// New data appeared in between us sending the read request and
				// the chunk becoming complete.
				return errRetry
			}
		}

		// The chunk has been marked complete. However, new data appeared
		// in between us sending the read request and the chunk becoming complete.
		if s.off < s.curChunk.Size {
			return errRetry
		}

		if err := s.ackCurrentChunk(category, addr); err != nil {
			return fmt.Errorf("ack current chunk: %v", err)
		}

		// need to read the next chunk so that we do not return empty
		// response
		s.curChunk = protocol.Chunk{}
		s.off = 0
		return errRetry
	}

	err = processFn(b.Bytes())
	if err == nil {
		s.off += uint64(b.Len())
	}

	return err
}

func (s *Simple) updateCurrentChunk(category, addr string) error {
	if s.curChunk.Name != "" {
		return nil
	}

	chunks, err := s.ListChunks(category, addr)
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

func (s *Simple) updateCurrentChunkCompleteStatus(category, addr string) error {
	chunks, err := s.ListChunks(category, addr)
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

// ListChunks returns the list of chunks for the appropriate Chukcha instance.
// TODO: extract this into a separate client.
func (s *Simple) ListChunks(category, addr string) ([]protocol.Chunk, error) {
	u := url.Values{}
	u.Add("category", category)

	listURL := fmt.Sprintf("%s/listChunks?%s", addr, u.Encode())

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

func (s *Simple) ackCurrentChunk(category, addr string) error {
	u := url.Values{}
	u.Add("chunk", s.curChunk.Name)
	u.Add("size", strconv.Itoa(int(s.off)))
	u.Add("category", category)

	resp, err := s.cl.Get(fmt.Sprintf(addr+"/ack?%s", u.Encode()))
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

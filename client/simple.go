package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sort"

	"github.com/YuriyNasretdinov/chukcha/protocol"
)

const defaultScratchSize = 64 * 1024

// Simple represents an instance of client connected to a set of Chukcha servers.
type Simple struct {
	Debug  bool
	Logger *log.Logger

	addrs []string
	cl    *Raw

	st *state
}

type ReadOffset struct {
	CurChunk          protocol.Chunk
	LastAckedChunkIdx int
	Off               uint64
}

type state struct {
	Offsets map[string]*ReadOffset
}

// NewSimple creates a new client for the Chukcha server.
func NewSimple(addrs []string) *Simple {
	return &Simple{
		addrs: addrs,
		cl:    NewRaw(&http.Client{}),
		st:    &state{Offsets: make(map[string]*ReadOffset)},
	}
}

// MarshalState returns the simple client state that stores
// the offsets that were already read.
func (s *Simple) MarshalState() ([]byte, error) {
	return json.Marshal(s.st)
}

// MarshalState returns the simple client state that stores
// the offsets that were already read.
func (s *Simple) RestoreSavedState(buf []byte) error {
	return json.Unmarshal(buf, &s.st)
}

func (s *Simple) logger() *log.Logger {
	if s.Logger == nil {
		return log.Default()
	}

	return s.Logger
}

// Send sends the messages to the Chukcha servers.
func (s *Simple) Send(ctx context.Context, category string, msgs []byte) error {
	return s.cl.Write(ctx, s.getAddr(), category, msgs)
}

var errRetry = errors.New("please retry the request")

// Process will either wait for new messages or return an
// error in case something goes wrong.
// The scratch buffer can be used to read the data.
// The read offset will advance only if processFn()
// returns no errors for the data being processed.
func (s *Simple) Process(ctx context.Context, category string, scratch []byte, processFn func([]byte) error) error {
	if scratch == nil {
		scratch = make([]byte, defaultScratchSize)
	}

	addr := s.getAddr()

	// If there are no servers known, populate the list of current chunks.
	if len(s.st.Offsets) == 0 {
		if err := s.updateCurrentChunks(ctx, category, addr); err != nil {
			return fmt.Errorf("updateCurrentChunk: %w", err)
		}
	}

	for instance := range s.st.Offsets {
		err := s.processInstance(ctx, addr, instance, category, scratch, processFn)
		if errors.Is(err, io.EOF) {
			continue
		}

		return err
	}

	return io.EOF
}

func (s *Simple) processInstance(ctx context.Context, addr, instance, category string, scratch []byte, processFn func([]byte) error) error {
	for {
		if err := s.updateCurrentChunks(ctx, category, addr); err != nil {
			return fmt.Errorf("updateCurrentChunk: %w", err)
		}

		err := s.process(ctx, addr, instance, category, scratch, processFn)
		if err == errRetry {
			if s.Debug {
				s.logger().Printf("Retrying reading category %q (got error %v)", category, err)
			}
			continue
		}
		return err
	}
}

func (s *Simple) getAddr() string {
	addrIdx := rand.Intn(len(s.addrs))
	return s.addrs[addrIdx]
}

func (s *Simple) process(ctx context.Context, addr, instance, category string, scratch []byte, processFn func([]byte) error) error {
	curCh := s.st.Offsets[instance]

	res, found, err := s.cl.Read(ctx, addr, category, curCh.CurChunk.Name, curCh.Off, scratch)
	if err != nil {
		return err
	} else if !found {
		if s.Debug {
			s.logger().Printf("Chunk %+v is missing at %q, probably hasn't replicated yet, skipping", curCh.CurChunk.Name, addr)
		}
		return nil
	}

	// 0 bytes read but no errors means the end of file by convention.
	if len(res) == 0 {
		if !curCh.CurChunk.Complete {
			if err := s.updateCurrentChunkCompleteStatus(ctx, curCh, instance, category, addr); err != nil {
				return fmt.Errorf("updateCurrentChunkCompleteStatus: %v", err)
			}

			if !curCh.CurChunk.Complete {
				// We actually did read until the end and no new data appeared
				// in between requests.
				if curCh.Off >= curCh.CurChunk.Size {
					return io.EOF
				}
			} else {
				// New data appeared in between us sending the read request and
				// the chunk becoming complete.
				return errRetry
			}
		}

		// The chunk has been marked complete. However, new data appeared
		// in between us sending the read request and the chunk becoming complete.
		if curCh.Off < curCh.CurChunk.Size {
			if s.Debug {
				s.logger().Printf(`errRetry: The chunk %q has been marked complete. However, new data appeared in between us sending the read request and the chunk becoming complete. (curCh.Off < curCh.CurChunk.Size) = (%v < %v)`, curCh.CurChunk.Name, curCh.Off, curCh.CurChunk.Size)
			}
			return errRetry
		}

		if err := s.cl.Ack(ctx, addr, category, curCh.CurChunk.Name, curCh.Off); err != nil {
			return fmt.Errorf("ack current chunk: %v", err)
		}

		// need to read the next chunk so that we do not return empty
		// response

		_, idx := protocol.ParseChunkFileName(curCh.CurChunk.Name)
		curCh.LastAckedChunkIdx = idx
		curCh.CurChunk = protocol.Chunk{}
		curCh.Off = 0

		if s.Debug {
			s.logger().Printf(`errRetry: need to read the next chunk so that we do not return empty response`)
		}
		return errRetry
	}

	err = processFn(res)
	if err == nil {
		curCh.Off += uint64(len(res))
	}

	return err
}

func (s *Simple) updateCurrentChunks(ctx context.Context, category, addr string) error {
	chunks, err := s.cl.ListChunks(ctx, addr, category, false)
	if err != nil {
		return fmt.Errorf("listChunks failed: %v", err)
	}

	if len(chunks) == 0 {
		return io.EOF
	}

	chunksByInstance := make(map[string][]protocol.Chunk)
	for _, c := range chunks {
		instance, chunkIdx := protocol.ParseChunkFileName(c.Name)
		if chunkIdx < 0 {
			continue
		}

		// We can have chunks that we already acknowledged in the other
		// replicas (because replication is asynchronous), so we need to
		// skip them.
		curChunk, exists := s.st.Offsets[instance]
		if exists && chunkIdx <= curChunk.LastAckedChunkIdx {
			continue
		}

		chunksByInstance[instance] = append(chunksByInstance[instance], c)
	}

	for instance, chunks := range chunksByInstance {
		curChunk, exists := s.st.Offsets[instance]
		if !exists {
			curChunk = &ReadOffset{}
		}

		// Name will be empty in two cases:
		//  1. It is the first time we try to read from this instance.
		//  2. We read the latest chunk until the end and need to start
		//     reading a new one.
		if curChunk.CurChunk.Name == "" {
			curChunk.CurChunk = s.getOldestChunk(chunks)
			curChunk.Off = 0
		}

		s.st.Offsets[instance] = curChunk
	}

	return nil
}

func (s *Simple) getOldestChunk(chunks []protocol.Chunk) protocol.Chunk {
	// We need to prioritise the chunks that are complete
	// so that we ack them.
	sort.Slice(chunks, func(i, j int) bool { return chunks[i].Name < chunks[j].Name })

	for _, c := range chunks {
		if c.Complete {
			return c
		}
	}

	return chunks[0]
}

func (s *Simple) updateCurrentChunkCompleteStatus(ctx context.Context, curCh *ReadOffset, instance, category, addr string) error {
	chunks, err := s.cl.ListChunks(ctx, addr, category, false)
	if err != nil {
		return fmt.Errorf("listChunks failed: %v", err)
	}

	// We need to prioritise the chunks that are complete
	// so that we ack them.
	for _, c := range chunks {
		chunkInstance, idx := protocol.ParseChunkFileName(c.Name)
		if idx < 0 {
			continue
		}

		if chunkInstance != instance {
			continue
		}

		if c.Name == curCh.CurChunk.Name {
			curCh.CurChunk = c
			return nil
		}
	}

	return nil
}

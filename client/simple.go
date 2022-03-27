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
	"time"

	"github.com/YuriyNasretdinov/chukcha/protocol"
)

const defaultScratchSize = 64 * 1024

// Simple represents an instance of client connected to a set of Chukcha servers.
type Simple struct {
	Logger *log.Logger

	pollInterval time.Duration
	acknowledge  bool
	debug        bool
	addrs        []string
	cl           *Raw

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
		addrs:        addrs,
		pollInterval: time.Millisecond * 100,
		acknowledge:  true,
		cl:           NewRaw(&http.Client{}),
		st:           &state{Offsets: make(map[string]*ReadOffset)},
	}
}

// SetPollInterval sets the interval that is used to poll
// Chukcha when there are no new messages to process.
// (default is 100ms)
func (s *Simple) SetPollInterval(d time.Duration) {
	s.pollInterval = d
}

// SetDebug either enables or disables debug logging for the client.
func (s *Simple) SetDebug(v bool) {
	s.debug = v
	s.cl.SetDebug(v)
}

// SetAcknowledge controls whether or not the client will acknowledge chunks.
func (s *Simple) SetAcknowledge(v bool) {
	s.acknowledge = v
}

// SetMinSyncReplicas sets the value of minSyncReplicas when sending the requests to
// Chukcha.
func (s *Simple) SetMinSyncReplicas(v uint) {
	s.cl.SetMinSyncReplicas(v)
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
//
// TODO: don't just write a random address, write to
// the preferred server and then try next one if
// request failed.
func (s *Simple) Send(ctx context.Context, category string, msgs []byte) error {
	return s.cl.Write(ctx, s.getAddr(), category, msgs)
}

var errRetry = errors.New("please retry the request")

// Process will either wait for new messages or return an
// error in case something goes wrong.
//
// The scratch buffer can be used to read the data.
// The size of scratch buffer determines the maximum
// batch size that will read (e.g. if scratch buffer's
// length is 1024 bytes, then no more than 1024 bytes
// at a time will be read from Chukcha). Suggested
// batch size is determined by your own needs, e.g.
// if you want to process data in big chunks, then
// scratch buffer needs to be 10 MiB or more.
//
// The read offset will advance only if processFn()
// returns no errors for the data being processed.
func (s *Simple) Process(ctx context.Context, category string, scratch []byte, processFn func([]byte) error) error {
	if scratch == nil {
		scratch = make([]byte, defaultScratchSize)
	}

	for {
		err := s.tryProcess(ctx, category, scratch, processFn)
		if err == nil {
			return nil
		} else if errors.Is(err, errRetry) {
			continue
		} else if !errors.Is(err, io.EOF) {
			return err
		}

		select {
		case <-time.After(s.pollInterval):
		case <-ctx.Done():
			return context.Canceled
		}
	}
}

func (s *Simple) tryProcess(ctx context.Context, category string, scratch []byte, processFn func([]byte) error) error {
	addr := s.getAddr()

	if err := s.updateOffsets(ctx, category, addr); err != nil {
		return fmt.Errorf("updateCurrentChunk: %w", err)
	}

	needRetry := false

	for instance := range s.st.Offsets {
		err := s.processInstance(ctx, addr, instance, category, scratch, processFn)
		if errors.Is(err, io.EOF) {
			continue
		} else if errors.Is(err, errRetry) {
			needRetry = true
			continue
		}

		return err
	}

	// Retry means "I finished processing, retry immediately",
	// which is different from io.EOF, which means "we read everything".
	if needRetry {
		return errRetry
	}

	return io.EOF
}

func (s *Simple) getAddr() string {
	addrIdx := rand.Intn(len(s.addrs))
	return s.addrs[addrIdx]
}

func (s *Simple) processInstance(ctx context.Context, addr, instance, category string, scratch []byte, processFn func([]byte) error) error {
	curCh := s.st.Offsets[instance]

	res, found, err := s.cl.Read(ctx, addr, category, curCh.CurChunk.Name, curCh.Off, scratch)
	if err != nil {
		return err
	} else if !found {
		// TODO: if there are newer chunks than the one that we are processing,
		// update current chunk.
		// If the chunk that we are reading right now was acknowledged already,
		// then it we will never read newer chunks. This can happen if our state
		// is stale (e.g. client crashed and didn't save the last offset).
		if s.debug {
			s.logger().Printf("Chunk %+v is missing at %q, probably hasn't replicated yet, skipping", curCh.CurChunk.Name, addr)
		}
		return io.EOF
	}

	if len(res) > 0 {
		err = processFn(res)
		if err == nil {
			curCh.Off += uint64(len(res))
		}

		return err
	}

	// 0 bytes read but no errors means the end of file by convention.
	if !curCh.CurChunk.Complete {
		if s.debug {
			s.logger().Printf("Chunk %+v was read until the end and is not complete, EOF", curCh.CurChunk.Name)
		}
		return io.EOF
	}

	// Get the next chunk before acknowledging the current one
	// so that we always have non-empty chunk name.
	// If we don't have a new chunk name, it can be because
	// the chunk list was requested from a different replica
	// that doesn't yet have new chunks.
	nextChunk, err := s.getNextChunkForInstance(ctx, addr, instance, category, curCh.CurChunk.Name)
	if errors.Is(err, errNoNewChunks) {
		if s.debug {
			s.logger().Printf("No new chunks after chunk %+v, EOF", curCh.CurChunk.Name)
		}
		return io.EOF
	} else if err != nil {
		return err
	}

	if s.acknowledge {
		if err := s.cl.Ack(ctx, addr, category, curCh.CurChunk.Name, curCh.Off); err != nil {
			return fmt.Errorf("ack current chunk: %w", err)
		}
	}

	if s.debug {
		s.logger().Printf("Setting next chunk to %q", nextChunk.Name)
	}

	_, idx := protocol.ParseChunkFileName(curCh.CurChunk.Name)
	curCh.LastAckedChunkIdx = idx
	curCh.CurChunk = nextChunk
	curCh.Off = 0

	if s.debug {
		s.logger().Printf(`errRetry: need to read the next chunk so that we do not return empty response`)
	}

	return errRetry
}

var errNoNewChunks = errors.New("no new chunks")

func (s *Simple) getNextChunkForInstance(ctx context.Context, addr, instance, category string, chunkName string) (protocol.Chunk, error) {
	_, idx := protocol.ParseChunkFileName(chunkName)
	lastAckedChunkIndexes := make(map[string]int)
	lastAckedChunkIndexes[instance] = idx
	chunksByInstance, err := s.getUnackedChunksGroupedByInstance(ctx, category, addr, lastAckedChunkIndexes)
	if err != nil {
		return protocol.Chunk{}, fmt.Errorf("getting chunks list before ack: %w", err)
	}
	if len(chunksByInstance[instance]) == 0 {
		return protocol.Chunk{}, fmt.Errorf("getting new chunks list before ack: unexpected error for instance %q: %w", instance, errNoNewChunks)
	}

	return chunksByInstance[instance][0], nil
}

func (s *Simple) getUnackedChunksGroupedByInstance(ctx context.Context, category, addr string, lastAckedChunkIndexes map[string]int) (map[string][]protocol.Chunk, error) {
	chunks, err := s.cl.ListChunks(ctx, addr, category, false)
	if err != nil {
		return nil, fmt.Errorf("listChunks failed: %w", err)
	}

	if len(chunks) == 0 {
		return nil, io.EOF
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
		lastAckedChunkIdx, exists := lastAckedChunkIndexes[instance]
		if exists && chunkIdx <= lastAckedChunkIdx {
			continue
		}

		chunksByInstance[instance] = append(chunksByInstance[instance], c)
	}

	return chunksByInstance, nil
}

func (s *Simple) updateOffsets(ctx context.Context, category, addr string) error {
	lastAckedChunkIndexes := make(map[string]int, len(s.st.Offsets))
	for instance, off := range s.st.Offsets {
		lastAckedChunkIndexes[instance] = off.LastAckedChunkIdx
	}

	chunksByInstance, err := s.getUnackedChunksGroupedByInstance(ctx, category, addr, lastAckedChunkIndexes)
	if err != nil {
		return err
	}

	for instance, chunks := range chunksByInstance {
		off, exists := s.st.Offsets[instance]
		if exists {
			s.updateCurrentChunkInfo(chunks, off)
			continue
		}

		s.st.Offsets[instance] = &ReadOffset{
			CurChunk:          s.getOldestChunk(chunks),
			LastAckedChunkIdx: -1, // can't use 0 because it would mean that chunk 0 is acked
			Off:               0,
		}
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

func (s *Simple) updateCurrentChunkInfo(chunks []protocol.Chunk, curCh *ReadOffset) {
	for _, c := range chunks {
		_, idx := protocol.ParseChunkFileName(c.Name)
		if idx < 0 {
			continue
		}

		if c.Name == curCh.CurChunk.Name {
			curCh.CurChunk = c
			return
		}
	}
}

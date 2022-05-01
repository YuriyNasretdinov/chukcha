package web

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/YuriyNasretdinov/chukcha/server"
	"github.com/YuriyNasretdinov/chukcha/server/replication"
	"github.com/valyala/fasthttp"
)

// Server implements a web server
type Server struct {
	logger       *log.Logger
	instanceName string
	dirname      string
	listenAddr   string

	replStorage *replication.Storage

	getOnDisk GetOnDiskFn
}

type GetOnDiskFn func(category string) (*server.OnDisk, error)

// NewServer creates *Server
func NewServer(logger *log.Logger, instanceName string, dirname string, listenAddr string, replStorage *replication.Storage, getOnDisk GetOnDiskFn) *Server {
	return &Server{
		logger:       logger,
		instanceName: instanceName,
		dirname:      dirname,
		listenAddr:   listenAddr,
		replStorage:  replStorage,
		getOnDisk:    getOnDisk,
	}
}

func (s *Server) handler(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Path()) {
	case "/write":
		s.writeHandler(ctx)
	case "/read":
		s.readHandler(ctx)
	case "/ack":
		s.ackHandler(ctx)
	case "/replication/ack":
		s.replicationAckHandler(ctx)
	case "/replication/events":
		s.replicationEventsHandler(ctx)
	case "/listChunks":
		s.listChunksHandler(ctx)
	case "/listCategories":
		s.listCategoriesHandler(ctx)
	default:
		ctx.WriteString("Hello world!")
	}
}

// TODO: check validity of a file name on Windows.
func isValidCategory(category string) bool {
	if category == "" {
		return false
	}

	cleanPath := filepath.Clean(category)
	if cleanPath != category {
		return false
	}

	if strings.ContainsAny(category, `/\.`) {
		return false
	}

	return true
}

func (s *Server) getStorageForCategory(category string) (*server.OnDisk, error) {
	if !isValidCategory(category) {
		return nil, errors.New("invalid category name")
	}

	return s.getOnDisk(category)
}

func (s *Server) writeHandler(ctx *fasthttp.RequestCtx) {
	storage, err := s.getStorageForCategory(string(ctx.QueryArgs().Peek("category")))
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(err.Error())
		return
	}

	chunkName, off, err := storage.Write(ctx, ctx.Request.Body())
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
		return
	}

	minSyncReplicas, err := ctx.QueryArgs().GetUint("min_sync_replicas")
	if err != nil && err != fasthttp.ErrNoArgValue {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(err.Error())
		return
	} else if minSyncReplicas > 0 {
		waitCtx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()

		if err := storage.Wait(waitCtx, chunkName, uint64(off), uint(minSyncReplicas)); err != nil {
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			ctx.WriteString(err.Error())
			return
		}
	}
}

func (s *Server) ackHandler(ctx *fasthttp.RequestCtx) {
	storage, err := s.getStorageForCategory(string(ctx.QueryArgs().Peek("category")))
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(err.Error())
		return
	}

	chunk := ctx.QueryArgs().Peek("chunk")
	if len(chunk) == 0 {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString("bad `chunk` GET param: chunk name must be provided")
		return
	}

	size, err := ctx.QueryArgs().GetUint("size")
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(fmt.Sprintf("bad `size` GET param: %v", err))
		return
	}

	if err := storage.Ack(ctx, string(chunk), uint64(size)); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
	}
}

// replicationAckHandler is used to let chunk owner (us) know that
// replica has successfully downloaded the chunk.
func (s *Server) replicationAckHandler(ctx *fasthttp.RequestCtx) {
	storage, err := s.getStorageForCategory(string(ctx.QueryArgs().Peek("category")))
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(err.Error())
		return
	}

	chunk := ctx.QueryArgs().Peek("chunk")
	if len(chunk) == 0 {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString("bad `chunk` GET param: chunk name must be provided")
		return
	}

	// instance is the name of instance that has successfully downloaded the
	// respective chunk part.
	instance := ctx.QueryArgs().Peek("instance")
	if len(instance) == 0 {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString("bad `instance` GET param: replica name must be provided")
		return
	}

	size, err := ctx.QueryArgs().GetUint("size")
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(fmt.Sprintf("bad `fromOff` GET param: %v", err))
		return
	}

	storage.ReplicationAck(ctx, string(chunk), string(instance), uint64(size))
}

// registerReplicationEvents is sending the events stream to the connected replica.
func (s *Server) replicationEventsHandler(ctx *fasthttp.RequestCtx) {
	// instance is the name of the connected replica.
	instance := ctx.QueryArgs().Peek("instance")
	if len(instance) == 0 {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString("bad `instance` GET param: replica name must be provided")
		return
	}

	eventsCh := make(chan replication.Chunk, 1000)

	ctx.Response.SetBodyStreamWriter(func(w *bufio.Writer) {
		wr := json.NewEncoder(w)

		for ch := range eventsCh {
			if err := wr.Encode(ch); err != nil {
				s.logger.Printf("error encoding event: %v", err)
				return
			}

			if err := w.Flush(); err != nil {
				s.logger.Printf("error flushing event: %v", err)
				return
			}
		}
	})

	s.replStorage.RegisterReplica(string(instance), eventsCh)
}

func (s *Server) readHandler(ctx *fasthttp.RequestCtx) {
	chunk := ctx.QueryArgs().Peek("chunk")
	if len(chunk) == 0 {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString("bad `chunk` GET param: chunk name must be provided")
		return
	}

	fromReplication, _ := ctx.QueryArgs().GetUint("from_replication")
	if fromReplication == 1 {
		// s.logger.Printf("sleeping for 8 seconds for request from replication for chunk %v", string(chunk))
		// time.Sleep(time.Second * 8)
	}

	storage, err := s.getStorageForCategory(string(ctx.QueryArgs().Peek("category")))
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(err.Error())
		return
	}

	off, err := ctx.QueryArgs().GetUint("off")
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(fmt.Sprintf("bad `off` GET param: %v", err))
		return
	}

	maxSize, err := ctx.QueryArgs().GetUint("maxSize")
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(fmt.Sprintf("bad `maxSize` GET param: %v", err))
		return
	}

	err = storage.Read(string(chunk), uint64(off), uint64(maxSize), ctx)
	if err != nil && err != io.EOF {
		if errors.Is(err, os.ErrNotExist) {
			ctx.SetStatusCode(fasthttp.StatusNotFound)
		} else {
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		}
		ctx.WriteString(err.Error())
		return
	}
}

func (s *Server) listChunksHandler(ctx *fasthttp.RequestCtx) {
	storage, err := s.getStorageForCategory(string(ctx.QueryArgs().Peek("category")))
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(err.Error())
		return
	}

	fromReplication, _ := ctx.QueryArgs().GetUint("from_replication")
	if fromReplication == 1 {
		// c.logger.Printf("sleeping for 8 seconds for request from replication for listing chunks")
		// time.Sleep(time.Second * 8)
	}

	chunks, err := storage.ListChunks()
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
		return
	}

	json.NewEncoder(ctx).Encode(chunks)
}

func (s *Server) listCategoriesHandler(ctx *fasthttp.RequestCtx) {
	res := make([]string, 0)
	dis, err := os.ReadDir(s.dirname)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
		return
	}

	for _, d := range dis {
		if d.IsDir() {
			res = append(res, d.Name())
		}
	}

	json.NewEncoder(ctx).Encode(res)
}

// Serve listens to HTTP connections
func (s *Server) Serve() error {
	return fasthttp.ListenAndServe(s.listenAddr, s.handler)
}

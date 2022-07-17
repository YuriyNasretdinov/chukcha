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

	"github.com/YuriyNasretdinov/chukcha/replication"
	"github.com/YuriyNasretdinov/chukcha/server"
	"github.com/valyala/fasthttp"
)

const maxReadSize = 16 * 1024 * 1024 // 16 MiB

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

type errWithCode struct {
	code int
	text string
}

// Error implements `error` type interface
func (e *errWithCode) Error() string {
	return e.text
}

// Code returns an HTTP status code of the response.
func (e *errWithCode) Code() int {
	return e.code
}

type withCode interface {
	Code() int
}

func (s *Server) withCode(err error, code int) *errWithCode {
	return &errWithCode{code: code, text: err.Error()}
}

func (s *Server) handler(ctx *fasthttp.RequestCtx) {
	var err error

	switch string(ctx.Path()) {
	case "/write":
		err = s.writeHandler(ctx)
	case "/read":
		err = s.readHandler(ctx)
	case "/ack":
		err = s.ackHandler(ctx)
	case "/replication/ack":
		err = s.replicationAckHandler(ctx)
	case "/replication/events":
		err = s.replicationEventsHandler(ctx)
	case "/listChunks":
		err = s.listChunksHandler(ctx)
	case "/listCategories":
		err = s.listCategoriesHandler(ctx)
	default:
		err = &errWithCode{code: fasthttp.StatusNotFound, text: "not found"}
	}

	if err == nil {
		return
	}

	var ec withCode
	if errors.As(err, &ec) {
		ctx.Response.SetStatusCode(ec.Code())
	} else {
		ctx.Response.SetStatusCode(fasthttp.StatusInternalServerError)
	}

	ctx.WriteString(err.Error())
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

func (s *Server) writeHandler(ctx *fasthttp.RequestCtx) error {
	storage, err := s.getStorageForCategory(string(ctx.QueryArgs().Peek("category")))
	if err != nil {
		return s.withCode(err, fasthttp.StatusBadRequest)
	}

	chunkName, off, err := storage.Write(ctx, ctx.Request.Body())
	if err != nil {
		return err
	}

	minSyncReplicas, err := ctx.QueryArgs().GetUint("min_sync_replicas")
	if err != nil && err != fasthttp.ErrNoArgValue {
		return s.withCode(err, fasthttp.StatusBadRequest)
	} else if minSyncReplicas > 0 {
		waitCtx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()

		if err := storage.Wait(waitCtx, chunkName, uint64(off), uint(minSyncReplicas)); err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) ackHandler(ctx *fasthttp.RequestCtx) error {
	storage, err := s.getStorageForCategory(string(ctx.QueryArgs().Peek("category")))
	if err != nil {
		return s.withCode(err, fasthttp.StatusBadRequest)
	}

	chunk := ctx.QueryArgs().Peek("chunk")
	if len(chunk) == 0 {
		return &errWithCode{code: fasthttp.StatusBadRequest, text: "bad `chunk` GET param: chunk name must be provided"}
	}

	size, err := ctx.QueryArgs().GetUint("size")
	if err != nil {
		return &errWithCode{code: fasthttp.StatusBadRequest, text: fmt.Sprintf("bad `size` GET param: %v", err)}
	}

	if err := storage.Ack(ctx, string(chunk), uint64(size)); err != nil {
		return err
	}

	return nil
}

// replicationAckHandler is used to let chunk owner (us) know that
// replica has successfully downloaded the chunk.
func (s *Server) replicationAckHandler(ctx *fasthttp.RequestCtx) error {
	storage, err := s.getStorageForCategory(string(ctx.QueryArgs().Peek("category")))
	if err != nil {
		return s.withCode(err, fasthttp.StatusBadRequest)
	}

	chunk := ctx.QueryArgs().Peek("chunk")
	if len(chunk) == 0 {
		return &errWithCode{code: fasthttp.StatusBadRequest, text: "bad `chunk` GET param: chunk name must be provided"}
	}

	// instance is the name of instance that has successfully downloaded the
	// respective chunk part.
	instance := ctx.QueryArgs().Peek("instance")
	if len(instance) == 0 {
		return &errWithCode{code: fasthttp.StatusBadRequest, text: "bad `instance` GET param: chunk name must be provided"}
	}

	size, err := ctx.QueryArgs().GetUint("size")
	if err != nil {
		return &errWithCode{code: fasthttp.StatusBadRequest, text: "bad `size` GET param: chunk name must be provided"}
	}

	storage.ReplicationAck(ctx, string(chunk), string(instance), uint64(size))
	return nil
}

// registerReplicationEvents is sending the events stream to the connected replica.
func (s *Server) replicationEventsHandler(ctx *fasthttp.RequestCtx) error {
	// instance is the name of the connected replica.
	instance := ctx.QueryArgs().Peek("instance")
	if len(instance) == 0 {
		return &errWithCode{code: fasthttp.StatusBadRequest, text: "bad `instance` GET param: replica name must be provided"}
	}

	eventsCh := make(chan replication.Message, 1000)

	ctx.Response.SetBodyStreamWriter(func(w *bufio.Writer) {
		wr := json.NewEncoder(w)

		for {
			select {
			case ch, ok := <-eventsCh:
				if !ok {
					return
				}

				if err := wr.Encode(ch); err != nil {
					s.logger.Printf("error encoding event: %v", err)
					return
				}
			case <-time.After(10 * time.Second):
				if err := wr.Encode(&replication.Chunk{}); err != nil {
					s.logger.Printf("error encoding heartbeat event: %v", err)
					return
				}
			}

			if err := w.Flush(); err != nil {
				s.logger.Printf("error flushing event: %v", err)
				return
			}
		}
	})

	s.replStorage.RegisterReplica(string(instance), eventsCh)
	return nil
}

func (s *Server) readHandler(ctx *fasthttp.RequestCtx) error {
	chunk := ctx.QueryArgs().Peek("chunk")
	if len(chunk) == 0 {
		return &errWithCode{code: fasthttp.StatusBadRequest, text: "bad `chunk` GET param: chunk name must be provided"}
	}

	fromReplication, _ := ctx.QueryArgs().GetUint("from_replication")
	if fromReplication == 1 {
		// s.logger.Printf("sleeping for 8 seconds for request from replication for chunk %v", string(chunk))
		// time.Sleep(time.Second * 8)
	}

	storage, err := s.getStorageForCategory(string(ctx.QueryArgs().Peek("category")))
	if err != nil {
		return s.withCode(err, fasthttp.StatusBadRequest)
	}

	off, err := ctx.QueryArgs().GetUint("off")
	if err != nil {
		return &errWithCode{code: fasthttp.StatusBadRequest, text: fmt.Sprintf("bad `off` GET param: %v", err)}
	}

	maxSize, err := ctx.QueryArgs().GetUint("max_size")
	if err != nil {
		return &errWithCode{code: fasthttp.StatusBadRequest, text: fmt.Sprintf("bad `max_size` GET param: %v", err)}
	} else if maxSize > maxReadSize {
		return &errWithCode{code: fasthttp.StatusBadRequest, text: fmt.Sprintf("bad `max_size` GET param: size can't exceed %d bytes", maxReadSize)}
	}

	err = storage.Read(string(chunk), uint64(off), uint64(maxSize), ctx)
	if err != nil && err != io.EOF {
		if errors.Is(err, os.ErrNotExist) {
			return s.withCode(err, fasthttp.StatusNotFound)
		}
		return err
	}

	return nil
}

func (s *Server) listChunksHandler(ctx *fasthttp.RequestCtx) error {
	storage, err := s.getStorageForCategory(string(ctx.QueryArgs().Peek("category")))
	if err != nil {
		return s.withCode(err, fasthttp.StatusBadRequest)
	}

	fromReplication, _ := ctx.QueryArgs().GetUint("from_replication")
	if fromReplication == 1 {
		// c.logger.Printf("sleeping for 8 seconds for request from replication for listing chunks")
		// time.Sleep(time.Second * 8)
	}

	chunks, err := storage.ListChunks()
	if err != nil {
		return err
	}

	return json.NewEncoder(ctx).Encode(chunks)
}

func (s *Server) listCategoriesHandler(ctx *fasthttp.RequestCtx) error {
	res := make([]string, 0)
	dis, err := os.ReadDir(s.dirname)
	if err != nil {
		return err
	}

	for _, d := range dis {
		if d.IsDir() {
			res = append(res, d.Name())
		}
	}

	return json.NewEncoder(ctx).Encode(res)
}

// Serve listens to HTTP connections
func (s *Server) Serve() error {
	return fasthttp.ListenAndServe(s.listenAddr, s.handler)
}

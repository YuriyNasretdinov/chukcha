package web

import (
	"github.com/YuriyNasretdinov/chukcha/server"
	"github.com/valyala/fasthttp"
)

const defaultBufSize = 512 * 1024

// Server implements a web server
type Server struct {
	s *server.InMemory
}

// NewServer creates *Server
func NewServer(s *server.InMemory) *Server {
	return &Server{s: s}
}

func (s *Server) handler(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Path()) {
	case "/write":
		s.writeHandler(ctx)
	case "/read":
		s.readHandler(ctx)
	default:
		ctx.WriteString("Hello world!")
	}
}

func (s *Server) writeHandler(ctx *fasthttp.RequestCtx) {
	if err := s.s.Send(ctx.Request.Body()); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
	}
}

func (s *Server) readHandler(ctx *fasthttp.RequestCtx) {
	buf := make([]byte, defaultBufSize)

	res, err := s.s.Receive(buf)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
		return
	}

	ctx.Write(res)
}

// Serve listens to HTTP connections
func (s *Server) Serve() error {
	return fasthttp.ListenAndServe(":8080", s.handler)
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"
)

type Event struct {
	Time           string `json:"time"`
	URL            string `json:"url"`
	IP             string `json:"ip"`
	UserAgent      string `json:"user_agent"`
	AcceptEncoding string `json:"accept_encoding"`
	AcceptLanguage string `json:"accept_language"`
	Name           string `json:"name"`
}

func (a *app) sendEvent(ctx context.Context, req *http.Request) error {
	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		log.Printf("parsing %q: %v", req.RemoteAddr, err)
	}

	e := &Event{
		Time:           time.Now().Format("2006-01-02 15:04:05"),
		URL:            req.URL.String(),
		IP:             host,
		UserAgent:      req.Header.Get("User-Agent"),
		AcceptEncoding: req.Header.Get("Accept-Encoding"),
		AcceptLanguage: req.Header.Get("Accept-Language"),
		Name:           req.Form.Get("name"),
	}

	buf, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshalling event: %v", err)
	}

	buf = append(buf, '\n')

	// start := time.Now()
	// defer func() { log.Printf("sent event to Chukcha in %s", time.Since(start)) }()

	return a.cl.Send(ctx, "events", buf)
}

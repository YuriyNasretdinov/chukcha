package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"

	"github.com/YuriyNasretdinov/chukcha/protocol"
)

// Raw is an HTTP client to Chukcha that allows you to
// send requests to Chukcha servers using it's actual API.
// The intended use case is to build your own clients if
// you need it.
type Raw struct {
	Logger *log.Logger

	minSyncReplicas uint
	debug           bool
	cl              *http.Client
}

// NewRaw creates a Raw client instance
func NewRaw(cl *http.Client) *Raw {
	if cl == nil {
		cl = &http.Client{}
	}

	return &Raw{
		cl: cl,
	}
}

// SetDebug either enables or disables debug logging for the client.
func (r *Raw) SetDebug(v bool) {
	r.debug = v
}

// SetMinSyncReplicas sets the value of minSyncReplicas when sending the requests to
// Chukcha.
func (r *Raw) SetMinSyncReplicas(v uint) {
	r.minSyncReplicas = v
}

func (r *Raw) logger() *log.Logger {
	if r.Logger == nil {
		return log.Default()
	}

	return r.Logger
}

// Write sends the events to the appropriate Chukcha server.
func (r *Raw) Write(ctx context.Context, addr string, category string, msgs []byte) (err error) {
	u := url.Values{}
	u.Add("category", category)
	if r.minSyncReplicas > 0 {
		u.Add("min_sync_replicas", strconv.FormatUint(uint64(r.minSyncReplicas), 10))
	}

	url := addr + "/write?" + u.Encode()

	if r.debug {
		debugMsgs := msgs
		if len(debugMsgs) > 128 {
			debugMsgs = []byte(fmt.Sprintf("%s... (%d bytes total)", msgs[0:128], len(msgs)))
		}
		r.logger().Printf("Sending to %s the following messages: %q", url, debugMsgs)
		defer func() { r.logger().Printf("Send result: err=%v", err) }()
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(msgs))
	if err != nil {
		return fmt.Errorf("making http request: %v", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := r.cl.Do(req)
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

func (r *Raw) Read(ctx context.Context, addr string, category string, chunk string, off uint64, scratch []byte) (res []byte, found bool, err error) {
	u := url.Values{}
	u.Add("off", strconv.FormatInt(int64(off), 10))
	u.Add("max_size", strconv.Itoa(len(scratch)))
	u.Add("chunk", chunk)
	u.Add("category", category)

	readURL := fmt.Sprintf("%s/read?%s", addr, u.Encode())

	if r.debug {
		r.logger().Printf("Reading from %s", readURL)
		defer func() { r.logger().Printf("Read returned: res=%d bytes, found=%v, err=%v", len(res), found, err) }()
	}

	req, err := http.NewRequestWithContext(ctx, "GET", readURL, nil)
	if err != nil {
		return nil, false, fmt.Errorf("creating Request: %v", err)
	}

	resp, err := r.cl.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("read %q: %v", readURL, err)
	}

	defer resp.Body.Close()

	b := bytes.NewBuffer(scratch[0:0])
	_, err = io.Copy(b, resp.Body)
	if err != nil {
		return nil, false, fmt.Errorf("read %q: %v", readURL, err)
	}

	// If the chunk does not exist, but we got it through ListChunks(),
	// it means that this chunk hasn't been replicated to this server
	// yet, but it is not an error because eventually it should appear.
	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	} else if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("GET %q: http code %d, %s", readURL, resp.StatusCode, b.String())
	}

	return b.Bytes(), true, nil
}

// ListChunks returns the list of chunks for the appropriate Chukcha instance.
func (r *Raw) ListChunks(ctx context.Context, addr, category string, fromReplication bool) ([]protocol.Chunk, error) {
	u := url.Values{}
	u.Add("category", category)
	if fromReplication {
		u.Add("from_replication", "1")
	}

	listURL := fmt.Sprintf("%s/listChunks?%s", addr, u.Encode())

	req, err := http.NewRequestWithContext(ctx, "GET", listURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating new http request: %w", err)
	}

	resp, err := r.cl.Do(req)
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

	if r.debug {
		r.logger().Printf("ListChunks(%q) returned %+v", addr, res)
	}

	return res, nil
}

// ListCategories returns the list of categories for the appropriate Chukcha instance.
func (r *Raw) ListCategories(ctx context.Context, addr string) ([]string, error) {
	listURL := fmt.Sprintf("%s/listCategories", addr)

	req, err := http.NewRequestWithContext(ctx, "GET", listURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating new http request: %w", err)
	}

	resp, err := r.cl.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		return nil, fmt.Errorf("listCategories error: %s", body)
	}

	var res []string
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}

	if r.debug {
		r.logger().Printf("listCategories(%q) returned %+v", addr, res)
	}

	return res, nil
}

func (r *Raw) Ack(ctx context.Context, addr, category, chunk string, size uint64) error {
	u := url.Values{}
	u.Add("chunk", chunk)
	u.Add("size", strconv.FormatInt(int64(size), 10))
	u.Add("category", category)

	if r.debug {
		r.logger().Printf("Acknowledging category %q chunk %q with size %d at %q", category, chunk, size, addr)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf(addr+"/ack?%s", u.Encode()), nil)
	if err != nil {
		return fmt.Errorf("creating Request: %v", err)
	}

	resp, err := r.cl.Do(req)
	if err != nil {
		return fmt.Errorf("executing request: %v", err)
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

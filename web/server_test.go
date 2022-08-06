package web

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/YuriyNasretdinov/chukcha/client"
	"github.com/YuriyNasretdinov/chukcha/protocol"
	"github.com/YuriyNasretdinov/chukcha/replication"
	"github.com/YuriyNasretdinov/chukcha/server"
	"github.com/phayes/freeport"
)

const defaultChunkSize = 1024 * 1024
const rotateChunkInterval = time.Minute

func NewTestServer(t *testing.T, instanceName string, dirName string, listenAddr string) *Server {
	t.Helper()

	replStorage := replication.NewStorage(log.Default(), instanceName)
	onDiskCreator := server.NewOnDiskCreator(log.Default(), dirName, instanceName, replStorage, defaultChunkSize, rotateChunkInterval)

	return NewServer(log.Default(), instanceName, dirName, listenAddr, replStorage, onDiskCreator.Get)
}

func getFreePort(t *testing.T) int {
	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("Failed to get a free port: %d", err)
	}
	return port
}

func waitForPort(t *testing.T, port int) {
	t.Helper()

	for i := 0; i <= 100; i++ {
		timeout := time.Millisecond * 50
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("localhost", fmt.Sprint(port)), timeout)
		if err != nil {
			time.Sleep(timeout)
			continue
		}
		conn.Close()
		break
	}
}

func StartDefaultTestServer(t *testing.T) (tmpDir string, srvAddr string, cl *client.Raw) {
	t.Helper()

	tmpDir = t.TempDir()

	port := getFreePort(t)
	listenAddr := fmt.Sprintf("127.0.0.1:%d", port)
	srv := NewTestServer(t, "test", tmpDir, listenAddr)
	go srv.Serve()
	waitForPort(t, port)

	return tmpDir, "http://" + listenAddr, client.NewRaw(&http.Client{Timeout: 30 * time.Second})
}

func TestListCategories(t *testing.T) {
	tmpDir, srvAddr, cl := StartDefaultTestServer(t)

	if err := os.MkdirAll(filepath.Join(tmpDir, "test"), 0777); err != nil {
		t.Fatalf("Failed to create a temp dir: %v", err)
	}

	cats, err := cl.ListCategories(context.Background(), srvAddr)
	if err != nil {
		t.Fatalf("ListCategories() = _, %v; want no errors", err)
	} else if want := []string{"test"}; !reflect.DeepEqual(cats, want) {
		t.Fatalf("ListCategories() = %v, nil; want %v, nil", cats, want)
	}
}

func TestListChunks(t *testing.T) {
	tmpDir, srvAddr, cl := StartDefaultTestServer(t)

	if err := os.MkdirAll(filepath.Join(tmpDir, "test"), 0777); err != nil {
		t.Fatalf("Failed to create a temp dir: %v", err)
	}

	ctx := context.Background()

	chunks, err := cl.ListChunks(ctx, srvAddr, "test", false)
	if err != nil {
		t.Fatalf(`ListChunks("test") = _, %v; want no errors`, err)
	} else if want := []protocol.Chunk(nil); !reflect.DeepEqual(chunks, want) {
		t.Fatalf(`ListChunks("test") = %v, nil; want %v, nil`, chunks, want)
	}

	helloWorld := []byte("Hello world\n")
	err = cl.Write(ctx, srvAddr, "test", helloWorld)
	if err != nil {
		t.Fatalf(`Write("test", %q) = %v; want no errors`, err, helloWorld)
	}

	chunk := protocol.Chunk{
		Name:     "test-chunk000000000",
		Complete: false,
		Size:     uint64(len(helloWorld)),
	}

	chunks, err = cl.ListChunks(ctx, srvAddr, "test", false)
	if err != nil {
		t.Fatalf(`ListChunks("test") after write = _, %v; want no errors`, err)
	} else if want := []protocol.Chunk{chunk}; !reflect.DeepEqual(chunks, want) {
		t.Fatalf(`ListChunks("test") after write = %v, nil; want %v, nil`, chunks, want)
	}
}

func TestReadWrite(t *testing.T) {
	_, srvAddr, cl := StartDefaultTestServer(t)
	ctx := context.Background()

	helloWorld := []byte("Hello world\n")
	err := cl.Write(ctx, srvAddr, "test", helloWorld)
	if err != nil {
		t.Fatalf(`Write("test", %q) = %v; want no errors`, err, helloWorld)
	}

	chunkName := "test-chunk000000000"
	res, found, err := cl.Read(ctx, srvAddr, "test", chunkName, 0, make([]byte, 100))

	if err != nil {
		t.Fatalf(`Read("test", %q) returned %v; want no errors`, chunkName, err)
	} else if !found {
		t.Errorf(`Read("test", %q) returned found=%v; want chunk to be present`, chunkName, found)
	} else if !bytes.Equal(res, helloWorld) {
		t.Errorf(`Read("test", %q) returned %q; want %q`, chunkName, string(res), string(helloWorld))
	}
}

func TestReadNonExistent(t *testing.T) {
	_, srvAddr, cl := StartDefaultTestServer(t)
	ctx := context.Background()

	chunkName := "test-chunk000000000"
	_, found, err := cl.Read(ctx, srvAddr, "test", chunkName, 0, make([]byte, 100))

	if err != nil {
		t.Fatalf(`Read("test", %q) returned %v; want no errors`, chunkName, err)
	} else if found {
		t.Errorf(`Read("test", %q) returned found=%v; want chunk not found response`, chunkName, found)
	}
}

func TestAckLastChunk(t *testing.T) {
	_, srvAddr, cl := StartDefaultTestServer(t)
	ctx := context.Background()

	helloWorld := []byte("Hello world\n")
	err := cl.Write(ctx, srvAddr, "test", helloWorld)
	if err != nil {
		t.Fatalf(`Write("test", %q) = %v; want no errors`, err, helloWorld)
	}

	chunkName := "test-chunk000000000"
	err = cl.Ack(ctx, srvAddr, "test", chunkName, uint64(len(helloWorld)))

	if err == nil {
		t.Fatalf(`Ack("test", %q) = nil; want an error when acknowledging the current chunk`, chunkName)
	}
}

func TestAckNonLastChunk(t *testing.T) {
	tmpDir, srvAddr, cl := StartDefaultTestServer(t)
	ctx := context.Background()

	// If we create an empty chunk file in the directory
	// where we haven't written yet through Chukcha server,
	// the server hasn't yet read this directory, so
	// it doesn't have any file pointers open to the chunks,
	// so upon first write it will read the directory and find
	// our empty chunk and start with the index + 1.
	if err := os.MkdirAll(filepath.Join(tmpDir, "test"), 0777); err != nil {
		t.Fatalf("Failed to create a temp dir: %v", err)
	}

	chunkName := "test-chunk000000000"

	if _, err := os.Create(filepath.Join(tmpDir, "test", chunkName)); err != nil {
		t.Fatalf("Failed to create an empty chunk: %v", err)
	}

	helloWorld := []byte("Hello world\n")
	err := cl.Write(ctx, srvAddr, "test", helloWorld)
	if err != nil {
		t.Fatalf(`Write("test", %q) = %v; want no errors`, err, helloWorld)
	}

	err = cl.Ack(ctx, srvAddr, "test", chunkName, 0)
	if err != nil {
		t.Fatalf(`Ack("test", %q) = %v; want no errors when acknowleding the non-last chunk`, chunkName, err)
	}
}

func TestNotFound(t *testing.T) {
	_, srvAddr, _ := StartDefaultTestServer(t)
	resp, err := http.Get(srvAddr + "/non-existent")
	if err != nil {
		t.Fatalf("http.Get(%s/nonexistent): %v; want no errors", srvAddr, err)
	} else if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("http.Get(%s/nonexistent): StatusCode=%v; want %v", srvAddr, resp.StatusCode, http.StatusNotFound)
	}
}

func expectBadRequest(t *testing.T, descr string, err error) {
	t.Helper()

	if err == nil {
		t.Errorf("%s: got no errors, expected bad request status code", descr)
	} else if !strings.Contains(err.Error(), "Bad Request") {
		t.Errorf(`%s: got an error %q; want error to contain "Bad Request"`, descr, err.Error())
	}
}

func TestBadRequest(t *testing.T) {
	_, srvAddr, cl := StartDefaultTestServer(t)
	ctx := context.Background()
	var err error

	_, _, err = cl.Read(ctx, srvAddr, "", "adfafsad", 0, make([]byte, 100))
	expectBadRequest(t, "Read(empty category)", err)

	_, _, err = cl.Read(ctx, srvAddr, "test", "", 0, make([]byte, 100))
	expectBadRequest(t, "Read(empty chunk)", err)

	_, _, err = cl.Read(ctx, srvAddr, "test", "chunkchunk", 0, make([]byte, 32*1024*1024))
	expectBadRequest(t, "Read(too big buffer size)", err)

	expectBadRequest(t, "Write(empty category)", cl.Write(ctx, srvAddr, "", nil))

	expectBadRequest(t, "Ack(empty category)", cl.Ack(ctx, srvAddr, "", "chunkchunk", 0))
	expectBadRequest(t, "Ack(empty chunk)", cl.Ack(ctx, srvAddr, "test", "", 0))

	_, err = cl.ListChunks(ctx, srvAddr, "", false)
	expectBadRequest(t, "ListChunks(empty category)", err)
}

func TestIsValidCategory(t *testing.T) {
	testCases := []struct {
		category string
		valid    bool
	}{
		{category: "", valid: false},
		{category: ".", valid: false},
		{category: "..", valid: false},
		{category: "numbers", valid: true},
		{category: "num\nbers", valid: true},
		{category: "_:num\nbe:rs", valid: true},
	}

	for _, tc := range testCases {
		got := isValidCategory(tc.category)
		want := tc.valid

		if got != want {
			t.Errorf("isValidCategory(%q) = %v; want %v", tc.category, got, want)
		}
	}
}

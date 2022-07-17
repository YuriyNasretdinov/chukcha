package web

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
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
		log.Fatalf("ListCategories() = _, %v; want no errors", err)
	} else if want := []string{"test"}; !reflect.DeepEqual(cats, want) {
		log.Fatalf("ListCategories() = %v, nil; want %v, nil", cats, want)
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
		log.Fatalf(`ListChunks("test") = _, %v; want no errors`, err)
	} else if want := []protocol.Chunk(nil); !reflect.DeepEqual(chunks, want) {
		log.Fatalf(`ListChunks("test") = %v, nil; want %v, nil`, chunks, want)
	}

	helloWorld := []byte("Hello world\n")
	err = cl.Write(ctx, srvAddr, "test", helloWorld)
	if err != nil {
		log.Fatalf(`Write("test", %q) = %v; want no errors`, err, helloWorld)
	}

	chunk := protocol.Chunk{
		Name:     "test-chunk000000000",
		Complete: false,
		Size:     uint64(len(helloWorld)),
	}

	chunks, err = cl.ListChunks(ctx, srvAddr, "test", false)
	if err != nil {
		log.Fatalf(`ListChunks("test") after write = _, %v; want no errors`, err)
	} else if want := []protocol.Chunk{chunk}; !reflect.DeepEqual(chunks, want) {
		log.Fatalf(`ListChunks("test") after write = %v, nil; want %v, nil`, chunks, want)
	}
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

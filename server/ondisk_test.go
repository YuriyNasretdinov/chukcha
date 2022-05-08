package server

import (
	"bytes"
	"context"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/YuriyNasretdinov/chukcha/protocol"
)

func TestInitLastChunkIdx(t *testing.T) {
	dir := getTempDir(t)

	testCreateFile(t, filepath.Join(dir, "moscow-chunk1"))
	testCreateFile(t, filepath.Join(dir, "moscow-chunk10"))
	srv := testNewOnDisk(t, dir)

	want := uint64(11)
	got := srv.lastChunkIdx

	if got != want {
		t.Errorf("Last chunk index = %d, want %d", got, want)
	}
}

func TestReadWrite(t *testing.T) {
	srv := testNewOnDisk(t, getTempDir(t))

	want := "one\ntwo\nthree\nfour\n"
	if _, _, err := srv.Write(context.Background(), []byte(want)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	chunks, err := srv.ListChunks()
	if err != nil {
		t.Fatalf("ListChunks(): %v", err)
	}

	if got, want := len(chunks), 1; want != got {
		t.Fatalf("len(ListChunks()) = %d, want %d", got, want)
	}

	chunk := chunks[0].Name

	var b bytes.Buffer
	if err := srv.Read(chunk, 0, uint64(len(want)), &b); err != nil {
		t.Fatalf("Read(%q, 0, %d) = %v, want no errors", chunk, uint64(len(want)), err)
	}

	got := b.String()
	if got != want {
		t.Errorf("Read(%q) = %q, want %q", chunk, got, want)
	}

	// Check that the last message is not chopped and only the first
	// three messages are returned if the buffer size is too small to
	// fit all four messages.
	want = "one\ntwo\nthree\n"
	b.Reset()
	if err := srv.Read(chunk, 0, uint64(len(want)+1), &b); err != nil {
		t.Fatalf("Read(%q, 0, %d) = %v, want no errors", chunk, uint64(len(want)+1), err)
	}

	got = b.String()
	if got != want {
		t.Errorf("Read(%q) = %q, want %q", chunk, got, want)
	}
}

func TestWriteChecksEOL(t *testing.T) {
	srv := testNewOnDisk(t, getTempDir(t))

	if _, _, err := srv.Write(context.Background(), []byte("lines\nwithout\nEOL")); err == nil {
		t.Fatalf("Write with a body that doesn't end with a new line character returned no errors")
	}
}

func TestWriteChecksMaxLength(t *testing.T) {
	srv := testNewOnDisk(t, getTempDir(t))

	if _, _, err := srv.Write(context.Background(), []byte("lines\nthat exceeds 10 characters at the end\n")); err == nil {
		t.Fatalf("Write with a body that contains lines that are longer than maxLine characters returned no errors")
	}
}

func TestAckOfTheLastChunk(t *testing.T) {
	srv := testNewOnDisk(t, getTempDir(t))

	want := "one\ntwo\nthree\nfour\n"
	if _, _, err := srv.Write(context.Background(), []byte(want)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	chunks, err := srv.ListChunks()
	if err != nil {
		t.Fatalf("ListChunks(): %v", err)
	}

	if got, want := len(chunks), 1; want != got {
		t.Fatalf("len(ListChunks()) = %d, want %d", got, want)
	}

	if err := srv.Ack(context.Background(), chunks[0].Name, chunks[0].Size); err == nil {
		t.Errorf("Ack(last chunk): got no errors, expected an error")
	}
}

func TestAckOfTheCompleteChunk(t *testing.T) {
	dir := getTempDir(t)
	srv := testNewOnDisk(t, dir)
	testCreateFile(t, filepath.Join(dir, "moscow-chunk1"))

	if err := srv.Ack(context.Background(), "moscow-chunk1", 0); err != nil {
		t.Errorf("Ack(moscow-chunk1) = %v, expected no errors", err)
	}
}

func getTempDir(t *testing.T) string {
	t.Helper()

	dir, err := os.MkdirTemp(os.TempDir(), "lastchunkidx")
	if err != nil {
		t.Fatalf("mkdir temp failed: %v", err)
	}

	t.Cleanup(func() { os.RemoveAll(dir) })

	return dir
}

type nilHooks struct{}

func (n *nilHooks) AfterCreatingChunk(ctx context.Context, category string, fileName string) {}

func (n *nilHooks) AfterAcknowledgeChunk(ctx context.Context, category string, fileName string) error {
	return nil
}

func testNewOnDisk(t *testing.T, dir string) *OnDisk {
	t.Helper()

	srv, err := NewOnDisk(log.Default(), dir, "test", "moscow", 20*1024*1024, 10, time.Minute, &nilHooks{})
	if err != nil {
		t.Fatalf("NewOnDisk(): %v", err)
	}

	return srv
}

func testCreateFile(t *testing.T, filename string) {
	t.Helper()

	if _, err := os.Create(filename); err != nil {
		t.Fatalf("could not create file %q: %v", filename, err)
	}
}

func TestParseChunkFileName(t *testing.T) {
	testCases := []struct {
		filename     string
		instanceName string
		chunkIdx     int
	}{
		{
			filename:     "Moscow-chunk0000000",
			instanceName: "Moscow",
			chunkIdx:     0,
		},
		{
			filename:     "Chelyabinsk-70-chunk00000123",
			instanceName: "Chelyabinsk-70",
			chunkIdx:     123,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.filename, func(t *testing.T) {
			instance, chunkIdx := protocol.ParseChunkFileName(tc.filename)

			if instance != tc.instanceName || chunkIdx != tc.chunkIdx {
				t.Errorf("parseChunkFileName(%q) = %q, %v; want %q, %v", tc.filename, instance, chunkIdx, tc.instanceName, tc.chunkIdx)
			}
		})
	}
}

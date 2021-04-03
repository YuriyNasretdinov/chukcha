package server

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
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

func TestGetFileDescriptor(t *testing.T) {
	dir := getTempDir(t)
	testCreateFile(t, filepath.Join(dir, "moscow-chunk1"))
	srv := testNewOnDisk(t, dir)

	testCases := []struct {
		desc     string
		filename string
		write    bool
		wantErr  bool
	}{
		{
			desc:     "Read from already existing file should not fail",
			filename: "moscow-chunk1",
			write:    false,
			wantErr:  false,
		},
		{
			desc:     "Should not overwrite existing files",
			filename: "moscow-chunk1",
			write:    true,
			wantErr:  true,
		},
		{
			desc:     "Should not be to read from files that don't exist",
			filename: "moscow-chunk2",
			write:    false,
			wantErr:  true,
		},
		{
			desc:     "Should be able to create files that don't exist",
			filename: "moscow-chunk2",
			write:    true,
			wantErr:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := srv.getFileDescriptor(tc.filename, tc.write)
			defer srv.forgetFileDescriptor(tc.filename)

			if tc.wantErr && err == nil {
				t.Errorf("wanted error, got not errors")
			} else if !tc.wantErr && err != nil {
				t.Errorf("wanted no errors, got error %v", err)
			}
		})
	}
}

func TestReadWrite(t *testing.T) {
	srv := testNewOnDisk(t, getTempDir(t))

	want := "one\ntwo\nthree\nfour\n"
	if err := srv.Write(context.Background(), []byte(want)); err != nil {
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

func TestAckOfTheLastChunk(t *testing.T) {
	srv := testNewOnDisk(t, getTempDir(t))

	want := "one\ntwo\nthree\nfour\n"
	if err := srv.Write(context.Background(), []byte(want)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	chunks, err := srv.ListChunks()
	if err != nil {
		t.Fatalf("ListChunks(): %v", err)
	}

	if got, want := len(chunks), 1; want != got {
		t.Fatalf("len(ListChunks()) = %d, want %d", got, want)
	}

	if err := srv.Ack(chunks[0].Name, chunks[0].Size); err == nil {
		t.Errorf("Ack(last chunk): got no errors, expected an error")
	}
}

func TestAckOfTheCompleteChunk(t *testing.T) {
	dir := getTempDir(t)
	srv := testNewOnDisk(t, dir)
	testCreateFile(t, filepath.Join(dir, "moscow-chunk1"))

	if err := srv.Ack("moscow-chunk1", 0); err != nil {
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

func (n *nilHooks) BeforeCreatingChunk(ctx context.Context, category string, fileName string) error {
	return nil
}

func testNewOnDisk(t *testing.T, dir string) *OnDisk {
	t.Helper()

	srv, err := NewOnDisk(dir, "test", "moscow", &nilHooks{})
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

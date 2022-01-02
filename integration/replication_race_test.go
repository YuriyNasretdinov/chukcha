package integration

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/YuriyNasretdinov/chukcha/client"
)

func ensureChunkDoesNotExist(t *testing.T, cl *client.Raw, addr string, chunk string, errMsg string) {
	chunks, err := cl.ListChunks(context.Background(), addr, "race", false)
	if err != nil {
		t.Fatalf("ListChunks() at %q returned an error: %v", addr, err)
	}

	for _, ch := range chunks {
		if ch.Name == chunk {
			t.Fatalf("Unexpected chunk %q at %q, wanted it to be absent (%s)", chunk, addr, errMsg)
		}
	}
}

func ensureChunkExists(t *testing.T, cl *client.Raw, addr string, chunk string, errMsg string) {
	chunks, err := cl.ListChunks(context.Background(), addr, "race", false)
	if err != nil {
		t.Fatalf("ListChunks() at %q returned an error: %v", addr, err)
	}

	for _, ch := range chunks {
		if ch.Name == chunk {
			return
		}
	}

	t.Fatalf("Chunk %q not found at %q, wanted it to be present", chunk, addr)
}

func waitUntilChunkAppears(t *testing.T, cl *client.Raw, addr string, chunk string, errMsg string) {
	t.Helper()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*10))
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("Timed out waiting for chunk %q to appear at %q (%s)", chunk, addr, errMsg)
		default:
		}

		chunks, err := cl.ListChunks(ctx, addr, "race", false)
		if err != nil {
			t.Fatalf("ListChunks() at %q returned an error: %v", addr, err)
		}

		for _, ch := range chunks {
			if ch.Name == chunk {
				return
			}
		}

		time.Sleep(time.Millisecond * 100)
	}
}

func moscowChunkName(idx int) string {
	return fmt.Sprintf("moscow-chunk%09d", idx)
}

func mustSend(t *testing.T, cl *client.Simple, ctx context.Context, category string, msgs []byte) {
	err := cl.Send(ctx, category, msgs)
	if err != nil {
		t.Fatalf("Failed to send the following messages: %q with error: %v", string(msgs), err)
	}
}

func TestReplicatingAlreadyAcknowledgedChunk(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	addrs, _ := runChukcha(t, true, tweaks{
		modifyInitArgs: func(t *testing.T, a *InitArgs) {
			a.DisableAcknowledge = true
			a.MaxChunkSize = 10
		},
	})

	moscowAddr := addrs[0]
	voronezhAddr := addrs[1]

	// Must be big enough to hold all the messages that we are sending.
	scratch := make([]byte, 1024)

	rawClient := client.NewRaw(&http.Client{})
	rawClient.SetDebug(true)
	moscowClient := client.NewSimple([]string{moscowAddr})
	moscowClient.SetDebug(true)
	moscowClient.Logger = log.Default()
	voronezhClient := client.NewSimple([]string{voronezhAddr})
	voronezhClient.SetDebug(true)
	voronezhClient.Logger = log.Default()

	firstMsg := "Moscow is having chunk0 which starts to replicate to Voronezh immediately\n"

	// Create chunk0 on Moscow
	mustSend(t, moscowClient, ctx, "race", []byte(firstMsg))
	ensureChunkExists(t, rawClient, moscowAddr, moscowChunkName(0), "Chunk0 must be present after first send")

	// Create chunk1 on Moscow
	mustSend(t, moscowClient, ctx, "race", []byte("Moscow now has chunk1 that is being written into currently\n"))
	ensureChunkExists(t, rawClient, moscowAddr, moscowChunkName(1), "Chunk1 must be present after second send")

	waitUntilChunkAppears(t, rawClient, voronezhAddr, moscowChunkName(1), "Voronezh must have chunk1 via replication")

	// Read Moscow's chunk0 until the end and acknowledge it.
	res, found, err := rawClient.Read(ctx, voronezhAddr, "race", moscowChunkName(0), 0, scratch)
	if err != nil || !found || !bytes.Equal(res, []byte(firstMsg)) {
		t.Fatalf("Read(chunk0) from Voronezh = %q, %v, %v; want %q, false, nil", string(res), found, err, firstMsg)
	}

	err = rawClient.Ack(ctx, voronezhAddr, "race", moscowChunkName(0), uint64(len(res)))
	if err != nil {
		t.Fatalf("Ack(chunk0) on Voronezh = %v; want no errors", err)
	}

	ensureChunkDoesNotExist(t, rawClient, voronezhAddr, moscowChunkName(0), "Chunk0 must have been acknowledged when reading from Voronezh")

	// Create chunk2 on Moscow
	mustSend(t, moscowClient, ctx, "race", []byte("Moscow now has chunk2 that is being written into currently, and this will also create an entry to replication which will try to download chunk0 on Voronezh once again, even though it was acknowledged on Voronezh, and, as acknowledge thread is disabled in this test, it will mean that Voronezh will download chunk0 once again\n"))
	ensureChunkExists(t, rawClient, moscowAddr, moscowChunkName(2), "Chunk2 must be present after third send()")

	// Make sure chunk2 has started downloading
	waitUntilChunkAppears(t, rawClient, voronezhAddr, moscowChunkName(2), "Chunk2 must be present on Voronezh via replication")

	// If everything works correctly, this would mean that Moscow chunk 0 will not be downloaded
	// to Voronezh, because it was already acknowledged there.
	ensureChunkDoesNotExist(t, rawClient, voronezhAddr, moscowChunkName(0), "Chunk0 must not be present on Voronezh because it was previously acknowledged")
}

package integration

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/YuriyNasretdinov/chukcha/client"
	"github.com/phayes/freeport"
)

const (
	maxN          = 10000000
	maxBufferSize = 1024 * 1024

	sendFmt = "Send: net %13s, cpu %13s (%.1f MiB)"
	recvFmt = "Recv: net %13s, cpu %13s"
)

func TestSimpleClientAndServerConcurrently(t *testing.T) {
	t.Parallel()
	simpleClientAndServerTest(t, true, false)
}

func TestSimpleClientAndServerSequentially(t *testing.T) {
	t.Parallel()
	simpleClientAndServerTest(t, false, false)
}

func TestSimpleClientWithReplicationSequentially(t *testing.T) {
	t.Parallel()
	simpleClientAndServerTest(t, false, true)
}

func TestSimpleClientWithReplicationConcurrently(t *testing.T) {
	t.Parallel()
	simpleClientAndServerTest(t, true, true)
}

func runEtcd(t *testing.T) (etcdPort int) {
	etcdPath, err := os.MkdirTemp(t.TempDir(), "etcd")
	if err != nil {
		t.Fatalf("Failed to create temp dir for etcd: %v", err)
	}

	etcdPeerPort, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port for etcd peer: %v", err)
	}

	etcdPort, err = freeport.GetFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port for etcd: %v", err)
	}

	etcdArgs := []string{"--data-dir", etcdPath,
		"--listen-client-urls", fmt.Sprintf("http://localhost:%d", etcdPort),
		"--advertise-client-urls", fmt.Sprintf("http://localhost:%d", etcdPort),
		"--listen-peer-urls", fmt.Sprintf("http://localhost:%d", etcdPeerPort)}

	log.Printf("Running `etcd %s`", strings.Join(etcdArgs, " "))

	cmd := exec.Command("etcd", etcdArgs...)
	cmd.Env = append(os.Environ(), "ETCD_UNSUPPORTED_ARCH=arm64")
	if err := cmd.Start(); err != nil {
		t.Fatalf("Could not run etcd: %v", err)
	}

	t.Cleanup(func() { cmd.Process.Kill() })

	log.Printf("Waiting for the etcd port localhost:%d to open", etcdPort)

	waitForPort(t, etcdPort, make(chan error, 1))

	return etcdPort
}

type tweaks struct {
	dbInitFn       func(t *testing.T, dbPath string)
	modifyInitArgs func(t *testing.T, a *InitArgs)
}

func runChukcha(t *testing.T, withReplica bool, w tweaks) (addrs []string, etcdAddr string) {
	t.Helper()

	log.SetFlags(log.Flags() | log.Lmicroseconds)

	port1, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}

	port2, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}

	dbPath := t.TempDir()

	// Chukcha creates empty chunks in background every 100ms,
	// so it is possible that the directory removal will fail
	// on the first try.
	t.Cleanup(func() {
		for i := 0; i < 10; i++ {
			if err := os.RemoveAll(dbPath); err == nil {
				return
			}
		}
	})

	if w.dbInitFn != nil {
		w.dbInitFn(t, dbPath)
	}

	etcdAddr = fmt.Sprintf("http://localhost:%d/", runEtcd(t))

	ports := []int{port1}
	if withReplica {
		ports = append(ports, port2)
	}

	for _, port := range ports {
		log.Printf("Running chukcha on port %d", port)

		dirName := dbPath
		instanceName := "moscow"

		// if it is a replica
		if port != port1 {
			instanceName = "voronezh"
			dirName = t.TempDir()
		}

		errCh := make(chan error, 1)
		go func(port int) {
			a := InitArgs{
				LogWriter:           log.Default().Writer(),
				EtcdAddr:            []string{etcdAddr},
				InstanceName:        instanceName,
				ClusterName:         "testRussia",
				DirName:             dirName,
				ListenAddr:          fmt.Sprintf("localhost:%d", port),
				MaxChunkSize:        20 * 1024 * 1024,
				RotateChunkInterval: 50 * time.Millisecond,
			}

			if w.modifyInitArgs != nil {
				w.modifyInitArgs(t, &a)
			}

			errCh <- InitAndServe(a)
		}(port)

		log.Printf("Waiting for the Chukcha port localhost:%d to open", port)
		waitForPort(t, port, make(chan error, 1))

		addrs = append(addrs, fmt.Sprintf("http://localhost:%d", port))
	}

	return addrs, etcdAddr
}

func simpleClientAndServerTest(t *testing.T, concurrent, withReplica bool) {
	t.Helper()

	ctx := context.Background()

	addrs, _ := runChukcha(t, withReplica, tweaks{
		dbInitFn: func(t *testing.T, dbPath string) {
			categoryPath := filepath.Join(dbPath, "numbers")
			os.MkdirAll(categoryPath, 0777)

			// Initialise the database contents with
			// a not easy-to-guess contents that must
			// be preserved when writing to this directory.
			ioutil.WriteFile(filepath.Join(categoryPath, fmt.Sprintf("moscow-chunk%09d", 1)), []byte("12345\n"), 0666)
		},
	})

	log.Printf("Starting the test")

	s := client.NewSimple(addrs)
	if os.Getenv("CHUKCHA_DEBUG") == "1" {
		s.SetDebug(true)
	}

	var want, got int64
	var err error

	// The contents of the chunk that already existed.
	alreadyWant := int64(12345)

	if concurrent {
		want, got, err = sendAndReceiveConcurrently(ctx, s, withReplica)
		if err != nil {
			t.Fatalf("sendAndReceiveConcurrently: %v", err)
		}

		want += alreadyWant
	} else {
		want, err = send(ctx, s)
		if err != nil {
			t.Fatalf("send error: %v", err)
		}

		want += alreadyWant

		sendFinishedCh := make(chan time.Time, 1)
		sendFinishedCh <- time.Now()
		got, err = receive(ctx, s, sendFinishedCh, withReplica, want)
		if err != nil {
			t.Fatalf("receive error: %v", err)
		}
	}

	if want != got {
		t.Errorf("the expected sum %d is not equal to the actual sum %d (delivered %1.f%%)", want, got, (float64(got)/float64(want))*100)
	}
}

func waitForPort(t *testing.T, port int, errCh chan error) {
	t.Helper()

	for i := 0; i <= 100; i++ {
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("InitAndServe failed: %v", err)
			}
		default:
		}

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

type sumAndErr struct {
	sum int64
	err error
}

// we must use the calculated constant because during concurrent
// send we don't yet know the actual sum during receive.
const expectedSumCalculatedManually = 50000005012345

func sendAndReceiveConcurrently(ctx context.Context, s *client.Simple, withReplica bool) (want, got int64, err error) {
	wantCh := make(chan sumAndErr, 1)
	gotCh := make(chan sumAndErr, 1)
	sendFinishedCh := make(chan time.Time, 1)

	go func() {
		want, err := send(ctx, s)
		log.Printf("Send finished")

		wantCh <- sumAndErr{
			sum: want,
			err: err,
		}
		sendFinishedCh <- time.Now()
	}()

	go func() {
		got, err := receive(ctx, s, sendFinishedCh, withReplica, expectedSumCalculatedManually)
		gotCh <- sumAndErr{
			sum: got,
			err: err,
		}
	}()

	wantRes := <-wantCh
	if wantRes.err != nil {
		return 0, 0, fmt.Errorf("send: %v", wantRes.err)
	}

	gotRes := <-gotCh
	if gotRes.err != nil {
		return 0, 0, fmt.Errorf("receive: %v", gotRes.err)
	}

	return wantRes.sum, gotRes.sum, err
}

func send(ctx context.Context, s *client.Simple) (sum int64, err error) {
	sendStart := time.Now()
	var networkTime time.Duration
	var sentBytes int

	defer func() {
		log.Printf(sendFmt, networkTime, time.Since(sendStart)-networkTime, float64(sentBytes)/1024/1024)
	}()

	buf := make([]byte, 0, maxBufferSize)

	for i := 0; i <= maxN; i++ {
		sum += int64(i)

		buf = strconv.AppendInt(buf, int64(i), 10)
		buf = append(buf, '\n')

		if len(buf) >= maxBufferSize {
			start := time.Now()
			if err := s.Send(ctx, "numbers", buf); err != nil {
				return 0, err
			}
			networkTime += time.Since(start)
			sentBytes += len(buf)

			buf = buf[0:0]
		}
	}

	if len(buf) != 0 {
		start := time.Now()
		if err := s.Send(ctx, "numbers", buf); err != nil {
			return 0, err
		}
		networkTime += time.Since(start)
		sentBytes += len(buf)
	}

	return sum, nil
}

var errRandomTemp = errors.New("a random temporary error occurred")

func receive(ctx context.Context, s *client.Simple, sendFinishedCh chan time.Time, withReplica bool, maxExpectedSum int64) (sum int64, err error) {
	buf := make([]byte, maxBufferSize)

	var parseTime time.Duration
	receiveStart := time.Now()
	defer func() {
		log.Printf(recvFmt, time.Since(receiveStart)-parseTime, parseTime)
	}()

	trimNL := func(r rune) bool { return r == '\n' }

	sendFinished := false
	var sendFinishedTs time.Time

	loopCnt := 0

	for {
		loopCnt++

		select {
		case ts := <-sendFinishedCh:
			log.Printf("Receive: got information that send finished")
			sendFinished = true
			sendFinishedTs = ts
		default:
		}

		err := s.Process(ctx, "numbers", buf, func(res []byte) error {
			if loopCnt%10 == 0 {
				return errRandomTemp
			}

			start := time.Now()

			ints := strings.Split(strings.TrimRightFunc(string(res), trimNL), "\n")
			for _, str := range ints {
				i, err := strconv.Atoi(str)
				if err != nil {
					return err
				}

				sum += int64(i)
			}

			parseTime += time.Since(start)
			return nil
		})

		log.Printf("Got sum %d, want sum %d", sum, maxExpectedSum)

		if errors.Is(err, errRandomTemp) {
			continue
		} else if err != nil {
			return 0, err
		}

		if sendFinished && (sum >= maxExpectedSum || time.Since(sendFinishedTs) >= 10*time.Second) {
			log.Printf("sum (%d) >= maxExpectedSum (%d) || time.Since(sendFinishedTs) (%s) >= 10*time.Second", sum, maxExpectedSum, time.Since(sendFinishedTs))
			return sum, nil
		}
	}
}

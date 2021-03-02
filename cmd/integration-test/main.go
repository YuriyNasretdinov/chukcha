package main

import (
	"fmt"
	"go/build"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/YuriyNasretdinov/chukcha/client"
)

const (
	maxN          = 10000000
	maxBufferSize = 1024 * 1024

	sendFmt = "Send: net %13s, cpu %13s (%.1f MiB)"
	recvFmt = "Recv: net %13s, cpu %13s"
)

func main() {
	log.SetFlags(log.Flags() | log.Lmicroseconds)

	goPath := os.Getenv("GOPATH")
	if goPath == "" {
		goPath = build.Default.GOPATH
	}

	log.Printf("Compiling chukcha")
	err := exec.Command("go", "install", "-v", "github.com/YuriyNasretdinov/chukcha").Run()
	if err != nil {
		log.Printf("Failed to build: %v", err)
	}

	// TODO: make port random
	port := 7537 // "test" in l33t

	// TODO: make db path random
	dbPath := "/tmp/chukcha.db"
	os.Remove(dbPath)

	log.Printf("Running chukcha on port %d", port)

	cmd := exec.Command(goPath+"/bin/chukcha", "-filename="+dbPath, fmt.Sprintf("-port=%d", port))
	cmd.Start()
	defer cmd.Process.Kill()

	log.Printf("Waiting for the port localhost:%d to open", port)
	for {
		timeout := time.Millisecond * 100
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("localhost", fmt.Sprint(port)), timeout)
		if err != nil {
			continue
		}
		conn.Close()
		break
	}

	log.Printf("Starting the test")

	s := client.NewSimple([]string{fmt.Sprintf("http://localhost:%d", port)})

	want, err := send(s)
	if err != nil {
		log.Fatalf("Send error: %v", err)
	}

	got, err := receive(s)
	if err != nil {
		log.Fatalf("Receive error: %v", err)
	}

	if want != got {
		log.Fatalf("The expected sum %d is not equal to the actual sum %d", want, got)
	}

	log.Printf("The test passed!")
}

func send(s *client.Simple) (sum int64, err error) {
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
			if err := s.Send(buf); err != nil {
				return 0, err
			}
			networkTime += time.Since(start)
			sentBytes += len(buf)

			buf = buf[0:0]
		}
	}

	if len(buf) != 0 {
		start := time.Now()
		if err := s.Send(buf); err != nil {
			return 0, err
		}
		networkTime += time.Since(start)
		sentBytes += len(buf)
	}

	return sum, nil
}

func receive(s *client.Simple) (sum int64, err error) {
	buf := make([]byte, maxBufferSize)

	var parseTime time.Duration
	receiveStart := time.Now()
	defer func() {
		log.Printf(recvFmt, time.Since(receiveStart)-parseTime, parseTime)
	}()

	trimNL := func(r rune) bool { return r == '\n' }

	for {
		res, err := s.Receive(buf)
		if err == io.EOF {
			return sum, nil
		} else if err != nil {
			return 0, err
		}

		start := time.Now()

		ints := strings.Split(strings.TrimRightFunc(string(res), trimNL), "\n")
		for _, str := range ints {
			i, err := strconv.Atoi(str)
			if err != nil {
				return 0, err
			}

			sum += int64(i)
		}

		parseTime += time.Since(start)
	}
}

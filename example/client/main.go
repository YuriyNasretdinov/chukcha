package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/YuriyNasretdinov/chukcha/client"
)

var categoryName = flag.String("category", "stdin", "The category being tested")
var debug = flag.Bool("debug", false, "Debug mode")
var ack = flag.Bool("ack", true, "Acknowledge reads")
var minSyncReplicas = flag.Uint("min-sync-replicas", 0, "How many replicas to wait when writing a message")
var addrsFlag = flag.String("addrs", "http://127.0.0.1:8080,http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083,http://127.0.0.1:8084", "List of Chukcha servers")

const simpleStateFilePath = "/tmp/simple-example-state-%s.json"

type readResult struct {
	ln  string
	err error
}

func main() {
	flag.Parse()

	rand.Seed(time.Now().UnixNano())
	ctx, cancel := context.WithCancel(context.Background())

	log.SetFlags(log.Flags() | log.Lmicroseconds)

	addrs := strings.Split(*addrsFlag, ",")

	cl := client.NewSimple(addrs)
	if buf, err := ioutil.ReadFile(fmt.Sprintf(simpleStateFilePath, *categoryName)); err == nil {
		if err := cl.RestoreSavedState(buf); err != nil {
			log.Printf("Could not restore saved client state: %v", err)
		}
	}

	cl.SetDebug(*debug)
	cl.SetMinSyncReplicas(*minSyncReplicas)
	cl.SetAcknowledge(*ack)

	fmt.Printf("Enter the messages into the prompt to send them to one of Chukcha replicas\n")

	go printContiniously(ctx, cl, *debug)

	rd := bufio.NewReader(os.Stdin)
	fmt.Printf("> ")

	sigCh := make(chan os.Signal, 5)
	sigChCopy := make(chan os.Signal, 5)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	readCh := make(chan readResult)
	go func() {
		for {
			ln, err := rd.ReadString('\n')
			readCh <- readResult{ln: ln, err: err}
		}
	}()

	go func() {
		s := <-sigCh
		cancel()
		sigChCopy <- s
	}()

	for {
		var ln string
		var err error

		select {
		case s := <-sigChCopy:
			log.Printf("Received signal %v", s)
			ln = ""
			err = io.EOF
		case r := <-readCh:
			ln = r.ln
			err = r.err
		}

		if err == io.EOF {
			saveState(cl)
			return
		} else if err != nil {
			log.Fatalf("Failed reading stdin: %v", err)
		}

		if !strings.HasSuffix(ln, "\n") {
			log.Fatalf("The line is incomplete: %q", ln)
		}

		if err := send(ctx, cl, ln); err != nil {
			log.Printf("Failed sending data to Chukcha: %v", err)
			fmt.Printf("(send unsuccessful)> ")
		} else {
			fmt.Printf("(send successful)> ")
		}

	}
}

func send(parentCtx context.Context, cl *client.Simple, ln string) error {
	ctx, cancel := context.WithTimeout(parentCtx, time.Minute+5*time.Second)
	defer cancel()

	start := time.Now()
	defer func() { log.Printf("Send() took %s", time.Since(start)) }()

	return cl.Send(ctx, *categoryName, []byte(ln))
}

func saveState(cl *client.Simple) {
	buf, err := cl.MarshalState()
	if err != nil {
		log.Printf("Failed marshalling client state: %v", err)
	} else {
		ioutil.WriteFile(fmt.Sprintf(simpleStateFilePath, *categoryName), buf, 0666)
	}
	fmt.Println("")
}

func process(parentCtx context.Context, cl *client.Simple, scratch []byte, cb func(b []byte) error) error {
	ctx, cancel := context.WithTimeout(parentCtx, 10*time.Second)
	defer cancel()

	start := time.Now()
	err := cl.Process(ctx, *categoryName, scratch, cb)
	if err == nil || (!strings.Contains(err.Error(), "context canceled") && !strings.Contains(err.Error(), "context deadline exceeded")) {
		log.Printf("Process() took %s (error %v)", time.Since(start), err)
	}

	return err
}

func printContiniously(ctx context.Context, cl *client.Simple, debug bool) {
	scratch := make([]byte, 1024*1024)

	for {
		err := process(ctx, cl, scratch, func(b []byte) error {
			fmt.Printf("\n")
			log.Printf("BATCH: %s", b)
			fmt.Printf("(invitation from Process)> ")
			return nil
		})

		if err != nil && !strings.Contains(err.Error(), "context canceled") && !strings.Contains(err.Error(), "context deadline exceeded") {
			log.Printf("Error processing batch: %v", err)
			time.Sleep(time.Second)
		}
	}
}

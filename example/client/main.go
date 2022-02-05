package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/YuriyNasretdinov/chukcha/client"
)

var categoryName = flag.String("category", "stdin", "The category being tested")
var debug = flag.Bool("debug", false, "Debug mode")
var minSyncReplicas = flag.Uint("min-sync-replicas", 0, "How many replicas to wait when writing a message")

const simpleStateFilePath = "/tmp/simple-example-state-%s.json"

type readResult struct {
	ln  string
	err error
}

func main() {
	flag.Parse()

	ctx := context.Background()

	addrs := []string{"http://127.0.0.1:8080", "http://127.0.0.1:8081"}

	cl := client.NewSimple(addrs)
	if buf, err := ioutil.ReadFile(fmt.Sprintf(simpleStateFilePath, *categoryName)); err == nil {
		if err := cl.RestoreSavedState(buf); err != nil {
			log.Printf("Could not restore saved client state: %v", err)
		}
	}

	cl.SetDebug(*debug)
	cl.SetMinSyncReplicas(*minSyncReplicas)

	fmt.Printf("Enter the messages into the prompt to send them to one of Chukcha replicas\n")

	go printContiniously(ctx, cl, *debug)

	rd := bufio.NewReader(os.Stdin)
	fmt.Printf("> ")

	sigCh := make(chan os.Signal, 5)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	readCh := make(chan readResult)
	go func() {
		for {
			ln, err := rd.ReadString('\n')
			readCh <- readResult{ln: ln, err: err}
		}
	}()

	for {
		var ln string
		var err error

		select {
		case s := <-sigCh:
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

		if err := cl.Send(ctx, *categoryName, []byte(ln)); err != nil {
			log.Printf("Failed sending data to Chukcha: %v", err)
		}

		fmt.Printf("(send successful)> ")
	}
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

func printContiniously(ctx context.Context, cl *client.Simple, debug bool) {
	scratch := make([]byte, 1024*1024)

	for {
		err := cl.Process(ctx, *categoryName, scratch, func(b []byte) error {
			fmt.Printf("\n")
			log.Printf("BATCH: %s", b)
			fmt.Printf("(invitation from Process)> ")
			return nil
		})

		if err != nil {
			log.Printf("Error processing batch: %v", err)
			time.Sleep(time.Second)
		}
	}
}

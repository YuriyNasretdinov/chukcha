package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/YuriyNasretdinov/chukcha/client"
)

const simpleStateFilePath = "/tmp/simple-example-state.json"

func main() {
	addrs := []string{"http://127.0.0.1:8080", "http://127.0.0.1:8081"}

	cl := client.NewSimple(addrs)
	if buf, err := ioutil.ReadFile(simpleStateFilePath); err == nil {
		if err := cl.RestoreSavedState(buf); err != nil {
			log.Printf("Could not restore saved client state: %v", err)
		}
	}

	// cl.Debug = true

	fmt.Printf("Enter the messages into the prompt to send them to one of Chukcha replicas\n")

	go printContiniously(cl)

	rd := bufio.NewReader(os.Stdin)
	fmt.Printf("> ")

	for {
		ln, err := rd.ReadString('\n')
		if err == io.EOF {
			buf, err := cl.MarshalState()
			if err != nil {
				log.Printf("Failed marshalling client state: %v", err)
			} else {
				ioutil.WriteFile(simpleStateFilePath, buf, 0666)
			}
			fmt.Println("")
			return
		} else if err != nil {
			log.Fatalf("Failed reading stdin: %v", err)
		}

		if !strings.HasSuffix(ln, "\n") {
			log.Fatalf("The line is incomplete: %q", ln)
		}

		if err := cl.Send("stdin", []byte(ln)); err != nil {
			log.Printf("Failed sending data to Chukcha: %v", err)
		}

		fmt.Printf("> ")
	}
}

func printContiniously(cl *client.Simple) {
	scratch := make([]byte, 1024*1024)

	for {
		cl.Process("stdin", scratch, func(b []byte) error {
			fmt.Printf("\n")
			log.Printf("BATCH: %s", b)
			fmt.Printf("> ")
			return nil
		})

		time.Sleep(time.Millisecond * 10000)
	}

}

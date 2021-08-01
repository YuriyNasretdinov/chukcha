package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/YuriyNasretdinov/chukcha/client"
)

func main() {
	addrs := []string{"http://127.0.0.1:8080", "http://127.0.0.1:8081"}

	cl := client.NewSimple(addrs)
	// cl.Debug = true

	fmt.Printf("Enter the messages into the prompt to send them to one of Chukcha replicas\n")

	go printContiniously(cl)

	rd := bufio.NewReader(os.Stdin)
	fmt.Printf("> ")

	for {
		ln, err := rd.ReadString('\n')
		if err != nil {
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

package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"

	"github.com/YuriyNasretdinov/chukcha/client"
)

const maxN = 10000000
const maxBufferSize = 1024 * 1024

func main() {
	s := client.NewSimple([]string{"http://localhost:8080"})

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
	var b bytes.Buffer

	for i := 0; i <= maxN; i++ {
		sum += int64(i)

		fmt.Fprintf(&b, "%d\n", i)

		if b.Len() >= maxBufferSize {
			if err := s.Send(b.Bytes()); err != nil {
				return 0, err
			}

			b.Reset()
		}
	}

	if b.Len() != 0 {
		if err := s.Send(b.Bytes()); err != nil {
			return 0, err
		}
	}

	return sum, nil
}

func receive(s *client.Simple) (sum int64, err error) {
	buf := make([]byte, maxBufferSize)

	for {
		res, err := s.Receive(buf)
		if err == io.EOF {
			return sum, nil
		} else if err != nil {
			return 0, err
		}

		ints := strings.Split(string(res), "\n")
		for _, str := range ints {
			if str == "" {
				continue
			}

			i, err := strconv.Atoi(str)
			if err != nil {
				return 0, err
			}

			sum += int64(i)
		}
	}
}

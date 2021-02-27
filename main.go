package main

import (
	"flag"
	"log"
	"os"

	"github.com/YuriyNasretdinov/chukcha/server"
	"github.com/YuriyNasretdinov/chukcha/web"
)

var (
	filename = flag.String("filename", "", "The filename where to put all the data")
	inmem    = flag.Bool("inmem", false, "Whether or not use in-memory storage instead of a disk-based one")
)

func main() {
	flag.Parse()

	var backend web.Storage

	if *inmem {
		backend = &server.InMemory{}
	} else {
		if *filename == "" {
			log.Fatalf("The flag `--filename` must be provided")
		}

		fp, err := os.OpenFile(*filename, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			log.Fatalf("Could not create file %q: %s", *filename, err)
		}
		defer fp.Close()

		backend = server.NewOnDisk(fp)
	}

	s := web.NewServer(backend)

	log.Printf("Listening connections")
	s.Serve()
}

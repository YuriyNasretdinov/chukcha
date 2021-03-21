package main

import (
	"flag"
	"log"

	"github.com/YuriyNasretdinov/chukcha/integration"
)

var (
	dirname = flag.String("dirname", "", "The directory name where to put all the data")
	port    = flag.Uint("port", 8080, "Network port to listen on")
)

func main() {
	flag.Parse()

	if *dirname == "" {
		log.Fatalf("The flag `--dirname` must be provided")
	}

	if err := integration.InitAndServe(*dirname, *port); err != nil {
		log.Fatalf("InitAndServe failed: %v", err)
	}
}

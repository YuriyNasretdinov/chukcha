package main

import (
	"flag"
	"log"

	"github.com/YuriyNasretdinov/chukcha/integration"
)

var (
	dirname  = flag.String("dirname", "", "The directory name where to put all the data")
	port     = flag.Uint("port", 8080, "Network port to listen on")
	etcdAddr = flag.String("etcd", "http://127.0.0.1:2379", "The network address of etcd server(s)")
)

func main() {
	flag.Parse()

	if *dirname == "" {
		log.Fatalf("The flag `--dirname` must be provided")
	}

	if *etcdAddr == "" {
		log.Fatalf("The flag `--etcd` must be provided")
	}

	if err := integration.InitAndServe(*etcdAddr, *dirname, *port); err != nil {
		log.Fatalf("InitAndServe failed: %v", err)
	}
}

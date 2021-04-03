package main

import (
	"flag"
	"log"

	"github.com/YuriyNasretdinov/chukcha/integration"
)

var (
	instanceName = flag.String("instance-name", "", "The unique instance name")
	dirname      = flag.String("dirname", "", "The directory name where to put all the data")
	listenAddr   = flag.String("listen", "127.0.0.1:8080", "Network adddress to listen on")
	etcdAddr     = flag.String("etcd", "http://127.0.0.1:2379", "The network address of etcd server(s)")
)

func main() {
	flag.Parse()

	if *instanceName == "" {
		log.Fatalf("The flag `--instance-name` must be provided")
	}

	if *dirname == "" {
		log.Fatalf("The flag `--dirname` must be provided")
	}

	if *etcdAddr == "" {
		log.Fatalf("The flag `--etcd` must be provided")
	}

	if err := integration.InitAndServe(*etcdAddr, *instanceName, *dirname, *listenAddr); err != nil {
		log.Fatalf("InitAndServe failed: %v", err)
	}
}

package main

import (
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/YuriyNasretdinov/chukcha/integration"
)

var (
	clusterName  = flag.String("cluster", "default", "The name of the cluster (must specify if sharing a single etcd instance with several Chukcha instances)")
	instanceName = flag.String("instance", "", "The unique instance name (e.g. chukcha1)")
	dirname      = flag.String("dirname", "", "The directory name where to put all the data")
	listenAddr   = flag.String("listen", "127.0.0.1:8080", "Network adddress to listen on")
	etcdAddr     = flag.String("etcd", "http://127.0.0.1:2379", "The network address of etcd server(s)")

	maxChunkSize        = flag.Uint64("max-chunk-size", 20*1024*1024, "maximum size of chunks stored")
	rotateChunkInterval = flag.Duration("rotate-chunk-interval", 10*time.Minute, "how often to create new chunks regardless of whether or not maximum chunk size was reached (useful to reclaim space)")
)

func main() {
	flag.Parse()

	if *clusterName == "" {
		log.Fatalf("The flag `--cluster` must not be empty")
	}

	if *instanceName == "" {
		log.Fatalf("The flag `--instance` must be provided")
	}

	if *dirname == "" {
		log.Fatalf("The flag `--dirname` must be provided")
	}

	if *etcdAddr == "" {
		log.Fatalf("The flag `--etcd` must be provided")
	}

	a := integration.InitArgs{
		LogWriter:           os.Stderr,
		EtcdAddr:            strings.Split(*etcdAddr, ","),
		ClusterName:         *clusterName,
		InstanceName:        *instanceName,
		DirName:             *dirname,
		ListenAddr:          *listenAddr,
		MaxChunkSize:        *maxChunkSize,
		RotateChunkInterval: *rotateChunkInterval,
	}

	if err := integration.InitAndServe(a); err != nil {
		log.Fatalf("InitAndServe failed: %v", err)
	}
}

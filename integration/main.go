package integration

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/YuriyNasretdinov/chukcha/server/replication"
	"github.com/YuriyNasretdinov/chukcha/web"
	"go.etcd.io/etcd/clientv3"
)

// InitAndServe checks validity of the supplied arguments and starts
// the web server on the specified port.
func InitAndServe(etcdAddr string, instanceName string, dirname string, listenAddr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(etcdAddr, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("creating new client: %w", err)
	}
	defer etcdClient.Close()

	_, err = etcdClient.Put(ctx, "test", "test")
	if err != nil {
		return fmt.Errorf("could not set the test key: %w", err)
	}

	_, err = etcdClient.Put(ctx, "peers/"+instanceName, listenAddr)
	if err != nil {
		return fmt.Errorf("could not register peer address in etcd: %w", err)
	}

	filename := filepath.Join(dirname, "write_test")
	fp, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("creating test file %q: %s", filename, err)
	}
	fp.Close()
	os.Remove(fp.Name())

	s := web.NewServer(etcdClient, instanceName, dirname, listenAddr, replication.NewStorage(etcdClient, instanceName))

	log.Printf("Listening connections")
	return s.Serve()
}

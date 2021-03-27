package integration

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/YuriyNasretdinov/chukcha/web"
	"go.etcd.io/etcd/client"
)

// InitAndServe checks validity of the supplied arguments and starts
// the web server on the specified port.
func InitAndServe(etcdAddr string, dirname string, port uint) error {
	// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	// defer cancel()

	cfg := client.Config{
		Endpoints:               strings.Split(etcdAddr, ","),
		Transport:               client.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}
	c, err := client.New(cfg)
	if err != nil {
		return fmt.Errorf("creating etcd client: %w", err)
	}
	kapi := client.NewKeysAPI(c)

	/*
		_, err = kapi.Set(ctx, "test", `test`, nil)
		if err != nil {
			return fmt.Errorf("could not set test key to etcd: %v", err)
		}
	*/

	filename := filepath.Join(dirname, "write_test")
	fp, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("creating test file %q: %s", filename, err)
	}
	fp.Close()
	os.Remove(fp.Name())

	s := web.NewServer(kapi, dirname, port)

	log.Printf("Listening connections")
	return s.Serve()
}

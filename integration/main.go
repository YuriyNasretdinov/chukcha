package integration

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/YuriyNasretdinov/chukcha/replication"
	"github.com/YuriyNasretdinov/chukcha/server"
	"github.com/YuriyNasretdinov/chukcha/web"
)

type InitArgs struct {
	LogWriter io.Writer
	Peers     []replication.Peer

	InstanceName string

	DirName    string
	ListenAddr string

	MaxChunkSize        uint64
	RotateChunkInterval time.Duration

	PProfAddr string

	// The next set of parameters is only set in tests.
	DisableAcknowledge bool
}

// InitAndServe checks validity of the supplied arguments and starts
// the web server on the specified port.
func InitAndServe(a InitArgs) error {
	logger := log.New(a.LogWriter, "["+a.InstanceName+"] ", log.LstdFlags|log.Lmicroseconds)

	filename := filepath.Join(a.DirName, "running.lock")
	fp, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("creating test file %q: %s", filename, err)
	}
	defer fp.Close()

	if err := syscall.Flock(int(fp.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("getting file lock for file %q failed, another instance is running? (error: %v)", filename, err)
	}

	if a.PProfAddr != "" {
		go func() {
			logger.Printf("Starting pprof server on %q", a.PProfAddr)
			logger.Println(http.ListenAndServe(a.PProfAddr, nil))
		}()
	}

	replStorage := replication.NewStorage(logger, a.InstanceName)

	creator := server.NewOnDiskCreator(logger, a.DirName, a.InstanceName, replStorage, a.MaxChunkSize, a.RotateChunkInterval)

	s := web.NewServer(logger, a.InstanceName, a.DirName, a.ListenAddr, replStorage, creator.Get)

	replClient := replication.NewClient(logger, a.DirName, creator, a.Peers, a.InstanceName)
	go replClient.Loop(context.Background())

	logger.Printf("Listening connections at %q", a.ListenAddr)
	return s.Serve()
}

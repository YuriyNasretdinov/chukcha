package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/YuriyNasretdinov/chukcha/server"
	"github.com/YuriyNasretdinov/chukcha/server/replication"
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

	creator := &OnDiskCreator{
		logger:              logger,
		dirName:             a.DirName,
		instanceName:        a.InstanceName,
		storages:            make(map[string]*server.OnDisk),
		maxChunkSize:        a.MaxChunkSize,
		rotateChunkInterval: a.RotateChunkInterval,
	}
	replStorage := replication.NewStorage(logger, creator, a.InstanceName)
	creator.replStorage = replStorage

	s := web.NewServer(logger, a.InstanceName, a.DirName, a.ListenAddr, replStorage, creator.Get)

	replClient := replication.NewClient(logger, a.DirName, creator, a.Peers, a.InstanceName)
	go replClient.Loop(context.Background(), a.DisableAcknowledge)

	logger.Printf("Listening connections at %q", a.ListenAddr)
	return s.Serve()
}

type OnDiskCreator struct {
	logger              *log.Logger
	dirName             string
	instanceName        string
	replStorage         *replication.Storage
	maxChunkSize        uint64
	rotateChunkInterval time.Duration

	m        sync.Mutex
	storages map[string]*server.OnDisk
}

// Stat returns information about the chunk: whether or not it exists and it's size.
// If file does not exist no error is returned.
func (c *OnDiskCreator) Stat(category string, fileName string) (size int64, exists bool, deleted bool, err error) {
	filePath := filepath.Join(c.dirName, category, fileName)

	st, err := os.Stat(filePath)
	if errors.Is(err, os.ErrNotExist) {
		_, deletedErr := os.Stat(filePath + server.DeletedSuffix)
		if errors.Is(deletedErr, os.ErrNotExist) {
			return 0, false, false, nil
		} else if deletedErr != nil {
			return 0, false, false, deletedErr
		}

		return 0, false, true, nil
	} else if err != nil {
		return 0, false, false, err
	}

	return st.Size(), true, false, nil
}

func (c *OnDiskCreator) WriteDirect(category string, fileName string, contents []byte) error {
	inst, err := c.Get(category)
	if err != nil {
		return err
	}

	return inst.WriteDirect(fileName, contents)
}

func (c *OnDiskCreator) SetReplicationDisabled(category string, v bool) error {
	inst, err := c.Get(category)
	if err != nil {
		return err
	}

	inst.SetReplicationDisabled(v)
	return nil
}

func (c *OnDiskCreator) Write(ctx context.Context, category string, msgs []byte) (chunkName string, off int64, err error) {
	inst, err := c.Get(category)
	if err != nil {
		return "", 0, err
	}

	return inst.Write(ctx, msgs)
}

func (c *OnDiskCreator) AckDirect(ctx context.Context, category string, chunk string) error {
	inst, err := c.Get(category)
	if err != nil {
		return err
	}

	return inst.AckDirect(chunk)
}

func (c *OnDiskCreator) Get(category string) (*server.OnDisk, error) {
	c.m.Lock()
	defer c.m.Unlock()

	storage, ok := c.storages[category]
	if ok {
		return storage, nil
	}

	storage, err := c.newOnDisk(c.logger, category)
	if err != nil {
		return nil, err
	}

	c.storages[category] = storage
	return storage, nil
}

func (c *OnDiskCreator) newOnDisk(logger *log.Logger, category string) (*server.OnDisk, error) {
	dir := filepath.Join(c.dirName, category)
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, fmt.Errorf("creating directory for the category: %v", err)
	}

	return server.NewOnDisk(logger, dir, category, c.instanceName, c.maxChunkSize, replication.BatchSize-1, c.rotateChunkInterval, c.replStorage)
}

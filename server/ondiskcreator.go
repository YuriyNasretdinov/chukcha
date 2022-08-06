package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/YuriyNasretdinov/chukcha/replication"
)

func NewOnDiskCreator(logger *log.Logger, dirName string, instanceName string, replStorage *replication.Storage, maxChunkSize uint64, rotateChunkInterval time.Duration) *OnDiskCreator {
	return &OnDiskCreator{
		logger:              logger,
		dirName:             dirName,
		instanceName:        instanceName,
		storages:            make(map[string]*OnDisk),
		maxChunkSize:        maxChunkSize,
		rotateChunkInterval: rotateChunkInterval,
		replStorage:         replStorage,
	}
}

type OnDiskCreator struct {
	logger              *log.Logger
	dirName             string
	instanceName        string
	replStorage         *replication.Storage
	maxChunkSize        uint64
	rotateChunkInterval time.Duration

	m        sync.Mutex
	storages map[string]*OnDisk
}

// Stat returns information about the chunk: whether or not it exists and it's size.
// If file does not exist no error is returned.
func (c *OnDiskCreator) Stat(category string, fileName string) (size int64, exists bool, deleted bool, err error) {
	filePath := filepath.Join(c.dirName, category, fileName)

	st, err := os.Stat(filePath)
	if errors.Is(err, os.ErrNotExist) {
		_, deletedErr := os.Stat(filePath + AcknowledgedSuffix)
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

func (c *OnDiskCreator) Get(category string) (*OnDisk, error) {
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

func (c *OnDiskCreator) newOnDisk(logger *log.Logger, category string) (*OnDisk, error) {
	dir := filepath.Join(c.dirName, category)
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, fmt.Errorf("creating directory for the category: %v", err)
	}

	return NewOnDisk(logger, dir, category, c.instanceName, c.maxChunkSize, replication.BatchSize-1, c.rotateChunkInterval, c.replStorage)
}

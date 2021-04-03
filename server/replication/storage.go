package replication

import (
	"context"
	"fmt"
	"strings"

	"go.etcd.io/etcd/clientv3"
)

// Storage provides hooks for the ondisk storage that will be called to
// ensure that chunks are replicated.
type Storage struct {
	client          *clientv3.Client
	currentInstance string
}

func NewStorage(client *clientv3.Client, currentInstance string) *Storage {
	return &Storage{
		client:          client,
		currentInstance: currentInstance,
	}
}

func (s *Storage) BeforeCreatingChunk(ctx context.Context, category string, fileName string) error {
	resp, err := s.client.Get(ctx, "peers/", clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("getting peers from etcd: %v", err)
	}

	for _, kv := range resp.Kvs {
		key := strings.TrimPrefix(string(kv.Key), "peers/")
		if key == s.currentInstance {
			continue
		}

		_, err = s.client.Put(ctx, "replication/"+key+"/"+category+"/"+fileName, s.currentInstance)
		if err != nil {
			return fmt.Errorf("could not write to replication queue for %q (%q): %w", key, string(kv.Value), err)
		}
	}

	return nil
}

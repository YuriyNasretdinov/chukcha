package replication

import (
	"context"
	"fmt"
)

// Storage provides hooks for the ondisk storage that will be called to
// ensure that chunks are replicated.
type Storage struct {
	client          *State
	currentInstance string
}

func NewStorage(client *State, currentInstance string) *Storage {
	return &Storage{
		client:          client,
		currentInstance: currentInstance,
	}
}

func (s *Storage) BeforeCreatingChunk(ctx context.Context, category string, fileName string) error {
	peers, err := s.client.ListPeers(ctx)
	if err != nil {
		return fmt.Errorf("getting peers from etcd: %v", err)
	}

	for _, p := range peers {
		if p.InstanceName == s.currentInstance {
			continue
		}

		if err := s.client.AddChunkToReplicationQueue(ctx, p.InstanceName, Chunk{
			Owner:    s.currentInstance,
			Category: category,
			FileName: fileName,
		}); err != nil {
			return fmt.Errorf("could not write to replication queue for %q (%q): %w", p.InstanceName, p.ListenAddr, err)
		}
	}

	return nil
}

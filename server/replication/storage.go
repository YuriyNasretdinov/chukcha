package replication

import (
	"context"
	"fmt"
	"log"
)

// Storage provides hooks for the ondisk storage that will be called to
// ensure that chunks are replicated.
type Storage struct {
	logger          *log.Logger
	client          *State
	currentInstance string
}

func NewStorage(logger *log.Logger, client *State, currentInstance string) *Storage {
	return &Storage{
		logger:          logger,
		client:          client,
		currentInstance: currentInstance,
	}
}

func (s *Storage) AfterCreatingChunk(ctx context.Context, category string, fileName string) error {
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

func (s *Storage) AfterAcknowledgeChunk(ctx context.Context, category string, fileName string) error {
	peers, err := s.client.ListPeers(ctx)
	if err != nil {
		return fmt.Errorf("getting peers from etcd: %v", err)
	}

	for _, p := range peers {
		if p.InstanceName == s.currentInstance {
			continue
		}

		if err := s.client.AddChunkToAcknowledgeQueue(ctx, p.InstanceName, Chunk{
			Owner:    s.currentInstance,
			Category: category,
			FileName: fileName,
		}); err != nil {
			return fmt.Errorf("could not write to replication queue for %q (%q): %w", p.InstanceName, p.ListenAddr, err)
		}
	}

	return nil
}

package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
)

const systemReplication = "_system-replication"
const systemAck = "_system-ack"

// Storage provides hooks for the ondisk storage that will be called to
// ensure that chunks are replicated.
type Storage struct {
	logger          *log.Logger
	dw              DirectWriter
	client          *State
	currentInstance string
}

func NewStorage(logger *log.Logger, dw DirectWriter, client *State, currentInstance string) *Storage {
	return &Storage{
		logger:          logger,
		dw:              dw,
		client:          client,
		currentInstance: currentInstance,
	}
}

func (s *Storage) AfterCreatingChunk(ctx context.Context, category string, fileName string) error {
	if err := s.dw.SetReplicationDisabled(systemReplication, true); err != nil {
		return fmt.Errorf("setting replication disabled: %v", err)
	}

	ch := Chunk{
		Owner:    s.currentInstance,
		Category: category,
		FileName: fileName,
	}

	buf, err := json.Marshal(&ch)
	if err != nil {
		return fmt.Errorf("marshalling chunk: %v", err)
	}
	buf = append(buf, '\n')

	if _, _, err := s.dw.Write(ctx, systemReplication, buf); err != nil {
		return fmt.Errorf("writing chunk to system replication category: %v", err)
	}

	return nil
}

func (s *Storage) AfterAcknowledgeChunk(ctx context.Context, category string, fileName string) error {
	peers, err := s.client.ListPeers(ctx)
	if err != nil {
		return fmt.Errorf("getting peers from etcd: %v", err)
	}

	ch := Chunk{
		Owner:    s.currentInstance,
		Category: category,
		FileName: fileName,
	}

	buf, err := json.Marshal(&ch)
	if err != nil {
		return fmt.Errorf("marshalling chunk: %v", err)
	}
	buf = append(buf, '\n')

	if _, _, err := s.dw.Write(ctx, systemAck, buf); err != nil {
		return fmt.Errorf("writing chunk to system ack category: %v", err)
	}

	for _, p := range peers {
		if p.InstanceName == s.currentInstance {
			continue
		}

		if err := s.client.AddChunkToAcknowledgeQueue(ctx, p.InstanceName, ch); err != nil {
			return fmt.Errorf("could not write to replication queue for %q (%q): %w", p.InstanceName, p.ListenAddr, err)
		}
	}

	return nil
}

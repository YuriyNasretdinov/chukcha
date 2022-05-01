package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
)

const systemCategoryPrefix = "_system-"
const systemReplication = systemCategoryPrefix + "replication"
const systemAck = systemCategoryPrefix + "ack"

// Storage provides hooks for the ondisk storage that will be called to
// ensure that chunks are replicated.
type Storage struct {
	logger          *log.Logger
	dw              DirectWriter
	currentInstance string

	mu                sync.Mutex
	connectedReplicas map[string]chan Chunk
}

func NewStorage(logger *log.Logger, dw DirectWriter, currentInstance string) *Storage {
	return &Storage{
		logger:            logger,
		dw:                dw,
		currentInstance:   currentInstance,
		connectedReplicas: make(map[string]chan Chunk),
	}
}

func (s *Storage) RegisterReplica(instanceName string, ch chan Chunk) {
	s.mu.Lock()
	s.connectedReplicas[instanceName] = ch
	s.mu.Unlock()

	select {
	case ch <- Chunk{}:
	default:
	}
}

func (s *Storage) AfterCreatingChunk(ctx context.Context, category string, fileName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	chunk := Chunk{
		Owner:    s.currentInstance,
		Category: category,
		FileName: fileName,
	}

	for instanceName, ch := range s.connectedReplicas {
		select {
		case ch <- chunk:
		default:
			delete(s.connectedReplicas, instanceName)
			close(ch)
		}
	}
}

func (s *Storage) AfterAcknowledgeChunk(ctx context.Context, category string, fileName string) error {
	if err := s.dw.SetReplicationDisabled(systemAck, true); err != nil {
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

	if _, _, err := s.dw.Write(ctx, systemAck, buf); err != nil {
		return fmt.Errorf("writing chunk to system ack category: %v", err)
	}

	return nil
}

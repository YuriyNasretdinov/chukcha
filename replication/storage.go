package replication

import (
	"context"
	"log"
	"sync"
)

// Storage provides hooks for the ondisk storage that will be called to
// ensure that chunks are replicated.
type Storage struct {
	logger          *log.Logger
	currentInstance string

	mu                sync.Mutex
	connectedReplicas map[string]chan Message
}

func NewStorage(logger *log.Logger, currentInstance string) *Storage {
	return &Storage{
		logger:            logger,
		currentInstance:   currentInstance,
		connectedReplicas: make(map[string]chan Message),
	}
}

func (s *Storage) RegisterReplica(instanceName string, ch chan Message) {
	s.mu.Lock()
	// nobody will ever write to this channel again so we must close it
	// to prevent goroutine leaks
	if oldCh, ok := s.connectedReplicas[instanceName]; ok {
		close(oldCh)
	}
	s.connectedReplicas[instanceName] = ch
	s.mu.Unlock()

	select {
	case ch <- Message{}:
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

	msg := Message{
		Type:  ChunkCreated,
		Chunk: chunk,
	}

	for instanceName, ch := range s.connectedReplicas {
		select {
		case ch <- msg:
		default:
			delete(s.connectedReplicas, instanceName)
			close(ch)
		}
	}
}

func (s *Storage) AfterAcknowledgeChunk(ctx context.Context, category string, fileName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	chunk := Chunk{
		Owner:    s.currentInstance,
		Category: category,
		FileName: fileName,
	}

	msg := Message{
		Type:  ChunkAcknowledged,
		Chunk: chunk,
	}

	for instanceName, ch := range s.connectedReplicas {
		select {
		case ch <- msg:
		default:
			delete(s.connectedReplicas, instanceName)
			close(ch)
		}
	}
}

package client

import (
	"testing"

	"github.com/YuriyNasretdinov/chukcha/protocol"
)

func TestUpdateCurrentChunkInfo(t *testing.T) {
	chunks := []protocol.Chunk{
		{Name: "moscow-chunk000000010", Complete: false, Size: 0},
		{Name: "voronezh-chunk000000000", Complete: true, Size: 5242896},
		{Name: "voronezh-chunk000000001", Complete: true, Size: 5242880},
		{Name: "voronezh-chunk000000002", Complete: true, Size: 5242880},
		{Name: "voronezh-chunk000000003", Complete: true, Size: 8388608},
		{Name: "voronezh-chunk000000004", Complete: true, Size: 4194304},
		{Name: "voronezh-chunk000000005", Complete: true, Size: 4194304},
		{Name: "voronezh-chunk000000006", Complete: true, Size: 6291456},
		{Name: "voronezh-chunk000000007", Complete: false, Size: 3145728},
	}

	off := &ReadOffset{
		CurChunk:          protocol.Chunk{Name: "voronezh-chunk000000000", Complete: false, Size: 5242882},
		LastAckedChunkIdx: 0,
		Off:               5242896,
	}

	s := NewSimple(nil)

	s.updateCurrentChunkInfo(chunks, off)

	if off.CurChunk.Complete != true {
		t.Errorf("Updating current chunk info did not update Complete status")
	}
}

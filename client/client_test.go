package client

import (
	"reflect"
	"testing"

	"github.com/YuriyNasretdinov/chukcha/protocol"
)

func TestSimpleMarshalUnmarshal(t *testing.T) {
	st := &state{
		Offsets: map[string]*ReadOffset{
			"Moscow": {
				CurChunk: protocol.Chunk{
					Name:     "Moscow-test000001",
					Complete: true,
					Size:     123456,
				},
				Off: 123,
			},
			"Voronezh": {
				CurChunk: protocol.Chunk{
					Name:     "Voronezh-test000001",
					Complete: true,
					Size:     345678,
				},
				Off: 345,
			},
		},
	}

	cl := NewSimple([]string{"http://localhost"})
	cl.st = st

	buf, err := cl.MarshalState()
	if err != nil {
		t.Errorf("MarshalState() = ..., %v; want no errors", err)
	}

	if l := len(buf); l <= 2 {
		t.Errorf("MarshalState() = byte slice of length %d, ...; want length to be at least 3", l)
	}

	cl = NewSimple([]string{"http://localhost"})
	err = cl.RestoreSavedState(buf)

	if err != nil {
		t.Errorf("RestoreSavedState(...) = %v; want no errors", err)
	}

	if !reflect.DeepEqual(st, cl.st) {
		t.Errorf("RestoreSavedState() restored %+v; want %+v", cl.st, st)
	}
}

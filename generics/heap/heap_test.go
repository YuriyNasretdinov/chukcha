package heap_test

import (
	"reflect"
	"testing"

	"github.com/YuriyNasretdinov/chukcha/generics/heap"
)

type someElement int

func (s someElement) Less(x someElement) bool {
	return s < x
}

func TestHopefullyMinHeap(t *testing.T) {
	h := heap.NewMin[someElement]()

	h.Push(3)
	h.Push(5)
	h.Push(10)
	h.Push(2)

	want := []someElement{2, 3, 5, 10}
	got := []someElement{h.Pop(), h.Pop(), h.Pop(), h.Pop()}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("[h.Pop(), ..., h.Pop()] = %v; want %v", got, want)
	}
}

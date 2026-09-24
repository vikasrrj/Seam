package reconcile

import (
	"testing"

	"example.com/seam/internal/model"
)

func TestWindowTouchesAreRangeBounded(t *testing.T) {
	w := &window{chunk: model.ChunkRange{Min: 10, Max: 20}, maxEvictedKeys: 1}
	if err := w.evictLocked(999); err != nil || len(w.evicted) != 0 {
		t.Fatalf("out-of-range touch retained: %v, %v", w.evicted, err)
	}
	if err := w.evictLocked(11); err != nil {
		t.Fatal(err)
	}
	if err := w.evictLocked(11); err != nil {
		t.Fatal("repeat touch should not consume budget:", err)
	}
	if err := w.evictLocked(12); err == nil {
		t.Fatal("unbounded touched-key set accepted")
	}
}

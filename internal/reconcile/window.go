package reconcile

import (
	"fmt"
	"sync"

	"example.com/seam/internal/kafka"
	"example.com/seam/internal/model"
)

// window is the mutable state of one reconciliation window: the primary-key
// chunk being backfilled, its snapshot candidates, and its position in the
// LOW/HIGH marker state machine.
//
// The sequential reconciler operates on one window at a time. In parallel mode
// each in-flight chunk owns a distinct window, so overlapping windows on the
// shared CDC stream never share mutable state.
type window struct {
	mu         sync.Mutex
	chunk      model.ChunkRange
	candidates map[int64]model.Row
	state      WindowState
	staged     bool
	// candidateCount is retained for metrics after candidates move to durable stage.
	candidateCount int

	// Coordinator-only fields (unused by the sequential path).
	durable         *model.Chunk       // the leased chunk being executed
	ready           bool               // candidates have been delivered by the worker
	highSeen        bool               // HIGH marker for this window has been consumed
	highGroup       []kafka.Record     // the source transaction group carrying HIGH
	highTransaction *kafka.Transaction // disk-backed source transaction carrying HIGH
	highWindows     []*window          // windows inside when HIGH's transaction arrived
	evicted         map[int64]struct{} // keys touched in-window before delivery
	maxEvictedKeys  int
	readyCh         chan struct{} // closed when candidates have been delivered
	doneCh          chan struct{} // closed after the destination commits the chunk
}

// evictLocked records an in-window eviction. If candidates have already been
// delivered the key is removed immediately; otherwise it is held until
// delivery so a change is never lost to a snapshot that arrives later.
// The caller must hold w.mu.
func (w *window) evictLocked(key int64) error {
	if key < w.chunk.Min || key > w.chunk.Max {
		return nil
	}
	if w.candidates != nil {
		delete(w.candidates, key)
		return nil
	}
	if w.evicted == nil {
		w.evicted = make(map[int64]struct{})
	}
	if _, present := w.evicted[key]; !present && w.maxEvictedKeys > 0 && len(w.evicted) >= w.maxEvictedKeys {
		return fmt.Errorf("window %s exceeded %d distinct touched keys before candidate delivery", w.chunk, w.maxEvictedKeys)
	}
	w.evicted[key] = struct{}{}
	return nil
}

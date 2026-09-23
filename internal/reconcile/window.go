package reconcile

import (
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
	candidates map[int64]model.Account
	state      WindowState

	// Coordinator-only fields (unused by the sequential path).
	durable   *model.Chunk      // the leased chunk being executed
	ready     bool              // candidates have been delivered by the worker
	highSeen  bool              // HIGH marker for this window has been consumed
	highGroup []kafka.Record    // the source transaction group carrying HIGH
	evicted   map[int64]struct{} // keys touched in-window before delivery
}

// evictLocked records an in-window eviction. If candidates have already been
// delivered the key is removed immediately; otherwise it is held until
// delivery so a change is never lost to a snapshot that arrives later.
// The caller must hold w.mu.
func (w *window) evictLocked(key int64) {
	if w.candidates != nil {
		delete(w.candidates, key)
		return
	}
	if w.evicted == nil {
		w.evicted = make(map[int64]struct{})
	}
	w.evicted[key] = struct{}{}
}
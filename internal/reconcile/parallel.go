package reconcile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"example.com/seam/internal/kafka"
	"example.com/seam/internal/model"
)

// runParallelWorkers runs the durable chunk store with a pool of concurrent
// chunk workers coordinated by a single CDC owner.
//
// Only the coordinator consumes Kafka: a partition has one offset owner, and
// the job has one checkpoint row. Workers lease distinct chunks, write
// LOW/HIGH markers onto the source (they flow into the same CDC stream), scan
// their snapshot, and hand the candidates to their window. The coordinator
// routes the single stream to every open window, evicts touched candidates,
// and commits chunk completions in chunk order so completed_through stays
// monotonic. The expensive part (snapshot scanning) runs in parallel; commit
// serialization is inherent to the single checkpoint.
func (r *Reconciler) runParallelWorkers(ctx context.Context) error {
	workers := r.cfg.Workers
	if workers < 1 {
		workers = 1
	}
	c := newCoordinator(ctx, r, workers)
	log.Printf("seam: parallel mode workers=%d job=%s", workers, r.cfg.JobID)

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	workerErrs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			err := c.workerLoop(wctx, id)
			c.workerDone(err)
			workerErrs <- err
		}(workerIDFor(r.cfg.WorkerID, i))
	}

	runErr := c.run(ctx)
	cancel() // stop workers once the coordinator has finished or failed
	wg.Wait()
	close(workerErrs)

	if runErr != nil && !isBenignError(runErr) {
		return runErr
	}
	for err := range workerErrs {
		if err != nil && !isBenignError(err) {
			return err
		}
	}
	return nil
}

func workerIDFor(base string, i int) string {
	if base == "" {
		return fmt.Sprintf("worker-%d", i)
	}
	return fmt.Sprintf("%s-%d", base, i)
}

func isBenignError(err error) bool {
	if err == nil {
		return true
	}
	return errors.Is(err, io.EOF) || errors.Is(err, context.Canceled)
}

// coord owns the single consumer and the checkpoint state machine for a
// parallel run. All mutations of the shared checkpoint happen on the
// coordinator goroutine; workers never touch the checkpoint.
type coord struct {
	r       *Reconciler
	workers int
	attempt string

	consumeCtx   context.Context
	consumerStop context.CancelFunc

	mu          sync.Mutex
	windows     map[int64]*window  // chunk.Min -> open window
	queue       map[int64]*window  // chunk.Min -> ready window waiting for in-order commit
	workersLeft int
	fatal       error

	batches   chan []kafka.Record
	deliverCh chan *window
	errCh     chan error
}

func newCoordinator(ctx context.Context, r *Reconciler, workers int) *coord {
	consumeCtx, consumerStop := context.WithCancel(ctx)
	return &coord{
		r:            r,
		workers:      workers,
		attempt:      r.cp.Attempt,
		consumeCtx:   consumeCtx,
		consumerStop: consumerStop,
		windows:      make(map[int64]*window),
		queue:        make(map[int64]*window),
		workersLeft:  workers,
		batches:      make(chan []kafka.Record, 1),
		deliverCh:    make(chan *window, workers*2),
		errCh:        make(chan error, 1),
	}
}

// workerLoop executes chunks end to end on the marker/snapshot side. The
// coordinator owns the stream and the commit.
func (c *coord) workerLoop(ctx context.Context, workerID string) error {
	r := c.r
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk, err := r.chunkStore.LeaseChunk(ctx, r.cfg.JobID, workerID, r.cfg.LeaseDuration)
		if err != nil {
			return fmt.Errorf("lease chunk: %w", err)
		}
		if chunk == nil {
			return nil
		}
		chunk.Attempt = c.attempt
		chunk.WorkerID = workerID
		if err := c.runWindow(ctx, chunk); err != nil {
			return err
		}
	}
}

// runWindow is the worker side of one chunk: register the window before the
// LOW marker can appear on the stream, write markers, scan the snapshot, and
// deliver the candidates to the coordinator.
func (c *coord) runWindow(ctx context.Context, chunk *model.Chunk) error {
	r := c.r
	chunkRange := chunk.Range()

	w := c.register(chunk)
	chunk.Status = model.ChunkScanning
	if err := r.updateChunkState(ctx, chunk); err != nil {
		return fmt.Errorf("update chunk scanning: %w", err)
	}

	if _, err := r.marker.WriteLow(ctx, r.cfg.JobID, c.attempt, chunkRange); err != nil {
		return fmt.Errorf("write low marker: %w", err)
	}

	rows, err := r.scanner.ReadChunk(ctx, chunkRange.Min, chunkRange.Max)
	if err != nil {
		return fmt.Errorf("read chunk %s: %w", chunkRange, err)
	}
	if n := len(rows); r.cfg.MaxInMemoryCandidates > 0 && n > r.cfg.MaxInMemoryCandidates {
		return fmt.Errorf("chunk %s read %d rows exceeds max in-memory candidates %d", chunkRange, n, r.cfg.MaxInMemoryCandidates)
	}
	chunk.RowsScanned = int64(len(rows))
	if err := r.updateChunkState(ctx, chunk); err != nil {
		return fmt.Errorf("update chunk rows_scanned: %w", err)
	}

	if _, err := r.marker.WriteHigh(ctx, r.cfg.JobID, c.attempt, chunkRange); err != nil {
		return fmt.Errorf("write high marker: %w", err)
	}
	chunk.Status = model.ChunkReconciling
	if err := r.updateChunkState(ctx, chunk); err != nil {
		return fmt.Errorf("update chunk reconciling: %w", err)
	}

	c.deliver(ctx, w, rows)
	return nil
}

// register makes the coordinator aware of a window before its LOW marker can
// reach the stream.
func (c *coord) register(chunk *model.Chunk) *window {
	w := &window{
		chunk:   chunk.Range(),
		durable: chunk,
		state:   OutsideWindow,
		evicted: map[int64]struct{}{},
	}
	c.mu.Lock()
	c.windows[chunk.ChunkMinID] = w
	c.mu.Unlock()
	return w
}

// workerDone records worker exit. A worker failure becomes fatal: the
// consumer is unblocked so the coordinator can observe it instead of waiting
// forever on a stream that can never complete the failed chunk.
func (c *coord) workerDone(err error) {
	c.mu.Lock()
	if err != nil {
		if c.fatal == nil {
			c.fatal = err
			c.consumerStop()
		}
	} else {
		c.workersLeft--
	}
	c.mu.Unlock()
}

// deliver hands the scanned snapshot of a window to the coordinator. Keys
// touched in-window before delivery are dropped so an early eviction is never
// lost to a later snapshot.
func (c *coord) deliver(ctx context.Context, w *window, rows []model.Account) {
	w.mu.Lock()
	candidates := make(map[int64]model.Account, len(rows))
	for _, a := range rows {
		candidates[a.ID] = a
	}
	for k := range w.evicted {
		delete(candidates, k)
	}
	w.evicted = nil
	w.candidates = candidates
	w.ready = true
	w.mu.Unlock()

	select {
	case c.deliverCh <- w:
	case <-ctx.Done():
	}
}

// run consumes the single CDC stream until every worker has exited and every
// window has been committed, then switches to CDC-only mode.
func (c *coord) run(ctx context.Context) error {
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		for {
			records, err := c.r.consumer.Poll(c.consumeCtx)
			if err != nil {
				select {
				case c.errCh <- err:
				case <-c.consumeCtx.Done():
				}
				return
			}
			if len(records) > 0 {
				select {
				case c.batches <- records:
				case <-c.consumeCtx.Done():
					return
				}
			} else {
				select {
				case <-c.consumeCtx.Done():
					return
				case <-time.After(50 * time.Millisecond):
				}
			}
		}
	}()

	for {
		c.mu.Lock()
		fatal := c.fatal
		done := c.workersLeft == 0 && len(c.windows) == 0 && len(c.queue) == 0
		c.mu.Unlock()
		if fatal != nil {
			stopPoller(c, pollDone)
			return fatal
		}
		if done {
			stopPoller(c, pollDone)
			return c.finish(ctx)
		}

		select {
		case batch := <-c.batches:
			if err := c.process(ctx, batch); err != nil {
				stopPoller(c, pollDone)
				return err
			}
		case w := <-c.deliverCh:
			if err := c.tryCommit(ctx, w); err != nil {
				stopPoller(c, pollDone)
				return err
			}
		case err := <-c.errCh:
			if err == nil {
				continue
			}
			if ctx.Err() != nil {
				stopPoller(c, pollDone)
				return ctx.Err()
			}
			c.mu.Lock()
			fatal = c.fatal
			c.mu.Unlock()
			if fatal != nil {
				stopPoller(c, pollDone)
				return fatal
			}
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				// The stream has no more records (test harness). Drain the
				// in-flight windows and finish when they commit.
				return c.drain(ctx, pollDone)
			}
			stopPoller(c, pollDone)
			return err
		case <-ctx.Done():
			stopPoller(c, pollDone)
			return ctx.Err()
		}
	}
}

// stopPoller cancels the poller context and waits for the poll goroutine to
// exit so the consumer is never used by two goroutines at once.
func stopPoller(c *coord, pollDone chan struct{}) {
	c.consumerStop()
	<-pollDone
}

// drain processes remaining deliveries and batches after the stream is
// exhausted, until all workers have exited and every window has committed.
func (c *coord) drain(ctx context.Context, pollDone chan struct{}) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		c.mu.Lock()
		fatal := c.fatal
		done := c.workersLeft == 0 && len(c.windows) == 0 && len(c.queue) == 0
		c.mu.Unlock()
		if fatal != nil {
			stopPoller(c, pollDone)
			return fatal
		}
		if done {
			stopPoller(c, pollDone)
			return c.finish(ctx)
		}
		select {
		case batch := <-c.batches:
			if err := c.process(ctx, batch); err != nil {
				stopPoller(c, pollDone)
				return err
			}
		case w := <-c.deliverCh:
			if err := c.tryCommit(ctx, w); err != nil {
				stopPoller(c, pollDone)
				return err
			}
		case <-ctx.Done():
			stopPoller(c, pollDone)
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// finish switches to CDC-only mode once the scan cursor has reached the upper
// bound, or fails loudly if the backfill is incomplete.
func (c *coord) finish(ctx context.Context) error {
	if c.r.cp.CompletedThrough >= c.r.cp.ScanUpperBound {
		log.Printf("seam: backfill complete, continuing cdc-only mode")
		return c.r.runCDCOnly(ctx)
	}
	return fmt.Errorf("backfill stalled: completed_through=%d upper_bound=%d",
		c.r.cp.CompletedThrough, c.r.cp.ScanUpperBound)
}

// process applies a poll batch to every open window and the destination.
func (c *coord) process(ctx context.Context, records []kafka.Record) error {
	if err := c.r.checkBatchBounds(records); err != nil {
		return err
	}
	for _, group := range groupByTransaction(records) {
		if err := c.processGroup(ctx, group); err != nil {
			return err
		}
	}
	return nil
}

// processGroup routes one source transaction group to every open window:
// markers advance each window's state machine, in-window account changes
// evict candidates, and the destination is updated. A group that carries a
// HIGH marker completes its window (via completeChunk, which applies the
// group's own records atomically) instead of being applied generically.
func (c *coord) processGroup(ctx context.Context, group []kafka.Record) error {
	if len(group) == 0 {
		return nil
	}

	var completers []*window
	c.mu.Lock()
	for _, w := range c.windows {
		w.mu.Lock()
		if w.state == InsideWindow {
			for _, rec := range group {
				if rec.Change.Account != nil {
					w.evictLocked(rec.Change.Account.ID)
				}
			}
		}
		wasComplete := w.state == CompletingWindow
		if _, err := c.r.evaluateMarkers(group, w); err != nil {
			w.mu.Unlock()
			c.mu.Unlock()
			return err
		}
		if w.state == CompletingWindow && !wasComplete {
			// First HIGH for this window: stash the transaction group that
			// carried it for the atomic completion commit.
			w.highSeen = true
			w.highGroup = group
			completers = append(completers, w)
		}
		w.mu.Unlock()
	}
	c.mu.Unlock()

	if len(completers) > 1 {
		return fmt.Errorf("source transaction completed %d windows; expected at most one", len(completers))
	}
	if len(completers) == 1 {
		return c.tryCommit(ctx, completers[0])
	}
	return c.r.applySourceTransaction(ctx, group, nil)
}

// tryCommit commits a window once it is both ready (candidates delivered) and
// HIGH-seen, enforcing that chunks commit in ascending order so the single
// checkpoint cursor never regresses.
func (c *coord) tryCommit(ctx context.Context, w *window) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	w.mu.Lock()
	ready := w.ready && w.highSeen
	group := w.highGroup
	w.mu.Unlock()
	if !ready {
		return nil
	}
	if w.chunk.Min != c.r.cp.CompletedThrough+1 {
		c.queue[w.chunk.Min] = w
		return nil
	}
	if err := c.r.completeChunk(ctx, group, w.durable, w); err != nil {
		return err
	}
	delete(c.windows, w.chunk.Min)
	delete(c.queue, w.chunk.Min)
	return c.drainQueueLocked(ctx)
}

// drainQueueLocked commits any windows held in the commit queue whose turn has
// arrived. The caller must hold c.mu.
func (c *coord) drainQueueLocked(ctx context.Context) error {
	for {
		next := c.r.cp.CompletedThrough + 1
		w, ok := c.queue[next]
		if !ok {
			return nil
		}
		w.mu.Lock()
		group := w.highGroup
		w.mu.Unlock()
		if err := c.r.completeChunk(ctx, group, w.durable, w); err != nil {
			return err
		}
		delete(c.windows, w.chunk.Min)
		delete(c.queue, w.chunk.Min)
	}
}
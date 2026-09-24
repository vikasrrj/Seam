package reconcile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
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
	if err := c.loadManifest(ctx); err != nil {
		return err
	}
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

	mu                    sync.Mutex
	windows               map[int64]*window // chunk.Min -> open window
	ordered               []model.ChunkRange
	completed             map[int64]bool
	nextOrdinal           int
	workersLeft           int
	fatal                 error
	scanCompleteAnnounced bool

	batches      chan []kafka.Record
	transactions chan []*kafka.Transaction
	errCh        chan error
	fatalCh      chan struct{}
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
		completed:    make(map[int64]bool),
		workersLeft:  workers,
		batches:      make(chan []kafka.Record, 1),
		transactions: make(chan []*kafka.Transaction, 1),
		errCh:        make(chan error, 1),
		fatalCh:      make(chan struct{}),
	}
}

func (c *coord) loadManifest(ctx context.Context) error {
	sealed, err := c.r.chunkStore.DiscoveryComplete(ctx, c.r.cfg.JobID)
	if err != nil {
		return fmt.Errorf("load discovery seal: %w", err)
	}
	if !sealed {
		return fmt.Errorf("chunk manifest is not sealed")
	}
	chunks, err := c.r.chunkStore.LoadChunks(ctx, c.r.cfg.JobID)
	if err != nil {
		return fmt.Errorf("load chunk manifest: %w", err)
	}
	byMin := make(map[int64]model.ChunkRange)
	for _, chunk := range chunks {
		if chunk.Attempt != c.attempt && chunk.Status != model.ChunkCompleted {
			continue
		}
		range_ := chunk.Range()
		if prior, ok := byMin[range_.Min]; ok && prior != range_ {
			return fmt.Errorf("overlapping chunk manifest at %d", range_.Min)
		}
		byMin[range_.Min] = range_
		if chunk.Status == model.ChunkCompleted {
			c.completed[range_.Min] = true
		}
	}
	for _, chunk := range byMin {
		c.ordered = append(c.ordered, chunk)
	}
	sort.Slice(c.ordered, func(i, j int) bool { return c.ordered[i].Min < c.ordered[j].Min })
	for i := 1; i < len(c.ordered); i++ {
		if c.ordered[i].Min <= c.ordered[i-1].Max {
			return fmt.Errorf("overlapping chunk ranges %s and %s", c.ordered[i-1], c.ordered[i])
		}
	}
	for c.nextOrdinal < len(c.ordered) && c.completed[c.ordered[c.nextOrdinal].Min] {
		c.nextOrdinal++
	}
	if len(c.ordered) == 0 && c.r.cp.CompletedThrough < c.r.cp.ScanUpperBound {
		return fmt.Errorf("unsealed or empty chunk manifest while backfill is incomplete")
	}
	return nil
}

// workerLoop executes chunks end to end on the marker/snapshot side. The
// coordinator owns the stream and the commit.
func (c *coord) workerLoop(ctx context.Context, workerID string) error {
	r := c.r
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk, err := r.chunkStore.LeaseChunkOwned(ctx, r.cfg.JobID, c.attempt, workerID, r.owner.OwnerID, r.owner.OwnerEpoch, r.cfg.LeaseDuration)
		if err != nil {
			return fmt.Errorf("lease chunk: %w", err)
		}
		if chunk == nil {
			return nil
		}
		chunk.Attempt = c.attempt
		chunk.WorkerID = workerID
		// A chunk whose lease expired was safely reassigned to this worker —
		// but only if no live window is still executing it. Two workers on the
		// same window would push a second LOW/HIGH marker pair for the same
		// range, so refuse loudly instead of corrupting the window.
		if c.windowOpen(chunk.ChunkMinID) {
			return fmt.Errorf("chunk %s already has an open window; refusing duplicate execution", chunk.Range())
		}
		stop := r.startHeartbeat(ctx, chunk, func(err error) { c.workerDone(err) })
		runErr := c.runWindow(ctx, chunk)
		if hbErr := stop(); runErr == nil {
			runErr = hbErr
		}
		if runErr != nil {
			return runErr
		}
	}
}

// windowOpen reports whether the coordinator has a live window for the given
// chunk key.
func (c *coord) windowOpen(minID int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.windows[minID]
	return ok
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

	rows, err := r.readChunk(ctx, chunkRange.Min, chunkRange.Max)
	if err != nil {
		return fmt.Errorf("read chunk %s: %w", chunkRange, err)
	}
	if n := len(rows); r.cfg.MaxInMemoryCandidates > 0 && n > r.cfg.MaxInMemoryCandidates {
		return fmt.Errorf("chunk %s read %d rows exceeds max in-memory candidates %d", chunkRange, n, r.cfg.MaxInMemoryCandidates)
	}
	w.candidateCount = len(rows)
	if w.staged {
		if err := r.stageCandidates(ctx, chunk, rows); err != nil {
			return fmt.Errorf("stage chunk %s candidates: %w", chunkRange, err)
		}
		rows = nil
	} else {
		chunk.RowsScanned = int64(len(rows))
		if err := r.updateChunkState(ctx, chunk); err != nil {
			return fmt.Errorf("update chunk rows_scanned: %w", err)
		}
	}

	if _, err := r.marker.WriteHigh(ctx, r.cfg.JobID, c.attempt, chunkRange); err != nil {
		return fmt.Errorf("write high marker: %w", err)
	}
	chunk.Status = model.ChunkReconciling
	if err := r.updateChunkState(ctx, chunk); err != nil {
		return fmt.Errorf("update chunk reconciling: %w", err)
	}

	if err := c.deliver(w, rows); err != nil {
		return fmt.Errorf("deliver chunk %s candidates: %w", chunkRange, err)
	}
	rows = nil
	select {
	case <-w.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// register makes the coordinator aware of a window before its LOW marker can
// reach the stream.
func (c *coord) register(chunk *model.Chunk) *window {
	w := &window{
		chunk:          chunk.Range(),
		durable:        chunk,
		maxEvictedKeys: c.r.cfg.MaxInMemoryCandidates,
		state:          OutsideWindow,
		staged:         c.r.durableCandidatesEnabled(),
		evicted:        map[int64]struct{}{},
		readyCh:        make(chan struct{}),
		doneCh:         make(chan struct{}),
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
			close(c.fatalCh)
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
func (c *coord) deliver(w *window, rows []model.Row) error {
	var candidates map[int64]model.Row
	if !w.staged {
		indexed, err := c.r.candidatesByKey(rows)
		if err != nil {
			return err
		}
		candidates = indexed
	}
	w.mu.Lock()
	if !w.staged {
		for k := range w.evicted {
			delete(candidates, k)
		}
		w.candidates = candidates
	}
	w.evicted = nil
	w.ready = true
	w.mu.Unlock()
	close(w.readyCh)
	return nil
}

// run consumes the single CDC stream until every worker has exited and every
// window has been committed, then switches to CDC-only mode.
func (c *coord) run(ctx context.Context) error {
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		for {
			if streaming, ok := c.r.consumer.(transactionConsumer); ok {
				transactions, err := streaming.PollTransactions(c.consumeCtx)
				if err != nil {
					select {
					case c.errCh <- err:
					case <-c.consumeCtx.Done():
					}
					return
				}
				if len(transactions) > 0 {
					select {
					case c.transactions <- transactions:
					case <-c.consumeCtx.Done():
						closeTransactions(transactions)
						return
					}
				} else {
					select {
					case <-c.consumeCtx.Done():
						return
					case <-time.After(50 * time.Millisecond):
					}
				}
				continue
			}
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
		done := c.workersLeft == 0 && len(c.windows) == 0
		c.mu.Unlock()
		if fatal != nil {
			stopPoller(c, pollDone)
			return fatal
		}
		if done {
			if err := c.finish(); err != nil {
				stopPoller(c, pollDone)
				return err
			}
			if !c.scanCompleteAnnounced {
				log.Printf("seam: backfill complete, continuing cdc-only mode")
				c.scanCompleteAnnounced = true
			}
		}

		select {
		case <-c.fatalCh:
			c.mu.Lock()
			fatal := c.fatal
			c.mu.Unlock()
			stopPoller(c, pollDone)
			return fatal
		case batch := <-c.batches:
			if err := c.process(ctx, batch); err != nil {
				stopPoller(c, pollDone)
				return err
			}
		case transactions := <-c.transactions:
			if err := c.processTransactions(ctx, transactions); err != nil {
				closeTransactions(transactions)
				stopPoller(c, pollDone)
				return err
			}
			closeTransactions(transactions)
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

func (c *coord) processTransactions(ctx context.Context, transactions []*kafka.Transaction) error {
	for _, transaction := range transactions {
		if err := c.processStreamTransaction(ctx, transaction); err != nil {
			return err
		}
	}
	return nil
}

func (c *coord) processStreamTransaction(ctx context.Context, transaction *kafka.Transaction) error {
	if err := c.r.validateStreamTransaction(transaction); err != nil {
		return err
	}
	if err := c.r.awaitCutoverPermission(ctx, transaction.FinalOffset+1); err != nil {
		return err
	}
	if transaction.FinalOffset < c.r.cp.NextKafkaOffset {
		return nil
	}
	alreadyApplied, err := c.r.sourceAlreadyApplied(ctx, transaction.Source)
	if err != nil {
		return err
	}
	if alreadyApplied {
		return c.r.applyStreamTransaction(ctx, transaction, nil)
	}

	var completers []*window
	var evictionWindows []*window
	c.mu.Lock()
	for _, w := range c.windows {
		w.mu.Lock()
		if w.state == InsideWindow {
			evictionWindows = append(evictionWindows, w)
		}
		wasComplete := w.state == CompletingWindow
		if _, err := c.r.evaluateStreamMarkers(transaction, w); err != nil {
			w.mu.Unlock()
			c.mu.Unlock()
			return err
		}
		if w.state == CompletingWindow && !wasComplete {
			w.highSeen = true
			w.highTransaction = transaction
			completers = append(completers, w)
		}
		w.mu.Unlock()
	}
	c.mu.Unlock()
	if len(completers) > 1 {
		return fmt.Errorf("source transaction completed %d windows; expected at most one", len(completers))
	}
	if len(completers) == 1 {
		completers[0].mu.Lock()
		completers[0].highWindows = append([]*window(nil), evictionWindows...)
		completers[0].mu.Unlock()
		select {
		case <-completers[0].readyCh:
		case <-c.consumeCtx.Done():
			return c.consumeCtx.Err()
		}
		return c.tryCommit(ctx, completers[0])
	}
	return c.r.applyStreamTransaction(ctx, transaction, evictionWindows)
}

// stopPoller cancels the poller context and waits for the poll goroutine to
// exit so the consumer is never used by two goroutines at once.
func stopPoller(c *coord, pollDone chan struct{}) {
	c.consumerStop()
	<-pollDone
	for {
		select {
		case transactions := <-c.transactions:
			closeTransactions(transactions)
		default:
			return
		}
	}
}

// drain processes remaining deliveries and batches after the stream is
// exhausted, until all workers have exited and every window has committed.
func (c *coord) drain(ctx context.Context, pollDone chan struct{}) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		c.mu.Lock()
		fatal := c.fatal
		done := c.workersLeft == 0 && len(c.windows) == 0
		c.mu.Unlock()
		if fatal != nil {
			stopPoller(c, pollDone)
			return fatal
		}
		if done && len(c.batches) == 0 && len(c.transactions) == 0 {
			stopPoller(c, pollDone)
			return c.finish()
		}
		select {
		case <-c.fatalCh:
			c.mu.Lock()
			fatal := c.fatal
			c.mu.Unlock()
			stopPoller(c, pollDone)
			return fatal
		case batch := <-c.batches:
			if err := c.process(ctx, batch); err != nil {
				stopPoller(c, pollDone)
				return err
			}
		case transactions := <-c.transactions:
			if err := c.processTransactions(ctx, transactions); err != nil {
				closeTransactions(transactions)
				stopPoller(c, pollDone)
				return err
			}
			closeTransactions(transactions)
		case <-ctx.Done():
			stopPoller(c, pollDone)
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// finish verifies that every discovered range has reached the upper bound.
func (c *coord) finish() error {
	if c.r.cp.CompletedThrough >= c.r.cp.ScanUpperBound {
		return nil
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
	if err := c.r.validateSourceGroup(group); err != nil {
		return err
	}
	if err := c.r.awaitCutoverPermission(ctx, group[len(group)-1].Offset+1); err != nil {
		return err
	}
	if group[len(group)-1].Offset < c.r.cp.NextKafkaOffset {
		return nil
	}
	// A producer may have durably published a source transaction, lost its
	// acknowledgement, and published it again at a new Kafka offset. Skip
	// marker state transitions as well as row effects for that retry.
	alreadyApplied, err := c.r.sourceAlreadyApplied(ctx, group[0].Change.Source)
	if err != nil {
		return err
	}
	if alreadyApplied {
		return c.r.applySourceTransaction(ctx, group, nil)
	}

	var completers []*window
	var evictionWindows []*window
	c.mu.Lock()
	for _, w := range c.windows {
		w.mu.Lock()
		if w.state == InsideWindow {
			evictionWindows = append(evictionWindows, w)
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
		completers[0].mu.Lock()
		completers[0].highWindows = append([]*window(nil), evictionWindows...)
		completers[0].mu.Unlock()
		// The stream cannot pass HIGH until the candidates have been finalized.
		// Later CDC would otherwise be overwritten by a delayed snapshot write.
		select {
		case <-completers[0].readyCh:
		case <-c.consumeCtx.Done():
			return c.consumeCtx.Err()
		}
		return c.tryCommit(ctx, completers[0])
	}
	return c.r.applySourceTransactionForWindows(ctx, group, evictionWindows)
}

// tryCommit finalizes this window at its HIGH prefix. Its destination writes
// may finish out of key order; CompletedThrough advances only across a
// contiguous prefix of the durable chunk manifest.
func (c *coord) tryCommit(ctx context.Context, w *window) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	w.mu.Lock()
	ready := w.ready && w.highSeen
	group := w.highGroup
	transaction := w.highTransaction
	evictionWindows := append([]*window(nil), w.highWindows...)
	w.mu.Unlock()
	if !ready {
		return nil
	}
	idx := sort.Search(len(c.ordered), func(i int) bool { return c.ordered[i].Min >= w.chunk.Min })
	if idx >= len(c.ordered) || c.ordered[idx] != w.chunk {
		return fmt.Errorf("chunk %s absent from manifest", w.chunk)
	}
	c.completed[w.chunk.Min] = true
	frontier := c.r.cp.CompletedThrough
	next := c.nextOrdinal
	for next < len(c.ordered) && c.completed[c.ordered[next].Min] {
		frontier = c.ordered[next].Max
		next++
	}
	var err error
	if transaction != nil {
		err = c.r.completeChunkStreamAt(ctx, transaction, w.durable, w, frontier, evictionWindows...)
	} else {
		err = c.r.completeChunkAt(ctx, group, w.durable, w, frontier, evictionWindows...)
	}
	if err != nil {
		delete(c.completed, w.chunk.Min)
		return err
	}
	c.nextOrdinal = next
	delete(c.windows, w.chunk.Min)
	close(w.doneCh)
	return nil
}

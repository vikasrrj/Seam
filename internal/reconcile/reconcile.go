// Package reconcile implements Seam's core algorithm: bounded historical chunks
// reconciled against an ordered live CDC stream using LOW/HIGH markers.
package reconcile

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"example.com/seam/internal/capture"
	"example.com/seam/internal/failpoint"
	"example.com/seam/internal/kafka"
	"example.com/seam/internal/model"
	"example.com/seam/internal/schema"
	"example.com/seam/internal/telemetry"
	"github.com/jackc/pgx/v5"
)

// Reconciler owns destination application during a Seam backfill.
type Reconciler struct {
	cfg             model.JobConfig
	cp              *model.Checkpoint
	owner           *model.Checkpoint // immutable leadership token shared with workers
	consumer        consumer
	cpStore         checkpointStore
	chunkStore      chunkStore
	marker          markerStore
	scanner         scanner
	sink            sink
	sizer           chunkSizer
	failpoints      *failpoint.Registry
	metrics         *telemetry.Metrics
	resources       resourceController
	codec           capture.JSONCodec
	disableEviction bool

	// schema is the durable source descriptor pinned on the job. It provides
	// the primary-key ordinal for window indexing and the schema epoch every
	// row change must carry; a mismatch fails the pipeline closed.
	schema   *schema.Schema
	schemaID string
}

// consumer reads decoded Kafka records.
type consumer interface {
	Poll(ctx context.Context) ([]kafka.Record, error)
	Close()
}

type resourceController interface {
	AcquireScan(context.Context) (func(), error)
	AcquireDestination(context.Context) (func(), error)
	SetAppliedOffset(int64)
}

// checkpointStore persists job progress and applied-transaction bookkeeping.
type checkpointStore interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	AssertLeadership(ctx context.Context, tx pgx.Tx, cp *model.Checkpoint) error
	UpdateCheckpoint(ctx context.Context, tx pgx.Tx, cp *model.Checkpoint, expectedOffset int64) error
	IsApplied(ctx context.Context, tx pgx.Tx, jobID, generation, lsn string) (bool, error)
	MarkApplied(ctx context.Context, tx pgx.Tx, jobID, generation, lsn string, xid uint32) error
}

type cutoverGateStore interface {
	CutoverTarget(context.Context, string) (*int64, error)
}

func (r *Reconciler) awaitCutoverPermission(ctx context.Context, nextOffset int64) error {
	store, ok := r.cpStore.(cutoverGateStore)
	if !ok {
		return nil
	}
	for {
		target, err := store.CutoverTarget(ctx, r.cfg.JobID)
		if err != nil {
			return fmt.Errorf("read cutover gate: %w", err)
		}
		if target == nil || nextOffset <= *target {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (r *Reconciler) beginOwned(ctx context.Context) (pgx.Tx, error) {
	var release func()
	if r.resources != nil {
		var err error
		release, err = r.resources.AcquireDestination(ctx)
		if err != nil {
			return nil, err
		}
	}
	tx, err := r.cpStore.Begin(ctx)
	if err != nil {
		if release != nil {
			release()
		}
		return nil, err
	}
	if release != nil {
		tx = &resourceTx{Tx: tx, release: release}
	}
	if err := r.cpStore.AssertLeadership(ctx, tx, r.owner); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}

type resourceTx struct {
	pgx.Tx
	release func()
	once    sync.Once
}

func (t *resourceTx) Commit(ctx context.Context) error {
	defer t.once.Do(t.release)
	return t.Tx.Commit(ctx)
}

func (t *resourceTx) Rollback(ctx context.Context) error {
	defer t.once.Do(t.release)
	return t.Tx.Rollback(ctx)
}

func (r *Reconciler) durableCandidatesEnabled() bool {
	_, storeOK := r.chunkStore.(durableCandidateStore)
	_, sinkOK := r.sink.(stagedCandidateSink)
	return storeOK && sinkOK
}

func (r *Reconciler) stageCandidates(ctx context.Context, chunk *model.Chunk, rows []model.Row) error {
	store, ok := r.chunkStore.(durableCandidateStore)
	if !ok {
		return fmt.Errorf("durable candidate store unavailable")
	}
	tx, err := r.beginOwned(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := store.StageCandidates(ctx, tx, chunk, rows, r.schema); err != nil {
		return err
	}
	chunk.RowsScanned = int64(len(rows))
	if err := r.chunkStore.UpdateChunk(ctx, tx, chunk); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// markerStore writes LOW/HIGH control markers on the source.
type markerStore interface {
	WriteLow(ctx context.Context, jobID, attempt string, chunk model.ChunkRange) (string, error)
	WriteHigh(ctx context.Context, jobID, attempt string, chunk model.ChunkRange) (string, error)
}

// scanner reads bounded primary-key chunks from the source table.
type scanner interface {
	NextChunk(ctx context.Context, completedThrough, upperBound int64, chunkSize int) (model.ChunkRange, bool, error)
	ReadChunk(ctx context.Context, minID, maxID int64) ([]model.Row, error)
}

// sink applies row changes and surviving snapshot candidates to the destination.
type sink interface {
	Apply(ctx context.Context, tx pgx.Tx, change model.Change) error
	WriteCandidates(ctx context.Context, tx pgx.Tx, candidates []model.Row) error
}

type batchSink interface {
	ApplyBatch(ctx context.Context, tx pgx.Tx, changes []model.Change) error
}

type durableCandidateStore interface {
	StageCandidates(ctx context.Context, tx pgx.Tx, chunk *model.Chunk, candidates []model.Row, descriptor *schema.Schema) error
	EvictCandidates(ctx context.Context, tx pgx.Tx, chunk *model.Chunk, keys []int64) error
	ClearCandidates(ctx context.Context, tx pgx.Tx, chunk *model.Chunk) error
}

type stagedCandidateSink interface {
	WriteStagedCandidates(ctx context.Context, tx pgx.Tx, chunk *model.Chunk) (int64, error)
}

// WindowState tracks where the reconciler is relative to the current markers.
type WindowState int

const (
	// OutsideWindow means no LOW marker for the current attempt has been seen.
	OutsideWindow WindowState = iota
	// InsideWindow means LOW seen, HIGH not yet seen.
	InsideWindow
	// CompletingWindow means HIGH has been seen and the chunk is being finalized.
	CompletingWindow
)

// Config wires the reconciler together.
type Config struct {
	JobConfig       model.JobConfig
	Checkpoint      *model.Checkpoint
	Consumer        consumer
	CheckpointStore checkpointStore
	ChunkStore      chunkStore
	MarkerStore     markerStore
	Scanner         scanner
	Sink            sink
	Adaptive        chunkSizer
	Failpoints      *failpoint.Registry
	Metrics         *telemetry.Metrics
	Resources       resourceController
	// SourceSchema is the job's durable source schema descriptor. It is
	// required: it pins the primary-key ordinal used for window indexing and
	// the schema epoch every row change must carry.
	SourceSchema *schema.Schema
	// DisableEviction is a deliberately broken mode used by Phase 2 tests to
	// demonstrate stale overwrite and delete resurrection.
	DisableEviction bool
}

// chunkStore persists per-chunk lifecycle state and leases.
type chunkStore interface {
	// LeaseChunk atomically claims the next chunk of the given attempt (pending
	// or expired-lease) for a worker.
	LeaseChunkOwned(ctx context.Context, jobID, attempt, workerID, ownerID string, ownerEpoch int64, leaseDuration time.Duration) (*model.Chunk, error)
	UpdateChunk(ctx context.Context, tx pgx.Tx, chunk *model.Chunk) error
	// HeartbeatChunk renews the lease owner's heartbeat and rolls its expiry.
	HeartbeatChunkOwned(ctx context.Context, chunk *model.Chunk, ownerID string, ownerEpoch int64, leaseDuration time.Duration) error
	LoadChunks(ctx context.Context, jobID string, statuses ...model.ChunkState) ([]model.Chunk, error)
	DiscoveryComplete(ctx context.Context, jobID string) (bool, error)
}

// chunkSizer recommends the next chunk size from measured latencies. It is
// optional; when nil, the configured fixed chunk size is used.
type chunkSizer interface {
	Suggest() int
	Observe(duration time.Duration, rows int)
}

// New builds a reconciler. It does not start consuming.
func New(cfg Config) *Reconciler {
	owner := *cfg.Checkpoint
	r := &Reconciler{
		cfg:             cfg.JobConfig,
		cp:              cfg.Checkpoint,
		owner:           &owner,
		consumer:        cfg.Consumer,
		cpStore:         cfg.CheckpointStore,
		chunkStore:      cfg.ChunkStore,
		marker:          cfg.MarkerStore,
		scanner:         cfg.Scanner,
		sink:            cfg.Sink,
		sizer:           cfg.Adaptive,
		failpoints:      cfg.Failpoints,
		metrics:         cfg.Metrics,
		resources:       cfg.Resources,
		disableEviction: cfg.DisableEviction,
		schema:          cfg.SourceSchema,
	}
	if r.schema != nil {
		r.schemaID = r.schema.Fingerprint
	}
	if r.failpoints == nil {
		r.failpoints = failpoint.NewRegistry()
	}
	if r.metrics == nil {
		r.metrics = telemetry.NewMetrics()
	}
	return r
}

// rowKey extracts the message primary key from a row change using the job's
// pinned descriptor.
func (r *Reconciler) changeKey(change *model.Change) (int64, error) {
	if r.schema == nil {
		return 0, fmt.Errorf("reconciler requires a source schema descriptor")
	}
	return r.schema.KeyFromChange(change)
}

func (r *Reconciler) rowKey(row *model.Row) (int64, error) {
	if r.schema == nil {
		return 0, fmt.Errorf("reconciler requires a source schema descriptor")
	}
	return r.schema.KeyFromRow(row)
}

// candidatesByKey indexes snapshot rows by their primary key for window
// eviction lookups. Keys are proven to lie inside the chunk by the scan.
func (r *Reconciler) candidatesByKey(rows []model.Row) (map[int64]model.Row, error) {
	indexed := make(map[int64]model.Row, len(rows))
	for i := range rows {
		key, err := r.rowKey(&rows[i])
		if err != nil {
			return nil, err
		}
		indexed[key] = rows[i]
	}
	return indexed, nil
}

func (r *Reconciler) readChunk(ctx context.Context, minID, maxID int64) ([]model.Row, error) {
	if r.resources == nil {
		return r.scanner.ReadChunk(ctx, minID, maxID)
	}
	release, err := r.resources.AcquireScan(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return r.scanner.ReadChunk(ctx, minID, maxID)
}

func (r *Reconciler) publishCheckpoint(cp model.Checkpoint) {
	*r.cp = cp
	if r.resources != nil {
		r.resources.SetAppliedOffset(cp.NextKafkaOffset)
	}
}

// Run drives the backfill and then continues applying live CDC. It returns when
// ctx is cancelled or a fatal error occurs.
func (r *Reconciler) Run(ctx context.Context) error {
	log.Printf("seam: starting job=%s generation=%s attempt=%s completed_through=%d upper_bound=%d next_offset=%d",
		r.cp.JobID, r.cp.Generation, r.cp.Attempt, r.cp.CompletedThrough, r.cp.ScanUpperBound, r.cp.NextKafkaOffset)

	if r.chunkStore != nil && r.cfg.Workers > 1 {
		return r.runParallelWorkers(ctx)
	}
	if r.chunkStore != nil {
		return r.runChunkStoreLoop(ctx)
	}
	return r.runLegacyLoop(ctx)
}

// runLegacyLoop uses the scanner to discover chunks on the fly. It is used
// when no chunk store is configured. When an adaptive sizer is configured, the
// chunk size is re-suggested per iteration from measured latency.
func (r *Reconciler) runLegacyLoop(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		size := r.cfg.ChunkSize
		if r.sizer != nil {
			size = r.sizer.Suggest()
		}
		chunkRange, ok, err := r.scanner.NextChunk(ctx, r.cp.CompletedThrough, r.cp.ScanUpperBound, size)
		if err != nil {
			return fmt.Errorf("next chunk: %w", err)
		}
		if !ok {
			log.Printf("seam: backfill complete, continuing cdc-only mode")
			return r.runCDCOnly(ctx)
		}
		chunk := &model.Chunk{
			JobID:      r.cfg.JobID,
			ChunkMinID: chunkRange.Min,
			ChunkMaxID: chunkRange.Max,
			Attempt:    r.cp.Attempt,
			Status:     model.ChunkScanning,
		}
		if err := r.runChunk(ctx, chunk); err != nil {
			return err
		}
	}
}

// runChunkStoreLoop leases chunks from durable state. This is the worker mode.
func (r *Reconciler) runChunkStoreLoop(ctx context.Context) error {
	sealed, err := r.chunkStore.DiscoveryComplete(ctx, r.cfg.JobID)
	if err != nil {
		return fmt.Errorf("validate chunk manifest: %w", err)
	}
	if !sealed {
		return fmt.Errorf("chunk manifest is not sealed")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk, err := r.chunkStore.LeaseChunkOwned(ctx, r.cfg.JobID, r.cp.Attempt, r.cfg.WorkerID, r.owner.OwnerID, r.owner.OwnerEpoch, r.cfg.LeaseDuration)
		if err != nil {
			return fmt.Errorf("lease chunk: %w", err)
		}
		if chunk == nil {
			log.Printf("seam: backfill complete, continuing cdc-only mode")
			return r.runCDCOnly(ctx)
		}
		chunk.Attempt = r.cp.Attempt
		chunk.Status = model.ChunkScanning
		stop := r.startHeartbeat(ctx, chunk, nil)
		runErr := r.runChunk(ctx, chunk)
		if hbErr := stop(); runErr == nil {
			runErr = hbErr
		}
		if runErr != nil {
			return runErr
		}
	}
}

// startHeartbeat renews the chunk lease on a fixed interval while the chunk is
// being executed. It returns a stop function that should be called once the
// chunk finishes. Any heartbeat error is reported to report (when non-nil) as
// soon as it happens — so a worker whose lease can no longer be renewed fails
// loudly instead of scanning on a lease it is about to lose — and is also
// returned by the stop function for callers that poll (sequential mode).
func (r *Reconciler) startHeartbeat(ctx context.Context, chunk *model.Chunk, report func(error)) func() error {
	if r.cfg.DisableHeartbeat || r.cfg.LeaseDuration <= 0 {
		return func() error { return nil }
	}
	interval := r.cfg.HeartbeatInterval
	if interval <= 0 {
		interval = r.cfg.LeaseDuration / 3
	}
	if interval <= 0 || interval >= r.cfg.LeaseDuration {
		interval = r.cfg.LeaseDuration / 3
	}
	if interval <= 0 {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	errCh := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := r.chunkStore.HeartbeatChunkOwned(ctx, chunk, r.owner.OwnerID, r.owner.OwnerEpoch, r.cfg.LeaseDuration); err != nil {
					if report != nil {
						report(err)
					}
					errCh <- err
					return
				}
			case <-done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return func() error {
		close(done)
		select {
		case err := <-errCh:
			return err
		default:
			return nil
		}
	}
}

// runCDCOnly applies live changes after the backfill cursor has reached the
// scan upper bound.
func (r *Reconciler) runCDCOnly(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if streaming, ok := r.consumer.(transactionConsumer); ok {
			transactions, err := streaming.PollTransactions(ctx)
			if err != nil {
				return err
			}
			for i, transaction := range transactions {
				if err := r.applyStreamTransaction(ctx, transaction, nil); err != nil {
					closeTransactions(transactions[i:])
					return err
				}
				transaction.Close()
			}
			continue
		}
		records, err := r.consumer.Poll(ctx)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			continue
		}
		if err := r.flushBatch(ctx, records); err != nil {
			return err
		}
	}
}

// runChunk executes one reconciliation window.
func (r *Reconciler) runChunk(ctx context.Context, chunk *model.Chunk) error {
	chunkRange := chunk.Range()
	w := &window{chunk: chunkRange, durable: chunk, state: OutsideWindow, staged: r.durableCandidatesEnabled()}

	log.Printf("seam: chunk %s starting", chunkRange)
	chunkStart := time.Now()

	if r.chunkStore != nil {
		chunk.Status = model.ChunkScanning
		if err := r.updateChunkState(ctx, chunk); err != nil {
			return fmt.Errorf("update chunk scanning: %w", err)
		}
	}

	// 1. Write LOW marker.
	lowID, err := r.marker.WriteLow(ctx, r.cfg.JobID, r.cp.Attempt, chunkRange)
	if err != nil {
		return fmt.Errorf("write low marker: %w", err)
	}
	if err := r.maybeWait(ctx, failpoint.AfterLowMarkerCommitted); err != nil {
		return err
	}

	// 2. Read historical chunk into candidate map.
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
		w.mu.Lock()
		w.candidates, err = r.candidatesByKey(rows)
		if err != nil {
			w.mu.Unlock()
			return fmt.Errorf("index chunk %s candidates: %w", chunkRange, err)
		}
		w.mu.Unlock()
	}
	log.Printf("seam: chunk %s read %d candidates", chunkRange, w.candidateCount)

	if r.chunkStore != nil && !w.staged {
		chunk.RowsScanned = int64(len(rows))
		if err := r.updateChunkState(ctx, chunk); err != nil {
			return fmt.Errorf("update chunk rows_scanned: %w", err)
		}
	}

	if err := r.maybeWait(ctx, failpoint.AfterChunkReadBeforeReconciliation); err != nil {
		return err
	}

	// 3. Write HIGH marker.
	highID, err := r.marker.WriteHigh(ctx, r.cfg.JobID, r.cp.Attempt, chunkRange)
	if err != nil {
		return fmt.Errorf("write high marker: %w", err)
	}
	log.Printf("seam: markers low=%s high=%s", lowID, highID)

	if r.chunkStore != nil {
		chunk.Status = model.ChunkReconciling
		if err := r.updateChunkState(ctx, chunk); err != nil {
			return fmt.Errorf("update chunk reconciling: %w", err)
		}
	}

	// 4. Consume Kafka until HIGH is processed.
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if streaming, ok := r.consumer.(transactionConsumer); ok {
			transactions, err := streaming.PollTransactions(ctx)
			if err != nil {
				return err
			}
			done, err := r.processChunkTransactions(ctx, transactions, chunk, w)
			closeTransactions(transactions)
			if err != nil {
				return err
			}
			if done {
				if r.sizer != nil {
					r.sizer.Observe(time.Since(chunkStart), len(rows))
				}
				return nil
			}
			continue
		}
		records, err := r.consumer.Poll(ctx)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			continue
		}
		done, err := r.processChunkRecords(ctx, records, chunk, w)
		if err != nil {
			return err
		}
		if done {
			if r.sizer != nil {
				r.sizer.Observe(time.Since(chunkStart), len(rows))
			}
			return nil
		}
	}
}

func (r *Reconciler) processChunkTransactions(ctx context.Context, transactions []*kafka.Transaction, chunk *model.Chunk, w *window) (bool, error) {
	for index, transaction := range transactions {
		if err := r.validateStreamTransaction(transaction); err != nil {
			return false, err
		}
		if err := r.awaitCutoverPermission(ctx, transaction.FinalOffset+1); err != nil {
			return false, err
		}
		applied, err := r.sourceAlreadyApplied(ctx, transaction.Source)
		if err != nil {
			return false, err
		}
		if applied {
			if err := r.applyStreamTransaction(ctx, transaction, nil); err != nil {
				return false, err
			}
			continue
		}
		state, err := r.evaluateStreamMarkers(transaction, w)
		if err != nil {
			return false, err
		}
		w.state = state
		if state == CompletingWindow {
			if err := r.completeChunkStreamAt(ctx, transaction, chunk, w, w.chunk.Max); err != nil {
				return false, err
			}
			for _, remaining := range transactions[index+1:] {
				if err := r.applyStreamTransaction(ctx, remaining, nil); err != nil {
					return false, err
				}
			}
			return true, nil
		}
		var windows []*window
		if state == InsideWindow {
			windows = []*window{w}
		}
		if err := r.applyStreamTransaction(ctx, transaction, windows); err != nil {
			return false, err
		}
	}
	return false, nil
}

// updateChunkState persists the current chunk state when a chunk store is configured.
func (r *Reconciler) updateChunkState(ctx context.Context, chunk *model.Chunk) error {
	if r.chunkStore == nil {
		return nil
	}
	tx, err := r.beginOwned(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := r.chunkStore.UpdateChunk(ctx, tx, chunk); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// processChunkRecords applies a batch of Kafka records while inside a chunk.
// It returns true when the chunk is complete.
func (r *Reconciler) processChunkRecords(ctx context.Context, records []kafka.Record, chunk *model.Chunk, w *window) (bool, error) {
	if err := r.checkBatchBounds(records); err != nil {
		return false, err
	}
	w.mu.Lock()
	state := w.state
	w.mu.Unlock()

	// Group records by source transaction.
	groups := groupByTransaction(records)

	for idx, group := range groups {
		if err := r.validateSourceGroup(group); err != nil {
			return false, err
		}
		if err := r.awaitCutoverPermission(ctx, group[len(group)-1].Offset+1); err != nil {
			return false, err
		}
		applied, err := r.sourceAlreadyApplied(ctx, group[0].Change.Source)
		if err != nil {
			return false, err
		}
		if applied {
			if err := r.applySourceTransaction(ctx, group, nil); err != nil {
				return false, err
			}
			continue
		}
		// Determine the effective window state for this group based on markers.
		newState, err := r.evaluateMarkers(group, w)
		if err != nil {
			return false, err
		}
		state = newState

		w.mu.Lock()
		w.state = state
		w.mu.Unlock()

		// If the group contains the HIGH marker for the current attempt, the
		// chunk completion transaction includes surviving candidates.
		if state == CompletingWindow {
			if err := r.completeChunk(ctx, group, chunk, w); err != nil {
				return false, err
			}
			// Drain any remaining groups in this batch in CDC-only mode. They
			// were fetched into this buffer; dropping them would skip source
			// transactions until the next poll re-fetch never happens (the
			// consumer position has already advanced).
			for _, g := range groups[idx:] {
				if skip, err := r.skipAlreadyCompletedGroup(g); err != nil {
					return false, err
				} else if skip {
					continue
				}
				applied, err := r.sourceAlreadyApplied(ctx, g[0].Change.Source)
				if err != nil {
					return false, err
				}
				if applied {
					if err := r.applySourceTransaction(ctx, g, nil); err != nil {
						return false, err
					}
					continue
				}
				if _, err := r.evaluateMarkers(g, w); err != nil {
					return false, err
				}
				if err := r.applySourceTransaction(ctx, g, nil); err != nil {
					return false, err
				}
			}
			return true, nil
		}

		// Otherwise apply the source transaction normally, evicting candidates
		// that are touched while inside the window.
		if err := r.applySourceTransaction(ctx, group, w); err != nil {
			return false, err
		}
	}

	return false, nil
}

// skipAlreadyCompletedGroup reports whether a group is a stale marker-only
// source transaction that must not be re-evaluated after the chunk completed.
func (r *Reconciler) skipAlreadyCompletedGroup(group []kafka.Record) (bool, error) {
	for _, rec := range group {
		if rec.Change.Marker != nil &&
			rec.Change.Marker.JobID == r.cfg.JobID &&
			rec.Change.Marker.Attempt == r.cp.Attempt {
			// All markers of the current attempt for this chunk were consumed
			// by completeChunk.
			return true, nil
		}
	}
	return false, nil
}

// evaluateMarkers scans a transaction group for markers belonging to the
// current attempt and the given window's chunk range, and updates the window
// state. Markers from other chunks or previous attempts are ignored so
// overlapping windows on the shared stream do not interfere.
func (r *Reconciler) evaluateMarkers(group []kafka.Record, w *window) (WindowState, error) {
	for _, rec := range group {
		if rec.Change.Marker == nil {
			continue
		}
		marker := rec.Change.Marker
		if marker.JobID != r.cfg.JobID || marker.Attempt != r.cp.Attempt {
			// Stale marker from a previous attempt; ignore.
			continue
		}
		if marker.ChunkMin != w.chunk.Min || marker.ChunkMax != w.chunk.Max {
			// Marker for a different window; ignore.
			continue
		}
		switch marker.Kind {
		case model.MarkerLow:
			if w.state != OutsideWindow {
				return w.state, fmt.Errorf("unexpected LOW marker %s in state %d (window chunk=%s)", marker.ID, w.state, w.chunk)
			}
			w.state = InsideWindow
		case model.MarkerHigh:
			if w.state != InsideWindow {
				return w.state, fmt.Errorf("unexpected HIGH marker %s in state %d (window chunk=%s)", marker.ID, w.state, w.chunk)
			}
			w.state = CompletingWindow
		default:
			return w.state, fmt.Errorf("unknown marker kind %q", marker.Kind)
		}
	}
	return w.state, nil
}

// applySourceTransaction applies one source transaction worth of Kafka records
// to the destination. When inside a reconciliation window, any touched
// candidate key is removed from the window's candidate map. Passing a nil
// window applies the transaction without eviction (CDC-only mode).
func (r *Reconciler) applySourceTransaction(ctx context.Context, group []kafka.Record, w *window) error {
	var windows []*window
	if w != nil {
		w.mu.Lock()
		inside := w.state == InsideWindow
		w.mu.Unlock()
		if inside {
			windows = append(windows, w)
		}
	}
	return r.applySourceTransactionForWindows(ctx, group, windows)
}

func (r *Reconciler) applySourceTransactionForWindows(ctx context.Context, group []kafka.Record, windows []*window) error {
	if len(group) == 0 {
		return nil
	}
	if err := r.validateSourceGroup(group); err != nil {
		return err
	}
	if err := r.awaitCutoverPermission(ctx, group[len(group)-1].Offset+1); err != nil {
		return err
	}
	if group[len(group)-1].Offset < r.cp.NextKafkaOffset {
		return nil
	}
	source := group[0].Change.Source

	tx, err := r.beginOwned(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	alreadyApplied, err := r.cpStore.IsApplied(ctx, tx, r.cfg.JobID, source.Generation, source.LSN)
	if err != nil {
		return err
	}
	cdcCount := 0
	if !alreadyApplied {
		rowChanges := make([]model.Change, 0, len(group))
		keys := make([]int64, 0, len(group))
		for i := range group {
			rec := &group[i]
			if rec.Change.Row != nil {
				cdcCount++
				key, err := r.changeKey(&rec.Change)
				if err != nil {
					return fmt.Errorf("transaction %s change at offset %d: %w", source, rec.Offset, err)
				}
				keys = append(keys, key)
				rowChanges = append(rowChanges, rec.Change)
			} else if rec.Change.Marker == nil {
				return fmt.Errorf("change has no payload")
			}
		}
		if !r.disableEviction {
			if err := r.evictWindows(ctx, tx, windows, keys); err != nil {
				return err
			}
		}
		if err := r.applyRowChanges(ctx, tx, rowChanges); err != nil {
			return err
		}
		if err := r.cpStore.MarkApplied(ctx, tx, r.cfg.JobID, source.Generation, source.LSN, source.XID); err != nil {
			return err
		}
	}
	lastOffset := group[len(group)-1].Offset
	newLSN := source.LSN
	if alreadyApplied {
		newLSN = ""
	}
	nextCP := *r.cp
	if err := r.updateCheckpoint(ctx, tx, &nextCP, lastOffset, newLSN); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit source transaction: %w", err)
	}
	r.publishCheckpoint(nextCP)
	if cdcCount > 0 {
		r.metrics.RecordCDC(cdcCount)
	}
	return nil
}

func (r *Reconciler) evictWindows(ctx context.Context, tx pgx.Tx, windows []*window, keys []int64) error {
	store, _ := r.chunkStore.(durableCandidateStore)
	for _, w := range windows {
		if w == nil {
			continue
		}
		if w.staged {
			if err := store.EvictCandidates(ctx, tx, w.durable, keys); err != nil {
				return err
			}
			continue
		}
		w.mu.Lock()
		for _, key := range keys {
			if err := w.evictLocked(key); err != nil {
				w.mu.Unlock()
				return err
			}
		}
		w.mu.Unlock()
	}
	return nil
}

func (r *Reconciler) applyRowChanges(ctx context.Context, tx pgx.Tx, changes []model.Change) error {
	if len(changes) == 0 {
		return nil
	}
	if batched, ok := r.sink.(batchSink); ok {
		return batched.ApplyBatch(ctx, tx, changes)
	}
	for i := range changes {
		if err := r.sink.Apply(ctx, tx, changes[i]); err != nil {
			return err
		}
	}
	return nil
}

// applyChange applies one change. If insideWindow and the change touches a
// candidate key, that candidate is evicted from the window.
func (r *Reconciler) applyChange(ctx context.Context, tx pgx.Tx, change model.Change, insideWindow bool, w *window) error {
	switch {
	case change.Row != nil:
		key, err := r.changeKey(&change)
		if err != nil {
			return err
		}
		if insideWindow && !r.disableEviction && w != nil {
			w.mu.Lock()
			err := w.evictLocked(key)
			w.mu.Unlock()
			if err != nil {
				return err
			}
		}
		return r.sink.Apply(ctx, tx, change)
	case change.Marker != nil:
		// Markers carry no destination mutation.
		return nil
	default:
		return fmt.Errorf("change has no payload")
	}
}

// completeChunk commits the chunk completion transaction: surviving candidates,
// checkpoint cursor, chunk state, and Kafka progress through HIGH.
//
// Durability protocol: all writes inside this function execute inside a single
// destination transaction. The chunk status is moved to Committing, then the
// survivors and checkpoint are written, then the status is moved to Completed.
// Only when the transaction commits do any of these changes become durable.
// If the process crashes before commit, the transaction rolls back and the
// chunk remains in its previous state; recovery will retry it under a new
// attempt without ever exposing a partially-completed chunk.
func (r *Reconciler) completeChunk(ctx context.Context, highGroup []kafka.Record, chunk *model.Chunk, w *window) error {
	return r.completeChunkAt(ctx, highGroup, chunk, w, w.chunk.Max)
}

func (r *Reconciler) completeChunkAt(ctx context.Context, highGroup []kafka.Record, chunk *model.Chunk, w *window, completedThrough int64, evictionWindows ...*window) error {
	if err := r.validateSourceGroup(highGroup); err != nil {
		return err
	}
	if err := r.awaitCutoverPermission(ctx, highGroup[len(highGroup)-1].Offset+1); err != nil {
		return err
	}
	if err := r.maybeWait(ctx, failpoint.AfterHighMarkerObserved); err != nil {
		return err
	}

	tx, err := r.beginOwned(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if r.chunkStore != nil {
		chunk.Status = model.ChunkCommitting
		chunk.HighLSN = &highGroup[0].Change.Source.LSN
		off := highGroup[len(highGroup)-1].Offset
		chunk.HighOffset = &off
		if err := r.chunkStore.UpdateChunk(ctx, tx, chunk); err != nil {
			return fmt.Errorf("update chunk committing: %w", err)
		}
	}

	// Apply any account changes that happened to be in the same source
	// transaction as the HIGH marker.
	source := highGroup[0].Change.Source
	cdcCount := 0
	rowChanges := make([]model.Change, 0, len(highGroup))
	keys := make([]int64, 0, len(highGroup))
	for i := range highGroup {
		rec := &highGroup[i]
		if rec.Change.Row != nil {
			cdcCount++
			key, err := r.changeKey(&rec.Change)
			if err != nil {
				return err
			}
			keys = append(keys, key)
			rowChanges = append(rowChanges, rec.Change)
		}
	}
	if len(evictionWindows) == 0 {
		evictionWindows = []*window{w}
	}
	if !r.disableEviction {
		if err := r.evictWindows(ctx, tx, evictionWindows, keys); err != nil {
			return err
		}
	}
	if err := r.applyRowChanges(ctx, tx, rowChanges); err != nil {
		return err
	}
	if err := r.cpStore.MarkApplied(ctx, tx, r.cfg.JobID, source.Generation, source.LSN, source.XID); err != nil {
		return err
	}

	if err := r.maybeWait(ctx, failpoint.BeforeChunkCommit); err != nil {
		return err
	}

	candidateCount := w.candidateCount
	if !w.staged && candidateCount == 0 {
		candidateCount = len(w.candidates)
	}
	var survivorCount int
	if w.staged {
		written, err := r.sink.(stagedCandidateSink).WriteStagedCandidates(ctx, tx, chunk)
		if err != nil {
			return err
		}
		survivorCount = int(written)
		if err := r.chunkStore.(durableCandidateStore).ClearCandidates(ctx, tx, chunk); err != nil {
			return err
		}
	} else {
		w.mu.Lock()
		survivors := make([]model.Row, 0, len(w.candidates))
		for _, row := range w.candidates {
			survivors = append(survivors, row)
		}
		w.candidates = nil
		w.mu.Unlock()
		survivorCount = len(survivors)
		if err := r.sink.WriteCandidates(ctx, tx, survivors); err != nil {
			return fmt.Errorf("write survivors: %w", err)
		}
	}

	if r.chunkStore != nil {
		chunk.RowsApplied = int64(cdcCount)
	}

	nextCP := *r.cp
	nextCP.CompletedThrough = completedThrough
	lastOffset := highGroup[len(highGroup)-1].Offset
	if err := r.updateCheckpoint(ctx, tx, &nextCP, lastOffset, source.LSN); err != nil {
		return err
	}

	if r.chunkStore != nil {
		chunk.Status = model.ChunkCompleted
		if err := r.chunkStore.UpdateChunk(ctx, tx, chunk); err != nil {
			return fmt.Errorf("update chunk completed: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit chunk completion: %w", err)
	}
	r.publishCheckpoint(nextCP)
	if cdcCount > 0 {
		r.metrics.RecordCDC(cdcCount)
	}
	if err := r.maybeWait(ctx, failpoint.AfterDestinationCommit); err != nil {
		return err
	}

	log.Printf("seam: chunk %s complete survivors=%d", w.chunk, survivorCount)
	r.metrics.RecordChunk(telemetry.ChunkStats{Candidates: candidateCount, Survivors: survivorCount})
	return nil
}

func (r *Reconciler) validateSourceGroup(group []kafka.Record) error {
	if len(group) == 0 {
		return fmt.Errorf("empty source transaction")
	}
	source := group[0].Change.Source
	if source.Generation != r.cp.Generation || source.LSN == "" {
		return fmt.Errorf("source transaction %s does not match job generation %s", source, r.cp.Generation)
	}
	if r.cfg.SourceSystemID != "" && source.SystemID != r.cfg.SourceSystemID {
		return fmt.Errorf("source system identifier %q does not match pinned %q", source.SystemID, r.cfg.SourceSystemID)
	}
	if err := r.validateSchemaEpoch(group); err != nil {
		return err
	}
	for _, record := range group[1:] {
		if record.Offset != group[0].Offset || record.Change.Source != source {
			return fmt.Errorf("Kafka record at offset %d mixes source transactions", group[0].Offset)
		}
	}
	return nil
}

// validateSchemaEpoch fails closed when any row change in the source
// transaction carries a schema epoch different from the one the job recorded.
// A schema drift under a running job must stop the pipeline, never be applied.
func (r *Reconciler) validateSchemaEpoch(group []kafka.Record) error {
	if r.schemaID == "" {
		return nil
	}
	for _, rec := range group {
		if rec.Change.Row == nil {
			continue
		}
		if rec.Change.SchemaID != r.schemaID {
			return fmt.Errorf("row change at offset %d carries schema epoch %q; job pins %q", rec.Offset, rec.Change.SchemaID, r.schemaID)
		}
	}
	return nil
}

func (r *Reconciler) sourceAlreadyApplied(ctx context.Context, source model.SourceTx) (bool, error) {
	tx, err := r.beginOwned(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	return r.cpStore.IsApplied(ctx, tx, r.cfg.JobID, source.Generation, source.LSN)
}

// updateCheckpoint persists a candidate checkpoint; the caller publishes it
// to memory only after the destination transaction commits.
func (r *Reconciler) updateCheckpoint(ctx context.Context, tx pgx.Tx, cp *model.Checkpoint, offset int64, lsn string) error {
	if offset+1 < cp.NextKafkaOffset {
		return fmt.Errorf("Kafka offset would regress from %d to %d", cp.NextKafkaOffset, offset+1)
	}
	expectedOffset := cp.NextKafkaOffset
	cp.NextKafkaOffset = offset + 1
	if lsn != "" {
		cp.LastAppliedLSN = lsn
	}
	return r.cpStore.UpdateCheckpoint(ctx, tx, cp, expectedOffset)
}

// flushBatch applies a batch of records during CDC-only mode.
func (r *Reconciler) flushBatch(ctx context.Context, records []kafka.Record) error {
	if err := r.checkBatchBounds(records); err != nil {
		return err
	}
	groups := groupByTransaction(records)
	for _, group := range groups {
		if err := r.applySourceTransaction(ctx, group, nil); err != nil {
			return err
		}
	}
	return nil
}

// checkBatchBounds limits Kafka envelopes rather than flattened row changes.
// One envelope is a complete source transaction and may contain many changes.
func (r *Reconciler) checkBatchBounds(records []kafka.Record) error {
	if r.cfg.MaxRecordsPerBatch <= 0 {
		return nil
	}
	seen := make(map[int64]struct{})
	for _, record := range records {
		seen[record.Offset] = struct{}{}
	}
	if len(seen) > r.cfg.MaxRecordsPerBatch {
		return fmt.Errorf("batch of %d Kafka records exceeds max records per batch %d", len(seen), r.cfg.MaxRecordsPerBatch)
	}
	return nil
}

// groupByTransaction groups changes by Kafka offset. The transport guarantees
// one complete source transaction per Kafka record; two records with the same
// source LSN are retries and must be deduplicated separately.
func groupByTransaction(records []kafka.Record) [][]kafka.Record {
	if len(records) == 0 {
		return nil
	}
	var groups [][]kafka.Record
	var current []kafka.Record
	var currentOffset int64
	first := true
	for _, rec := range records {
		if first || currentOffset != rec.Offset {
			if len(current) > 0 {
				groups = append(groups, current)
			}
			current = nil
			currentOffset = rec.Offset
			first = false
		}
		current = append(current, rec)
	}
	if len(current) > 0 {
		groups = append(groups, current)
	}
	return groups
}

func (r *Reconciler) maybeWait(ctx context.Context, name failpoint.Name) error {
	p := r.failpoints.Get(name)
	if p == nil {
		return nil
	}
	log.Printf("seam: failpoint waiting at %s", p)
	if err := p.Reach(ctx); err != nil {
		return err
	}
	log.Printf("seam: failpoint resumed at %s", p)
	return nil
}

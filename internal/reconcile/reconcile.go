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
	"example.com/seam/internal/telemetry"
	"github.com/jackc/pgx/v5"
)

// Reconciler owns destination application during a Seam backfill.
type Reconciler struct {
	cfg        model.JobConfig
	cp         *model.Checkpoint
	consumer   consumer
	cpStore    checkpointStore
	chunkStore chunkStore
	marker     markerStore
	scanner    scanner
	sink       sink
	failpoints *failpoint.Registry
	metrics    *telemetry.Metrics
	codec           capture.JSONCodec
	disableEviction bool

	// mutable state protected by mu during a chunk
	mu          sync.Mutex
	chunk       model.ChunkRange
	candidates  map[int64]model.Account
	windowState WindowState
}

// consumer reads decoded Kafka records.
type consumer interface {
	Poll(ctx context.Context) ([]kafka.Record, error)
	Close()
}

// checkpointStore persists job progress and applied-transaction bookkeeping.
type checkpointStore interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	UpdateCheckpoint(ctx context.Context, tx pgx.Tx, cp *model.Checkpoint) error
	MarkApplied(ctx context.Context, tx pgx.Tx, jobID, generation, lsn string, xid uint32) error
}

// markerStore writes LOW/HIGH control markers on the source.
type markerStore interface {
	WriteLow(ctx context.Context, jobID, attempt string, chunk model.ChunkRange) (string, error)
	WriteHigh(ctx context.Context, jobID, attempt string, chunk model.ChunkRange) (string, error)
}

// scanner reads bounded primary-key chunks from the source table.
type scanner interface {
	NextChunk(ctx context.Context, completedThrough, upperBound int64, chunkSize int) (model.ChunkRange, bool, error)
	ReadChunk(ctx context.Context, minID, maxID int64) ([]model.Account, error)
}

// sink applies row changes and surviving snapshot candidates to the destination.
type sink interface {
	Apply(ctx context.Context, tx pgx.Tx, change model.Change) error
	WriteCandidates(ctx context.Context, tx pgx.Tx, candidates []model.Account) error
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
	Failpoints      *failpoint.Registry
	Metrics         *telemetry.Metrics
	// DisableEviction is a deliberately broken mode used by Phase 2 tests to
	// demonstrate stale overwrite and delete resurrection.
	DisableEviction bool
}

// chunkStore persists per-chunk lifecycle state and leases.
type chunkStore interface {
	LeaseChunk(ctx context.Context, jobID, workerID string, leaseDuration time.Duration) (*model.Chunk, error)
	UpdateChunk(ctx context.Context, tx pgx.Tx, chunk *model.Chunk) error
}

// New builds a reconciler. It does not start consuming.
func New(cfg Config) *Reconciler {
	r := &Reconciler{
		cfg:             cfg.JobConfig,
		cp:              cfg.Checkpoint,
		consumer:        cfg.Consumer,
		cpStore:         cfg.CheckpointStore,
		chunkStore:      cfg.ChunkStore,
		marker:          cfg.MarkerStore,
		scanner:         cfg.Scanner,
		sink:            cfg.Sink,
		failpoints:      cfg.Failpoints,
		metrics:         cfg.Metrics,
		disableEviction: cfg.DisableEviction,
	}
	if r.failpoints == nil {
		r.failpoints = failpoint.NewRegistry()
	}
	if r.metrics == nil {
		r.metrics = telemetry.NewMetrics()
	}
	return r
}

// Run drives the backfill and then continues applying live CDC. It returns when
// ctx is cancelled or a fatal error occurs.
func (r *Reconciler) Run(ctx context.Context) error {
	log.Printf("seam: starting job=%s generation=%s attempt=%s completed_through=%d upper_bound=%d next_offset=%d",
		r.cp.JobID, r.cp.Generation, r.cp.Attempt, r.cp.CompletedThrough, r.cp.ScanUpperBound, r.cp.NextKafkaOffset)

	if r.chunkStore != nil {
		return r.runChunkStoreLoop(ctx)
	}
	return r.runLegacyLoop(ctx)
}

// runLegacyLoop uses the scanner to discover chunks on the fly. It is used
// when no chunk store is configured.
func (r *Reconciler) runLegacyLoop(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunkRange, ok, err := r.scanner.NextChunk(ctx, r.cp.CompletedThrough, r.cp.ScanUpperBound, r.cfg.ChunkSize)
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
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk, err := r.chunkStore.LeaseChunk(ctx, r.cfg.JobID, r.cfg.WorkerID, r.cfg.LeaseDuration)
		if err != nil {
			return fmt.Errorf("lease chunk: %w", err)
		}
		if chunk == nil {
			log.Printf("seam: backfill complete, continuing cdc-only mode")
			return r.runCDCOnly(ctx)
		}
		chunk.Attempt = r.cp.Attempt
		chunk.Status = model.ChunkScanning
		if err := r.runChunk(ctx, chunk); err != nil {
			return err
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
	r.mu.Lock()
	r.chunk = chunkRange
	r.windowState = OutsideWindow
	r.mu.Unlock()

	log.Printf("seam: chunk %s starting", chunkRange)

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
	rows, err := r.scanner.ReadChunk(ctx, chunkRange.Min, chunkRange.Max)
	if err != nil {
		return fmt.Errorf("read chunk %s: %w", chunkRange, err)
	}
	r.mu.Lock()
	r.candidates = make(map[int64]model.Account, len(rows))
	for _, account := range rows {
		r.candidates[account.ID] = account
	}
	r.mu.Unlock()
	log.Printf("seam: chunk %s read %d candidates", chunkRange, len(rows))

	if r.chunkStore != nil {
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
		records, err := r.consumer.Poll(ctx)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			continue
		}
		done, err := r.processChunkRecords(ctx, records, chunk)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

// updateChunkState persists the current chunk state when a chunk store is configured.
func (r *Reconciler) updateChunkState(ctx context.Context, chunk *model.Chunk) error {
	if r.chunkStore == nil {
		return nil
	}
	tx, err := r.cpStore.Begin(ctx)
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
func (r *Reconciler) processChunkRecords(ctx context.Context, records []kafka.Record, chunk *model.Chunk) (bool, error) {
	r.mu.Lock()
	state := r.windowState
	r.mu.Unlock()

	// Group records by source transaction.
	groups := groupByTransaction(records)

	for idx, group := range groups {
		// Determine the effective window state for this group based on markers.
		newState, err := r.evaluateMarkers(group, state)
		if err != nil {
			return false, err
		}
		state = newState

		r.mu.Lock()
		r.windowState = state
		r.mu.Unlock()

		// If the group contains the HIGH marker for the current attempt, the
		// chunk completion transaction includes surviving candidates.
		if state == CompletingWindow {
			if err := r.completeChunk(ctx, group, chunk); err != nil {
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
				if _, err := r.evaluateMarkers(g, OutsideWindow); err != nil {
					return false, err
				}
				if err := r.applySourceTransaction(ctx, g, false); err != nil {
					return false, err
				}
			}
			return true, nil
		}

		// Otherwise apply the source transaction normally, evicting candidates
		// that are touched while inside the window.
		if err := r.applySourceTransaction(ctx, group, state == InsideWindow); err != nil {
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
// current attempt and updates the window state.
func (r *Reconciler) evaluateMarkers(group []kafka.Record, state WindowState) (WindowState, error) {
	for _, rec := range group {
		if rec.Change.Marker == nil {
			continue
		}
		marker := rec.Change.Marker
		if marker.JobID != r.cfg.JobID || marker.Attempt != r.cp.Attempt {
			// Stale marker from a previous attempt; ignore.
			continue
		}
		switch marker.Kind {
		case model.MarkerLow:
			if state != OutsideWindow {
				return state, fmt.Errorf("unexpected LOW marker %s in state %d (window chunk=%s)", marker.ID, state, r.chunk)
			}
			state = InsideWindow
		case model.MarkerHigh:
			if state != InsideWindow {
				return state, fmt.Errorf("unexpected HIGH marker %s in state %d (window chunk=%s)", marker.ID, state, r.chunk)
			}
			state = CompletingWindow
		default:
			return state, fmt.Errorf("unknown marker kind %q", marker.Kind)
		}
	}
	return state, nil
}

// applySourceTransaction applies one source transaction worth of Kafka records
// to the destination. When inside the reconciliation window, any touched
// candidate key is removed from the candidate map.
func (r *Reconciler) applySourceTransaction(ctx context.Context, group []kafka.Record, insideWindow bool) error {
	if len(group) == 0 {
		return nil
	}
	source := group[0].Change.Source

	tx, err := r.cpStore.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// NOTE: A source transaction may be split across two Kafka poll batches.
	// Its continuation group shares the same commit LSN, so IsApplied alone
	// cannot gate application. Every account operation below is idempotent
	// (upsert of an exact row value / delete by id), so re-applying a group is
	// always safe. MarkApplied still records the LSN for bookkeeping.
	cdcCount := 0
	for _, rec := range group {
		if err := r.applyChange(ctx, tx, rec.Change, insideWindow); err != nil {
			return err
		}
		if rec.Change.Account != nil {
			cdcCount++
		}
	}
	if cdcCount > 0 {
		r.metrics.RecordCDC(cdcCount)
	}
	if err := r.cpStore.MarkApplied(ctx, tx, r.cfg.JobID, source.Generation, source.LSN, source.XID); err != nil {
		return err
	}

	lastOffset := group[len(group)-1].Offset
	if err := r.updateCheckpoint(ctx, tx, lastOffset, source.LSN); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit source transaction: %w", err)
	}
	return nil
}

// applyChange applies one change. If insideWindow and the change touches a
// candidate key, that candidate is evicted.
func (r *Reconciler) applyChange(ctx context.Context, tx pgx.Tx, change model.Change, insideWindow bool) error {
	switch {
	case change.Account != nil:
		if insideWindow && !r.disableEviction {
			r.mu.Lock()
			delete(r.candidates, change.Account.ID)
			r.mu.Unlock()
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
func (r *Reconciler) completeChunk(ctx context.Context, highGroup []kafka.Record, chunk *model.Chunk) error {
	if err := r.maybeWait(ctx, failpoint.AfterHighMarkerObserved); err != nil {
		return err
	}

	tx, err := r.cpStore.Begin(ctx)
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
	// transaction as the HIGH marker (they are idempotent, so unconditional
	// application is safe).
	source := highGroup[0].Change.Source
	cdcCount := 0
	for _, rec := range highGroup {
		if rec.Change.Account != nil {
			cdcCount++
			if !r.disableEviction {
				r.mu.Lock()
				delete(r.candidates, rec.Change.Account.ID)
				r.mu.Unlock()
			}
			if err := r.sink.Apply(ctx, tx, rec.Change); err != nil {
				return err
			}
		}
	}
	if cdcCount > 0 {
		r.metrics.RecordCDC(cdcCount)
	}
	if err := r.cpStore.MarkApplied(ctx, tx, r.cfg.JobID, source.Generation, source.LSN, source.XID); err != nil {
		return err
	}

	if err := r.maybeWait(ctx, failpoint.BeforeChunkCommit); err != nil {
		return err
	}

	r.mu.Lock()
	candidateCount := len(r.candidates)
	survivors := make([]model.Account, 0, candidateCount)
	for _, account := range r.candidates {
		survivors = append(survivors, account)
	}
	r.candidates = nil
	r.mu.Unlock()

	if r.chunkStore != nil {
		chunk.RowsApplied = int64(cdcCount)
	}

	if err := r.sink.WriteCandidates(ctx, tx, survivors); err != nil {
		return fmt.Errorf("write survivors: %w", err)
	}

	r.cp.CompletedThrough = r.chunk.Max
	lastOffset := highGroup[len(highGroup)-1].Offset
	if err := r.updateCheckpoint(ctx, tx, lastOffset, source.LSN); err != nil {
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
	if err := r.maybeWait(ctx, failpoint.AfterDestinationCommit); err != nil {
		return err
	}

	log.Printf("seam: chunk %s complete survivors=%d", r.chunk, len(survivors))
	r.metrics.RecordChunk(telemetry.ChunkStats{Candidates: candidateCount, Survivors: len(survivors)})
	return nil
}

// updateCheckpoint mutates the in-memory checkpoint and persists it.
func (r *Reconciler) updateCheckpoint(ctx context.Context, tx pgx.Tx, offset int64, lsn string) error {
	r.cp.NextKafkaOffset = offset + 1
	if lsn != "" {
		r.cp.LastAppliedLSN = lsn
	}
	return r.cpStore.UpdateCheckpoint(ctx, tx, r.cp)
}

// flushBatch applies a batch of records during CDC-only mode.
func (r *Reconciler) flushBatch(ctx context.Context, records []kafka.Record) error {
	groups := groupByTransaction(records)
	for _, group := range groups {
		if err := r.applySourceTransaction(ctx, group, false); err != nil {
			return err
		}
	}
	return nil
}

// groupByTransaction splits records into contiguous groups by source transaction.
func groupByTransaction(records []kafka.Record) [][]kafka.Record {
	if len(records) == 0 {
		return nil
	}
	var groups [][]kafka.Record
	var current []kafka.Record
	var currentSource *model.SourceTx
	for _, rec := range records {
		src := &rec.Change.Source
		if currentSource == nil || *currentSource != *src {
			if len(current) > 0 {
				groups = append(groups, current)
			}
			current = nil
			currentSource = src
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



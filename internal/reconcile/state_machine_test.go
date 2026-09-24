package reconcile

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"example.com/seam/internal/failpoint"
	"example.com/seam/internal/kafka"
	"example.com/seam/internal/model"
	"github.com/jackc/pgx/v5"
)

// transactionalModelSink is an in-memory destination with transaction
// semantics. Mutations become visible only when the fake pgx transaction
// commits, so a cancellation before completeChunk's commit accurately models
// destination rollback instead of leaking writes from an aborted attempt.
type transactionalModelSink struct {
	mu         sync.Mutex
	rows       map[int64]model.Row
	applyCount map[string]int
}

func newTransactionalModelSink() *transactionalModelSink {
	return &transactionalModelSink{
		rows:       make(map[int64]model.Row),
		applyCount: make(map[string]int),
	}
}

func (s *transactionalModelSink) Apply(_ context.Context, tx pgx.Tx, change model.Change) error {
	if change.Row == nil {
		return fmt.Errorf("model sink received change without row payload")
	}
	row := *change.Row
	op := change.Op
	lsn := change.Source.LSN
	modelTx, ok := tx.(*fakeTx)
	if !ok {
		return fmt.Errorf("model sink requires fakeTx, got %T", tx)
	}
	modelTx.onCommit = append(modelTx.onCommit, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if op == model.OpDelete {
			delete(s.rows, rowID(&row))
		} else {
			s.rows[rowID(&row)] = row
		}
		s.applyCount[lsn]++
	})
	return nil
}

func (s *transactionalModelSink) WriteCandidates(_ context.Context, tx pgx.Tx, candidates []model.Row) error {
	cloned := append([]model.Row(nil), candidates...)
	modelTx, ok := tx.(*fakeTx)
	if !ok {
		return fmt.Errorf("model sink requires fakeTx, got %T", tx)
	}
	modelTx.onCommit = append(modelTx.onCommit, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, row := range cloned {
			s.rows[rowID(&row)] = row
		}
	})
	return nil
}

func (s *transactionalModelSink) snapshot() map[int64]model.Row {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneAccounts(s.rows)
}

func (s *transactionalModelSink) counts() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int, len(s.applyCount))
	for lsn, count := range s.applyCount {
		out[lsn] = count
	}
	return out
}

// strictModelCheckpointStore is the durable state used across simulated
// process restarts. It enforces the same offset compare-and-swap contract as
// the PostgreSQL store and records every committed offset for monotonicity
// assertions.
type strictModelCheckpointStore struct {
	mu            sync.Mutex
	cp            model.Checkpoint
	applied       map[string]bool
	offsetHistory []int64
	invariantErr  error
}

func newStrictModelCheckpointStore(cp model.Checkpoint) *strictModelCheckpointStore {
	return &strictModelCheckpointStore{
		cp:            cp,
		applied:       make(map[string]bool),
		offsetHistory: []int64{cp.NextKafkaOffset},
	}
}

func (s *strictModelCheckpointStore) Begin(context.Context) (pgx.Tx, error) {
	return &fakeTx{}, nil
}

func (s *strictModelCheckpointStore) AssertLeadership(context.Context, pgx.Tx, *model.Checkpoint) error {
	return nil
}

func (s *strictModelCheckpointStore) IsApplied(_ context.Context, _ pgx.Tx, jobID, generation, lsn string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applied[modelAppliedKey(jobID, generation, lsn)], nil
}

func (s *strictModelCheckpointStore) MarkApplied(_ context.Context, tx pgx.Tx, jobID, generation, lsn string, _ uint32) error {
	modelTx, ok := tx.(*fakeTx)
	if !ok {
		return fmt.Errorf("model checkpoint store requires fakeTx, got %T", tx)
	}
	key := modelAppliedKey(jobID, generation, lsn)
	modelTx.onCommit = append(modelTx.onCommit, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.applied[key] = true
	})
	return nil
}

func (s *strictModelCheckpointStore) UpdateCheckpoint(_ context.Context, tx pgx.Tx, cp *model.Checkpoint, expectedOffset int64) error {
	modelTx, ok := tx.(*fakeTx)
	if !ok {
		return fmt.Errorf("model checkpoint store requires fakeTx, got %T", tx)
	}
	s.mu.Lock()
	current := s.cp
	s.mu.Unlock()
	if current.JobID != cp.JobID || current.Generation != cp.Generation || current.Attempt != cp.Attempt {
		return fmt.Errorf("checkpoint identity changed: durable=%s/%s/%s candidate=%s/%s/%s",
			current.JobID, current.Generation, current.Attempt, cp.JobID, cp.Generation, cp.Attempt)
	}
	if current.NextKafkaOffset != expectedOffset {
		return fmt.Errorf("checkpoint compare-and-swap failed: durable=%d expected=%d", current.NextKafkaOffset, expectedOffset)
	}
	if cp.NextKafkaOffset < current.NextKafkaOffset {
		return fmt.Errorf("checkpoint offset regressed: durable=%d candidate=%d", current.NextKafkaOffset, cp.NextKafkaOffset)
	}
	next := *cp
	modelTx.onCommit = append(modelTx.onCommit, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.cp.NextKafkaOffset != expectedOffset && s.invariantErr == nil {
			s.invariantErr = fmt.Errorf("checkpoint changed before commit: durable=%d expected=%d", s.cp.NextKafkaOffset, expectedOffset)
			return
		}
		if next.NextKafkaOffset < s.cp.NextKafkaOffset && s.invariantErr == nil {
			s.invariantErr = fmt.Errorf("checkpoint commit regressed: durable=%d candidate=%d", s.cp.NextKafkaOffset, next.NextKafkaOffset)
			return
		}
		s.cp = next
		s.offsetHistory = append(s.offsetHistory, next.NextKafkaOffset)
	})
	return nil
}

func (s *strictModelCheckpointStore) checkpoint() model.Checkpoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cp
}

func (s *strictModelCheckpointStore) beginAttempt(attempt string) model.Checkpoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cp.Attempt = attempt
	return s.cp
}

func (s *strictModelCheckpointStore) assertInvariants(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.invariantErr != nil {
		t.Fatal(s.invariantErr)
	}
	for i := 1; i < len(s.offsetHistory); i++ {
		if s.offsetHistory[i] < s.offsetHistory[i-1] {
			t.Fatalf("checkpoint history regressed at %d: %v", i, s.offsetHistory)
		}
	}
}

func modelAppliedKey(jobID, generation, lsn string) string {
	return jobID + "\x00" + generation + "\x00" + lsn
}

type generatedScenario struct {
	initial           map[int64]model.Row
	sourceAfterWindow map[int64]model.Row
	expectedFinal     map[int64]model.Row
	oldStream         []kafka.Record
	postStream        []model.Change
	wantApplyCount    map[string]int
}

func generateScenario(seed int64) generatedScenario {
	rng := rand.New(rand.NewSource(seed))
	initial := make(map[int64]model.Row)
	for id := int64(1); id <= 6; id++ {
		initial[id] = account(id, fmt.Sprintf("initial-%d", id), id*100)
	}
	source := cloneAccounts(initial)
	wantApplyCount := make(map[string]int)
	chunk := model.ChunkRange{Min: 1, Max: 6}
	stream := []kafka.Record{rec(0, lowMarker("model-job", "gen:0:attempt:0", chunk))}
	offset := int64(1)
	var firstTransaction []model.Change

	for txIndex := 0; txIndex < 6; txIndex++ {
		lsn := fmt.Sprintf("seed-%d-tx-%d", seed, txIndex)
		changeCount := 1 + rng.Intn(3)
		transaction := make([]model.Change, 0, changeCount)
		for changeIndex := 0; changeIndex < changeCount; changeIndex++ {
			id := int64(1 + rng.Intn(9))
			op := []model.Operation{model.OpInsert, model.OpUpdate, model.OpDelete}[rng.Intn(3)]
			change := model.Change{
				Op:       op,
				SchemaID: testSourceSchema().Fingerprint,
				Row:      accountPtr(id, fmt.Sprintf("s%d-t%d-c%d", seed, txIndex, changeIndex), int64(rng.Intn(100000))),
				Source:   model.SourceTx{Generation: "gen:0", LSN: lsn, XID: uint32(txIndex + 1)},
			}
			transaction = append(transaction, change)
			applyReference(source, change)
			wantApplyCount[lsn]++
		}
		if txIndex == 0 {
			firstTransaction = append([]model.Change(nil), transaction...)
		}
		for _, change := range transaction {
			stream = append(stream, kafka.Record{Offset: offset, Change: change})
		}
		offset++
		if txIndex == 1 {
			// A producer retry republishes the complete first transaction at a
			// new Kafka offset with the same source LSN.
			for _, change := range firstTransaction {
				stream = append(stream, kafka.Record{Offset: offset, Change: change})
			}
			offset++
		}
	}
	stream = append(stream, rec(offset, highMarker("model-job", "gen:0:attempt:0", chunk)))

	post := model.Change{
		Op:       model.OpInsert,
		SchemaID: testSourceSchema().Fingerprint,
		Row:      accountPtr(12, fmt.Sprintf("post-%d", seed), seed+9000),
		Source:   model.SourceTx{Generation: "gen:0", LSN: fmt.Sprintf("seed-%d-post", seed), XID: 1000},
	}
	expected := cloneAccounts(source)
	applyReference(expected, post)
	wantApplyCount[post.Source.LSN] = 1

	return generatedScenario{
		initial:           initial,
		sourceAfterWindow: source,
		expectedFinal:     expected,
		oldStream:         stream,
		postStream:        []model.Change{post, post}, // duplicate at a new Kafka offset
		wantApplyCount:    wantApplyCount,
	}
}

func applyReference(rows map[int64]model.Row, change model.Change) {
	if change.Op == model.OpDelete {
		delete(rows, rowID(change.Row))
		return
	}
	rows[rowID(change.Row)] = *change.Row
}

func cloneAccounts(in map[int64]model.Row) map[int64]model.Row {
	out := make(map[int64]model.Row, len(in))
	for id, row := range in {
		out[id] = row
	}
	return out
}

func recordsAtOrAfter(records []kafka.Record, offset int64) []kafka.Record {
	out := make([]kafka.Record, 0, len(records))
	for _, record := range records {
		if record.Offset >= offset {
			out = append(out, record)
		}
	}
	return out
}

func runUntilCrash(t *testing.T, cfg Config, point failpoint.Name) {
	t.Helper()
	registry := failpoint.NewRegistry()
	barrier := registry.Install(point)
	cfg.Failpoints = registry
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	done := make(chan error, 1)
	go func() { done <- New(cfg).Run(ctx) }()
	if err := barrier.WaitForTrigger(ctx); err != nil {
		cancel()
		t.Fatalf("failpoint %s was not reached: %v", point, err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("first run at %s: %v", point, err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("reconciler did not stop at %s", point)
	}
}

func modelConfig(cp *model.Checkpoint, store *strictModelCheckpointStore, sink *transactionalModelSink, rows map[int64]model.Row, records []kafka.Record) Config {
	return Config{
		JobConfig:       model.JobConfig{JobID: cp.JobID, ChunkSize: 100},
		Checkpoint:      cp,
		Consumer:        &fakeConsumer{batches: [][]kafka.Record{records}},
		CheckpointStore: store,
		MarkerStore:     &fakeMarkerStore{},
		Scanner:         &fakeScanner{upperBound: cp.ScanUpperBound, rows: rows},
		Sink:            sink,
		SourceSchema:    testSourceSchema(),
	}
}

// TestReconciler_DeterministicCrashReplayModel exercises the production
// reconciler state machine against an independent reference model. Every seed
// includes multi-row transactions, mixed operations, a replayed source
// transaction, a process crash, an attempt restart when needed, and a
// duplicate post-backfill transaction.
func TestReconciler_DeterministicCrashReplayModel(t *testing.T) {
	points := []failpoint.Name{
		failpoint.AfterLowMarkerCommitted,
		failpoint.AfterChunkReadBeforeReconciliation,
		failpoint.AfterHighMarkerObserved,
		failpoint.BeforeChunkCommit,
		failpoint.AfterDestinationCommit,
	}
	for seed := int64(1); seed <= 12; seed++ {
		scenario := generateScenario(seed)
		for _, point := range points {
			t.Run(fmt.Sprintf("seed_%02d/%s", seed, point), func(t *testing.T) {
				initialCP := model.Checkpoint{
					JobID:            "model-job",
					Generation:       "gen:0",
					Attempt:          "gen:0:attempt:0",
					ScanUpperBound:   6,
					CompletedThrough: 0,
					NextKafkaOffset:  0,
					Active:           true,
				}
				store := newStrictModelCheckpointStore(initialCP)
				sink := newTransactionalModelSink()
				firstCP := initialCP
				first := modelConfig(&firstCP, store, sink, scenario.initial, scenario.oldStream)
				runUntilCrash(t, first, point)

				durable := store.checkpoint()
				if durable.CompletedThrough < durable.ScanUpperBound {
					durable = store.beginAttempt("gen:0:attempt:1")
				}
				recoveryRecords := recordsAtOrAfter(scenario.oldStream, durable.NextKafkaOffset)
				nextOffset := scenario.oldStream[len(scenario.oldStream)-1].Offset + 1
				if durable.CompletedThrough < durable.ScanUpperBound {
					chunk := model.ChunkRange{Min: 1, Max: durable.ScanUpperBound}
					recoveryRecords = append(recoveryRecords,
						rec(nextOffset, lowMarker(durable.JobID, durable.Attempt, chunk)),
						rec(nextOffset+1, highMarker(durable.JobID, durable.Attempt, chunk)),
					)
					nextOffset += 2
				}
				for _, change := range scenario.postStream {
					recoveryRecords = append(recoveryRecords, kafka.Record{Offset: nextOffset, Change: change})
					nextOffset++
				}

				recoveryCP := durable
				recovery := modelConfig(&recoveryCP, store, sink, scenario.sourceAfterWindow, recoveryRecords)
				if err := runReconciler(t, recovery, 3*time.Second); err != nil {
					t.Fatalf("recovery run: %v", err)
				}

				if got := sink.snapshot(); !reflect.DeepEqual(got, scenario.expectedFinal) {
					t.Fatalf("final destination mismatch\n got: %s\nwant: %s", formatRows(got), formatRows(scenario.expectedFinal))
				}
				if got := sink.counts(); !reflect.DeepEqual(got, scenario.wantApplyCount) {
					t.Fatalf("source transaction application counts = %v, want %v", got, scenario.wantApplyCount)
				}
				finalCP := store.checkpoint()
				if finalCP.CompletedThrough != finalCP.ScanUpperBound {
					t.Fatalf("backfill incomplete: completed_through=%d upper_bound=%d", finalCP.CompletedThrough, finalCP.ScanUpperBound)
				}
				if finalCP.NextKafkaOffset != nextOffset {
					t.Fatalf("checkpoint next offset=%d, want %d", finalCP.NextKafkaOffset, nextOffset)
				}
				store.assertInvariants(t)
			})
		}
	}
}

func formatRows(rows map[int64]model.Row) string {
	ids := make([]int64, 0, len(rows))
	for id := range rows {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		row := rows[id]
		parts = append(parts, fmt.Sprintf("%d=%s/%d", id, rowOwner(&row), rowBalance(&row)))
	}
	return fmt.Sprintf("%v", parts)
}

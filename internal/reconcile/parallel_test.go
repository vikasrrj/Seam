package reconcile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/seam/internal/kafka"
	"example.com/seam/internal/model"
	"github.com/jackc/pgx/v5"
)

// ---------- parallel-mode fakes ----------

// chanConsumer delivers scripted batches over a channel: Poll blocks until a
// batch is pushed, and returns io.EOF once the channel is closed. This lets
// tests orchestrate stream timing deterministically around worker progress.
type chanConsumer struct {
	ch chan []kafka.Record
}

func newChanConsumer() *chanConsumer {
	return &chanConsumer{ch: make(chan []kafka.Record, 16)}
}

func (c *chanConsumer) Poll(ctx context.Context) ([]kafka.Record, error) {
	select {
	case batch, ok := <-c.ch:
		if !ok {
			return nil, io.EOF
		}
		return batch, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *chanConsumer) Close() {}

func (c *chanConsumer) push(batch []kafka.Record) { c.ch <- batch }

func (c *chanConsumer) finish() { close(c.ch) }

// safeCheckpointStore is a concurrency-safe checkpoint store that hands out a
// fresh transaction per Begin so worker and coordinator goroutines never share
// mutable fakeTx state.
type safeCheckpointStore struct {
	mu          sync.Mutex
	checkpoints []*model.Checkpoint
	applied     []model.SourceTx
}

func (s *safeCheckpointStore) Begin(ctx context.Context) (pgx.Tx, error) { return &fakeTx{}, nil }
func (s *safeCheckpointStore) AssertLeadership(context.Context, pgx.Tx, *model.Checkpoint) error {
	return nil
}

func (s *safeCheckpointStore) UpdateCheckpoint(ctx context.Context, tx pgx.Tx, cp *model.Checkpoint, expectedOffset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	clone := *cp
	s.checkpoints = append(s.checkpoints, &clone)
	return nil
}

func (s *safeCheckpointStore) MarkApplied(ctx context.Context, tx pgx.Tx, jobID, generation, lsn string, xid uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = append(s.applied, model.SourceTx{Generation: generation, LSN: lsn, XID: xid})
	return nil
}

func (s *safeCheckpointStore) IsApplied(ctx context.Context, tx pgx.Tx, jobID, generation, lsn string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, applied := range s.applied {
		if applied.Generation == generation && applied.LSN == lsn {
			return true, nil
		}
	}
	return false, nil
}

func (s *safeCheckpointStore) snapshot() []*model.Checkpoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*model.Checkpoint, len(s.checkpoints))
	for i, cp := range s.checkpoints {
		clone := *cp
		out[i] = &clone
	}
	return out
}

func (s *safeCheckpointStore) latest() *model.Checkpoint {
	cps := s.snapshot()
	if len(cps) == 0 {
		return nil
	}
	return cps[len(cps)-1]
}

// gateScanner wraps a fakeScanner and can hold ReadChunk for specific chunk
// ranges until the test opens their gate, making worker interleavings
// deterministic.
type gateScanner struct {
	inner *fakeScanner
	mu    sync.Mutex
	gates map[string]chan struct{}
}

func newGateScanner(sc *fakeScanner) *gateScanner {
	return &gateScanner{inner: sc, gates: map[string]chan struct{}{}}
}

func gateKey(min, max int64) string { return fmt.Sprintf("%d/%d", min, max) }

func (g *gateScanner) gate(min, max int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.gates[gateKey(min, max)] = make(chan struct{})
}

func (g *gateScanner) open(min, max int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if ch, ok := g.gates[gateKey(min, max)]; ok {
		close(ch)
	}
}

func (g *gateScanner) UpperBound(ctx context.Context) (int64, error) { return g.inner.UpperBound(ctx) }
func (g *gateScanner) NextChunk(ctx context.Context, completedThrough, upperBound int64, chunkSize int) (model.ChunkRange, bool, error) {
	return g.inner.NextChunk(ctx, completedThrough, upperBound, chunkSize)
}
func (g *gateScanner) ReadChunk(ctx context.Context, minID, maxID int64) ([]model.Row, error) {
	g.mu.Lock()
	ch, ok := g.gates[gateKey(minID, maxID)]
	g.mu.Unlock()
	if ok {
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return g.inner.ReadChunk(ctx, minID, maxID)
}

// errScanner fails ReadChunk for a specific range, simulating a scan failure
// on one worker's chunk.
type errScanner struct {
	inner   *fakeScanner
	failMin int64
}

func (s *errScanner) UpperBound(ctx context.Context) (int64, error) { return s.inner.UpperBound(ctx) }
func (s *errScanner) NextChunk(ctx context.Context, completedThrough, upperBound int64, chunkSize int) (model.ChunkRange, bool, error) {
	return s.inner.NextChunk(ctx, completedThrough, upperBound, chunkSize)
}
func (s *errScanner) ReadChunk(ctx context.Context, minID, maxID int64) ([]model.Row, error) {
	if minID == s.failMin {
		return nil, errors.New("injected read chunk failure")
	}
	return s.inner.ReadChunk(ctx, minID, maxID)
}

// ---------- helpers ----------

func seedPendingChunk(cs *fakeChunkStore, jobID, attempt string, min, max int64) {
	cs.CreateChunk(context.Background(), &model.Chunk{
		JobID:      jobID,
		ChunkMinID: min,
		ChunkMaxID: max,
		Attempt:    attempt,
		Status:     model.ChunkPending,
	})
}

// parallelConfig builds a Config wired for parallel mode: a durable chunk
// store seeded with the given ranges, the given scanner, and a channel
// consumer the test controls.
func parallelConfig(rows map[int64]model.Row, chunks [][2]int64, workers int, scanner scanner, consumer *chanConsumer) (Config, *fakeChunkStore, *fakeSink, *fakeMarkerStore, *safeCheckpointStore) {
	if rows == nil {
		rows = map[int64]model.Row{}
	}
	var maxID int64 = math.MinInt64
	var minID int64 = math.MaxInt64
	for id := range rows {
		if id > maxID {
			maxID = id
		}
		if id < minID {
			minID = id
		}
	}
	if maxID == math.MinInt64 {
		maxID = 0
		minID = 1
	}
	completedThrough := int64(0)
	if minID <= 0 {
		completedThrough = math.MinInt64
	}

	cpStore := &safeCheckpointStore{}
	sink := &fakeSink{}
	marker := &fakeMarkerStore{}
	chunkStore := &fakeChunkStore{}

	const jobID = "test-job"
	const attempt = "gen:0:attempt:0"
	for _, c := range chunks {
		seedPendingChunk(chunkStore, jobID, attempt, c[0], c[1])
	}

	cfg := Config{
		JobConfig: model.JobConfig{
			JobID:         jobID,
			ChunkSize:     10,
			KafkaTopic:    "test.topic",
			KafkaBrokers:  []string{"localhost:9092"},
			WorkerID:      "test-worker",
			LeaseDuration: time.Minute,
			Workers:       workers,
		},
		Checkpoint: &model.Checkpoint{
			JobID:            jobID,
			Generation:       "gen:0",
			Attempt:          attempt,
			ScanUpperBound:   maxID,
			CompletedThrough: completedThrough,
			NextKafkaOffset:  0,
		},
		Consumer:        consumer,
		CheckpointStore: cpStore,
		ChunkStore:      chunkStore,
		MarkerStore:     marker,
		Scanner:         scanner,
		Sink:            sink,
		SourceSchema:    testSourceSchema(),
		Metrics:         nil,
	}
	return cfg, chunkStore, sink, marker, cpStore
}

// runParallelAsync starts the reconciler and returns a channel carrying its
// error; benign stream-exhaustion errors are converted to nil.
func runParallelAsync(t *testing.T, cfg Config, timeout time.Duration) chan error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	ch := make(chan error, 1)
	go func() {
		defer cancel()
		err := New(cfg).Run(ctx)
		if isBenignError(err) {
			err = nil
		}
		ch <- err
	}()
	return ch
}

func awaitRun(t *testing.T, ch chan error, timeout time.Duration) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(timeout):
		t.Fatal("run did not finish within timeout")
		return nil
	}
}

func waitFor(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", desc)
}

func lowMarkerCount(s *fakeMarkerStore) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, m := range s.markers {
		if m.Kind == model.MarkerLow {
			n++
		}
	}
	return n
}

func (s *safeCheckpointStore) nextOffset() int64 {
	cp := s.latest()
	if cp == nil {
		return 0
	}
	return cp.NextKafkaOffset
}

func completedChunks(s *fakeChunkStore) int { return s.chunksWithStatus(model.ChunkCompleted) }

// firstCompletedThroughIndex returns the index of the first recorded checkpoint
// whose CompletedThrough reached the given target, or -1.
func firstCompletedThroughIndex(cps []*model.Checkpoint, target int64) int {
	for i, cp := range cps {
		if cp.CompletedThrough >= target {
			return i
		}
	}
	return -1
}

// ---------- tests ----------

func TestParallel_DuplicateLowMarkerAtNewOffset(t *testing.T) {
	rows := map[int64]model.Row{1: account(1, "one", 10)}
	consumer := newChanConsumer()
	cfg, chunkStore, _, markerStore, _ := parallelConfig(rows,
		[][2]int64{{1, 1}}, 2, &fakeScanner{upperBound: 1, rows: rows}, consumer)
	runCh := runParallelAsync(t, cfg, 5*time.Second)
	waitFor(t, 2*time.Second, "LOW marker", func() bool { return lowMarkerCount(markerStore) == 1 })
	low := lowMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 1, Max: 1})
	low.Source.LSN = "lsn-duplicate-low"
	consumer.push([]kafka.Record{
		rec(0, low),
		rec(1, low),
		rec(2, highMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 1, Max: 1})),
	})
	waitFor(t, 2*time.Second, "chunk completion after duplicate marker", func() bool { return completedChunks(chunkStore) == 1 })
	consumer.finish()
	if err := awaitRun(t, runCh, 5*time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestParallel_SparseFirstRangeCompletes(t *testing.T) {
	rows := map[int64]model.Row{10: account(10, "ten", 10)}
	consumer := newChanConsumer()
	cfg, chunkStore, _, markerStore, cpStore := parallelConfig(rows,
		[][2]int64{{10, 10}}, 2, &fakeScanner{upperBound: 10, rows: rows}, consumer)
	runCh := runParallelAsync(t, cfg, 5*time.Second)
	waitFor(t, 2*time.Second, "LOW marker", func() bool { return lowMarkerCount(markerStore) == 1 })
	consumer.push([]kafka.Record{
		rec(0, lowMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 10, Max: 10})),
		rec(1, highMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 10, Max: 10})),
	})
	waitFor(t, 2*time.Second, "sparse chunk completion", func() bool { return completedChunks(chunkStore) == 1 })
	consumer.finish()
	if err := awaitRun(t, runCh, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if cpStore.latest().CompletedThrough != 10 {
		t.Fatalf("sparse frontier did not advance: %+v", cpStore.latest())
	}
}

func TestParallel_DoesNotApplyAfterHighBeforeCandidates(t *testing.T) {
	rows := map[int64]model.Row{1: account(1, "old", 10)}
	scanner := newGateScanner(&fakeScanner{upperBound: 1, rows: rows})
	scanner.gate(1, 1)
	consumer := newChanConsumer()
	cfg, chunkStore, sink, markerStore, cpStore := parallelConfig(rows,
		[][2]int64{{1, 1}}, 2, scanner, consumer)
	runCh := runParallelAsync(t, cfg, 5*time.Second)
	waitFor(t, 2*time.Second, "LOW marker", func() bool { return lowMarkerCount(markerStore) == 1 })
	consumer.push([]kafka.Record{
		rec(0, lowMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 1, Max: 1})),
		rec(1, highMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 1, Max: 1})),
		rec(2, accChange(model.OpUpdate, 1, "new", 20)),
	})
	select {
	case err := <-runCh:
		t.Fatalf("run ended before scan completed: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	if len(sink.changes()) != 0 {
		t.Fatal("post-HIGH CDC reached destination before snapshot finalization")
	}
	scanner.open(1, 1)
	waitFor(t, 2*time.Second, "chunk completion", func() bool { return completedChunks(chunkStore) == 1 })
	waitFor(t, 2*time.Second, "later CDC", func() bool { return len(sink.changes()) == 1 })
	consumer.finish()
	if err := awaitRun(t, runCh, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if cpStore.latest().NextKafkaOffset != 3 || rowOwner(sink.changes()[0].Row) != "new" {
		t.Fatalf("later CDC was not applied after HIGH: cp=%+v changes=%+v", cpStore.latest(), sink.changes())
	}
}

func TestParallel_HigherRangeHighBeforeLowerRange(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 1), 2: account(2, "two", 2),
		10: account(10, "ten", 10), 11: account(11, "eleven", 11),
	}
	scanner := newGateScanner(&fakeScanner{upperBound: 11, rows: rows})
	scanner.gate(1, 2)
	consumer := newChanConsumer()
	cfg, chunkStore, _, markerStore, cpStore := parallelConfig(rows,
		[][2]int64{{1, 2}, {10, 11}}, 2, scanner, consumer)
	runCh := runParallelAsync(t, cfg, 5*time.Second)
	waitFor(t, 2*time.Second, "two LOW markers", func() bool { return lowMarkerCount(markerStore) == 2 })
	lowA := lowMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 1, Max: 2})
	lowB := lowMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 10, Max: 11})
	highB := highMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 10, Max: 11})
	highA := highMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 1, Max: 2})
	lowA.Source.LSN, lowB.Source.LSN = "lsn-low-a", "lsn-low-b"
	highB.Source.LSN, highA.Source.LSN = "lsn-high-b", "lsn-high-a"
	consumer.push([]kafka.Record{
		rec(0, lowA),
		rec(1, lowB),
		rec(2, highB),
		rec(3, highA),
	})
	waitFor(t, 2*time.Second, "higher range finalized", func() bool { return completedChunks(chunkStore) == 1 })
	if cpStore.latest().CompletedThrough != 0 {
		t.Fatalf("frontier jumped over unfinished range: %+v", cpStore.latest())
	}
	scanner.open(1, 2)
	waitFor(t, 2*time.Second, "both ranges finalized", func() bool { return completedChunks(chunkStore) == 2 })
	consumer.finish()
	if err := awaitRun(t, runCh, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if cpStore.latest().CompletedThrough != 11 || cpStore.latest().NextKafkaOffset != 4 {
		t.Fatalf("frontier did not advance across sparse ranges: %+v", cpStore.latest())
	}
}

// TestParallel_TwoWorkersCompleteBackfill proves two workers reconcile two
// chunks against one shared stream: survivors are written, chunks complete,
// the checkpoint advances, and in-window evictions that arrive before the
// snapshot (workers gated behind the scan) are never lost.
func TestParallel_TwoWorkersCompleteBackfill(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		2: account(2, "two", 200),
		3: account(3, "three", 300),
		4: account(4, "four", 400),
	}
	scanner := newGateScanner(&fakeScanner{upperBound: 4, rows: rows})
	// Gate both scans so HIGH cannot finalize before delivery.
	scanner.gate(1, 2)
	scanner.gate(3, 4)

	consumer := newChanConsumer()
	cfg, chunkStore, sink, markerStore, cpStore := parallelConfig(rows,
		[][2]int64{{1, 2}, {3, 4}}, 2, scanner, consumer)

	runCh := runParallelAsync(t, cfg, 10*time.Second)

	// Both workers registered their windows (LOW markers written).
	waitFor(t, 5*time.Second, "two LOW markers", func() bool { return lowMarkerCount(markerStore) >= 2 })

	// Scripted stream: in-window updates evict id 1 (chunk A) and delete id 3
	// (chunk B), then a CDC-only event after all windows close.
	consumer.push([]kafka.Record{
		rec(0, lowMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 1, Max: 2})),
		rec(1, accChange(model.OpUpdate, 1, "one-updated", 150)),
		rec(2, highMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 1, Max: 2})),
		rec(3, lowMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 3, Max: 4})),
		rec(4, accChange(model.OpDelete, 3, "", 0)),
		rec(5, highMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 3, Max: 4})),
		rec(6, accChange(model.OpUpdate, 7, "seven-new", 700)),
	})

	// The stream reaches HIGH and must stop before the later CDC event.
	waitFor(t, 5*time.Second, "in-window update", func() bool { return len(sink.changes()) == 1 })

	// Release the scans; deliveries must apply the pre-recorded evictions.
	scanner.open(1, 2)
	scanner.open(3, 4)

	waitFor(t, 5*time.Second, "chunks completed", func() bool { return completedChunks(chunkStore) == 2 })
	waitFor(t, 5*time.Second, "stream consumed", func() bool { return len(sink.changes()) == 3 })
	consumer.finish()

	if err := awaitRun(t, runCh, 10*time.Second); err != nil {
		t.Fatalf("parallel run: %v", err)
	}

	ids := sink.survivorIDs()
	// ids 1 and 3 were touched in-window and evicted; id 7 was pure CDC.
	if len(ids) != 2 || !contains(ids, 2) || !contains(ids, 4) {
		t.Fatalf("expected survivors {2,4}, got %v", ids)
	}
	changes := sink.changes()
	if len(changes) != 3 {
		t.Fatalf("expected 3 CDC applications, got %d", len(changes))
	}
	if cpStore.latest().CompletedThrough != 4 {
		t.Fatalf("expected completed_through=4, got %d", cpStore.latest().CompletedThrough)
	}

	// CompletedThrough must never regress on the single checkpoint row.
	cps := cpStore.snapshot()
	lastCT := int64(0)
	for _, cp := range cps {
		if cp.CompletedThrough < lastCT {
			t.Fatalf("completed_through regressed: %d -> %d", lastCT, cp.CompletedThrough)
		}
		lastCT = cp.CompletedThrough
	}

	// Each chunk leased by a distinct worker id.
	chunkStore.mu.Lock()
	defer chunkStore.mu.Unlock()
	workers := map[string]bool{}
	for _, ch := range chunkStore.chunks {
		workers[ch.WorkerID] = true
	}
	if len(workers) != 2 {
		t.Fatalf("expected 2 distinct worker ids, got %v", workers)
	}
}

// TestParallel_OutOfOrderScansCommitInOrder forces chunk B to finish its scan
// before chunk A while chunk A is the next-in-order commit. The commit gate
// must hold B until A commits so completed_through never jumps ahead.
func TestParallel_OutOfOrderScansCommitInOrder(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		2: account(2, "two", 200),
		3: account(3, "three", 300),
		4: account(4, "four", 400),
	}
	scanner := newGateScanner(&fakeScanner{upperBound: 4, rows: rows})
	scanner.gate(1, 2) // chunk A slow; chunk B (3,4) scans immediately

	consumer := newChanConsumer()
	cfg, chunkStore, sink, markerStore, cpStore := parallelConfig(rows,
		[][2]int64{{1, 2}, {3, 4}}, 2, scanner, consumer)

	runCh := runParallelAsync(t, cfg, 10*time.Second)
	waitFor(t, 5*time.Second, "two LOW markers", func() bool { return lowMarkerCount(markerStore) >= 2 })

	consumer.push([]kafka.Record{
		rec(0, lowMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 1, Max: 2})),
		rec(1, highMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 1, Max: 2})),
		rec(2, lowMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 3, Max: 4})),
		rec(3, highMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 3, Max: 4})),
		// Tail CDC must wait until the earlier HIGH has finalized.
		rec(4, accChange(model.OpUpdate, 9, "nine", 900)),
	})
	// B's worker delivered already; A is still gated. Nothing may commit yet
	// because A is next in order.
	if completedChunks(chunkStore) != 0 {
		t.Fatal("chunk committed before next-in-order chunk was ready")
	}

	scanner.open(1, 2)
	waitFor(t, 5*time.Second, "chunks completed", func() bool { return completedChunks(chunkStore) == 2 })
	waitFor(t, 5*time.Second, "stream consumed", func() bool { return len(sink.changes()) == 1 })
	consumer.finish()

	if err := awaitRun(t, runCh, 10*time.Second); err != nil {
		t.Fatalf("parallel run: %v", err)
	}

	cps := cpStore.snapshot()
	aIdx := firstCompletedThroughIndex(cps, 2)
	bIdx := firstCompletedThroughIndex(cps, 4)
	if aIdx < 0 || bIdx < 0 {
		t.Fatalf("expected completions for both chunks, found A=%d B=%d", aIdx, bIdx)
	}
	if aIdx >= bIdx {
		t.Fatalf("chunk B committed before chunk A: A at %d, B at %d", aIdx, bIdx)
	}
	if len(sink.survivorIDs()) != 4 {
		t.Fatalf("expected 4 survivors, got %v", sink.survivorIDs())
	}
	if cpStore.latest().CompletedThrough != 4 {
		t.Fatalf("expected completed_through=4, got %d", cpStore.latest().CompletedThrough)
	}
}

// TestParallel_WorkerReusesPoolChannel proves a worker can execute more than
// one chunk: with three chunks and two workers, the faster worker picks up the
// third range after finishing its first, and completion stays in order.
func TestParallel_WorkerReusesPoolChannel(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		2: account(2, "two", 200),
		3: account(3, "three", 300),
		4: account(4, "four", 400),
		5: account(5, "five", 500),
		6: account(6, "six", 600),
	}
	scanner := newGateScanner(&fakeScanner{upperBound: 6, rows: rows})
	// A is gated so B can finish first; C is ungated for whichever worker picks
	// it up second.
	scanner.gate(1, 2)

	consumer := newChanConsumer()
	cfg, chunkStore, sink, markerStore, cpStore := parallelConfig(rows,
		[][2]int64{{1, 2}, {3, 4}, {5, 6}}, 2, scanner, consumer)

	runCh := runParallelAsync(t, cfg, 15*time.Second)
	waitFor(t, 5*time.Second, "two LOW markers", func() bool { return lowMarkerCount(markerStore) >= 2 })

	// Segment 1: chunks A and B, with a tail CDC record proving consumption.
	consumer.push([]kafka.Record{
		rec(0, lowMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 1, Max: 2})),
		rec(1, highMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 1, Max: 2})),
		rec(2, lowMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 3, Max: 4})),
		rec(3, highMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 3, Max: 4})),
		rec(4, accChange(model.OpUpdate, 9, "nine", 900)),
	})
	// Release A: A commits (next in order) then B drains.
	scanner.open(1, 2)
	waitFor(t, 5*time.Second, "chunks A and B completed",
		func() bool { return completedChunks(chunkStore) == 2 && lowMarkerCount(markerStore) >= 3 })
	waitFor(t, 5*time.Second, "segment 1 consumed", func() bool { return len(sink.changes()) == 1 })

	// The freed worker leased chunk C and wrote its LOW marker; now stream its
	// segment and wait for the commit.
	consumer.push([]kafka.Record{
		rec(5, lowMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 5, Max: 6})),
		rec(6, highMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 5, Max: 6})),
	})
	waitFor(t, 5*time.Second, "chunk C completed", func() bool { return completedChunks(chunkStore) == 3 })
	consumer.finish()

	if err := awaitRun(t, runCh, 15*time.Second); err != nil {
		t.Fatalf("parallel run: %v", err)
	}

	if len(sink.survivorIDs()) != 6 {
		t.Fatalf("expected 6 survivors, got %v", sink.survivorIDs())
	}
	if cpStore.latest().CompletedThrough != 6 {
		t.Fatalf("expected completed_through=6, got %d", cpStore.latest().CompletedThrough)
	}
}

// TestParallel_WorkerFailureFailsRun verifies a scan failure on one worker
// aborts the run instead of stalling forever waiting for a window that can
// never complete.
func TestParallel_WorkerFailureFailsRun(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		2: account(2, "two", 200),
		3: account(3, "three", 300),
		4: account(4, "four", 400),
	}
	scanner := &errScanner{inner: &fakeScanner{upperBound: 4, rows: rows}, failMin: 3}
	consumer := newChanConsumer()
	cfg, chunkStore, _, _, _ := parallelConfig(rows,
		[][2]int64{{1, 2}, {3, 4}}, 2, scanner, consumer)

	runCh := runParallelAsync(t, cfg, 10*time.Second)

	// The stream stays empty: the chunk for [3,4] can never complete, so the
	// run must fail via the worker's error, not hang.
	consumer.finish()

	err := awaitRun(t, runCh, 10*time.Second)
	if err == nil {
		t.Fatal("expected run to fail after worker scan error")
	}
	if !strings.Contains(err.Error(), "read chunk") {
		t.Fatalf("unexpected error: %v", err)
	}
	if completedChunks(chunkStore) != 0 {
		t.Fatalf("expected no chunks to complete after failure, got %d", completedChunks(chunkStore))
	}
}

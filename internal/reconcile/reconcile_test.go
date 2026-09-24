package reconcile

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/seam/internal/failpoint"
	"example.com/seam/internal/kafka"
	"example.com/seam/internal/model"
	"example.com/seam/internal/schema"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ---------- fakes ----------

// fakeConn returns a single transaction.
type fakeConn struct {
	tx *fakeTx
}

func (c *fakeConn) Begin(ctx context.Context) (pgx.Tx, error) { return c.tx, nil }
func (c *fakeConn) Close(ctx context.Context) error           { return nil }

// fakeTx records commit/rollback and satisfies pgx.Tx.
type fakeTx struct {
	committed  bool
	rolledBack bool
	onCommit   []func()
}

func (t *fakeTx) Begin(ctx context.Context) (pgx.Tx, error) {
	return nil, fmt.Errorf("not implemented")
}
func (t *fakeTx) Commit(ctx context.Context) error {
	if t.rolledBack {
		return fmt.Errorf("commit after rollback")
	}
	t.committed = true
	for _, fn := range t.onCommit {
		fn()
	}
	t.onCommit = nil
	return nil
}
func (t *fakeTx) Rollback(ctx context.Context) error {
	if !t.committed {
		t.rolledBack = true
		t.onCommit = nil
	}
	return nil
}
func (t *fakeTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}
func (t *fakeTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults { return nil }
func (t *fakeTx) LargeObjects() pgx.LargeObjects                               { return pgx.LargeObjects{} }
func (t *fakeTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return nil, fmt.Errorf("not implemented")
}
func (t *fakeTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, fmt.Errorf("not implemented")
}
func (t *fakeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return nil, fmt.Errorf("not implemented")
}
func (t *fakeTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row { return nil }
func (t *fakeTx) Conn() *pgx.Conn                                               { return nil }

// fakeCheckpointStore records checkpoint and applied-transaction calls.
type fakeCheckpointStore struct {
	checkpoints []*model.Checkpoint
	applied     []model.SourceTx
}

func (s *fakeCheckpointStore) Begin(ctx context.Context) (pgx.Tx, error) { return &fakeTx{}, nil }
func (s *fakeCheckpointStore) AssertLeadership(context.Context, pgx.Tx, *model.Checkpoint) error {
	return nil
}
func (s *fakeCheckpointStore) UpdateCheckpoint(ctx context.Context, tx pgx.Tx, cp *model.Checkpoint, expectedOffset int64) error {
	clone := *cp
	tx.(*fakeTx).onCommit = append(tx.(*fakeTx).onCommit, func() { s.checkpoints = append(s.checkpoints, &clone) })
	return nil
}
func (s *fakeCheckpointStore) MarkApplied(ctx context.Context, tx pgx.Tx, jobID, generation, lsn string, xid uint32) error {
	tx.(*fakeTx).onCommit = append(tx.(*fakeTx).onCommit, func() {
		s.applied = append(s.applied, model.SourceTx{Generation: generation, LSN: lsn, XID: xid})
	})
	return nil
}
func (s *fakeCheckpointStore) IsApplied(ctx context.Context, tx pgx.Tx, jobID, generation, lsn string) (bool, error) {
	for _, applied := range s.applied {
		if applied.Generation == generation && applied.LSN == lsn {
			return true, nil
		}
	}
	return false, nil
}

// fakeChunkStore is an in-memory chunk state store for unit tests.
type fakeChunkStore struct {
	mu     sync.Mutex
	chunks []*model.Chunk
}

func (s *fakeChunkStore) CreateChunk(ctx context.Context, chunk *model.Chunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	clone := *chunk
	s.chunks = append(s.chunks, &clone)
	return nil
}

func (s *fakeChunkStore) LeaseChunkOwned(ctx context.Context, jobID, attempt, workerID, ownerID string, ownerEpoch int64, leaseDuration time.Duration) (*model.Chunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ch := range s.chunks {
		if ch.JobID == jobID && ch.Attempt == attempt && ch.Status == model.ChunkPending {
			now := time.Now()
			expiry := now.Add(leaseDuration)
			ch.Status = model.ChunkLeased
			ch.WorkerID = workerID
			ch.LeaseToken++
			ch.LeaseStart = &now
			ch.LeaseExpiry = &expiry
			ch.HeartbeatAt = &now
			clone := *ch
			return &clone, nil
		}
	}
	return nil, nil
}

func (s *fakeChunkStore) HeartbeatChunkOwned(ctx context.Context, chunk *model.Chunk, ownerID string, ownerEpoch int64, leaseDuration time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ch := range s.chunks {
		if ch.JobID == chunk.JobID && ch.ChunkMinID == chunk.ChunkMinID && ch.ChunkMaxID == chunk.ChunkMaxID && ch.Attempt == chunk.Attempt {
			if ch.WorkerID != chunk.WorkerID || ch.LeaseToken != chunk.LeaseToken {
				return fmt.Errorf("chunk no longer owned by %q", chunk.WorkerID)
			}
			now := time.Now()
			expiry := now.Add(leaseDuration)
			ch.HeartbeatAt = &now
			ch.LeaseExpiry = &expiry
			return nil
		}
	}
	return fmt.Errorf("chunk not found")
}

func (s *fakeChunkStore) LoadChunks(ctx context.Context, jobID string, statuses ...model.ChunkState) ([]model.Chunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var chunks []model.Chunk
	for _, chunk := range s.chunks {
		if chunk.JobID == jobID {
			chunks = append(chunks, *chunk)
		}
	}
	return chunks, nil
}

func (s *fakeChunkStore) DiscoveryComplete(ctx context.Context, jobID string) (bool, error) {
	return true, nil
}

func (s *fakeChunkStore) UpdateChunk(ctx context.Context, tx pgx.Tx, chunk *model.Chunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ch := range s.chunks {
		if ch.JobID == chunk.JobID && ch.ChunkMinID == chunk.ChunkMinID && ch.ChunkMaxID == chunk.ChunkMaxID && ch.Attempt == chunk.Attempt {
			if ch.WorkerID != chunk.WorkerID || ch.LeaseToken != chunk.LeaseToken {
				return fmt.Errorf("stale chunk lease")
			}
			*ch = *chunk
			return nil
		}
	}
	return fmt.Errorf("chunk not found")
}

func (s *fakeChunkStore) chunksWithStatus(st model.ChunkState) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, ch := range s.chunks {
		if ch.Status == st {
			count++
		}
	}
	return count
}

// fakeMarkerStore records marker writes.
type fakeMarkerStore struct {
	mu      sync.Mutex
	markers []model.Marker
}

func (s *fakeMarkerStore) WriteLow(ctx context.Context, jobID, attempt string, chunk model.ChunkRange) (string, error) {
	return s.write("low", jobID, attempt, chunk), nil
}
func (s *fakeMarkerStore) WriteHigh(ctx context.Context, jobID, attempt string, chunk model.ChunkRange) (string, error) {
	return s.write("high", jobID, attempt, chunk), nil
}
func (s *fakeMarkerStore) write(kind, jobID, attempt string, chunk model.ChunkRange) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := fmt.Sprintf("%s:%s:%s:%d:%d", jobID, attempt, kind, chunk.Min, chunk.Max)
	s.markers = append(s.markers, model.Marker{
		ID:       id,
		Kind:     model.MarkerKind(kind),
		JobID:    jobID,
		Attempt:  attempt,
		ChunkMin: chunk.Min,
		ChunkMax: chunk.Max,
	})
	return id
}

// fakeScanner returns a configured sequence of chunks and rows.
type fakeScanner struct {
	upperBound int64
	rows       map[int64]model.Row
}

func (s *fakeScanner) UpperBound(ctx context.Context) (int64, error) { return s.upperBound, nil }
func (s *fakeScanner) NextChunk(ctx context.Context, completedThrough, upperBound int64, chunkSize int) (model.ChunkRange, bool, error) {
	if completedThrough >= upperBound {
		return model.ChunkRange{}, false, nil
	}
	minID := completedThrough + 1
	maxID := minID + int64(chunkSize) - 1
	if maxID > upperBound || maxID < minID {
		maxID = upperBound
	}
	return model.ChunkRange{Min: minID, Max: maxID}, true, nil
}
func (s *fakeScanner) ReadChunk(ctx context.Context, minID, maxID int64) ([]model.Row, error) {
	var out []model.Row
	for id := minID; id <= maxID; id++ {
		if row, ok := s.rows[id]; ok {
			out = append(out, row)
		}
	}
	return out, nil
}

// fakeSink records applied changes and written survivors.
type fakeSink struct {
	mu        sync.Mutex
	applied   []model.Change
	survivors []model.Row
}

func (s *fakeSink) Apply(ctx context.Context, tx pgx.Tx, change model.Change) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = append(s.applied, change)
	return nil
}
func (s *fakeSink) WriteCandidates(ctx context.Context, tx pgx.Tx, candidates []model.Row) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.survivors = append(s.survivors, candidates...)
	return nil
}
func (s *fakeSink) changes() []model.Change {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]model.Change(nil), s.applied...)
}
func (s *fakeSink) survivorIDs() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []int64
	for _, row := range s.survivors {
		ids = append(ids, rowID(&row))
	}
	return ids
}

// fakeConsumer replays a scripted list of batches.
type fakeConsumer struct {
	mu      sync.Mutex
	batches [][]kafka.Record
	closed  bool
}

func (c *fakeConsumer) Poll(ctx context.Context) ([]kafka.Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.batches) == 0 {
		return nil, context.Canceled
	}
	batch := c.batches[0]
	c.batches = c.batches[1:]
	return batch, nil
}
func (c *fakeConsumer) Close() { c.closed = true }

func (c *fakeConsumer) remaining() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.batches)
}

// ---------- helpers ----------

func testSourceSchema() *schema.Schema {
	s := &schema.Schema{
		Namespace:       "public",
		Table:           "accounts",
		ReplicaIdentity: "f",
		PKOrdinal:       1,
		Columns: []schema.Column{
			{Ordinal: 1, Name: "id", TypeName: "int8", TypeOID: 20, PrimaryKey: true},
			{Ordinal: 2, Name: "owner", TypeName: "text", TypeOID: 25},
			{Ordinal: 3, Name: "balance_cents", TypeName: "int8", TypeOID: 20},
		},
	}
	s.Fingerprint = s.FingerprintFor()
	return s
}

func account(id int64, owner string, balance int64) model.Row {
	return model.Row{Values: []model.Value{
		model.Int64Value(id),
		model.TextValue(owner),
		model.Int64Value(balance),
	}}
}

// rowID/rowOwner/rowBalance read the positional fixture rows (id at ordinal 0,
// owner at 1, balance at 2). They return sentinel values for nil or malformed
// rows so assertions fail loudly instead of panicking.
func rowID(row *model.Row) int64 {
	if row == nil || len(row.Values) == 0 || row.Values[0].Kind != model.ValueInt64 {
		return -1
	}
	return row.Values[0].Int
}

func rowOwner(row *model.Row) string {
	if row == nil || len(row.Values) < 2 || row.Values[1].Kind != model.ValueText {
		return "<invalid>"
	}
	return row.Values[1].Text
}

func rowBalance(row *model.Row) int64 {
	if row == nil || len(row.Values) < 3 || row.Values[2].Kind != model.ValueInt64 {
		return -1
	}
	return row.Values[2].Int
}

func accountPtr(id int64, owner string, balance int64) *model.Row {
	row := account(id, owner, balance)
	return &row
}

func accChange(op model.Operation, id int64, owner string, balance int64) model.Change {
	return model.Change{
		Op:       op,
		SchemaID: testSourceSchema().Fingerprint,
		Row:      accountPtr(id, owner, balance),
		Source:   model.SourceTx{Generation: "gen:0", LSN: fmt.Sprintf("lsn-%d", id)},
	}
}

func lowMarker(jobID, attempt string, chunk model.ChunkRange) model.Change {
	return model.Change{
		Marker: &model.Marker{
			ID:       fmt.Sprintf("%s:%s:low:%d:%d", jobID, attempt, chunk.Min, chunk.Max),
			Kind:     model.MarkerLow,
			JobID:    jobID,
			Attempt:  attempt,
			ChunkMin: chunk.Min,
			ChunkMax: chunk.Max,
		},
		Source: model.SourceTx{Generation: "gen:0", LSN: "lsn-low"},
	}
}

func highMarker(jobID, attempt string, chunk model.ChunkRange) model.Change {
	return model.Change{
		Marker: &model.Marker{
			ID:       fmt.Sprintf("%s:%s:high:%d:%d", jobID, attempt, chunk.Min, chunk.Max),
			Kind:     model.MarkerHigh,
			JobID:    jobID,
			Attempt:  attempt,
			ChunkMin: chunk.Min,
			ChunkMax: chunk.Max,
		},
		Source: model.SourceTx{Generation: "gen:0", LSN: "lsn-high"},
	}
}

func rec(offset int64, change model.Change) kafka.Record {
	// A Kafka record is one complete source transaction. Default test changes
	// get distinct commit LSNs; tests for producer retries set an explicit LSN.
	defaultLSN := change.Source.LSN == "lsn-low" || change.Source.LSN == "lsn-high"
	if change.Row != nil && change.Source.LSN == fmt.Sprintf("lsn-%d", rowID(change.Row)) {
		defaultLSN = true
	}
	if defaultLSN {
		change.Source.LSN = fmt.Sprintf("lsn-offset-%d", offset)
	}
	return kafka.Record{Offset: offset, Change: change}
}

func runReconciler(t *testing.T, cfg Config, timeout time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := cfg.Consumer.(*fakeConsumer).run(ctx, cfg)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (c *fakeConsumer) run(ctx context.Context, cfg Config) error {
	r := New(cfg)
	return r.Run(ctx)
}

func setup(rows map[int64]model.Row, chunkSize int, batches [][]kafka.Record) (Config, *fakeSink, *fakeCheckpointStore, *fakeConsumer) {
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

	cpStore := &fakeCheckpointStore{}
	sink := &fakeSink{}
	marker := &fakeMarkerStore{}
	scanner := &fakeScanner{upperBound: maxID, rows: rows}
	consumer := &fakeConsumer{batches: batches}

	cfg := Config{
		JobConfig: model.JobConfig{
			JobID:        "test-job",
			ChunkSize:    chunkSize,
			KafkaTopic:   "test.topic",
			KafkaBrokers: []string{"localhost:9092"},
		},
		Checkpoint: &model.Checkpoint{
			JobID:            "test-job",
			Generation:       "gen:0",
			Attempt:          "gen:0:attempt:0",
			ScanUpperBound:   maxID,
			CompletedThrough: completedThrough,
			NextKafkaOffset:  0,
		},
		Consumer:        consumer,
		CheckpointStore: cpStore,
		MarkerStore:     marker,
		Scanner:         scanner,
		Sink:            sink,
		SourceSchema:    testSourceSchema(),
		Metrics:         nil,
	}
	return cfg, sink, cpStore, consumer
}

// ---------- existing simple eviction test ----------

func TestCandidateEviction(t *testing.T) {
	candidates := map[int64]model.Row{
		1: account(1, "a", 100),
		2: account(2, "b", 200),
		3: account(3, "c", 300),
	}

	evict(candidates, model.Change{Op: model.OpUpdate, Row: accountPtr(2, "b", 200)})
	if _, ok := candidates[2]; ok {
		t.Fatal("expected id 2 to be evicted")
	}
	if len(candidates) != 2 {
		t.Fatalf("expected 2 survivors, got %d", len(candidates))
	}

	evict(candidates, model.Change{Op: model.OpDelete, Row: accountPtr(3, "c", 300)})
	if _, ok := candidates[3]; ok {
		t.Fatal("expected id 3 to be evicted")
	}

	evict(candidates, model.Change{Op: model.OpInsert, Row: accountPtr(4, "d", 400)})
	if len(candidates) != 1 {
		t.Fatalf("expected 1 survivor, got %d", len(candidates))
	}
}

func evict(candidates map[int64]model.Row, change model.Change) {
	if change.Row != nil {
		delete(candidates, rowID(change.Row))
	}
}

// ---------- correctness tests ----------

func TestReconciler_StaticBackfill(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		2: account(2, "two", 200),
		3: account(3, "three", 300),
	}
	chunk := model.ChunkRange{Min: 1, Max: 3}
	batches := [][]kafka.Record{
		{
			rec(0, lowMarker("test-job", "gen:0:attempt:0", chunk)),
			rec(1, highMarker("test-job", "gen:0:attempt:0", chunk)),
		},
	}
	cfg, sink, cpStore, _ := setup(rows, 10, batches)

	if err := runReconciler(t, cfg, 5*time.Second); err != nil {
		t.Fatalf("run reconciler: %v", err)
	}

	ids := sink.survivorIDs()
	if len(ids) != 3 {
		t.Fatalf("expected 3 survivors, got %v", ids)
	}
	last := cpStore.checkpoints[len(cpStore.checkpoints)-1]
	if last.CompletedThrough != 3 {
		t.Fatalf("expected completed_through=3, got %d", last.CompletedThrough)
	}
}

func TestReconciler_ConcurrentUpdateNotOverwritten(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		2: account(2, "two", 200),
		3: account(3, "three", 300),
	}
	chunk := model.ChunkRange{Min: 1, Max: 3}
	batches := [][]kafka.Record{
		{
			rec(0, lowMarker("test-job", "gen:0:attempt:0", chunk)),
			rec(1, accChange(model.OpUpdate, 2, "two-updated", 250)),
			rec(2, highMarker("test-job", "gen:0:attempt:0", chunk)),
		},
	}
	cfg, sink, _, _ := setup(rows, 10, batches)

	if err := runReconciler(t, cfg, 5*time.Second); err != nil {
		t.Fatalf("run reconciler: %v", err)
	}

	// Candidate 2 was evicted and the CDC update applied.
	ids := sink.survivorIDs()
	if contains(ids, 2) {
		t.Fatalf("expected id 2 to be evicted from snapshot, survivors=%v", ids)
	}
	changes := sink.changes()
	if len(changes) != 1 || rowOwner(changes[0].Row) != "two-updated" {
		t.Fatalf("expected CDC update to be applied, changes=%v", changes)
	}
}

func TestReconciler_ConcurrentDeleteNotResurrected(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		2: account(2, "two", 200),
		3: account(3, "three", 300),
	}
	chunk := model.ChunkRange{Min: 1, Max: 3}
	batches := [][]kafka.Record{
		{
			rec(0, lowMarker("test-job", "gen:0:attempt:0", chunk)),
			rec(1, accChange(model.OpDelete, 2, "", 0)),
			rec(2, highMarker("test-job", "gen:0:attempt:0", chunk)),
		},
	}
	cfg, sink, _, _ := setup(rows, 10, batches)

	if err := runReconciler(t, cfg, 5*time.Second); err != nil {
		t.Fatalf("run reconciler: %v", err)
	}

	ids := sink.survivorIDs()
	if contains(ids, 2) {
		t.Fatalf("expected id 2 to be evicted, survivors=%v", ids)
	}
	changes := sink.changes()
	if len(changes) != 1 || changes[0].Op != model.OpDelete {
		t.Fatalf("expected CDC delete to be applied, changes=%v", changes)
	}
}

func TestReconciler_ConcurrentInsertInChunkRange(t *testing.T) {
	// Sparse keys: chunk will be [1,5] but only ids 1,3,5 exist initially.
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		3: account(3, "three", 300),
		5: account(5, "five", 500),
	}
	chunk := model.ChunkRange{Min: 1, Max: 5}
	batches := [][]kafka.Record{
		{
			rec(0, lowMarker("test-job", "gen:0:attempt:0", chunk)),
			rec(1, accChange(model.OpInsert, 2, "two-inserted", 200)),
			rec(2, highMarker("test-job", "gen:0:attempt:0", chunk)),
		},
	}
	cfg, sink, _, _ := setup(rows, 10, batches)

	if err := runReconciler(t, cfg, 5*time.Second); err != nil {
		t.Fatalf("run reconciler: %v", err)
	}

	// Inserted row was not a candidate; it is applied via CDC.
	changes := sink.changes()
	if len(changes) != 1 || rowID(changes[0].Row) != 2 {
		t.Fatalf("expected CDC insert to be applied, changes=%v", changes)
	}
}

func TestReconciler_SameRowManyChangesDuringChunk(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		2: account(2, "two", 200),
		3: account(3, "three", 300),
	}
	chunk := model.ChunkRange{Min: 1, Max: 3}
	batches := [][]kafka.Record{
		{
			rec(0, lowMarker("test-job", "gen:0:attempt:0", chunk)),
			rec(1, accChange(model.OpUpdate, 2, "two-v1", 201)),
			rec(2, accChange(model.OpUpdate, 2, "two-v2", 202)),
			rec(3, accChange(model.OpUpdate, 2, "two-v3", 203)),
			rec(4, highMarker("test-job", "gen:0:attempt:0", chunk)),
		},
	}
	cfg, sink, _, _ := setup(rows, 10, batches)

	if err := runReconciler(t, cfg, 5*time.Second); err != nil {
		t.Fatalf("run reconciler: %v", err)
	}

	ids := sink.survivorIDs()
	if contains(ids, 2) {
		t.Fatalf("expected id 2 to be evicted, survivors=%v", ids)
	}
	changes := sink.changes()
	if len(changes) != 3 {
		t.Fatalf("expected 3 CDC updates, got %d", len(changes))
	}
	last := changes[len(changes)-1]
	if rowOwner(last.Row) != "two-v3" || rowBalance(last.Row) != 203 {
		t.Fatalf("expected final value two-v3/203, got %s/%d", rowOwner(last.Row), rowBalance(last.Row))
	}
}

func TestReconciler_ChunkRetryIsIdempotent(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		2: account(2, "two", 200),
	}
	chunk := model.ChunkRange{Min: 1, Max: 2}
	update := accChange(model.OpUpdate, 2, "two-updated", 250)
	batches0 := [][]kafka.Record{chunkBatch("test-job", "gen:0:attempt:0", chunk, update)}
	cfg, sink, cpStore, _ := setup(rows, 10, batches0)

	// First run completes the chunk.
	if err := runReconciler(t, cfg, 5*time.Second); err != nil {
		t.Fatalf("first run: %v", err)
	}
	firstSurvivors := sink.survivorIDs()
	firstChanges := sink.changes()

	// Reset state and reprocess with a new attempt using new markers.
	sink.applied = nil
	sink.survivors = nil
	cpStore.checkpoints = nil
	cpStore.applied = nil
	cfg.Checkpoint = &model.Checkpoint{
		JobID:            "test-job",
		Generation:       "gen:0",
		Attempt:          "gen:0:attempt:1",
		ScanUpperBound:   2,
		CompletedThrough: 0,
		NextKafkaOffset:  0,
	}
	batches1 := [][]kafka.Record{chunkBatch("test-job", "gen:0:attempt:1", chunk, update)}
	cfg.Consumer = &fakeConsumer{batches: batches1}

	if err := runReconciler(t, cfg, 5*time.Second); err != nil {
		t.Fatalf("retry run: %v", err)
	}

	secondSurvivors := sink.survivorIDs()
	secondChanges := sink.changes()

	if len(firstSurvivors) != len(secondSurvivors) {
		t.Fatalf("survivor count differed after retry: first=%v second=%v", firstSurvivors, secondSurvivors)
	}
	// The new attempt writes new markers; the replayed CDC update is applied
	// once. Final state is consistent because the sink is idempotent.
	if len(secondChanges) != len(firstChanges) {
		t.Fatalf("retry changed number of CDC applications: first=%d second=%d", len(firstChanges), len(secondChanges))
	}
	final := secondChanges[len(secondChanges)-1]
	if rowOwner(final.Row) != "two-updated" {
		t.Fatalf("expected final value two-updated, got %s", rowOwner(final.Row))
	}
}

func chunkBatch(jobID, attempt string, chunk model.ChunkRange, changes ...model.Change) []kafka.Record {
	batch := []kafka.Record{rec(0, lowMarker(jobID, attempt, chunk))}
	for i, ch := range changes {
		batch = append(batch, rec(int64(i+1), ch))
	}
	batch = append(batch, rec(int64(len(changes)+1), highMarker(jobID, attempt, chunk)))
	return batch
}

// crashInject runs a reconciler until it reaches the given failpoint, cancels
// the context to simulate a crash, and then restarts with a new attempt.
// It returns the second reconciler's error and the final sink state.
func crashInject(t *testing.T, rows map[int64]model.Row, chunkSize int, fpName failpoint.Name, makeBatches func(attempt string) [][]kafka.Record, newAttempt string) (*fakeSink, error) {
	t.Helper()

	attempt0 := "gen:0:attempt:0"
	oldBatches := makeBatches(attempt0)
	cfg, sink, cpStore, _ := setup(rows, chunkSize, oldBatches)
	initialCP := *cfg.Checkpoint
	fp := failpoint.NewRegistry()
	fp.Install(fpName)
	cfg.Failpoints = fp

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	recErr := make(chan error, 1)
	go func() {
		recErr <- New(cfg).Run(ctx)
	}()

	p := fp.Get(fpName)
	if err := p.WaitForTrigger(ctx); err != nil {
		cancel()
		t.Fatalf("failpoint did not trigger: %v", err)
	}

	// Simulate crash.
	cancel()
	select {
	case err := <-recErr:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("first run error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first reconciler did not stop")
	}

	// Restart from the durable checkpoint with a new attempt. If no checkpoint
	// was committed before the crash, fall back to the initial checkpoint.
	var lastCP model.Checkpoint
	if len(cpStore.checkpoints) > 0 {
		lastCP = *cpStore.checkpoints[len(cpStore.checkpoints)-1]
	} else {
		lastCP = initialCP
	}
	sink.applied = nil
	sink.survivors = nil
	cfg.Checkpoint = &model.Checkpoint{
		JobID:            lastCP.JobID,
		Generation:       lastCP.Generation,
		Attempt:          newAttempt,
		ScanUpperBound:   lastCP.ScanUpperBound,
		CompletedThrough: lastCP.CompletedThrough,
		NextKafkaOffset:  lastCP.NextKafkaOffset,
	}
	var lastOldOffset int64 = -1
	for _, batch := range oldBatches {
		for _, record := range batch {
			if record.Offset > lastOldOffset {
				lastOldOffset = record.Offset
			}
		}
	}
	newBatches := makeBatches(newAttempt)
	for i := range newBatches {
		for j := range newBatches[i] {
			newBatches[i][j].Offset += lastOldOffset + 1
			if newBatches[i][j].Change.Marker != nil {
				newBatches[i][j].Change.Source.LSN = fmt.Sprintf("recovery-marker-lsn-%d", newBatches[i][j].Offset)
			}
		}
	}
	cfg.Consumer = &fakeConsumer{batches: append(oldBatches, newBatches...)}
	cfg.Failpoints = failpoint.NewRegistry()

	return sink, runReconciler(t, cfg, 5*time.Second)
}

func TestReconciler_DuplicateCDCEventNotCorrupt(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		2: account(2, "two", 200),
	}
	chunk := model.ChunkRange{Min: 1, Max: 2}
	// Same source transaction delivered twice.
	dup := accChange(model.OpUpdate, 2, "two-updated", 250)
	dup.Source.LSN = "lsn-dup"
	batches := [][]kafka.Record{
		{
			rec(0, lowMarker("test-job", "gen:0:attempt:0", chunk)),
			rec(1, dup),
			rec(2, highMarker("test-job", "gen:0:attempt:0", chunk)),
		},
		{
			rec(3, dup),
		},
	}
	cfg, sink, _, consumer := setup(rows, 10, batches)

	if err := runReconciler(t, cfg, 5*time.Second); err != nil {
		t.Fatalf("run reconciler: %v", err)
	}

	// The duplicate batch should be consumed (otherwise Poll would block).
	if consumer.remaining() != 0 {
		t.Fatalf("expected all batches consumed, got %d remaining", consumer.remaining())
	}
	changes := sink.changes()
	if len(changes) != 1 {
		t.Fatalf("expected source transaction to be applied once, got %d", len(changes))
	}
	final := changes[len(changes)-1]
	if rowOwner(final.Row) != "two-updated" {
		t.Fatalf("expected final value two-updated, got %s", rowOwner(final.Row))
	}
}

func TestReconciler_DuplicateLowMarkerDoesNotRestartWindow(t *testing.T) {
	rows := map[int64]model.Row{1: account(1, "one", 10)}
	chunk := model.ChunkRange{Min: 1, Max: 1}
	low := lowMarker("test-job", "gen:0:attempt:0", chunk)
	low.Source.LSN = "duplicate-low"
	cfg, sink, _, _ := setup(rows, 1, [][]kafka.Record{{
		rec(0, low), rec(1, low), rec(2, highMarker("test-job", "gen:0:attempt:0", chunk)),
	}})
	if err := runReconciler(t, cfg, time.Second); err != nil {
		t.Fatal(err)
	}
	if got := sink.survivorIDs(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("duplicate LOW lost candidate: %v", got)
	}
}

func TestReconciler_OldDuplicateCannotOverwriteNewerUpdate(t *testing.T) {
	first := accChange(model.OpUpdate, 7, "old", 10)
	first.Source.LSN = "lsn-first"
	second := accChange(model.OpUpdate, 7, "new", 20)
	second.Source.LSN = "lsn-second"
	cfg, sink, _, _ := setup(nil, 10, [][]kafka.Record{
		{rec(0, first)},
		{rec(1, second)},
		{rec(2, first)},
	})
	cfg.Checkpoint.CompletedThrough = cfg.Checkpoint.ScanUpperBound
	if err := runReconciler(t, cfg, 5*time.Second); err != nil {
		t.Fatalf("run reconciler: %v", err)
	}
	changes := sink.changes()
	if len(changes) != 2 || rowOwner(changes[1].Row) != "new" {
		t.Fatalf("late replay overwrote newer update: %+v", changes)
	}
}

func TestReconciler_RejectsWrongSourceCluster(t *testing.T) {
	change := accChange(model.OpInsert, 1, "wrong", 10)
	change.Source.SystemID = "other-cluster"
	cfg, sink, _, _ := setup(nil, 10, [][]kafka.Record{{rec(0, change)}})
	cfg.JobConfig.SourceSystemID = "pinned-cluster"
	if err := runReconciler(t, cfg, time.Second); err == nil || !strings.Contains(err.Error(), "source system identifier") {
		t.Fatalf("expected source identity rejection, got %v", err)
	}
	if len(sink.changes()) != 0 {
		t.Fatal("wrong-cluster record changed destination")
	}
}

func contains(ids []int64, target int64) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}

// TestReconciler_MarkerStateMachineErrors covers unexpected marker ordering.
func TestReconciler_MarkerStateMachineErrors(t *testing.T) {
	tests := []struct {
		name    string
		batches [][]kafka.Record
	}{
		{
			name: "high before low",
			batches: [][]kafka.Record{{
				rec(0, highMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 1, Max: 1})),
			}},
		},
		{
			name: "duplicate low",
			batches: [][]kafka.Record{{
				rec(0, lowMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 1, Max: 1})),
				rec(1, lowMarker("test-job", "gen:0:attempt:0", model.ChunkRange{Min: 1, Max: 1})),
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, _, _, _ := setup(map[int64]model.Row{1: account(1, "one", 100)}, 10, tt.batches)
			err := runReconciler(t, cfg, 2*time.Second)
			if err == nil {
				t.Fatal("expected error from invalid marker sequence")
			}
		})
	}
}

// TestReconciler_StaleMarkersIgnored verifies markers from a previous attempt
// are ignored when a chunk is retried with a new attempt.
func TestReconciler_StaleMarkersIgnored(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
	}
	chunk := model.ChunkRange{Min: 1, Max: 1}
	oldAttempt := "gen:0:attempt:0"
	newAttempt := "gen:0:attempt:1"
	batches := [][]kafka.Record{
		{
			// Stale markers from previous attempt appear first.
			rec(0, lowMarker("test-job", oldAttempt, chunk)),
			rec(1, highMarker("test-job", oldAttempt, chunk)),
			// New attempt markers follow.
			rec(2, lowMarker("test-job", newAttempt, chunk)),
			rec(3, highMarker("test-job", newAttempt, chunk)),
		},
	}
	cfg, sink, cpStore, _ := setup(rows, 10, batches)
	cfg.Checkpoint.Attempt = newAttempt

	if err := runReconciler(t, cfg, 5*time.Second); err != nil {
		t.Fatalf("run reconciler: %v", err)
	}

	if len(sink.survivorIDs()) != 1 {
		t.Fatalf("expected 1 survivor, got %v", sink.survivorIDs())
	}
	last := cpStore.checkpoints[len(cpStore.checkpoints)-1]
	if last.CompletedThrough != 1 {
		t.Fatalf("expected chunk to complete with new attempt, completed_through=%d", last.CompletedThrough)
	}
}

// TestReconciler_MixedConcurrentOperations verifies INSERT, UPDATE and DELETE
// happening to different keys inside the same window.
func TestReconciler_MixedConcurrentOperations(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		2: account(2, "two", 200),
		3: account(3, "three", 300),
		4: account(4, "four", 400),
	}
	chunk := model.ChunkRange{Min: 1, Max: 4}
	batches := [][]kafka.Record{
		{
			rec(0, lowMarker("test-job", "gen:0:attempt:0", chunk)),
			rec(1, accChange(model.OpUpdate, 1, "one-updated", 101)),
			rec(2, accChange(model.OpDelete, 2, "", 0)),
			rec(3, accChange(model.OpInsert, 5, "five-inserted", 500)),
			rec(4, accChange(model.OpUpdate, 3, "three-updated", 303)),
			rec(5, highMarker("test-job", "gen:0:attempt:0", chunk)),
		},
	}
	cfg, sink, _, _ := setup(rows, 10, batches)

	if err := runReconciler(t, cfg, 5*time.Second); err != nil {
		t.Fatalf("run reconciler: %v", err)
	}

	survivors := sink.survivorIDs()
	if contains(survivors, 1) || contains(survivors, 2) || contains(survivors, 3) {
		t.Fatalf("expected updated/deleted rows evicted, survivors=%v", survivors)
	}
	if !contains(survivors, 4) {
		t.Fatalf("expected untouched row 4 to survive, survivors=%v", survivors)
	}

	changes := sink.changes()
	if len(changes) != 4 {
		t.Fatalf("expected 4 CDC changes, got %d", len(changes))
	}
}

// TestReconciler_CDCAfterHighAppliesLater checks that a change committed after
// the HIGH marker is applied after the chunk commits.
func TestReconciler_CDCAfterHighAppliesLater(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		2: account(2, "two", 200),
	}
	chunk := model.ChunkRange{Min: 1, Max: 2}
	batches := [][]kafka.Record{
		{
			rec(0, lowMarker("test-job", "gen:0:attempt:0", chunk)),
			rec(1, highMarker("test-job", "gen:0:attempt:0", chunk)),
			// Change committed after HIGH is in the same poll batch but after the marker.
			rec(2, accChange(model.OpUpdate, 1, "one-late", 999)),
		},
	}
	cfg, sink, _, _ := setup(rows, 10, batches)

	if err := runReconciler(t, cfg, 5*time.Second); err != nil {
		t.Fatalf("run reconciler: %v", err)
	}

	// Survivors include id 1 because the update happened after HIGH.
	survivors := sink.survivorIDs()
	if !contains(survivors, 1) {
		t.Fatalf("expected id 1 to survive (update after HIGH), survivors=%v", survivors)
	}
	changes := sink.changes()
	if len(changes) != 1 || rowOwner(changes[0].Row) != "one-late" {
		t.Fatalf("expected late CDC update to be applied, changes=%v", changes)
	}
}

// TestReconciler_BackpressureAndBoundedBatches verifies the reconciler drains
// all records from a poll batch before polling again, keeping memory bounded
// to the batch size.
func TestReconciler_BackpressureAndBoundedBatches(t *testing.T) {
	rows := map[int64]model.Row{1: account(1, "one", 100)}
	chunk := model.ChunkRange{Min: 1, Max: 1}
	// Two batches ensure the second is not fetched until the first is drained.
	batches := [][]kafka.Record{
		{
			rec(0, lowMarker("test-job", "gen:0:attempt:0", chunk)),
			rec(1, highMarker("test-job", "gen:0:attempt:0", chunk)),
		},
		{
			rec(2, accChange(model.OpInsert, 2, "two", 200)),
		},
	}
	cfg, sink, _, consumer := setup(rows, 10, batches)

	if err := runReconciler(t, cfg, 5*time.Second); err != nil {
		t.Fatalf("run reconciler: %v", err)
	}

	if consumer.remaining() != 0 {
		t.Fatalf("expected all batches consumed, got %d remaining", consumer.remaining())
	}
	if len(sink.changes()) != 1 {
		t.Fatalf("expected post-backfill CDC change, got %d", len(sink.changes()))
	}
}

// TestFailpointPause verifies failpoints can pause and resume the reconciler.
func TestFailpointPause(t *testing.T) {
	rows := map[int64]model.Row{1: account(1, "one", 100)}
	chunk := model.ChunkRange{Min: 1, Max: 1}
	batches := [][]kafka.Record{
		{
			rec(0, lowMarker("test-job", "gen:0:attempt:0", chunk)),
			rec(1, highMarker("test-job", "gen:0:attempt:0", chunk)),
		},
	}
	cfg, _, _, _ := setup(rows, 10, batches)
	fp := failpoint.NewRegistry()
	fp.Install(failpoint.AfterLowMarkerCommitted)
	cfg.Failpoints = fp

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- New(cfg).Run(ctx)
	}()

	p := fp.Get(failpoint.AfterLowMarkerCommitted)
	if err := p.WaitForTrigger(ctx); err != nil {
		t.Fatalf("failpoint did not trigger: %v", err)
	}
	p.Resume()

	select {
	case err := <-done:
		if err != nil && err != context.DeadlineExceeded && err != context.Canceled {
			t.Fatalf("reconciler error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reconciler did not finish")
	}
}

func TestCrash_AfterLowMarkerCommitted(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		2: account(2, "two", 200),
	}
	chunk := model.ChunkRange{Min: 1, Max: 2}
	sink, err := crashInject(t, rows, 10, failpoint.AfterLowMarkerCommitted,
		func(a string) [][]kafka.Record { return [][]kafka.Record{chunkBatch("test-job", a, chunk)} },
		"gen:0:attempt:1")
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	if len(sink.survivorIDs()) != 2 {
		t.Fatalf("expected 2 survivors after recovery, got %v", sink.survivorIDs())
	}
}

func TestCrash_AfterChunkRead(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		2: account(2, "two", 200),
	}
	chunk := model.ChunkRange{Min: 1, Max: 2}
	sink, err := crashInject(t, rows, 10, failpoint.AfterChunkReadBeforeReconciliation,
		func(a string) [][]kafka.Record { return [][]kafka.Record{chunkBatch("test-job", a, chunk)} },
		"gen:0:attempt:1")
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	if len(sink.survivorIDs()) != 2 {
		t.Fatalf("expected 2 survivors after recovery, got %v", sink.survivorIDs())
	}
}

func TestCrash_AfterHighMarkerObserved(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		2: account(2, "two", 200),
	}
	chunk := model.ChunkRange{Min: 1, Max: 2}
	sink, err := crashInject(t, rows, 10, failpoint.AfterHighMarkerObserved,
		func(a string) [][]kafka.Record { return [][]kafka.Record{chunkBatch("test-job", a, chunk)} },
		"gen:0:attempt:1")
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	if len(sink.survivorIDs()) != 2 {
		t.Fatalf("expected 2 survivors after recovery, got %v", sink.survivorIDs())
	}
}

func TestCrash_BeforeChunkCommit(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		2: account(2, "two", 200),
	}
	chunk := model.ChunkRange{Min: 1, Max: 2}
	sink, err := crashInject(t, rows, 10, failpoint.BeforeChunkCommit,
		func(a string) [][]kafka.Record { return [][]kafka.Record{chunkBatch("test-job", a, chunk)} },
		"gen:0:attempt:1")
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	if len(sink.survivorIDs()) != 2 {
		t.Fatalf("expected 2 survivors after recovery, got %v", sink.survivorIDs())
	}
}

func TestCrash_AfterDestinationCommit(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		2: account(2, "two", 200),
	}
	chunk := model.ChunkRange{Min: 1, Max: 2}
	sink, err := crashInject(t, rows, 10, failpoint.AfterDestinationCommit,
		func(a string) [][]kafka.Record { return [][]kafka.Record{chunkBatch("test-job", a, chunk)} },
		"gen:0:attempt:1")
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	// After the commit the chunk is already complete; the second attempt sees
	// the checkpoint advanced past the chunk and finishes with no extra survivors.
	if len(sink.survivorIDs()) != 0 {
		t.Fatalf("expected 0 survivors after already-committed chunk, got %v", sink.survivorIDs())
	}
}

// TestAtomicChunkCompletion verifies that chunk state, checkpoint, and
// survivors are committed in the same transaction.
func TestAtomicChunkCompletion(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		2: account(2, "two", 200),
	}
	chunk := model.ChunkRange{Min: 1, Max: 2}
	attempt0 := "gen:0:attempt:0"
	cfg, sink, cpStore, _ := setup(rows, 10, [][]kafka.Record{chunkBatch("test-job", attempt0, chunk)})

	chunkStore := &fakeChunkStore{}
	chunkStore.CreateChunk(context.Background(), &model.Chunk{
		JobID:      cfg.JobConfig.JobID,
		ChunkMinID: 1,
		ChunkMaxID: 2,
		Attempt:    attempt0,
		Status:     model.ChunkPending,
	})
	cfg.ChunkStore = chunkStore
	cfg.JobConfig.WorkerID = "test-worker"
	cfg.JobConfig.LeaseDuration = time.Minute

	if err := runReconciler(t, cfg, 5*time.Second); err != nil {
		t.Fatalf("run reconciler: %v", err)
	}

	if chunkStore.chunksWithStatus(model.ChunkCompleted) != 1 {
		t.Fatalf("expected chunk completed, got status counts: pending=%d completed=%d",
			chunkStore.chunksWithStatus(model.ChunkPending), chunkStore.chunksWithStatus(model.ChunkCompleted))
	}
	if len(sink.survivorIDs()) != 2 {
		t.Fatalf("expected 2 survivors, got %v", sink.survivorIDs())
	}
	lastCP := cpStore.checkpoints[len(cpStore.checkpoints)-1]
	if lastCP.CompletedThrough != 2 {
		t.Fatalf("expected checkpoint advanced to 2, got %d", lastCP.CompletedThrough)
	}
}

// TestReconciler_ChunkStoreMode verifies the reconciler leases chunks from a
// durable chunk store, processes them, and transitions their status.
func TestReconciler_ChunkStoreMode(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		2: account(2, "two", 200),
		3: account(3, "three", 300),
	}
	chunk := model.ChunkRange{Min: 1, Max: 3}
	batches := [][]kafka.Record{chunkBatch("test-job", "gen:0:attempt:0", chunk)}
	cfg, sink, _, _ := setup(rows, 10, batches)

	chunkStore := &fakeChunkStore{}
	chunkStore.CreateChunk(context.Background(), &model.Chunk{
		JobID:      cfg.JobConfig.JobID,
		ChunkMinID: 1,
		ChunkMaxID: 3,
		Attempt:    cfg.Checkpoint.Attempt,
		Status:     model.ChunkPending,
	})
	cfg.ChunkStore = chunkStore
	cfg.JobConfig.WorkerID = "test-worker"
	cfg.JobConfig.LeaseDuration = time.Minute

	if err := runReconciler(t, cfg, 5*time.Second); err != nil {
		t.Fatalf("run reconciler: %v", err)
	}

	if chunkStore.chunksWithStatus(model.ChunkCompleted) != 1 {
		t.Fatalf("expected 1 completed chunk, got pending=%d completed=%d",
			chunkStore.chunksWithStatus(model.ChunkPending), chunkStore.chunksWithStatus(model.ChunkCompleted))
	}
	if len(sink.survivorIDs()) != 3 {
		t.Fatalf("expected 3 survivors, got %v", sink.survivorIDs())
	}
}

// TestBoundedMemory_CandidateCap fails the job when a chunk read exceeds the
// configured in-memory candidate limit instead of growing memory without bound.
func TestBoundedMemory_CandidateCap(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		2: account(2, "two", 200),
		3: account(3, "three", 300),
	}
	chunk := model.ChunkRange{Min: 1, Max: 3}
	cfg, _, _, _ := setup(rows, 10, [][]kafka.Record{chunkBatch("test-job", "gen:0:attempt:0", chunk)})
	cfg.JobConfig.MaxInMemoryCandidates = 2 // smaller than the chunk's 3 rows

	err := runReconciler(t, cfg, 5*time.Second)
	if err == nil {
		t.Fatal("expected error when chunk exceeds max in-memory candidates")
	}
	if !strings.Contains(err.Error(), "max in-memory candidates") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestBoundedMemory_BatchCap fails the job when the consumer hands over a
// batch larger than the configured record limit.
func TestBoundedMemory_BatchCap(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		2: account(2, "two", 200),
	}
	chunk := model.ChunkRange{Min: 1, Max: 2}
	cfg, _, _, _ := setup(rows, 10, [][]kafka.Record{chunkBatch("test-job", "gen:0:attempt:0", chunk)})
	cfg.JobConfig.MaxRecordsPerBatch = 1 // the batch has 2 records (low, high)

	err := runReconciler(t, cfg, 5*time.Second)
	if err == nil {
		t.Fatal("expected error when batch exceeds max records per batch")
	}
	if !strings.Contains(err.Error(), "max records per batch") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestBoundedMemory_DefaultsUnlimited verifies the zero-value config keeps the
// legacy unlimited behavior so fakes and small tests stay simple.
func TestBoundedMemory_DefaultsUnlimited(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		2: account(2, "two", 200),
	}
	chunk := model.ChunkRange{Min: 1, Max: 2}
	cfg, sink, _, _ := setup(rows, 10, [][]kafka.Record{chunkBatch("test-job", "gen:0:attempt:0", chunk)})

	if err := runReconciler(t, cfg, 5*time.Second); err != nil {
		t.Fatalf("run reconciler: %v", err)
	}
	if len(sink.survivorIDs()) != 2 {
		t.Fatalf("expected 2 survivors, got %v", sink.survivorIDs())
	}
}

// fakeChunkSizer records adaptive sizing interactions for tests.
type fakeChunkSizer struct {
	mu          sync.Mutex
	size        int
	suggestions int
	observes    []time.Duration
}

func (s *fakeChunkSizer) Suggest() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.suggestions++
	return s.size
}

func (s *fakeChunkSizer) Observe(d time.Duration, rows int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observes = append(s.observes, d)
}

// TestAdaptiveChunkSizeConsulted verifies the legacy loop asks the adaptive
// sizer for each chunk size and feeds completed-chunk durations back to it.
func TestAdaptiveChunkSizeConsulted(t *testing.T) {
	rows := map[int64]model.Row{
		1: account(1, "one", 100),
		2: account(2, "two", 200),
	}
	chunk := model.ChunkRange{Min: 1, Max: 2}
	cfg, sink, _, _ := setup(rows, 10, [][]kafka.Record{chunkBatch("test-job", "gen:0:attempt:0", chunk)})

	sizer := &fakeChunkSizer{size: 10}
	cfg.Adaptive = sizer

	if err := runReconciler(t, cfg, 5*time.Second); err != nil {
		t.Fatalf("run reconciler: %v", err)
	}
	if len(sink.survivorIDs()) != 2 {
		t.Fatalf("expected 2 survivors, got %v", sink.survivorIDs())
	}
	sizer.mu.Lock()
	defer sizer.mu.Unlock()
	if sizer.suggestions == 0 {
		t.Fatal("adaptive sizer was never asked for a chunk size")
	}
	if len(sizer.observes) != 1 {
		t.Fatalf("expected 1 completed-chunk observation, got %d", len(sizer.observes))
	}
}

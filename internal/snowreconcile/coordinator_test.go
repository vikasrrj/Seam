package snowreconcile

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/seam/internal/model"
	seamsnowflake "example.com/seam/internal/snowflake"
)

type callLog struct {
	mu    sync.Mutex
	calls []string
}

func (log *callLog) add(call string) {
	log.mu.Lock()
	defer log.mu.Unlock()
	log.calls = append(log.calls, call)
}

func (log *callLog) index(call string) int {
	log.mu.Lock()
	defer log.mu.Unlock()
	for index, value := range log.calls {
		if value == call {
			return index
		}
	}
	return -1
}

type fakeScanner struct {
	log       *callLog
	upper     int64
	pages     []model.ChunkRange
	pageIndex int
	rows      []model.Row
	blockRead bool
}

func (scanner *fakeScanner) UpperBound(context.Context) (int64, error) {
	scanner.log.add("upper")
	return scanner.upper, nil
}

func (scanner *fakeScanner) NextChunkFrom(_ context.Context, _ *int64, _ int64, _ int) (model.ChunkRange, bool, error) {
	scanner.log.add("discover")
	if scanner.pageIndex >= len(scanner.pages) {
		return model.ChunkRange{}, false, nil
	}
	page := scanner.pages[scanner.pageIndex]
	scanner.pageIndex++
	return page, true, nil
}

func (scanner *fakeScanner) ReadChunk(ctx context.Context, _, _ int64) ([]model.Row, error) {
	scanner.log.add("scan")
	if scanner.blockRead {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return scanner.rows, nil
}

type fakeMarkers struct{ log *callLog }

func (markers *fakeMarkers) WriteLow(context.Context, string, string, model.ChunkRange) (string, error) {
	markers.log.add("low")
	return "low-marker", nil
}

func (markers *fakeMarkers) WriteHigh(context.Context, string, string, model.ChunkRange) (string, error) {
	markers.log.add("high")
	return "high-marker", nil
}

type fakeWarehouse struct {
	log          *callLog
	mu           sync.Mutex
	state        seamsnowflake.BackfillState
	leaseGiven   bool
	finalized    bool
	stageErr     error
	heartbeatErr error
	manifest     seamsnowflake.BackfillSpec
}

func (warehouse *fakeWarehouse) PrepareBackfill(context.Context, string, string, string) (*seamsnowflake.BackfillJob, error) {
	warehouse.log.add("prepare")
	warehouse.mu.Lock()
	defer warehouse.mu.Unlock()
	if warehouse.state == "" {
		warehouse.state = seamsnowflake.BackfillDiscovering
	}
	return &seamsnowflake.BackfillJob{State: warehouse.state}, nil
}

func (warehouse *fakeWarehouse) SealBackfillManifest(_ context.Context, spec seamsnowflake.BackfillSpec) (*seamsnowflake.BackfillJob, error) {
	warehouse.log.add("seal-manifest")
	warehouse.mu.Lock()
	defer warehouse.mu.Unlock()
	warehouse.manifest = spec
	warehouse.state = seamsnowflake.BackfillRunning
	return &seamsnowflake.BackfillJob{Spec: spec, State: warehouse.state}, nil
}

func (warehouse *fakeWarehouse) LoadBackfill(context.Context, string) (*seamsnowflake.BackfillJob, error) {
	warehouse.log.add("load-job")
	warehouse.mu.Lock()
	defer warehouse.mu.Unlock()
	return &seamsnowflake.BackfillJob{Spec: warehouse.manifest, State: warehouse.state}, nil
}

func (warehouse *fakeWarehouse) LeaseChunk(_ context.Context, jobID, attempt, workerID string, _ time.Duration) (*seamsnowflake.ChunkLease, error) {
	warehouse.log.add("lease")
	warehouse.mu.Lock()
	defer warehouse.mu.Unlock()
	if warehouse.leaseGiven {
		return nil, nil
	}
	warehouse.leaseGiven = true
	return &seamsnowflake.ChunkLease{
		StreamID: "stream", JobID: jobID, Attempt: attempt, WorkerID: workerID, LeaseToken: 1,
		Range: model.ChunkRange{Min: math.MinInt64, Max: 10},
	}, nil
}

func (warehouse *fakeWarehouse) RenewChunkLease(context.Context, *seamsnowflake.ChunkLease, time.Duration) error {
	warehouse.log.add("heartbeat")
	return warehouse.heartbeatErr
}

func (warehouse *fakeWarehouse) BeginChunkScan(context.Context, *seamsnowflake.ChunkLease, string) error {
	warehouse.log.add("begin-scan")
	return nil
}

func (warehouse *fakeWarehouse) StageSnapshot(context.Context, *seamsnowflake.ChunkLease, []model.Row) error {
	warehouse.log.add("stage")
	return warehouse.stageErr
}

func (warehouse *fakeWarehouse) SealChunkScan(context.Context, *seamsnowflake.ChunkLease, string) error {
	warehouse.log.add("seal-chunk")
	return nil
}

func (warehouse *fakeWarehouse) WaitForMarker(context.Context, string, time.Duration) error {
	warehouse.log.add("wait-high")
	return nil
}

func (warehouse *fakeWarehouse) FinalizeChunk(context.Context, *seamsnowflake.ChunkLease) error {
	warehouse.log.add("finalize")
	warehouse.mu.Lock()
	defer warehouse.mu.Unlock()
	warehouse.finalized = true
	warehouse.state = seamsnowflake.BackfillReadyToVerify
	return nil
}

func TestCoordinatorActivatesRouteBeforeSamplingSource(t *testing.T) {
	log := &callLog{}
	warehouse := &fakeWarehouse{log: log}
	scanner := &fakeScanner{
		log: log, upper: 10, pages: []model.ChunkRange{{Min: 1, Max: 10}},
		rows: []model.Row{{Values: []model.Value{model.Int64Value(1)}}},
	}
	coordinator, err := New(Config{
		JobID: "job", Attempt: "attempt", ShadowTable: "ACCOUNTS_SHADOW", ChunkSize: 100,
		Workers: 1, WorkerID: "worker", Lease: time.Second, Heartbeat: 500 * time.Millisecond, Poll: time.Millisecond,
	}, warehouse, scanner, &fakeMarkers{log: log})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Run(context.Background()); err != nil {
		t.Fatalf("run coordinator: %v", err)
	}
	for before, after := range map[string]string{
		"prepare": "upper", "upper": "discover", "discover": "seal-manifest",
		"low": "begin-scan", "begin-scan": "scan", "scan": "stage",
		"stage": "high", "high": "seal-chunk", "seal-chunk": "wait-high", "wait-high": "finalize",
	} {
		if log.index(before) < 0 || log.index(after) < 0 || log.index(before) >= log.index(after) {
			t.Fatalf("expected %q before %q, calls=%v", before, after, log.calls)
		}
	}
	if len(warehouse.manifest.Chunks) != 1 || warehouse.manifest.Chunks[0] != (model.ChunkRange{Min: math.MinInt64, Max: 10}) {
		t.Fatalf("unexpected sealed manifest: %+v", warehouse.manifest.Chunks)
	}
}

func TestCoordinatorStopsBeforeHighWhenSnapshotStageFails(t *testing.T) {
	log := &callLog{}
	warehouse := &fakeWarehouse{log: log, stageErr: errors.New("warehouse unavailable")}
	scanner := &fakeScanner{log: log, upper: 10, pages: []model.ChunkRange{{Min: 1, Max: 10}}}
	coordinator, err := New(Config{
		JobID: "job", Attempt: "attempt", ShadowTable: "ACCOUNTS_SHADOW", ChunkSize: 100,
		Workers: 1, WorkerID: "worker", Lease: time.Second, Heartbeat: 500 * time.Millisecond, Poll: time.Millisecond,
	}, warehouse, scanner, &fakeMarkers{log: log})
	if err != nil {
		t.Fatal(err)
	}
	err = coordinator.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "warehouse unavailable") {
		t.Fatalf("error = %v", err)
	}
	if log.index("high") >= 0 || log.index("finalize") >= 0 {
		t.Fatalf("coordinator crossed failed stage boundary: %v", log.calls)
	}
}

func TestLeaseHeartbeatFailureCancelsBlockedScan(t *testing.T) {
	log := &callLog{}
	warehouse := &fakeWarehouse{log: log, heartbeatErr: errors.New("lease fenced")}
	scanner := &fakeScanner{log: log, upper: 10, pages: []model.ChunkRange{{Min: 1, Max: 10}}, blockRead: true}
	coordinator, err := New(Config{
		JobID: "job", Attempt: "attempt", ShadowTable: "ACCOUNTS_SHADOW", ChunkSize: 100,
		Workers: 1, WorkerID: "worker", Lease: 50 * time.Millisecond, Heartbeat: 5 * time.Millisecond, Poll: time.Millisecond,
	}, warehouse, scanner, &fakeMarkers{log: log})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = coordinator.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "lease heartbeat failed: lease fenced") {
		t.Fatalf("error = %v", err)
	}
	if log.index("high") >= 0 {
		t.Fatalf("HIGH was written after lease loss: %v", log.calls)
	}
}

type manifestScanner struct {
	pages []model.ChunkRange
	index int
}

func (*manifestScanner) UpperBound(context.Context) (int64, error) { return 0, nil }
func (scanner *manifestScanner) NextChunkFrom(context.Context, *int64, int64, int) (model.ChunkRange, bool, error) {
	if scanner.index >= len(scanner.pages) {
		return model.ChunkRange{}, false, nil
	}
	page := scanner.pages[scanner.index]
	scanner.index++
	return page, true, nil
}
func (*manifestScanner) ReadChunk(context.Context, int64, int64) ([]model.Row, error) {
	return nil, nil
}

func TestDiscoverManifestCoversSparseAndDeletedRanges(t *testing.T) {
	scanner := &manifestScanner{pages: []model.ChunkRange{{Min: -5, Max: 5}, {Min: 20, Max: 30}}}
	manifest, err := DiscoverManifest(context.Background(), scanner, 50, 10)
	if err != nil {
		t.Fatal(err)
	}
	want := []model.ChunkRange{{Min: math.MinInt64, Max: 5}, {Min: 6, Max: 30}, {Min: 31, Max: 50}}
	if len(manifest) != len(want) {
		t.Fatalf("manifest = %v, want %v", manifest, want)
	}
	for index := range want {
		if manifest[index] != want[index] {
			t.Fatalf("manifest[%d] = %v, want %v", index, manifest[index], want[index])
		}
	}
}

func TestDiscoverManifestRejectsOverlappingPage(t *testing.T) {
	scanner := &manifestScanner{pages: []model.ChunkRange{{Min: 1, Max: 10}, {Min: 10, Max: 20}}}
	_, err := DiscoverManifest(context.Background(), scanner, 30, 10)
	if err == nil || !strings.Contains(err.Error(), "invalid source discovery page") {
		t.Fatalf("error = %v", err)
	}
}

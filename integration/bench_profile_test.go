//go:build integration

package integration

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"example.com/seam/internal/checkpoint"
	"example.com/seam/internal/kafka"
	"example.com/seam/internal/marker"
	"example.com/seam/internal/model"
	resourcecontrol "example.com/seam/internal/resource"
	"example.com/seam/internal/scan"
	"example.com/seam/internal/schema"
	"example.com/seam/internal/sink"
	"github.com/jackc/pgx/v5"
)

type profileInterval struct {
	start time.Time
	end   time.Time
}

type benchProfile struct {
	mu               sync.Mutex
	intervals        map[string][]profileInterval
	measurementStart time.Time
}

type postgresProfile struct {
	walLSNBytes      int64
	xactCommit       int64
	xactRollback     int64
	blksRead         int64
	blksHit          int64
	tempFiles        int64
	tempBytes        int64
	walRecords       int64
	walFPI           int64
	walBytes         int64
	walWrites        int64
	walSyncs         int64
	relationReads    int64
	relationWrites   int64
	relationExtends  int64
	relationHits     int64
	userRowsInserted int64
	userRowsUpdated  int64
	userRowsDeleted  int64
}

func resetPostgresProfile(ctx context.Context, dsn string) (string, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return "", err
	}
	defer conn.Close(context.Background())
	if _, err = conn.Exec(ctx, `SELECT pg_stat_reset(); SELECT pg_stat_reset_shared('wal'); SELECT pg_stat_reset_shared('io')`); err != nil {
		return "", err
	}
	var walLSN string
	if err := conn.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&walLSN); err != nil {
		return "", err
	}
	return walLSN, nil
}

func readPostgresProfile(ctx context.Context, dsn, baselineWALLSN string) (postgresProfile, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return postgresProfile{}, err
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, `SELECT pg_stat_force_next_flush()`); err != nil {
		return postgresProfile{}, err
	}
	var result postgresProfile
	if err := conn.QueryRow(ctx, `SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), $1::pg_lsn)::bigint`, baselineWALLSN).Scan(&result.walLSNBytes); err != nil {
		return postgresProfile{}, err
	}
	if err := conn.QueryRow(ctx, `
		SELECT xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes
		FROM pg_stat_database WHERE datname = current_database()`).Scan(
		&result.xactCommit, &result.xactRollback, &result.blksRead, &result.blksHit, &result.tempFiles, &result.tempBytes); err != nil {
		return postgresProfile{}, err
	}
	if err := conn.QueryRow(ctx, `SELECT wal_records, wal_fpi, wal_bytes::bigint, wal_write, wal_sync FROM pg_stat_wal`).Scan(
		&result.walRecords, &result.walFPI, &result.walBytes, &result.walWrites, &result.walSyncs); err != nil {
		return postgresProfile{}, err
	}
	if err := conn.QueryRow(ctx, `
		SELECT COALESCE(sum(reads), 0), COALESCE(sum(writes), 0), COALESCE(sum(extends), 0), COALESCE(sum(hits), 0)
		FROM pg_stat_io WHERE backend_type = 'client backend' AND object = 'relation'`).Scan(
		&result.relationReads, &result.relationWrites, &result.relationExtends, &result.relationHits); err != nil {
		return postgresProfile{}, err
	}
	if err := conn.QueryRow(ctx, `
		SELECT COALESCE(sum(n_tup_ins), 0), COALESCE(sum(n_tup_upd), 0), COALESCE(sum(n_tup_del), 0)
		FROM pg_stat_user_tables`).Scan(&result.userRowsInserted, &result.userRowsUpdated, &result.userRowsDeleted); err != nil {
		return postgresProfile{}, err
	}
	return result, nil
}

func printPostgresProfile(role string, profile postgresProfile) {
	fmt.Printf("SEAM_POSTGRES role=%s wal_lsn_bytes=%d xact_commit=%d xact_rollback=%d blks_read=%d blks_hit=%d temp_files=%d temp_bytes=%d wal_records=%d wal_fpi=%d wal_bytes=%d wal_writes=%d wal_syncs=%d relation_reads=%d relation_writes=%d relation_extends=%d relation_hits=%d rows_inserted=%d rows_updated=%d rows_deleted=%d\n",
		role, profile.walLSNBytes, profile.xactCommit, profile.xactRollback, profile.blksRead, profile.blksHit,
		profile.tempFiles, profile.tempBytes, profile.walRecords, profile.walFPI, profile.walBytes,
		profile.walWrites, profile.walSyncs, profile.relationReads, profile.relationWrites,
		profile.relationExtends, profile.relationHits, profile.userRowsInserted,
		profile.userRowsUpdated, profile.userRowsDeleted)
}

func newBenchProfile() *benchProfile {
	return &benchProfile{intervals: make(map[string][]profileInterval)}
}

func (p *benchProfile) reset() {
	p.mu.Lock()
	p.intervals = make(map[string][]profileInterval)
	p.measurementStart = time.Now()
	p.mu.Unlock()
}

func (p *benchProfile) start() func(string) {
	started := time.Now()
	return func(name string) {
		ended := time.Now()
		p.mu.Lock()
		if started.Before(p.measurementStart) {
			started = p.measurementStart
		}
		if !ended.After(started) {
			p.mu.Unlock()
			return
		}
		p.intervals[name] = append(p.intervals[name], profileInterval{start: started, end: ended})
		p.mu.Unlock()
	}
}

type profileSummary struct {
	name           string
	count          int
	service        time.Duration
	union          time.Duration
	maxConcurrency int
}

func (p *benchProfile) summaries() []profileSummary {
	p.mu.Lock()
	defer p.mu.Unlock()
	summaries := make([]profileSummary, 0, len(p.intervals))
	for name, source := range p.intervals {
		intervals := append([]profileInterval(nil), source...)
		sort.Slice(intervals, func(i, j int) bool { return intervals[i].start.Before(intervals[j].start) })
		summary := profileSummary{name: name, count: len(intervals)}
		for _, interval := range intervals {
			summary.service += interval.end.Sub(interval.start)
		}
		if len(intervals) > 0 {
			unionStart, unionEnd := intervals[0].start, intervals[0].end
			for _, interval := range intervals[1:] {
				if interval.start.After(unionEnd) {
					summary.union += unionEnd.Sub(unionStart)
					unionStart, unionEnd = interval.start, interval.end
				} else if interval.end.After(unionEnd) {
					unionEnd = interval.end
				}
			}
			summary.union += unionEnd.Sub(unionStart)
		}
		type event struct {
			at    time.Time
			delta int
		}
		events := make([]event, 0, len(intervals)*2)
		for _, interval := range intervals {
			events = append(events, event{at: interval.start, delta: 1}, event{at: interval.end, delta: -1})
		}
		sort.Slice(events, func(i, j int) bool {
			if events[i].at.Equal(events[j].at) {
				return events[i].delta < events[j].delta
			}
			return events[i].at.Before(events[j].at)
		})
		active := 0
		for _, event := range events {
			active += event.delta
			if active > summary.maxConcurrency {
				summary.maxConcurrency = active
			}
		}
		summaries = append(summaries, summary)
	}
	sort.Slice(summaries, func(i, j int) bool {
		if summaries[i].union == summaries[j].union {
			return summaries[i].name < summaries[j].name
		}
		return summaries[i].union > summaries[j].union
	})
	return summaries
}

func (p *benchProfile) report(iterations int, reconcileWall time.Duration) {
	for _, summary := range p.summaries() {
		average := time.Duration(0)
		if summary.count > 0 {
			average = summary.service / time.Duration(summary.count)
		}
		wallPercent := 0.0
		if reconcileWall > 0 {
			wallPercent = 100 * float64(summary.union) / float64(reconcileWall)
		}
		fmt.Printf("SEAM_PROFILE operation=%s count_per_op=%.2f service_ms_per_op=%.3f union_ms_per_op=%.3f avg_ms=%.3f max_concurrency=%d reconcile_wall_pct=%.2f\n",
			summary.name,
			float64(summary.count)/float64(iterations),
			float64(summary.service.Microseconds())/1000/float64(iterations),
			float64(summary.union.Microseconds())/1000/float64(iterations),
			float64(average.Microseconds())/1000,
			summary.maxConcurrency,
			wallPercent,
		)
	}
}

type profiledConsumer struct {
	base    *kafka.Consumer
	profile *benchProfile
}

func (p *profiledConsumer) Poll(ctx context.Context) (records []kafka.Record, err error) {
	finish := p.profile.start()
	defer func() { finish("kafka-poll-decode") }()
	return p.base.Poll(ctx)
}

func (p *profiledConsumer) PollTransactions(ctx context.Context) (transactions []*kafka.Transaction, err error) {
	finish := p.profile.start()
	defer func() { finish("kafka-poll-reassemble") }()
	return p.base.PollTransactions(ctx)
}

func (p *profiledConsumer) Close() { p.base.Close() }

type profiledMarkerStore struct {
	base    *marker.Store
	profile *benchProfile
}

func (p *profiledMarkerStore) WriteLow(ctx context.Context, jobID, attempt string, chunk model.ChunkRange) (id string, err error) {
	finish := p.profile.start()
	defer func() { finish("source-marker-low") }()
	return p.base.WriteLow(ctx, jobID, attempt, chunk)
}

func (p *profiledMarkerStore) WriteHigh(ctx context.Context, jobID, attempt string, chunk model.ChunkRange) (id string, err error) {
	finish := p.profile.start()
	defer func() { finish("source-marker-high") }()
	return p.base.WriteHigh(ctx, jobID, attempt, chunk)
}

type profiledScanner struct {
	base    *scan.ChunkReader
	profile *benchProfile
}

func (p *profiledScanner) NextChunk(ctx context.Context, completedThrough, upperBound int64, chunkSize int) (chunk model.ChunkRange, ok bool, err error) {
	finish := p.profile.start()
	defer func() { finish("source-next-chunk") }()
	return p.base.NextChunk(ctx, completedThrough, upperBound, chunkSize)
}

func (p *profiledScanner) ReadChunk(ctx context.Context, minID, maxID int64) (rows []model.Row, err error) {
	finish := p.profile.start()
	defer func() { finish("source-scan") }()
	return p.base.ReadChunk(ctx, minID, maxID)
}

type profiledResources struct {
	base    *resourcecontrol.Controller
	profile *benchProfile
}

func (p *profiledResources) AcquireScan(ctx context.Context) (release func(), err error) {
	finishWait := p.profile.start()
	release, err = p.base.AcquireScan(ctx)
	finishWait("source-permit-wait")
	if err != nil {
		return nil, err
	}
	baseRelease := release
	finishHold := p.profile.start()
	return func() {
		finishHold("source-permit-hold")
		baseRelease()
	}, nil
}

func (p *profiledResources) AcquireDestination(ctx context.Context) (release func(), err error) {
	finishWait := p.profile.start()
	release, err = p.base.AcquireDestination(ctx)
	finishWait("destination-permit-wait")
	if err != nil {
		return nil, err
	}
	baseRelease := release
	finishHold := p.profile.start()
	return func() {
		finishHold("destination-permit-hold")
		baseRelease()
	}, nil
}

func (p *profiledResources) SetAppliedOffset(offset int64) { p.base.SetAppliedOffset(offset) }

type profiledStore struct {
	base    *checkpoint.Store
	profile *benchProfile
}

func (p *profiledStore) Begin(ctx context.Context) (tx pgx.Tx, err error) {
	finish := p.profile.start()
	defer func() { finish("destination-tx-begin") }()
	tx, err = p.base.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &profiledTx{Tx: tx, profile: p.profile}, nil
}

func (p *profiledStore) AssertLeadership(ctx context.Context, tx pgx.Tx, cp *model.Checkpoint) (err error) {
	finish := p.profile.start()
	defer func() { finish("leadership-check") }()
	return p.base.AssertLeadership(ctx, tx, cp)
}

func (p *profiledStore) UpdateCheckpoint(ctx context.Context, tx pgx.Tx, cp *model.Checkpoint, expectedOffset int64) (err error) {
	finish := p.profile.start()
	defer func() { finish("checkpoint-update") }()
	return p.base.UpdateCheckpoint(ctx, tx, cp, expectedOffset)
}

func (p *profiledStore) IsApplied(ctx context.Context, tx pgx.Tx, jobID, generation, lsn string) (applied bool, err error) {
	finish := p.profile.start()
	defer func() { finish("ledger-lookup") }()
	return p.base.IsApplied(ctx, tx, jobID, generation, lsn)
}

func (p *profiledStore) MarkApplied(ctx context.Context, tx pgx.Tx, jobID, generation, lsn string, xid uint32) (err error) {
	finish := p.profile.start()
	defer func() { finish("ledger-insert") }()
	return p.base.MarkApplied(ctx, tx, jobID, generation, lsn, xid)
}

func (p *profiledStore) CutoverTarget(ctx context.Context, jobID string) (target *int64, err error) {
	finish := p.profile.start()
	defer func() { finish("cutover-gate-read") }()
	return p.base.CutoverTarget(ctx, jobID)
}

func (p *profiledStore) LeaseChunkOwned(ctx context.Context, jobID, attempt, workerID, ownerID string, ownerEpoch int64, leaseDuration time.Duration) (chunk *model.Chunk, err error) {
	finish := p.profile.start()
	defer func() { finish("chunk-lease") }()
	return p.base.LeaseChunkOwned(ctx, jobID, attempt, workerID, ownerID, ownerEpoch, leaseDuration)
}

func (p *profiledStore) UpdateChunk(ctx context.Context, tx pgx.Tx, chunk *model.Chunk) (err error) {
	finish := p.profile.start()
	defer func() { finish("chunk-update") }()
	return p.base.UpdateChunk(ctx, tx, chunk)
}

func (p *profiledStore) HeartbeatChunkOwned(ctx context.Context, chunk *model.Chunk, ownerID string, ownerEpoch int64, leaseDuration time.Duration) (err error) {
	finish := p.profile.start()
	defer func() { finish("chunk-heartbeat") }()
	return p.base.HeartbeatChunkOwned(ctx, chunk, ownerID, ownerEpoch, leaseDuration)
}

func (p *profiledStore) LoadChunks(ctx context.Context, jobID string, statuses ...model.ChunkState) (chunks []model.Chunk, err error) {
	finish := p.profile.start()
	defer func() { finish("chunk-load") }()
	return p.base.LoadChunks(ctx, jobID, statuses...)
}

func (p *profiledStore) DiscoveryComplete(ctx context.Context, jobID string) (complete bool, err error) {
	finish := p.profile.start()
	defer func() { finish("manifest-check") }()
	return p.base.DiscoveryComplete(ctx, jobID)
}

func (p *profiledStore) StageCandidates(ctx context.Context, tx pgx.Tx, chunk *model.Chunk, candidates []model.Row, descriptor *schema.Schema) (err error) {
	finish := p.profile.start()
	defer func() { finish("candidate-stage") }()
	return p.base.StageCandidates(ctx, tx, chunk, candidates, descriptor)
}

func (p *profiledStore) EvictCandidates(ctx context.Context, tx pgx.Tx, chunk *model.Chunk, keys []int64) (err error) {
	finish := p.profile.start()
	defer func() { finish("candidate-evict") }()
	return p.base.EvictCandidates(ctx, tx, chunk, keys)
}

func (p *profiledStore) ClearCandidates(ctx context.Context, tx pgx.Tx, chunk *model.Chunk) (err error) {
	finish := p.profile.start()
	defer func() { finish("candidate-clear") }()
	return p.base.ClearCandidates(ctx, tx, chunk)
}

type profiledTx struct {
	pgx.Tx
	profile *benchProfile
}

func (p *profiledTx) Commit(ctx context.Context) (err error) {
	finish := p.profile.start()
	defer func() { finish("destination-tx-commit") }()
	return p.Tx.Commit(ctx)
}

func (p *profiledTx) Rollback(ctx context.Context) (err error) {
	finish := p.profile.start()
	defer func() { finish("destination-tx-rollback") }()
	return p.Tx.Rollback(ctx)
}

type profiledSink struct {
	base    *sink.Mutator
	profile *benchProfile
}

func (p *profiledSink) Apply(ctx context.Context, tx pgx.Tx, change model.Change) (err error) {
	finish := p.profile.start()
	defer func() { finish("destination-apply-one") }()
	return p.base.Apply(ctx, tx, change)
}

func (p *profiledSink) ApplyBatch(ctx context.Context, tx pgx.Tx, changes []model.Change) (err error) {
	finish := p.profile.start()
	defer func() { finish("destination-apply-batch") }()
	return p.base.ApplyBatch(ctx, tx, changes)
}

func (p *profiledSink) WriteCandidates(ctx context.Context, tx pgx.Tx, candidates []model.Row) (err error) {
	finish := p.profile.start()
	defer func() { finish("destination-write-candidates") }()
	return p.base.WriteCandidates(ctx, tx, candidates)
}

func (p *profiledSink) WriteStagedCandidates(ctx context.Context, tx pgx.Tx, chunk *model.Chunk) (rows int64, err error) {
	finish := p.profile.start()
	defer func() { finish("destination-apply-candidates") }()
	return p.base.WriteStagedCandidates(ctx, tx, chunk)
}

func TestProfiledResourcesRelease(t *testing.T) {
	base, err := resourcecontrol.New(resourcecontrol.Config{
		MaxSourceScans: 1,
		MaxDestTx:      1,
		MaxCDCLag:      1,
		PollInterval:   time.Second,
		EndOffset:      func(context.Context) (int64, error) { return 0, nil },
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	profile := newBenchProfile()
	resources := &profiledResources{base: base, profile: profile}
	for i := 0; i < 2; i++ {
		release, err := resources.AcquireDestination(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		release()
	}
	var holdCount int
	for _, summary := range profile.summaries() {
		if summary.name == "destination-permit-hold" {
			holdCount = summary.count
		}
	}
	if holdCount != 2 {
		t.Fatalf("destination permit hold count = %d, want 2", holdCount)
	}
}

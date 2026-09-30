package snowflake

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"regexp"
	"strings"
	"testing"
	"time"

	"example.com/seam/internal/model"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestBackfillManifestMustBeGapFreeAndSealed(t *testing.T) {
	cfg := testConfig()
	valid := BackfillSpec{
		JobID: "job", Attempt: "attempt", ShadowTable: "ACCOUNTS_SHADOW", ScanUpperBound: 100,
		Chunks: []model.ChunkRange{{Min: math.MinInt64, Max: -1}, {Min: 0, Max: 100}},
	}
	if err := valid.validate(cfg); err != nil {
		t.Fatalf("valid manifest: %v", err)
	}
	tests := []struct {
		name string
		edit func(*BackfillSpec)
		want string
	}{
		{name: "gap", edit: func(spec *BackfillSpec) { spec.Chunks[1].Min = 1 }, want: "not gap-free"},
		{name: "overlap", edit: func(spec *BackfillSpec) { spec.Chunks[1].Min = -1 }, want: "not gap-free"},
		{name: "unsealed suffix", edit: func(spec *BackfillSpec) { spec.Chunks[1].Max = 99 }, want: "ends at"},
		{name: "live table collision", edit: func(spec *BackfillSpec) { spec.ShadowTable = "accounts" }, want: "must differ"},
		{name: "empty manifest", edit: func(spec *BackfillSpec) { spec.Chunks = nil }, want: "at least one"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := valid
			spec.Chunks = append([]model.ChunkRange(nil), valid.Chunks...)
			test.edit(&spec)
			if err := spec.validate(cfg); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestCreateBackfillPublishesManifestAndRouteAtomically(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := &Store{db: db, cfg: testConfig(), schema: testSchema()}
	spec := BackfillSpec{
		JobID: "job", Attempt: "attempt", ShadowTable: "ACCOUNTS_SHADOW", ScanUpperBound: 10,
		Chunks: []model.ChunkRange{{Min: math.MinInt64, Max: 0}, {Min: 1, Max: 10}},
	}
	mock.ExpectQuery("SELECT ATTEMPT, STATE, SHADOW_ROUTE_ID").
		WithArgs("stream", "job").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM .*BACKFILL_JOBS").
		WithArgs("stream", string(BackfillCompleted), string(BackfillFailed)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec(regexp.QuoteMeta("CREATE TRANSIENT TABLE \"DB\".\"PUBLIC\".\"ACCOUNTS_SHADOW\"")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT NEXT_OFFSET FROM .*OFFSETS").WithArgs("stream").
		WillReturnRows(sqlmock.NewRows([]string{"next_offset"}).AddRow(42))
	mock.ExpectExec("INSERT INTO .*BACKFILL_JOBS").
		WithArgs("stream", "job", "attempt", string(BackfillDiscovering), "shadow:job:attempt", "ACCOUNTS_SHADOW", int64(42)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO .*ROUTES").
		WithArgs("stream", "shadow:job:attempt", "ACCOUNTS_SHADOW").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT ATTEMPT, STATE, SHADOW_TABLE, SHADOW_ROUTE_ID, START_OFFSET").
		WithArgs("stream", "job").
		WillReturnRows(sqlmock.NewRows([]string{"attempt", "state", "table", "route", "offset"}).AddRow("attempt", string(BackfillDiscovering), "ACCOUNTS_SHADOW", "shadow:job:attempt", 42))
	mock.ExpectExec("INSERT INTO .*BACKFILL_CHUNKS").
		WithArgs("stream", "job", "attempt", int64(math.MinInt64), int64(0), string(model.ChunkPending), "stream", "job", "attempt", int64(1), int64(10), string(model.ChunkPending)).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec("UPDATE .*BACKFILL_JOBS").
		WithArgs(string(BackfillRunning), int64(10), 2, "stream", "job", "attempt", string(BackfillDiscovering)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	job, err := store.CreateBackfill(context.Background(), spec)
	if err != nil {
		t.Fatalf("create backfill: %v", err)
	}
	if job.StartOffset != 42 || job.ShadowRouteID != "shadow:job:attempt" || job.State != BackfillRunning {
		t.Fatalf("unexpected job: %+v", job)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseChunkRetriesLostCompareAndSet(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := &Store{db: db, cfg: testConfig(), schema: testSchema()}
	selection := func(token int64) *sqlmock.Rows {
		return sqlmock.NewRows([]string{"min", "max", "token"}).AddRow(int64(1), int64(10), token)
	}
	mock.ExpectQuery("SELECT CHUNK_MIN, CHUNK_MAX, LEASE_TOKEN").WillReturnRows(selection(4))
	mock.ExpectExec("UPDATE .*BACKFILL_CHUNKS").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT CHUNK_MIN, CHUNK_MAX, LEASE_TOKEN").WillReturnRows(selection(5))
	mock.ExpectExec("UPDATE .*BACKFILL_CHUNKS").WillReturnResult(sqlmock.NewResult(0, 1))

	lease, err := store.LeaseChunk(context.Background(), "job", "attempt", "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if lease == nil || lease.LeaseToken != 6 || lease.Range != (model.ChunkRange{Min: 1, Max: 10}) {
		t.Fatalf("unexpected lease: %+v", lease)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestFinalizeChunkRollsBackTargetMergeWhenLeaseWasFenced(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := &Store{db: db, cfg: testConfig(), schema: testSchema()}
	lease := &ChunkLease{
		StreamID: "stream", JobID: "job", Attempt: "attempt", Range: model.ChunkRange{Min: 1, Max: 10},
		WorkerID: "worker", LeaseToken: 7,
	}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT C.LOW_MARKER_ID, C.HIGH_MARKER_ID").
		WillReturnRows(sqlmock.NewRows([]string{"low", "high", "rows", "route", "table"}).AddRow("low-id", "high-id", 5, "shadow:job:attempt", "ACCOUNTS_SHADOW"))
	mock.ExpectQuery("SELECT SOURCE_LSN FROM .*MARKERS").
		WillReturnRows(sqlmock.NewRows([]string{"source_lsn"}).AddRow("32"))
	mock.ExpectQuery("SELECT 1 FROM .*MARKERS").
		WillReturnRows(sqlmock.NewRows([]string{"one"}).AddRow(1))
	mock.ExpectExec("MERGE INTO .*ACCOUNTS_SHADOW").WillReturnResult(sqlmock.NewResult(0, 5))
	mock.ExpectExec("UPDATE .*BACKFILL_CHUNKS").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	err = store.FinalizeChunk(context.Background(), lease)
	if err == nil || !strings.Contains(err.Error(), "fenced during merge") {
		t.Fatalf("error = %v, want fenced rollback", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCreateBackfillDoesNotAdoptUnknownShadowTable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := &Store{db: db, cfg: testConfig(), schema: testSchema()}
	spec := BackfillSpec{
		JobID: "job", Attempt: "attempt", ShadowTable: "ACCOUNTS_SHADOW", ScanUpperBound: 0,
		Chunks: []model.ChunkRange{{Min: math.MinInt64, Max: 0}},
	}
	mock.ExpectQuery("SELECT ATTEMPT, STATE, SHADOW_ROUTE_ID").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM .*BACKFILL_JOBS").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec("CREATE TRANSIENT TABLE").WillReturnError(errors.New("object already exists"))
	_, err = store.CreateBackfill(context.Background(), spec)
	if err == nil || !strings.Contains(err.Error(), "refuses to reuse") {
		t.Fatalf("error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotMergeLetsPostLowCDCClockWin(t *testing.T) {
	query, err := mergeSnapshotSQL(testConfig(), testSchema(), "ACCOUNTS_SHADOW")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"K.STREAM_ID = ?", "K.ROUTE_ID = ?", "K.SOURCE_LSN <= ?", "WHEN MATCHED THEN UPDATE", "WHEN NOT MATCHED THEN INSERT",
	} {
		if !strings.Contains(query, required) {
			t.Fatalf("snapshot merge lacks %q: %s", required, query)
		}
	}
	if strings.Contains(query, "WHEN NOT MATCHED BY SOURCE") {
		t.Fatalf("snapshot merge must not delete CDC-only rows: %s", query)
	}
}

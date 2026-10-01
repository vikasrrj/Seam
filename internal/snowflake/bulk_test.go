package snowflake

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"math"
	"os"
	"regexp"
	"strings"
	"testing"

	"example.com/seam/internal/model"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestWriteSnapshotFileIsDeterministicAndLossless(t *testing.T) {
	cfg := testConfig().withDefaults()
	cfg.BulkTempDir = t.TempDir()
	store := &Store{cfg: cfg, schema: testSchema()}
	lease := bulkTestLease()
	rows := []model.Row{
		{Values: []model.Value{model.Int64Value(math.MaxInt64), model.TextValue("a < b")}},
		{Values: []model.Value{model.Int64Value(7), model.NullValue()}},
	}

	first, err := store.writeSnapshotFile(lease, rows)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(first.LocalDirectory) })
	second, err := store.writeSnapshotFile(lease, rows)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(second.LocalDirectory) })

	firstBytes, err := os.ReadFile(first.LocalPath)
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := os.ReadFile(second.LocalPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(firstBytes) != string(secondBytes) || first.ContentSHA256 != second.ContentSHA256 || first.ID != second.ID {
		t.Fatal("identical chunk contents did not produce an identical file identity")
	}
	if first.Rows != len(rows) || first.Bytes != int64(len(firstBytes)) {
		t.Fatalf("file metadata = rows %d bytes %d", first.Rows, first.Bytes)
	}

	input, err := os.Open(first.LocalPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = input.Close() })
	compressed, err := gzip.NewReader(input)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = compressed.Close() })
	decoder := json.NewDecoder(compressed)
	var encoded snapshotFileRow
	if err := decoder.Decode(&encoded); err != nil {
		t.Fatal(err)
	}
	if encoded.PK != "9223372036854775807" || encoded.ChunkMin != "-9223372036854775808" {
		t.Fatalf("integer strings lost precision: %+v", encoded)
	}
	var payload map[string]any
	if err := json.Unmarshal(encoded.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["id"] != "9223372036854775807" || payload["name"] != "a < b" {
		t.Fatalf("payload = %#v", payload)
	}
}

func TestWriteSnapshotFileContentChangesIdentity(t *testing.T) {
	cfg := testConfig().withDefaults()
	cfg.BulkTempDir = t.TempDir()
	store := &Store{cfg: cfg, schema: testSchema()}
	lease := bulkTestLease()
	first, err := store.writeSnapshotFile(lease, []model.Row{{Values: []model.Value{model.Int64Value(1), model.TextValue("first")}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(first.LocalDirectory) })
	second, err := store.writeSnapshotFile(lease, []model.Row{{Values: []model.Value{model.Int64Value(1), model.TextValue("second")}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(second.LocalDirectory) })
	if first.ID == second.ID || first.ContentSHA256 == second.ContentSHA256 {
		t.Fatal("changed contents reused a bulk file identity")
	}
}

func TestWriteSnapshotFileRejectsOutOfRangeRowsAndCleansUp(t *testing.T) {
	temp := t.TempDir()
	cfg := testConfig().withDefaults()
	cfg.BulkTempDir = temp
	store := &Store{cfg: cfg, schema: testSchema()}
	lease := bulkTestLease()
	lease.Range = model.ChunkRange{Min: 1, Max: 10}
	_, err := store.writeSnapshotFile(lease, []model.Row{{Values: []model.Value{model.Int64Value(11), model.TextValue("outside")}}})
	if err == nil || !strings.Contains(err.Error(), "outside leased chunk") {
		t.Fatalf("error = %v", err)
	}
	entries, err := os.ReadDir(temp)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("temporary files remain after rejected row: %v", entries)
	}
}

func TestStageEmptySnapshotIsLeaseFencedAndAtomic(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cfg := testConfig().withDefaults()
	store := &Store{db: db, cfg: cfg, schema: testSchema()}
	lease := bulkTestLease()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT STATE FROM .*BACKFILL_CHUNKS").
		WithArgs("stream", "job", "attempt", int64(math.MinInt64), int64(math.MaxInt64), "worker", int64(7)).
		WillReturnRows(sqlmock.NewRows([]string{"state"}).AddRow(string(model.ChunkScanning)))
	mock.ExpectExec("DELETE FROM .*SNAPSHOT_STAGE").
		WithArgs("stream", "job", "attempt", int64(math.MinInt64)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("UPDATE .*BACKFILL_CHUNKS").
		WithArgs("stream", "job", "attempt", int64(math.MinInt64), "worker", int64(7), string(model.ChunkScanning)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := store.StageSnapshot(context.Background(), lease, nil); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCopySnapshotFileRollsBackOnRowCountMismatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cfg := testConfig().withDefaults()
	store := &Store{db: db, cfg: cfg, schema: testSchema()}
	lease := bulkTestLease()
	file := &snapshotFile{ID: "file", StageDirectory: "@\"DB\".\"SEAM_INTERNAL\".\"SNAPSHOT_FILES\"/file", Rows: 2}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT STATE FROM .*BACKFILL_CHUNKS").
		WillReturnRows(sqlmock.NewRows([]string{"state"}).AddRow(string(model.ChunkScanning)))
	mock.ExpectExec("DELETE FROM .*SNAPSHOT_STAGE").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(bulkCopySQL(cfg, file.StageDirectory))).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM .*SNAPSHOT_STAGE").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectRollback()
	err = store.copySnapshotFile(context.Background(), lease, file)
	if err == nil || !strings.Contains(err.Error(), "loaded 1 rows, expected 2") {
		t.Fatalf("error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCopySnapshotFileCommitsManifestAndProgressTogether(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cfg := testConfig().withDefaults()
	store := &Store{db: db, cfg: cfg, schema: testSchema()}
	lease := bulkTestLease()
	file := &snapshotFile{ID: "file", StageDirectory: "@\"DB\".\"SEAM_INTERNAL\".\"SNAPSHOT_FILES\"/file", Rows: 2}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT STATE FROM .*BACKFILL_CHUNKS").
		WillReturnRows(sqlmock.NewRows([]string{"state"}).AddRow(string(model.ChunkScanning)))
	mock.ExpectExec("DELETE FROM .*SNAPSHOT_STAGE").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(bulkCopySQL(cfg, file.StageDirectory))).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM .*SNAPSHOT_STAGE").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	mock.ExpectExec("UPDATE .*BACKFILL_FILES").
		WithArgs(bulkFileLoaded, "stream", "job", "attempt", int64(math.MinInt64), int64(7), "file", bulkFilePrepared, bulkFileUploaded).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE .*BACKFILL_CHUNKS").
		WithArgs(2, "stream", "job", "attempt", int64(math.MinInt64), "worker", int64(7), string(model.ChunkScanning)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := store.copySnapshotFile(context.Background(), lease, file); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestUploadSnapshotFileRejectsNonSuccessStatus(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cfg := testConfig().withDefaults()
	store := &Store{db: db, cfg: cfg, schema: testSchema()}
	file := &snapshotFile{
		LocalPath:      "/tmp/snapshot.json.gz",
		StageDirectory: "@\"DB\".\"SEAM_INTERNAL\".\"SNAPSHOT_FILES\"/file",
	}
	mock.ExpectQuery("PUT .*SNAPSHOT_FILES.*AUTO_COMPRESS = FALSE OVERWRITE = TRUE PARALLEL = 4").
		WillReturnRows(sqlmock.NewRows([]string{"source", "target", "source_size", "target_size", "source_compression", "target_compression", "status", "message"}).
			AddRow("source", "target", "10", "10", "GZIP", "GZIP", "FAILED", "rejected"))
	err = store.uploadSnapshotFile(context.Background(), bulkTestLease(), file)
	if err == nil || !strings.Contains(err.Error(), `status "FAILED": rejected`) {
		t.Fatalf("error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestBulkCopySQLUsesExactStageAndRetrySafeReload(t *testing.T) {
	stage := "@\"DB\".\"SEAM_INTERNAL\".\"SNAPSHOT_FILES\"/file"
	query := bulkCopySQL(testConfig(), stage)
	for _, required := range []string{stage, "FORCE = TRUE", "ON_ERROR = ABORT_STATEMENT", "TO_NUMBER($1:pk::VARCHAR)"} {
		if !strings.Contains(query, required) {
			t.Fatalf("bulk COPY lacks %q: %s", required, query)
		}
	}
}

func bulkTestLease() *ChunkLease {
	return &ChunkLease{
		StreamID: "stream", JobID: "job", Attempt: "attempt",
		Range:    model.ChunkRange{Min: math.MinInt64, Max: math.MaxInt64},
		WorkerID: "worker", LeaseToken: 7,
	}
}

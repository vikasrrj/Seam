package snowflake

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"regexp"
	"strings"
	"testing"

	"example.com/seam/internal/model"
	"github.com/DATA-DOG/go-sqlmock"
)

var (
	benchmarkEncodedPK      int64
	benchmarkEncodedPayload string
)

func BenchmarkEncodeSnapshotRow(b *testing.B) {
	for _, payloadBytes := range []int{16, 1024} {
		b.Run(fmt.Sprintf("payload_%d", payloadBytes), func(b *testing.B) {
			sourceSchema := testSchema()
			row := model.Row{Values: []model.Value{
				model.Int64Value(math.MaxInt64),
				model.TextValue(strings.Repeat("x", payloadBytes)),
			}}
			b.SetBytes(int64(payloadBytes + 8))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				pk, payload, err := encodeRow(sourceSchema, &row)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkEncodedPK = pk
				benchmarkEncodedPayload = payload
			}
		})
	}
}

func BenchmarkWriteSnapshotFile(b *testing.B) {
	const payloadBytes = 96
	for _, rowCount := range []int{100, 1000, 2500} {
		b.Run(fmt.Sprintf("rows_%d", rowCount), func(b *testing.B) {
			cfg := testConfig().withDefaults()
			cfg.BulkTempDir = b.TempDir()
			store := &Store{cfg: cfg, schema: testSchema()}
			lease := bulkTestLease()
			rows := make([]model.Row, rowCount)
			payload := strings.Repeat("x", payloadBytes)
			for index := range rows {
				rows[index] = model.Row{Values: []model.Value{
					model.Int64Value(int64(index + 1)),
					model.TextValue(payload),
				}}
			}
			files := make([]*snapshotFile, 0, b.N)
			b.SetBytes(int64(rowCount * (payloadBytes + 8)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				file, err := store.writeSnapshotFile(lease, rows)
				if err != nil {
					b.Fatal(err)
				}
				files = append(files, file)
			}
			b.StopTimer()
			if len(files) > 0 {
				b.ReportMetric(float64(files[len(files)-1].Bytes), "compressed-bytes/file")
			}
			for _, file := range files {
				if err := os.RemoveAll(file.LocalDirectory); err != nil {
					b.Error(err)
				}
			}
		})
	}
}

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

func TestPrepareSnapshotFileIsOneLeaseConditionalStatement(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cfg := testConfig().withDefaults()
	store := &Store{db: db, cfg: cfg, schema: testSchema()}
	lease := bulkTestLease()
	file := &snapshotFile{ID: "file", StagePath: "@stage/file", ContentSHA256: "hash", Rows: 2, Bytes: 100}
	mock.ExpectExec("MERGE INTO .*BACKFILL_FILES.*WHERE EXISTS .*BACKFILL_CHUNKS.*WHEN MATCHED THEN UPDATE SET .*STATE = S.STATE").
		WithArgs("stream", "job", "attempt", int64(math.MinInt64), int64(7), "file", "@stage/file", "hash", 2, int64(100),
			bulkFilePrepared, "stream", "job", "attempt", int64(math.MinInt64), int64(math.MaxInt64), "worker", int64(7), string(model.ChunkScanning)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := store.prepareSnapshotFile(context.Background(), lease, file); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPrepareSnapshotFileRejectsFencedLease(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := &Store{db: db, cfg: testConfig().withDefaults(), schema: testSchema()}
	mock.ExpectExec("MERGE INTO .*BACKFILL_FILES").WillReturnResult(sqlmock.NewResult(0, 0))
	err = store.prepareSnapshotFile(context.Background(), bulkTestLease(), &snapshotFile{})
	if err == nil || !strings.Contains(err.Error(), "lease expired or was fenced") {
		t.Fatalf("error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
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
	mock.ExpectQuery(regexp.QuoteMeta(bulkCopySQL(cfg, file.StageDirectory))).
		WillReturnRows(sqlmock.NewRows([]string{"file", "status", "rows_loaded"}).AddRow("file", "LOADED", "1"))
	mock.ExpectRollback()
	err = store.copySnapshotFile(context.Background(), lease, file)
	if err == nil || !strings.Contains(err.Error(), "loaded 1 rows, expected 2") {
		t.Fatalf("error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCopySnapshotFileCommitsCopyAndProgressTogether(t *testing.T) {
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
	mock.ExpectQuery(regexp.QuoteMeta(bulkCopySQL(cfg, file.StageDirectory))).
		WillReturnRows(sqlmock.NewRows([]string{"file", "status", "rows_loaded"}).AddRow("file", "LOADED", "2"))
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

func TestUploadSnapshotFileLeavesPreparedManifestForAtomicCopy(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := &Store{db: db, cfg: testConfig().withDefaults(), schema: testSchema()}
	file := &snapshotFile{LocalPath: "/tmp/snapshot.json.gz", StageDirectory: "@stage/file"}
	mock.ExpectQuery("PUT .*AUTO_COMPRESS = FALSE OVERWRITE = TRUE PARALLEL = 4").
		WillReturnRows(sqlmock.NewRows([]string{"source", "target", "source_size", "target_size", "source_compression", "target_compression", "status", "message"}).
			AddRow("source", "target", "10", "10", "GZIP", "GZIP", "UPLOADED", ""))
	if err := store.uploadSnapshotFile(context.Background(), bulkTestLease(), file); err != nil {
		t.Fatal(err)
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

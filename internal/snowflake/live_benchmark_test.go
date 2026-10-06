//go:build snowflake_integration

package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/seam/internal/model"
	_ "github.com/snowflakedb/gosnowflake/v2"
)

func BenchmarkLiveSnowflakeStageSnapshot(b *testing.B) {
	dsn := os.Getenv("SNOWFLAKE_DSN")
	database := os.Getenv("SNOWFLAKE_DATABASE")
	if dsn == "" || database == "" {
		b.Skip("SNOWFLAKE_DSN and SNOWFLAKE_DATABASE are required")
	}
	loader := SnapshotLoader(envOrBenchmarkDefault("SEAM_SNOWFLAKE_BENCH_LOADER", string(SnapshotLoaderBulk)))
	if loader != SnapshotLoaderBulk && loader != SnapshotLoaderSQL {
		b.Fatalf("SEAM_SNOWFLAKE_BENCH_LOADER must be %q or %q", SnapshotLoaderBulk, SnapshotLoaderSQL)
	}
	payloadBytes := benchmarkPositiveInt(b, "SEAM_SNOWFLAKE_BENCH_PAYLOAD_BYTES", 96)
	uploadParallel := benchmarkPositiveInt(b, "SEAM_SNOWFLAKE_UPLOAD_PARALLEL", 4)
	workers := benchmarkPositiveInt(b, "SEAM_SNOWFLAKE_BENCH_WORKERS", 1)
	if workers != 1 && workers != 4 {
		b.Fatal("SEAM_SNOWFLAKE_BENCH_WORKERS must be 1 or 4")
	}
	for _, rowCount := range benchmarkRowCounts(b) {
		b.Run(fmt.Sprintf("%s/rows_%d/bytes_%d/workers_%d", loader, rowCount, payloadBytes, workers), func(b *testing.B) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			db, err := sql.Open("snowflake", dsn)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(workers)
			if err := db.PingContext(ctx); err != nil {
				b.Fatal(err)
			}
			suffix := fmt.Sprintf("%X", time.Now().UnixNano())
			cfg := Config{
				Database: database, Schema: "SEAM_BENCH_" + suffix,
				InternalSchema: "SEAM_BENCH_INTERNAL_" + suffix,
				LiveTable:      "ACCOUNTS", StreamID: "bench-" + suffix,
				TopicID: "benchmark-topic", Partition: 0, SnapshotLoader: loader,
				BulkTempDir: os.TempDir(), UploadParallel: uploadParallel,
			}
			defer func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
				defer cleanupCancel()
				for _, schemaName := range []string{cfg.Schema, cfg.InternalSchema} {
					_, _ = db.ExecContext(cleanupCtx, "DROP SCHEMA IF EXISTS "+quoteIdentifier(cfg.Database)+"."+quoteIdentifier(schemaName)+" CASCADE")
				}
			}()
			store, err := NewStore(db, cfg, testSchema())
			if err != nil {
				b.Fatal(err)
			}
			if err := store.EnsureObjects(ctx, 0); err != nil {
				b.Fatal(err)
			}
			chunks := make([]model.ChunkRange, workers)
			for worker := range chunks {
				chunks[worker] = model.ChunkRange{Min: int64(worker*rowCount + 1), Max: int64((worker + 1) * rowCount)}
			}
			chunks[0].Min = math.MinInt64
			spec := BackfillSpec{
				JobID: "benchmark", Attempt: "attempt-1", ShadowTable: "ACCOUNTS_SHADOW",
				ScanUpperBound: int64(rowCount * workers),
				Chunks:         chunks,
			}
			if _, err := store.CreateBackfill(ctx, spec); err != nil {
				b.Fatal(err)
			}
			leases := make([]*ChunkLease, workers)
			workerRows := make([][]model.Row, workers)
			payload := strings.Repeat("x", payloadBytes)
			for worker := range leases {
				lease, err := store.LeaseChunk(ctx, spec.JobID, spec.Attempt, fmt.Sprintf("benchmark-worker-%d", worker), 10*time.Minute)
				if err != nil {
					b.Fatal(err)
				}
				if lease == nil {
					b.Fatal("no benchmark chunk available")
				}
				low := fmt.Sprintf("low-%d", worker)
				if _, err := db.ExecContext(ctx, "INSERT INTO "+cfg.internal("MARKERS")+" (STREAM_ID, MARKER_ID, KIND, JOB_ID, ATTEMPT, CHUNK_MIN, CHUNK_MAX, SOURCE_TX, FINAL_OFFSET, SOURCE_LSN) VALUES (?, ?, 'low', ?, ?, ?, ?, 'benchmark-low', 0, 1)", cfg.StreamID, low, spec.JobID, spec.Attempt, lease.Range.Min, lease.Range.Max); err != nil {
					b.Fatal(err)
				}
				if err := store.BeginChunkScan(ctx, lease, low); err != nil {
					b.Fatal(err)
				}
				rows := make([]model.Row, rowCount)
				for index := range rows {
					rows[index] = model.Row{Values: []model.Value{model.Int64Value(lease.Range.Max - int64(rowCount) + int64(index+1)), model.TextValue(payload)}}
				}
				leases[worker], workerRows[worker] = lease, rows
			}
			logicalBytes := int64(workers * rowCount * (len(payload) + 8))
			var historyStart string
			if err := db.QueryRowContext(ctx, "SELECT CURRENT_TIMESTAMP()::VARCHAR").Scan(&historyStart); err != nil {
				b.Fatal(err)
			}
			var phaseMu sync.Mutex
			phases := make(map[string]time.Duration)
			ctx = WithPhaseObserver(ctx, func(name string, elapsed time.Duration) {
				phaseMu.Lock()
				phases[name] += elapsed
				phaseMu.Unlock()
			})
			b.SetBytes(logicalBytes)
			b.ReportAllocs()
			iterationTimes := make([]time.Duration, b.N)
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				started := time.Now()
				if workers == 1 {
					if err := store.StageSnapshot(ctx, leases[0], workerRows[0]); err != nil {
						b.Fatal(err)
					}
				} else {
					errs := make(chan error, workers)
					for worker := range leases {
						go func() { errs <- store.StageSnapshot(ctx, leases[worker], workerRows[worker]) }()
					}
					var firstErr error
					for range workers {
						if err := <-errs; err != nil && firstErr == nil {
							firstErr = err
						}
					}
					if firstErr != nil {
						b.Fatal(firstErr)
					}
				}
				iterationTimes[iteration] = time.Since(started)
			}
			b.StopTimer()
			for iteration, elapsed := range iterationTimes {
				b.Logf("staging iteration=%d elapsed_ms=%.3f", iteration, float64(elapsed)/float64(time.Millisecond))
			}
			for name, elapsed := range phases {
				b.ReportMetric(float64(elapsed)/float64(time.Millisecond)/float64(b.N), name+"-ms/op")
			}
			b.ReportMetric(float64(workers*rowCount*b.N)/b.Elapsed().Seconds(), "rows/s")
			b.ReportMetric(float64(logicalBytes*int64(b.N))/b.Elapsed().Seconds()/(1024*1024), "MiB/s")
			if workers == 1 {
				reportWarehouseHistory(b, ctx, db, cfg, historyStart)
			} else {
				b.Log("warehouse history unavailable for pooled concurrent sessions; phase metrics are overlapping service time per wave, not wall percentages")
			}
			var count, distinct, mismatches int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*), COUNT(DISTINCT PK), COALESCE(COUNT_IF(PK IS DISTINCT FROM TRY_TO_NUMBER(PAYLOAD:id::VARCHAR) OR PAYLOAD:name::VARCHAR IS DISTINCT FROM ? OR PK < 1 OR PK > ?), 0) FROM "+cfg.internal("SNAPSHOT_STAGE")+" WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ?", payload, workers*rowCount, cfg.StreamID, spec.JobID, spec.Attempt).Scan(&count, &distinct, &mismatches); err != nil {
				b.Fatal(err)
			}
			if count != workers*rowCount || distinct != workers*rowCount || mismatches != 0 {
				b.Fatalf("staged rows=%d distinct=%d expected=%d payload mismatches=%d", count, distinct, workers*rowCount, mismatches)
			}
		})
	}
}

// Query history is collected outside the timer. Only statement types and
// numeric metrics are printed; SQL text, bind values, and credentials are not.
func reportWarehouseHistory(b *testing.B, ctx context.Context, db *sql.DB, cfg Config, started string) {
	b.Helper()
	query := `SELECT QUERY_TYPE, COALESCE(WAREHOUSE_SIZE, 'none'), COUNT(*),
		SUM(TOTAL_ELAPSED_TIME), SUM(COMPILATION_TIME), SUM(EXECUTION_TIME),
		SUM(QUEUED_PROVISIONING_TIME), SUM(QUEUED_OVERLOAD_TIME), SUM(TRANSACTION_BLOCKED_TIME),
		SUM(BYTES_SCANNED)
		FROM TABLE(` + quoteIdentifier(cfg.Database) + `.INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(RESULT_LIMIT => 10000))
		WHERE START_TIME > TO_TIMESTAMP_LTZ(?) AND END_TIME IS NOT NULL
		AND TOTAL_ELAPSED_TIME >= 0 AND EXECUTION_STATUS = 'SUCCESS'
		AND QUERY_TEXT NOT ILIKE '%QUERY_HISTORY_BY_SESSION%'
		GROUP BY QUERY_TYPE, WAREHOUSE_SIZE ORDER BY QUERY_TYPE`
	rows, err := db.QueryContext(ctx, query, started)
	if err != nil {
		b.Logf("warehouse history unavailable: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var kind, size string
		var count int64
		var total, compile, execution, provision, queue, blocked, scanned sql.NullInt64
		if err := rows.Scan(&kind, &size, &count, &total, &compile, &execution, &provision, &queue, &blocked, &scanned); err != nil {
			b.Fatal(err)
		}
		b.Logf("WAREHOUSE type=%s size=%s count=%d total_ms=%v compile_ms=%v execution_ms=%v provision_ms=%v overload_ms=%v blocked_ms=%v scan_bytes=%v spill_bytes=unavailable", kind, size, count, nullableHistoryMetric(total), nullableHistoryMetric(compile), nullableHistoryMetric(execution), nullableHistoryMetric(provision), nullableHistoryMetric(queue), nullableHistoryMetric(blocked), nullableHistoryMetric(scanned))
	}
	if err := rows.Err(); err != nil {
		b.Fatal(err)
	}
}

func nullableHistoryMetric(value sql.NullInt64) any {
	if !value.Valid {
		return "unavailable"
	}
	return value.Int64
}

func benchmarkRowCounts(b *testing.B) []int {
	b.Helper()
	parts := strings.Split(envOrBenchmarkDefault("SEAM_SNOWFLAKE_BENCH_ROWS", "100,1000,5000"), ",")
	rows := make([]int, 0, len(parts))
	for _, part := range parts {
		value, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || value < 1 {
			b.Fatalf("SEAM_SNOWFLAKE_BENCH_ROWS contains invalid positive integer %q", part)
		}
		rows = append(rows, value)
	}
	return rows
}

func benchmarkPositiveInt(b *testing.B, name string, fallback int) int {
	b.Helper()
	value, err := strconv.Atoi(envOrBenchmarkDefault(name, strconv.Itoa(fallback)))
	if err != nil || value < 1 {
		b.Fatalf("%s must be a positive integer", name)
	}
	return value
}

func envOrBenchmarkDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

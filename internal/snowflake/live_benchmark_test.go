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
	for _, rowCount := range benchmarkRowCounts(b) {
		b.Run(fmt.Sprintf("%s/rows_%d/bytes_%d", loader, rowCount, payloadBytes), func(b *testing.B) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			db, err := sql.Open("snowflake", dsn)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
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
			spec := BackfillSpec{
				JobID: "benchmark", Attempt: "attempt-1", ShadowTable: "ACCOUNTS_SHADOW",
				ScanUpperBound: int64(rowCount),
				Chunks:         []model.ChunkRange{{Min: math.MinInt64, Max: int64(rowCount)}},
			}
			if _, err := store.CreateBackfill(ctx, spec); err != nil {
				b.Fatal(err)
			}
			lease, err := store.LeaseChunk(ctx, spec.JobID, spec.Attempt, "benchmark-worker", 10*time.Minute)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO "+cfg.internal("MARKERS")+" (STREAM_ID, MARKER_ID, KIND, JOB_ID, ATTEMPT, CHUNK_MIN, CHUNK_MAX, SOURCE_TX, FINAL_OFFSET, SOURCE_LSN) VALUES (?, 'low', 'low', ?, ?, ?, ?, 'benchmark-low', 0, 1)", cfg.StreamID, spec.JobID, spec.Attempt, math.MinInt64, int64(rowCount)); err != nil {
				b.Fatal(err)
			}
			if err := store.BeginChunkScan(ctx, lease, "low"); err != nil {
				b.Fatal(err)
			}
			rows := make([]model.Row, rowCount)
			payload := strings.Repeat("x", payloadBytes)
			for index := range rows {
				rows[index] = model.Row{Values: []model.Value{model.Int64Value(int64(index + 1)), model.TextValue(payload)}}
			}
			logicalBytes := int64(rowCount * (len(payload) + 8))
			b.SetBytes(logicalBytes)
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if err := store.StageSnapshot(ctx, lease, rows); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(rowCount*b.N)/b.Elapsed().Seconds(), "rows/s")
			b.ReportMetric(float64(logicalBytes*int64(b.N))/b.Elapsed().Seconds()/(1024*1024), "MiB/s")
		})
	}
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

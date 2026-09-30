//go:build snowflake_integration

package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"example.com/seam/internal/model"
	_ "github.com/snowflakedb/gosnowflake/v2"
)

// BenchmarkLiveSnowflakeStageSnapshot measures the current parameterized
// snapshot staging path in isolation. It is not an end-to-end backfill claim:
// PostgreSQL scan, Kafka, marker catch-up, final MERGE, and validation are
// deliberately outside the timer.
func BenchmarkLiveSnowflakeStageSnapshot(b *testing.B) {
	dsn := os.Getenv("SNOWFLAKE_DSN")
	database := os.Getenv("SNOWFLAKE_DATABASE")
	if dsn == "" || database == "" {
		b.Skip("SNOWFLAKE_DSN and SNOWFLAKE_DATABASE are required")
	}
	for _, rowCount := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("rows_%d", rowCount), func(b *testing.B) {
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
				TopicID: "benchmark-topic", Partition: 0,
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
			payload := strings.Repeat("x", 96)
			for index := range rows {
				rows[index] = model.Row{Values: []model.Value{model.Int64Value(int64(index + 1)), model.TextValue(payload)}}
			}
			b.SetBytes(int64(rowCount * (len(payload) + 8)))
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if err := store.StageSnapshot(ctx, lease, rows); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(rowCount*b.N)/b.Elapsed().Seconds(), "rows/s")
		})
	}
}

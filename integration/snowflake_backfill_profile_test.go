//go:build integration && snowflake_integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/seam/integration/itest"
	"example.com/seam/internal/capture"
	"example.com/seam/internal/kafka"
	"example.com/seam/internal/marker"
	"example.com/seam/internal/scan"
	"example.com/seam/internal/schema"
	seamsnowflake "example.com/seam/internal/snowflake"
	"example.com/seam/internal/snowreconcile"
	_ "github.com/snowflakedb/gosnowflake/v2"
)

// TestSnowflakeBackfillProfile measures the complete initial backfill through
// ready_to_verify. It is opt-in because it resets the dedicated integration
// stack and creates live Snowflake objects. Seeding and object creation remain
// outside the timed region; preparation, manifest discovery, LOW/HIGH marker
// transport, source scans, staging, finalization, and completion are inside.
func TestSnowflakeBackfillProfile(t *testing.T) {
	if os.Getenv("SEAM_RUN_SNOWFLAKE_BACKFILL_PROFILE") != "1" {
		t.Skip("set SEAM_RUN_SNOWFLAKE_BACKFILL_PROFILE=1")
	}
	dsn := os.Getenv("SNOWFLAKE_DSN")
	database := os.Getenv("SNOWFLAKE_DATABASE")
	if dsn == "" || database == "" {
		t.Skip("SNOWFLAKE_DSN and SNOWFLAKE_DATABASE are required")
	}
	if !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*$`).MatchString(database) {
		t.Fatalf("invalid SNOWFLAKE_DATABASE identifier %q", database)
	}
	rows := profilePositiveInt(t, "SEAM_SNOWFLAKE_PROFILE_ROWS", 10_000)
	chunkSize := profilePositiveInt(t, "SEAM_SNOWFLAKE_PROFILE_CHUNK_SIZE", 1_000)
	workers := profilePositiveInt(t, "SEAM_SNOWFLAKE_PROFILE_WORKERS", 4)
	poll := time.Duration(profilePositiveInt(t, "SEAM_SNOWFLAKE_PROFILE_POLL_MS", 250)) * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	if err := itest.ResetTables(ctx); err != nil {
		t.Fatal(err)
	}
	source, err := itest.SourceConn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	itest.CloseOnCleanup(t, "Snowflake profile source", source)
	if _, err := source.Exec(ctx, `INSERT INTO accounts(id, owner, balance_cents)
		SELECT id, 'profile-' || id::text, id * 100 FROM generate_series(1, $1) AS g(id)`, rows); err != nil {
		t.Fatal(err)
	}
	descriptor, err := schema.LoadFromConn(ctx, source, "public", "accounts")
	if err != nil {
		t.Fatal(err)
	}

	reader, err := capture.StartReader(ctx, capture.ReaderConfig{
		SQLDSN: itest.SourceDSN(), ReplicationDSN: itest.SourceReplDSN(),
		Slot: "seam_itest_slot", Publication: "seam_pub", Table: "accounts",
		KafkaBrokers: itest.KafkaBrokers(), KafkaTopic: itest.KafkaTopic(), Generation: "gen:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	readerCtx, stopReader := context.WithCancel(ctx)
	readerDone := make(chan error, 1)
	go func() { readerDone <- reader.Run(readerCtx) }()
	t.Cleanup(func() {
		stopReader()
		_ = reader.Close()
		select {
		case err := <-readerDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("capture reader: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("capture reader did not stop")
		}
	})

	topicID, err := kafka.TopicIdentity(ctx, itest.KafkaBrokers(), itest.KafkaTopic())
	if err != nil {
		t.Fatal(err)
	}
	earliest, err := kafka.EarliestOffset(ctx, itest.KafkaBrokers(), itest.KafkaTopic())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(workers + 2)
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	suffix := fmt.Sprintf("%X", time.Now().UnixNano())
	cfg := seamsnowflake.Config{
		Database: database, Schema: "SEAM_PROFILE_" + suffix,
		InternalSchema: "SEAM_PROFILE_INTERNAL_" + suffix,
		LiveTable:      "ACCOUNTS", StreamID: "profile-" + suffix,
		TopicID: topicID, Partition: 0, SnapshotLoader: seamsnowflake.SnapshotLoaderBulk,
		BulkTempDir: os.TempDir(), UploadParallel: 4,
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		for _, schemaName := range []string{cfg.Schema, cfg.InternalSchema} {
			statement := `DROP SCHEMA IF EXISTS "` + strings.ToUpper(database) + `"."` + strings.ToUpper(schemaName) + `" CASCADE`
			if _, err := db.ExecContext(cleanupCtx, statement); err != nil {
				t.Errorf("drop Snowflake profile schema %s: %v", schemaName, err)
			}
		}
	})
	store, err := seamsnowflake.NewStore(db, cfg, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureObjects(ctx, earliest); err != nil {
		t.Fatal(err)
	}

	var phaseMu sync.Mutex
	phaseTotals := make(map[string]time.Duration)
	phaseCounts := make(map[string]int)
	profileCtx := seamsnowflake.WithPhaseObserver(ctx, func(name string, elapsed time.Duration) {
		phaseMu.Lock()
		phaseTotals[name] += elapsed
		phaseCounts[name]++
		phaseMu.Unlock()
	})

	sink, err := startTestSnowflakeSink(profileCtx, store, topicID, "profile-sink", 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sink.stopAndRelease(store); err != nil {
			t.Errorf("profile sink: %v", err)
		}
	})
	markers := marker.NewStore(itest.SourceDSN())
	if err := markers.EnsureTable(ctx); err != nil {
		t.Fatal(err)
	}
	scanner, err := scan.NewChunkReader(ctx, itest.SourceDSN())
	if err != nil {
		t.Fatal(err)
	}
	if err := scanner.SetLimits(max(rows, chunkSize), 256<<20); err != nil {
		t.Fatal(err)
	}
	coordinator, err := snowreconcile.New(snowreconcile.Config{
		JobID: "snowflake-profile-" + suffix, Attempt: "attempt-1", ShadowTable: "ACCOUNTS_SHADOW",
		ChunkSize: chunkSize, Workers: workers, WorkerID: "profile-worker",
		Lease: 2 * time.Minute, Heartbeat: 20 * time.Second, Poll: poll,
	}, store, scanner, markers)
	if err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	if err := coordinator.Run(profileCtx); err != nil {
		t.Fatal(err)
	}
	wall := time.Since(started)

	sourceRows, err := scanner.ReadChunk(ctx, math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	sourceRows, err = seamsnowflake.NormalizeSourceRows(descriptor, sourceRows)
	if err != nil {
		t.Fatal(err)
	}
	destinationRows, err := store.ReadRows(ctx, "ACCOUNTS_SHADOW", math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	assertExactRows(t, descriptor, sourceRows, destinationRows)

	names := make([]string, 0, len(phaseTotals))
	for name := range phaseTotals {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		t.Logf("SNOWFLAKE_BACKFILL_PHASE phase=%s count=%d service_ms=%.3f", name, phaseCounts[name], float64(phaseTotals[name])/float64(time.Millisecond))
	}
	t.Logf("SNOWFLAKE_BACKFILL_RESULT rows=%d chunk_size=%d workers=%d poll_ms=%d wall_ms=%.3f rows_per_second=%.3f exact_rows=%d", rows, chunkSize, workers, poll.Milliseconds(), float64(wall)/float64(time.Millisecond), float64(rows)/wall.Seconds(), len(destinationRows))
}

func profilePositiveInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		t.Fatalf("%s must be a positive integer", name)
	}
	return parsed
}

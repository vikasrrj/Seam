//go:build snowflake_integration

package snowflake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"example.com/seam/internal/model"
	_ "github.com/snowflakedb/gosnowflake/v2"
)

// TestLiveSnowflakeControlPlane exercises real Snowflake DDL and transactions
// in two uniquely named schemas. It never uses or modifies the configured
// application schemas. Run it explicitly with -tags=snowflake_integration.
func TestLiveSnowflakeControlPlane(t *testing.T) {
	dsn := os.Getenv("SNOWFLAKE_DSN")
	database := os.Getenv("SNOWFLAKE_DATABASE")
	if dsn == "" || database == "" {
		t.Skip("SNOWFLAKE_DSN and SNOWFLAKE_DATABASE are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	suffix := fmt.Sprintf("%X", time.Now().UnixNano())
	cfg := Config{
		Database: database, Schema: "SEAM_IT_" + suffix,
		InternalSchema: "SEAM_IT_INTERNAL_" + suffix,
		LiveTable:      "ACCOUNTS", StreamID: "integration-" + suffix,
		TopicID: "integration-topic", Partition: 0,
	}
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		for _, schemaName := range []string{cfg.Schema, cfg.InternalSchema} {
			if _, err := db.ExecContext(cleanupCtx, "DROP SCHEMA IF EXISTS "+quoteIdentifier(cfg.Database)+"."+quoteIdentifier(schemaName)+" CASCADE"); err != nil {
				t.Errorf("drop integration schema %s: %v", schemaName, err)
			}
		}
	})

	store, err := NewStore(db, cfg, testSchema())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureObjects(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureObjects(ctx, 99); err != nil {
		t.Fatalf("idempotent EnsureObjects: %v", err)
	}
	if next, err := store.NextOffset(ctx); err != nil || next != 0 {
		t.Fatalf("NextOffset() = %d, %v; want 0, nil", next, err)
	}
	leaseA, err := store.AcquireSinkLease(ctx, "integration-owner-a", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireSinkLease(ctx, "integration-owner-b", 30*time.Second); err == nil {
		t.Fatal("second Snowflake sink acquired a live lease")
	} else {
		var held *LeaseHeldError
		if !errors.As(err, &held) || held.OwnerID != leaseA.OwnerID || held.Epoch != leaseA.Epoch {
			t.Fatalf("second owner error = %v", err)
		}
	}
	if err := store.RenewSinkLease(ctx, leaseA, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	leaseTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.fenceSinkLease(ctx, leaseTx, leaseA); err != nil {
		_ = leaseTx.Rollback()
		t.Fatal(err)
	}
	if err := leaseTx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseSinkLease(ctx, leaseA); err != nil {
		t.Fatal(err)
	}
	leaseB, err := store.AcquireSinkLease(ctx, "integration-owner-b", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if leaseB.Epoch <= leaseA.Epoch {
		t.Fatalf("lease epoch did not advance: first=%d second=%d", leaseA.Epoch, leaseB.Epoch)
	}
	staleTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.fenceSinkLease(ctx, staleTx, leaseA); err == nil {
		_ = staleTx.Rollback()
		t.Fatal("expired owner passed transactional sink fence")
	}
	if err := staleTx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseSinkLease(ctx, leaseB); err != nil {
		t.Fatal(err)
	}

	job, err := store.PrepareBackfill(ctx, "job", "attempt-1", "ACCOUNTS_SHADOW")
	if err != nil {
		t.Fatal(err)
	}
	if job.State != BackfillDiscovering {
		t.Fatalf("prepared state = %s", job.State)
	}
	spec := BackfillSpec{
		JobID: "job", Attempt: "attempt-1", ShadowTable: "ACCOUNTS_SHADOW",
		ScanUpperBound: 10,
		Chunks:         []model.ChunkRange{{Min: math.MinInt64, Max: 10}},
	}
	job, err = store.SealBackfillManifest(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if job.State != BackfillRunning || len(job.Spec.Chunks) != 1 {
		t.Fatalf("sealed job = %+v", job)
	}
	loaded, err := store.LoadBackfill(ctx, "job")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State != BackfillRunning || len(loaded.Spec.Chunks) != 1 {
		t.Fatalf("loaded job = %+v", loaded)
	}

	if _, err := db.ExecContext(ctx, "UPDATE "+cfg.internal("BACKFILL_JOBS")+" SET STATE = ?, COMPLETED_CHUNKS = TOTAL_CHUNKS WHERE STREAM_ID = ? AND JOB_ID = ?", string(BackfillReadyToVerify), cfg.StreamID, "job"); err != nil {
		t.Fatal(err)
	}
	insertMarker := func(id string, offset int64) {
		t.Helper()
		if _, err := db.ExecContext(ctx, "INSERT INTO "+cfg.internal("MARKERS")+" (STREAM_ID, MARKER_ID, KIND, JOB_ID, ATTEMPT, SOURCE_TX, FINAL_OFFSET, SOURCE_LSN) VALUES (?, ?, 'barrier', ?, ?, ?, ?, ?)", cfg.StreamID, id, "job", "attempt-1", "source:"+id, offset, offset+1); err != nil {
			t.Fatal(err)
		}
	}
	insertMarker("validation-marker", 0)
	validated, err := store.PrepareValidationClone(ctx, "job", "validation-marker", "ACCOUNTS_VALIDATION")
	if err != nil {
		t.Fatal(err)
	}
	if !validated.ValidationReady || validated.State != BackfillVerifying {
		t.Fatalf("validation clone state = %+v", validated)
	}
	if err := store.MarkValidated(ctx, "job"); err != nil {
		t.Fatal(err)
	}
	insertMarker("promotion-marker", 1)
	if err := store.PromoteView(ctx, "job", "promotion-marker"); err != nil {
		t.Fatal(err)
	}
	if err := store.PromoteView(ctx, "job", "promotion-marker"); err != nil {
		t.Fatalf("idempotent promotion retry: %v", err)
	}
	promoted, err := store.LoadBackfill(ctx, "job")
	if err != nil {
		t.Fatal(err)
	}
	if promoted.State != BackfillCompleted || promoted.PromotionMarkerID != "promotion-marker" {
		t.Fatalf("promoted job = %+v", promoted)
	}
	var rows int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+cfg.target(cfg.LiveTable)).Scan(&rows); err != nil {
		t.Fatalf("query promoted stable view: %v", err)
	}
}

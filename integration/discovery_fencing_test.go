//go:build integration

package integration

import (
	"context"
	"math"
	"testing"
	"time"

	"example.com/seam/integration/itest"
	"example.com/seam/internal/checkpoint"
	"example.com/seam/internal/model"
)

func TestDiscoveryResumeAndLeaseFencing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	store := checkpoint.NewStore(itest.DestDSN())
	if err := store.EnsureTables(ctx); err != nil {
		t.Fatal(err)
	}
	if err := itest.ResetTables(ctx); err != nil {
		t.Fatal(err)
	}
	source, err := itest.SourceConn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	itest.CloseOnCleanup(t, "source connection", source)
	for _, id := range []int64{5, 100, 150} {
		if _, err := source.Exec(ctx, `INSERT INTO accounts(id, owner, balance_cents) VALUES($1, 'owner', 1)`, id); err != nil {
			t.Fatal(err)
		}
	}
	srcSchema, err := itest.SourceDescriptor(ctx, "accounts")
	if err != nil {
		t.Fatal(err)
	}
	cp, err := store.CreateJob(ctx, model.JobConfig{JobID: itest.JobID("discovery-fencing"), SourceDSN: itest.SourceDSN(), KafkaTopic: itest.KafkaTopic()}, srcSchema, 150)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DiscoverAndCreateChunks(ctx, cp.JobID, cp.Attempt, itest.SourceDSN(), srcSchema, 150, 1); err != nil {
		t.Fatal(err)
	}
	sealed, err := store.DiscoveryComplete(ctx, cp.JobID)
	if err != nil || !sealed {
		t.Fatalf("manifest not sealed: %v, %v", sealed, err)
	}
	chunks, err := store.LoadChunks(ctx, cp.JobID)
	if err != nil || len(chunks) != 3 {
		t.Fatalf("unexpected chunks: %+v, %v", chunks, err)
	}
	for i, expected := range []model.ChunkRange{{Min: math.MinInt64, Max: 5}, {Min: 6, Max: 100}, {Min: 101, Max: 150}} {
		if chunks[i].Range() != expected {
			t.Fatalf("chunk %d = %s, expected %s", i, chunks[i].Range(), expected)
		}
	}

	dest, err := itest.DestConn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	itest.CloseOnCleanup(t, "destination connection", dest)
	// Recreate the durable state left by a crash immediately after chunk one.
	if _, err := dest.Exec(ctx, `DELETE FROM seam_chunks WHERE job_id = $1 AND chunk_min_id > $2`, cp.JobID, int64(math.MinInt64)); err != nil {
		t.Fatal(err)
	}
	if _, err := dest.Exec(ctx, `UPDATE seam_jobs SET discovery_cursor = 5, discovery_complete = FALSE WHERE job_id = $1`, cp.JobID); err != nil {
		t.Fatal(err)
	}
	if err := store.DiscoverAndCreateChunks(ctx, cp.JobID, cp.Attempt, itest.SourceDSN(), srcSchema, 150, 1); err != nil {
		t.Fatal(err)
	}
	chunks, err = store.LoadChunks(ctx, cp.JobID)
	if err != nil || len(chunks) != 3 {
		t.Fatalf("resume omitted ranges: %+v, %v", chunks, err)
	}

	first, err := store.LeaseChunk(ctx, cp.JobID, cp.Attempt, "same-worker", time.Minute)
	if err != nil || first == nil {
		t.Fatalf("first lease: %+v, %v", first, err)
	}
	if _, err := dest.Exec(ctx, `UPDATE seam_chunks SET lease_expiry = NOW() - INTERVAL '1 second' WHERE job_id = $1 AND chunk_min_id = $2`, cp.JobID, first.ChunkMinID); err != nil {
		t.Fatal(err)
	}
	second, err := store.LeaseChunk(ctx, cp.JobID, cp.Attempt, "same-worker", time.Minute)
	if err != nil || second == nil || second.LeaseToken <= first.LeaseToken {
		t.Fatalf("lease token did not advance: first=%+v second=%+v err=%v", first, second, err)
	}
	if err := store.HeartbeatChunk(ctx, first, time.Minute); err == nil {
		t.Fatal("stale lease heartbeat succeeded")
	}
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateChunk(ctx, tx, first); err == nil {
		tx.Rollback(ctx)
		t.Fatal("stale worker wrote chunk with reused worker ID")
	}
	tx.Rollback(ctx)
	if err := store.BeginAttempt(ctx, cp.JobID, cp.Attempt, "gen:0:attempt:1", cp.NextKafkaOffset); err != nil {
		t.Fatal(err)
	}
	tx, err = store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateChunk(ctx, tx, second); err == nil {
		tx.Rollback(ctx)
		t.Fatal("old attempt wrote after recovery fence")
	}
	tx.Rollback(ctx)
}

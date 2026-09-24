//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"example.com/seam/integration/itest"
	"example.com/seam/internal/checkpoint"
	"example.com/seam/internal/model"
)

// TestLeadershipEpochFencesStaleProcess proves that a process which outlives
// its lease cannot commit destination data, advance the checkpoint, lease new
// work, or heartbeat old work after a successor advances the epoch.
func TestLeadershipEpochFencesStaleProcess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := itest.ResetTables(ctx); err != nil {
		t.Fatal(err)
	}

	source, err := itest.SourceConn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	itest.CloseOnCleanup(t, "source connection", source)
	for _, id := range []int64{1, 2} {
		if _, err := source.Exec(ctx, `INSERT INTO accounts(id, owner, balance_cents) VALUES($1, 'source', 1)`, id); err != nil {
			t.Fatal(err)
		}
	}

	store := checkpoint.NewStore(itest.DestDSN())
	srcSchema, err := itest.SourceDescriptor(ctx, "accounts")
	if err != nil {
		t.Fatal(err)
	}
	cp, err := store.CreateJob(ctx, model.JobConfig{
		JobID: itest.JobID("leadership-fencing"), SourceDSN: itest.SourceDSN(), KafkaTopic: itest.KafkaTopic(),
	}, srcSchema, 2)
	if err != nil {
		t.Fatal(err)
	}
	owner1, err := store.AcquireLeadership(ctx, cp.JobID, "owner-1", 800*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireLeadership(ctx, cp.JobID, "contender-too-early", time.Second); err == nil {
		t.Fatal("contender displaced a live owner")
	}
	if err := store.DiscoverAndCreateChunksForOwned(ctx, owner1, itest.SourceDSN(), srcSchema, 2, 1); err != nil {
		t.Fatal(err)
	}
	leased, err := store.LeaseChunkOwned(ctx, cp.JobID, cp.Attempt, "worker-1", owner1.OwnerID, owner1.OwnerEpoch, 5*time.Second)
	if err != nil || leased == nil {
		t.Fatalf("lease as first owner: chunk=%+v err=%v", leased, err)
	}

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AssertLeadership(ctx, tx, owner1); err != nil {
		t.Fatal(err)
	}
	wait := time.Until(*owner1.OwnerLeaseExpiry) + 50*time.Millisecond
	if wait > 0 {
		time.Sleep(wait)
	}

	type acquireResult struct {
		cp  *model.Checkpoint
		err error
	}
	acquired := make(chan acquireResult, 1)
	go func() {
		cp, err := store.AcquireLeadership(ctx, owner1.JobID, "owner-2", 5*time.Second)
		acquired <- acquireResult{cp: cp, err: err}
	}()
	select {
	case result := <-acquired:
		tx.Rollback(ctx)
		t.Fatalf("takeover did not wait for in-flight destination transaction: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	if _, err := tx.Exec(ctx, `INSERT INTO accounts(id, owner, balance_cents) VALUES(9999, 'stale', 1)`); err != nil {
		tx.Rollback(ctx)
		t.Fatal(err)
	}
	owner1.NextKafkaOffset++
	if err := store.UpdateCheckpoint(ctx, tx, owner1, owner1.NextKafkaOffset-1); err == nil {
		tx.Rollback(ctx)
		t.Fatal("expired owner advanced checkpoint")
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	var owner2 *model.Checkpoint
	select {
	case result := <-acquired:
		if result.err != nil {
			t.Fatal(result.err)
		}
		owner2 = result.cp
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if owner2.OwnerEpoch <= owner1.OwnerEpoch {
		t.Fatalf("epoch did not advance: old=%d new=%d", owner1.OwnerEpoch, owner2.OwnerEpoch)
	}
	if err := store.RenewLeadership(ctx, owner1, time.Second); err == nil {
		t.Fatal("stale owner renewed leadership")
	}
	if err := store.HeartbeatChunkOwned(ctx, leased, owner1.OwnerID, owner1.OwnerEpoch, time.Second); err == nil {
		t.Fatal("stale owner heartbeated a chunk")
	}
	if chunk, err := store.LeaseChunkOwned(ctx, cp.JobID, cp.Attempt, "stale-worker", owner1.OwnerID, owner1.OwnerEpoch, time.Second); err != nil || chunk != nil {
		t.Fatalf("stale owner leased work: chunk=%+v err=%v", chunk, err)
	}
	if chunk, err := store.LeaseChunkOwned(ctx, cp.JobID, cp.Attempt, "worker-2", owner2.OwnerID, owner2.OwnerEpoch, time.Second); err != nil || chunk == nil {
		t.Fatalf("new owner could not lease pending work: chunk=%+v err=%v", chunk, err)
	}

	dest, err := itest.DestConn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	itest.CloseOnCleanup(t, "destination connection", dest)
	var staleRows int
	if err := dest.QueryRow(ctx, `SELECT COUNT(*) FROM accounts WHERE id = 9999`).Scan(&staleRows); err != nil {
		t.Fatal(err)
	}
	if staleRows != 0 {
		t.Fatal("stale destination mutation committed without its checkpoint")
	}

	if err := store.ReleaseLeadership(ctx, owner2); err != nil {
		t.Fatal(err)
	}
	owner3, err := store.AcquireLeadership(ctx, cp.JobID, "owner-3", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if owner3.OwnerEpoch <= owner2.OwnerEpoch {
		t.Fatalf("epoch was reused after clean release: old=%d new=%d", owner2.OwnerEpoch, owner3.OwnerEpoch)
	}
}

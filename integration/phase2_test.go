//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"example.com/seam/integration/itest"
	"example.com/seam/internal/capture"
	"example.com/seam/internal/checkpoint"
	"example.com/seam/internal/failpoint"
	"example.com/seam/internal/kafka"
	"example.com/seam/internal/marker"
	"example.com/seam/internal/model"
	"example.com/seam/internal/reconcile"
	"example.com/seam/internal/scan"
	"example.com/seam/internal/sink"
)

// phase2Run drives one chunk with a failpoint and returns the reconciler and
// channels so callers can verify races.
func phase2Run(t *testing.T, disableEviction bool) (*reconcile.Reconciler, func(), *checkpoint.Store, context.CancelFunc, *failpoint.Registry, string) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)

	if err := itest.ResetTables(ctx); err != nil {
		t.Fatalf("reset tables: %v", err)
	}

	src, err := itest.SourceConn(ctx)
	if err != nil {
		t.Fatalf("source conn: %v", err)
	}
	for i := int64(1); i <= 5; i++ {
		if _, err := src.Exec(ctx,
			`INSERT INTO accounts (id, owner, balance_cents) VALUES ($1, $2, $3)`,
			i, fmt.Sprintf("owner-%d", i), i*100); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	src.Close(context.Background())

	readerCfg := capture.ReaderConfig{
		SQLDSN:         itest.SourceDSN(),
		ReplicationDSN: itest.SourceReplDSN(),
		Slot:           "seam_itest_slot",
		Publication:    "seam_pub",
		KafkaBrokers:   itest.KafkaBrokers(),
		KafkaTopic:     itest.KafkaTopic(),
		Generation:     "gen:0",
	}
	reader, err := capture.StartReader(ctx, readerCfg)
	if err != nil {
		t.Fatalf("start capture reader: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })

	recCtx, recCancel := context.WithCancel(ctx)

	capErr := make(chan error, 1)
	go func() {
		if err := reader.Run(recCtx); err != nil && !errors.Is(err, context.Canceled) {
			capErr <- err
		}
		close(capErr)
	}()
	t.Cleanup(func() {
		_ = reader.Close()
		select {
		case err := <-capErr:
			if err != nil {
				t.Errorf("capture reader error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("capture reader did not stop")
		}
	})

	jobID := itest.JobID("phase2")
	jobCfg := model.JobConfig{
		JobID:             jobID,
		SourceDSN:         itest.SourceDSN(),
		SourceReplDSN:     itest.SourceReplDSN(),
		SourceSlot:        "seam_itest_slot",
		SourcePublication: "seam_pub",
		DestDSN:           itest.DestDSN(),
		KafkaBrokers:      itest.KafkaBrokers(),
		KafkaTopic:        itest.KafkaTopic(),
		ChunkSize:         10,
	}
	cpStore := checkpoint.NewStore(itest.DestDSN())
	if err := cpStore.EnsureTables(ctx); err != nil {
		t.Fatalf("ensure tables: %v", err)
	}
	chunkReader, err := scan.NewChunkReader(ctx, itest.SourceDSN())
	if err != nil {
		t.Fatalf("chunk reader: %v", err)
	}
	upperBound, err := chunkReader.UpperBound(ctx)
	if err != nil {
		t.Fatalf("upper bound: %v", err)
	}
	mutator, err := sink.NewMutatorFor("accounts", chunkReader.Schema())
	if err != nil {
		t.Fatalf("sink: %v", err)
	}
	cp, err := cpStore.CreateJob(ctx, jobCfg, chunkReader.Schema(), upperBound)
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	consumer, err := kafka.NewConsumer(itest.KafkaBrokers(), itest.KafkaTopic(), cp.NextKafkaOffset, func(data []byte) (model.Change, error) {
		var codec capture.JSONCodec
		return codec.Decode(data)
	})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	t.Cleanup(func() { consumer.Close() })

	fp := failpoint.NewRegistry()
	fp.Install(failpoint.AfterChunkReadBeforeReconciliation)

	rec := reconcile.New(reconcile.Config{
		JobConfig:       jobCfg,
		Checkpoint:      cp,
		Consumer:        consumer,
		CheckpointStore: cpStore,
		MarkerStore:     marker.NewStore(itest.SourceDSN()),
		Scanner:         chunkReader,
		Sink:            mutator,
		SourceSchema:    chunkReader.Schema(),
		Failpoints:      fp,
		DisableEviction: disableEviction,
	})

	recErr := make(chan error, 1)
	go func() {
		recErr <- rec.Run(recCtx)
	}()

	stop := func() {
		recCancel()
		select {
		case err := <-recErr:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("reconciler error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("reconciler did not stop")
		}
	}
	t.Cleanup(stop)

	return rec, func() { fp.Resume(failpoint.AfterChunkReadBeforeReconciliation) }, cpStore, recCancel, fp, jobID
}

// TestPhase2_NaiveStaleUpdate demonstrates that without candidate eviction the
// snapshot overwrites a concurrent update.
func TestPhase2_NaiveStaleUpdate(t *testing.T) {
	_, resume, cpStore, cancel, fp, jobID := phase2Run(t, true)
	defer cancel()

	ctx, done := context.WithTimeout(context.Background(), 30*time.Second)
	defer done()

	if err := fp.Get(failpoint.AfterChunkReadBeforeReconciliation).WaitForTrigger(ctx); err != nil {
		t.Fatalf("failpoint did not trigger: %v", err)
	}

	src, err := itest.SourceConn(ctx)
	if err != nil {
		t.Fatalf("source conn: %v", err)
	}
	if _, err := src.Exec(ctx, `UPDATE accounts SET owner = 'vikas' WHERE id = 3`); err != nil {
		t.Fatalf("update: %v", err)
	}
	src.Close(context.Background())

	dst, err := itest.DestConn(ctx)
	if err != nil {
		t.Fatalf("dest conn: %v", err)
	}
	itest.CloseOnCleanup(t, "destination connection", dst)

	// Wait for CDC to apply the update.
	for i := 0; i < 40; i++ {
		var owner string
		if err := dst.QueryRow(ctx, `SELECT owner FROM accounts WHERE id = 3`).Scan(&owner); err == nil && owner == "vikas" {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	// Resume the naive snapshot.
	resume()

	// Wait for the chunk to complete.
	for i := 0; i < 40; i++ {
		var completed int64
		if err := dst.QueryRow(ctx, `SELECT completed_through_id FROM seam_checkpoints WHERE job_id = $1`, jobID).Scan(&completed); err == nil && completed >= 4 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	// Without eviction, the snapshot overwrites the CDC update.
	var owner string
	if err := dst.QueryRow(ctx, `SELECT owner FROM accounts WHERE id = 3`).Scan(&owner); err != nil {
		t.Fatalf("read dest row: %v", err)
	}
	if owner != "owner-3" {
		t.Fatalf("expected stale value owner-3, got %q", owner)
	}
	fmt.Println("Phase 2 stale update reproduced: naive snapshot overwrote concurrent update")
	_ = cpStore
}

// TestPhase2_NaiveDeleteResurrection demonstrates that without candidate
// eviction the snapshot resurrects a concurrent delete.
func TestPhase2_NaiveDeleteResurrection(t *testing.T) {
	_, resume, cpStore, cancel, fp, jobID := phase2Run(t, true)
	defer cancel()

	ctx, done := context.WithTimeout(context.Background(), 30*time.Second)
	defer done()

	if err := fp.Get(failpoint.AfterChunkReadBeforeReconciliation).WaitForTrigger(ctx); err != nil {
		t.Fatalf("failpoint did not trigger: %v", err)
	}

	src, err := itest.SourceConn(ctx)
	if err != nil {
		t.Fatalf("source conn: %v", err)
	}
	if _, err := src.Exec(ctx, `DELETE FROM accounts WHERE id = 3`); err != nil {
		t.Fatalf("delete: %v", err)
	}
	src.Close(context.Background())

	dst, err := itest.DestConn(ctx)
	if err != nil {
		t.Fatalf("dest conn: %v", err)
	}
	itest.CloseOnCleanup(t, "destination connection", dst)

	// Wait for CDC to apply the delete.
	for i := 0; i < 40; i++ {
		var exists bool
		if err := dst.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM accounts WHERE id = 3)`).Scan(&exists); err == nil && !exists {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	// Resume the naive snapshot.
	resume()

	// Wait for the chunk to complete.
	for i := 0; i < 40; i++ {
		var completed int64
		if err := dst.QueryRow(ctx, `SELECT completed_through_id FROM seam_checkpoints WHERE job_id = $1`, jobID).Scan(&completed); err == nil && completed >= 4 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	// Without eviction, the snapshot resurrects the deleted row.
	var exists bool
	if err := dst.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM accounts WHERE id = 3)`).Scan(&exists); err != nil {
		t.Fatalf("read dest row: %v", err)
	}
	if !exists {
		t.Fatal("expected row 3 to be resurrected by naive snapshot")
	}
	fmt.Println("Phase 2 delete resurrection reproduced: naive snapshot recreated deleted row")
	_ = cpStore
}

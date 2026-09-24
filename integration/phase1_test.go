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
	"example.com/seam/internal/kafka"
	"example.com/seam/internal/marker"
	"example.com/seam/internal/model"
	"example.com/seam/internal/reconcile"
	"example.com/seam/internal/scan"
	"example.com/seam/internal/sink"
)

// TestPhase1_BoundedSnapshot proves a static source table is copied completely
// using bounded chunks.
func TestPhase1_BoundedSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	if err := itest.ResetTables(ctx); err != nil {
		t.Fatalf("reset tables: %v", err)
	}

	src, err := itest.SourceConn(ctx)
	if err != nil {
		t.Fatalf("source conn: %v", err)
	}
	for i := int64(1); i <= 25; i++ {
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
	defer reader.Close()

	recCtx, recCancel := context.WithCancel(ctx)
	defer recCancel()

	capErr := make(chan error, 1)
	go func() {
		if err := reader.Run(recCtx); err != nil && !errors.Is(err, context.Canceled) {
			capErr <- err
		}
		close(capErr)
	}()

	jobCfg := model.JobConfig{
		JobID:             itest.JobID("phase1"),
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
	defer consumer.Close()

	rec := reconcile.New(reconcile.Config{
		JobConfig:       jobCfg,
		Checkpoint:      cp,
		Consumer:        consumer,
		CheckpointStore: cpStore,
		MarkerStore:     marker.NewStore(itest.SourceDSN()),
		Scanner:         chunkReader,
		Sink:            mutator,
		SourceSchema:    chunkReader.Schema(),
	})

	recErr := make(chan error, 1)
	go func() {
		recErr <- rec.Run(recCtx)
	}()

	// Wait for backfill to finish (completed_through reaches upper_bound).
	dst, err := itest.DestConn(ctx)
	if err != nil {
		t.Fatalf("dest conn: %v", err)
	}
	itest.CloseOnCleanup(t, "destination connection", dst)

	var completed int64 = -1
	for i := 0; i < 60; i++ {
		if err := dst.QueryRow(ctx, `SELECT completed_through_id FROM seam_checkpoints WHERE job_id = $1`, jobCfg.JobID).Scan(&completed); err != nil {
			t.Fatalf("read checkpoint: %v", err)
		}
		if completed >= upperBound {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if completed < upperBound {
		t.Fatalf("backfill did not complete: completed_through=%d upper_bound=%d", completed, upperBound)
	}

	// Verify every source row is present in destination.
	var count int
	if err := dst.QueryRow(ctx, `SELECT COUNT(*) FROM accounts`).Scan(&count); err != nil {
		t.Fatalf("count dest: %v", err)
	}
	if count != 25 {
		t.Fatalf("expected 25 rows, got %d", count)
	}
	for i := int64(1); i <= 25; i++ {
		var owner string
		var balance int64
		if err := dst.QueryRow(ctx, `SELECT owner, balance_cents FROM accounts WHERE id = $1`, i).Scan(&owner, &balance); err != nil {
			t.Fatalf("read dest row %d: %v", i, err)
		}
		if owner != fmt.Sprintf("owner-%d", i) || balance != i*100 {
			t.Fatalf("row %d mismatch: owner=%s balance=%d", i, owner, balance)
		}
	}

	recCancel()
	select {
	case err := <-recErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("reconciler error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reconciler did not stop")
	}

	_ = reader.Close()
	select {
	case err := <-capErr:
		if err != nil {
			t.Fatalf("capture reader error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("capture reader did not stop")
	}

	fmt.Println("Phase 1 bounded snapshot verified")
}

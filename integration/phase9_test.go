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

// TestPhase9_CDCContinuity proves that live changes after the backfill are
// streamed to the destination without interruption.
func TestPhase9_CDCContinuity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

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
	defer reader.Close()

	recCtx, recCancel := context.WithCancel(ctx)
	capErr := make(chan error, 1)
	go func() {
		if err := reader.Run(recCtx); err != nil && !errors.Is(err, context.Canceled) {
			capErr <- err
		}
		close(capErr)
	}()
	defer func() {
		_ = reader.Close()
		select {
		case err := <-capErr:
			if err != nil {
				t.Errorf("capture reader error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("capture reader did not stop")
		}
	}()

	jobCfg := model.JobConfig{
		JobID:             itest.JobID("phase9"),
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

	fp := failpoint.NewRegistry()
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
	})

	recErr := make(chan error, 1)
	go func() {
		recErr <- rec.Run(recCtx)
	}()

	dst, err := itest.DestConn(ctx)
	if err != nil {
		t.Fatalf("dest conn: %v", err)
	}
	itest.CloseOnCleanup(t, "destination connection", dst)

	var completed int64
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

	// Apply live changes on the source after backfill.
	src, err = itest.SourceConn(ctx)
	if err != nil {
		t.Fatalf("source conn for live changes: %v", err)
	}
	if _, err := src.Exec(ctx, `INSERT INTO accounts (id, owner, balance_cents) VALUES (6, 'live-insert', 600)`); err != nil {
		t.Fatalf("live insert: %v", err)
	}
	if _, err := src.Exec(ctx, `UPDATE accounts SET balance_cents = 12345 WHERE id = 1`); err != nil {
		t.Fatalf("live update: %v", err)
	}
	if _, err := src.Exec(ctx, `DELETE FROM accounts WHERE id = 2`); err != nil {
		t.Fatalf("live delete: %v", err)
	}
	src.Close(context.Background())

	// Wait for CDC to reach destination.
	for i := 0; i < 60; i++ {
		var count int
		if err := dst.QueryRow(ctx, `SELECT COUNT(*) FROM accounts`).Scan(&count); err != nil {
			t.Fatalf("count dest rows: %v", err)
		}
		if count == 4 { // 5 initial + 1 insert - 1 delete
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	var balance int64
	if err := dst.QueryRow(ctx, `SELECT balance_cents FROM accounts WHERE id = 1`).Scan(&balance); err != nil {
		t.Fatalf("read updated row: %v", err)
	}
	if balance != 12345 {
		t.Fatalf("expected balance 12345, got %d", balance)
	}

	var liveOwner string
	if err := dst.QueryRow(ctx, `SELECT owner FROM accounts WHERE id = 6`).Scan(&liveOwner); err != nil {
		t.Fatalf("read inserted row: %v", err)
	}
	if liveOwner != "live-insert" {
		t.Fatalf("expected owner 'live-insert', got %q", liveOwner)
	}

	var deletedExists bool
	if err := dst.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM accounts WHERE id = 2)`).Scan(&deletedExists); err != nil {
		t.Fatalf("check deleted row: %v", err)
	}
	if deletedExists {
		t.Fatal("expected row 2 to be deleted on destination")
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

	fmt.Println("Phase 9 CDC continuity verified")
}

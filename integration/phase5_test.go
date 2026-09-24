//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"math"
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
	"example.com/seam/internal/recovery"
	"example.com/seam/internal/scan"
	"example.com/seam/internal/sink"
)

// TestPhase5_CrashAfterChunkRead proves recovery re-reads the unfinished chunk
// with a new attempt and completes it.
func TestPhase5_CrashAfterChunkRead(t *testing.T) {
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

	capErr := make(chan error, 1)
	go func() {
		if err := reader.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
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
		JobID:             itest.JobID("phase5"),
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
	// The job is durable state. Recovery requires incomplete chunks to justify
	// a new attempt, so discovery must seal the manifest before the first run,
	// exactly as the production --start-fresh path does.
	if err := cpStore.DiscoverAndCreateChunks(ctx, cp.JobID, cp.Attempt, itest.SourceDSN(), chunkReader.Schema(), upperBound, jobCfg.ChunkSize); err != nil {
		t.Fatalf("discover chunks: %v", err)
	}

	// First attempt: start seam, let it read the chunk, then crash.
	recCtx1, recCancel1 := context.WithCancel(ctx)
	consumer1, err := kafka.NewConsumer(itest.KafkaBrokers(), itest.KafkaTopic(), cp.NextKafkaOffset, func(data []byte) (model.Change, error) {
		var codec capture.JSONCodec
		return codec.Decode(data)
	})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	fp1 := failpoint.NewRegistry()
	fp1.Install(failpoint.AfterChunkReadBeforeReconciliation)

	rec1 := reconcile.New(reconcile.Config{
		JobConfig:       jobCfg,
		Checkpoint:      cp,
		Consumer:        consumer1,
		CheckpointStore: cpStore,
		MarkerStore:     marker.NewStore(itest.SourceDSN()),
		Scanner:         chunkReader,
		Sink:            mutator,
		SourceSchema:    chunkReader.Schema(),
		Failpoints:      fp1,
	})

	recErr1 := make(chan error, 1)
	go func() {
		recErr1 <- rec1.Run(recCtx1)
	}()

	if err := fp1.Get(failpoint.AfterChunkReadBeforeReconciliation).WaitForTrigger(ctx); err != nil {
		t.Fatalf("failpoint did not trigger: %v", err)
	}

	// Simulate crash: stop reconciler without resuming the failpoint.
	recCancel1()
	select {
	case err := <-recErr1:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("first reconciler error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first reconciler did not stop")
	}
	consumer1.Close()
	time.Sleep(2 * time.Second)

	// Recover: load checkpoint and bump attempt.
	result, err := recovery.Recover(ctx, cpStore, jobCfg.JobID, jobCfg.SourceDSN)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if result.Checkpoint.Attempt == cp.Attempt {
		t.Fatalf("expected new attempt after recovery, got %s", result.Checkpoint.Attempt)
	}
	if result.Checkpoint.CompletedThrough != math.MinInt64 {
		t.Fatalf("expected completed_through to stay at %d, got %d", math.MinInt64, result.Checkpoint.CompletedThrough)
	}

	// Second attempt: resume from recovered checkpoint.
	recCtx2, recCancel2 := context.WithCancel(ctx)
	defer recCancel2()
	consumer2, err := kafka.NewConsumer(itest.KafkaBrokers(), itest.KafkaTopic(), result.Checkpoint.NextKafkaOffset, func(data []byte) (model.Change, error) {
		var codec capture.JSONCodec
		return codec.Decode(data)
	})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	defer consumer2.Close()

	rec2 := reconcile.New(reconcile.Config{
		JobConfig:       jobCfg,
		Checkpoint:      result.Checkpoint,
		Consumer:        consumer2,
		CheckpointStore: cpStore,
		MarkerStore:     marker.NewStore(itest.SourceDSN()),
		Scanner:         chunkReader,
		Sink:            mutator,
		SourceSchema:    chunkReader.Schema(),
	})

	recErr2 := make(chan error, 1)
	go func() {
		recErr2 <- rec2.Run(recCtx2)
	}()

	// Verify the chunk completes.
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
		t.Fatalf("backfill did not complete after recovery: completed_through=%d upper_bound=%d", completed, upperBound)
	}

	var count int
	if err := dst.QueryRow(ctx, `SELECT COUNT(*) FROM accounts`).Scan(&count); err != nil {
		t.Fatalf("count dest: %v", err)
	}
	if count != 5 {
		t.Fatalf("expected 5 rows, got %d", count)
	}

	recCancel2()
	select {
	case err := <-recErr2:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("second reconciler error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second reconciler did not stop")
	}

	fmt.Println("Phase 5 crash recovery verified")
}

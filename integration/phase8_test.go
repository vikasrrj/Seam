//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
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
	"example.com/seam/internal/telemetry"
)

// TestPhase8_ResourceBoundsAndTelemetry proves that the backfill respects the
// configured chunk size and emits telemetry for each completed chunk.
func TestPhase8_ResourceBoundsAndTelemetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	if err := itest.ResetTables(ctx); err != nil {
		t.Fatalf("reset tables: %v", err)
	}

	src, err := itest.SourceConn(ctx)
	if err != nil {
		t.Fatalf("source conn: %v", err)
	}
	const rowCount = 50
	const chunkSize = 10
	for i := int64(1); i <= rowCount; i++ {
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
		JobID:             itest.JobID("phase8"),
		SourceDSN:         itest.SourceDSN(),
		SourceReplDSN:     itest.SourceReplDSN(),
		SourceSlot:        "seam_itest_slot",
		SourcePublication: "seam_pub",
		DestDSN:           itest.DestDSN(),
		KafkaBrokers:      itest.KafkaBrokers(),
		KafkaTopic:        itest.KafkaTopic(),
		ChunkSize:         chunkSize,
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

	metrics := telemetry.NewMetrics()
	var callbackChunks atomic.Int64
	metrics.OnChunkComplete(func(stats telemetry.ChunkStats) {
		callbackChunks.Add(1)
		if stats.Candidates > chunkSize {
			t.Errorf("chunk exceeded size limit: %d > %d", stats.Candidates, chunkSize)
		}
	})

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
		Metrics:         metrics,
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
	for i := 0; i < 120; i++ {
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

	recCancel()
	select {
	case err := <-recErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("reconciler error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reconciler did not stop")
	}

	if metrics.ChunksCompleted() != rowCount/chunkSize {
		t.Fatalf("expected %d chunks, got %d", rowCount/chunkSize, metrics.ChunksCompleted())
	}
	if metrics.CandidatesSeen() != rowCount {
		t.Fatalf("expected %d candidates seen, got %d", rowCount, metrics.CandidatesSeen())
	}
	if callbackChunks.Load() != rowCount/chunkSize {
		t.Fatalf("expected %d chunk callbacks, got %d", rowCount/chunkSize, callbackChunks.Load())
	}

	var destRows int
	if err := dst.QueryRow(ctx, `SELECT COUNT(*) FROM accounts`).Scan(&destRows); err != nil {
		t.Fatalf("count destination rows: %v", err)
	}
	if destRows != rowCount {
		t.Fatalf("expected %d destination rows, got %d", rowCount, destRows)
	}

	fmt.Println("Phase 8 resource bounds and telemetry verified")
}

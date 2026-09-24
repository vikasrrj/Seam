//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"example.com/seam/integration/itest"
	"example.com/seam/internal/capture"
	"example.com/seam/internal/checkpoint"
	"example.com/seam/internal/kafka"
	"example.com/seam/internal/marker"
	"example.com/seam/internal/model"
	"example.com/seam/internal/promotion"
	"example.com/seam/internal/reconcile"
	"example.com/seam/internal/scan"
	"example.com/seam/internal/sink"
)

// TestOnlineShadowResync runs the production durable-chunk coordinator twice
// against one capture stream, changes the source while shadow is active, and
// promotes only after the two independent frontiers have caught up.
func TestOnlineShadowResync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if err := itest.ResetTables(ctx); err != nil {
		t.Fatal(err)
	}
	source, err := itest.SourceConn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	itest.CloseOnCleanup(t, "source connection", source)
	if _, err := source.Exec(ctx, `INSERT INTO accounts(id, owner, balance_cents)
		SELECT id, 'original', id * 100 FROM generate_series(1, 20) AS g(id)`); err != nil {
		t.Fatal(err)
	}
	store := checkpoint.NewStore(itest.DestDSN())
	if err := store.EnsureDestinationTableFor(ctx, "accounts_shadow"); err != nil {
		t.Fatal(err)
	}
	dest, err := itest.DestConn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	itest.CloseOnCleanup(t, "destination connection", dest)
	if _, err := dest.Exec(ctx, `TRUNCATE accounts_shadow`); err != nil {
		t.Fatal(err)
	}
	markers := marker.NewStore(itest.SourceDSN())
	systemID, err := capture.SourceSystemID(ctx, itest.SourceDSN())
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := kafka.TopicIdentity(ctx, itest.KafkaBrokers(), itest.KafkaTopic())
	if err != nil {
		t.Fatal(err)
	}
	reader, err := capture.StartReader(ctx, capture.ReaderConfig{
		SQLDSN: itest.SourceDSN(), ReplicationDSN: itest.SourceReplDSN(),
		Slot: "seam_itest_slot", Publication: "seam_pub", KafkaBrokers: itest.KafkaBrokers(),
		KafkaTopic: itest.KafkaTopic(), Generation: "gen:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	readerCtx, stopReader := context.WithCancel(ctx)
	readerDone := make(chan error, 1)
	go func() { readerDone <- reader.Run(readerCtx) }()
	t.Cleanup(func() {
		stopReader()
		reader.Close()
		select {
		case err := <-readerDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("capture stopped unexpectedly: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("capture did not stop")
		}
	})
	startJob := func(jobID, table string) (chan error, context.CancelFunc) {
		t.Helper()
		end, err := kafka.EndOffset(ctx, itest.KafkaBrokers(), itest.KafkaTopic())
		if err != nil {
			t.Fatal(err)
		}
		barrierID, err := markers.WriteBarrier(ctx, jobID, "gen:0:attempt:0")
		if err != nil {
			t.Fatal(err)
		}
		start, err := kafka.WaitForBarrier(ctx, itest.KafkaBrokers(), itest.KafkaTopic(), barrierID, end, benchDecode)
		if err != nil {
			t.Fatal(err)
		}
		cfg := model.JobConfig{
			JobID: jobID, SourceDSN: itest.SourceDSN(), SourceReplDSN: itest.SourceReplDSN(),
			SourceSlot: "seam_itest_slot", SourcePublication: "seam_pub", SourceSystemID: systemID,
			DestDSN: itest.DestDSN(), DestTable: table, KafkaBrokers: itest.KafkaBrokers(),
			KafkaTopic: itest.KafkaTopic(), KafkaTopicID: topicID, ChunkSize: 5,
			Workers: 2, WorkerID: jobID, LeaseDuration: 30 * time.Second,
			MaxInMemoryCandidates: 100, MaxRecordsPerBatch: 100, HeartbeatInterval: 5 * time.Second,
		}
		chunkReader, err := scan.NewChunkReader(ctx, itest.SourceDSN())
		if err != nil {
			t.Fatal(err)
		}
		upper, err := chunkReader.UpperBound(ctx)
		if err != nil {
			t.Fatal(err)
		}
		cp, err := store.CreateJobAt(ctx, cfg, chunkReader.Schema(), upper, start)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.DiscoverAndCreateChunks(ctx, jobID, cp.Attempt, itest.SourceDSN(), chunkReader.Schema(), upper, cfg.ChunkSize); err != nil {
			t.Fatal(err)
		}
		consumer, err := kafka.NewConsumer(itest.KafkaBrokers(), itest.KafkaTopic(), start, benchDecode, kafka.WithExpectedTopicID(topicID))
		if err != nil {
			t.Fatal(err)
		}
		mutator, err := sink.NewMutatorFor(table, chunkReader.Schema())
		if err != nil {
			t.Fatal(err)
		}
		recCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		rec := reconcile.New(reconcile.Config{
			JobConfig: cfg, Checkpoint: cp, Consumer: consumer, CheckpointStore: store,
			ChunkStore: store, MarkerStore: markers, Scanner: chunkReader, Sink: mutator, SourceSchema: chunkReader.Schema(),
		})
		go func() { done <- rec.Run(recCtx) }()
		t.Cleanup(func() {
			stop()
			consumer.Close()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Errorf("%s reconciler did not stop", jobID)
			}
		})
		return done, stop
	}
	liveJobID := itest.JobID("online-live")
	shadowJobID := itest.JobID("online-shadow")
	liveDone, _ := startJob(liveJobID, "accounts")
	for {
		cp, err := store.LoadCheckpoint(ctx, liveJobID)
		if err != nil {
			t.Fatal(err)
		}
		if cp.CompletedThrough >= cp.ScanUpperBound {
			break
		}
		select {
		case err := <-liveDone:
			t.Fatalf("live reconciler exited early: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	shadowDone, _ := startJob(shadowJobID, "accounts_shadow")
	if _, err := source.Exec(ctx, `UPDATE accounts SET owner = 'changed', balance_cents = 777 WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(ctx, `DELETE FROM accounts WHERE id = 2`); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(ctx, `INSERT INTO accounts VALUES (100, 'new', 10000)`); err != nil {
		t.Fatal(err)
	}
	var beforeCount int64
	if err := dest.QueryRow(ctx, `SELECT count(*) FROM accounts`).Scan(&beforeCount); err != nil || beforeCount == 0 {
		t.Fatalf("public live table was unavailable during shadow load: count=%d err=%v", beforeCount, err)
	}
	result, err := promotion.Promote(ctx, promotion.Config{
		SourceDSN: itest.SourceDSN(), DestDSN: itest.DestDSN(),
		KafkaBrokers: itest.KafkaBrokers(), KafkaTopic: itest.KafkaTopic(),
		LiveJobID: liveJobID, ShadowJobID: shadowJobID,
	})
	if err != nil {
		select {
		case runErr := <-liveDone:
			t.Logf("live reconciler: %v", runErr)
		default:
		}
		select {
		case runErr := <-shadowDone:
			t.Logf("shadow reconciler: %v", runErr)
		default:
		}
		t.Fatal(err)
	}
	var owner string
	var balance int64
	if err := dest.QueryRow(ctx, `SELECT owner, balance_cents FROM accounts WHERE id = 1`).Scan(&owner, &balance); err != nil || owner != "changed" || balance != 777 {
		t.Fatalf("promoted update missing: owner=%q balance=%d err=%v", owner, balance, err)
	}
	var count int64
	if err := dest.QueryRow(ctx, `SELECT count(*) FROM accounts WHERE id = 2`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("promoted delete missing: count=%d err=%v", count, err)
	}
	if err := dest.QueryRow(ctx, `SELECT count(*) FROM accounts WHERE id = 100`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("promoted insert missing: count=%d err=%v", count, err)
	}
	if err := dest.QueryRow(ctx, `SELECT count(*) FROM `+result.RetiredTable).Scan(&count); err != nil || count != 20 {
		t.Fatalf("retired live table missing: count=%d err=%v", count, err)
	}
	shadowCP, err := store.LoadCheckpoint(ctx, shadowJobID)
	if err != nil || shadowCP.Active {
		t.Fatalf("shadow not fenced: %+v, %v", shadowCP, err)
	}
	retry, err := promotion.Promote(ctx, promotion.Config{
		SourceDSN: itest.SourceDSN(), DestDSN: itest.DestDSN(),
		KafkaBrokers: itest.KafkaBrokers(), KafkaTopic: itest.KafkaTopic(),
		LiveJobID: liveJobID, ShadowJobID: shadowJobID,
	})
	if err != nil || !retry.AlreadyPromoted || retry.RetiredTable != result.RetiredTable {
		t.Fatalf("promotion retry failed: %+v, %v", retry, err)
	}
	select {
	case err := <-liveDone:
		t.Fatalf("live reconciler stopped after cutover: %v", err)
	default:
	}
}

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
	"example.com/seam/internal/kafka"
	"example.com/seam/internal/marker"
	"example.com/seam/internal/model"
	"example.com/seam/internal/reconcile"
	"example.com/seam/internal/scan"
	"example.com/seam/internal/sink"
)

// TestPhase6_HardCases exercises edge cases in reconciliation.
func TestPhase6_HardCases(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	if err := itest.ResetTables(ctx); err != nil {
		t.Fatalf("reset tables: %v", err)
	}

	src, err := itest.SourceConn(ctx)
	if err != nil {
		t.Fatalf("source conn: %v", err)
	}

	rows := []struct {
		id      int64
		owner   string
		balance int64
	}{
		{-10, "neg-ten", -1000},
		{-5, "neg-five", -500},
		{0, "zero", 0},
		{1, "one", 100},
		{2, "two", 200},
		{3, "three", 300},
		{4, "four", 400},
		{5, "five", 500},
		{6, "six", 600},
		{7, "seven", 700},
		{8, "eight", 800},
		{9, "nine", 900},
		{10, "ten", 1000},
		{math.MaxInt64 - 1, "max-minus-one", math.MaxInt64 - 1},
	}
	for _, r := range rows {
		if _, err := src.Exec(ctx,
			`INSERT INTO accounts (id, owner, balance_cents) VALUES ($1, $2, $3)`,
			r.id, r.owner, r.balance); err != nil {
			t.Fatalf("insert %d: %v", r.id, err)
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
		JobID:             itest.JobID("phase6"),
		SourceDSN:         itest.SourceDSN(),
		SourceReplDSN:     itest.SourceReplDSN(),
		SourceSlot:        "seam_itest_slot",
		SourcePublication: "seam_pub",
		DestDSN:           itest.DestDSN(),
		KafkaBrokers:      itest.KafkaBrokers(),
		KafkaTopic:        itest.KafkaTopic(),
		ChunkSize:         5,
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

	recCtx, recCancel := context.WithCancel(ctx)
	defer recCancel()
	recErr := make(chan error, 1)
	go func() {
		recErr <- rec.Run(recCtx)
	}()

	// Give seam a moment to start chunking, then apply concurrent mutations.
	time.Sleep(2 * time.Second)
	src, err = itest.SourceConn(ctx)
	if err != nil {
		t.Fatalf("source conn: %v", err)
	}
	// Case 1: snapshot row -> UPDATE
	if _, err := src.Exec(ctx, `UPDATE accounts SET owner = 'one-updated' WHERE id = 1`); err != nil {
		t.Fatalf("update 1: %v", err)
	}
	// Case 2: snapshot row -> DELETE
	if _, err := src.Exec(ctx, `DELETE FROM accounts WHERE id = 2`); err != nil {
		t.Fatalf("delete 2: %v", err)
	}
	// Case 3: DELETE then INSERT same ID
	if _, err := src.Exec(ctx, `DELETE FROM accounts WHERE id = 3`); err != nil {
		t.Fatalf("delete 3: %v", err)
	}
	if _, err := src.Exec(ctx, `INSERT INTO accounts (id, owner, balance_cents) VALUES (3, 'three-reborn', 3000)`); err != nil {
		t.Fatalf("insert 3: %v", err)
	}
	// Case 4: multiple UPDATEs to same ID
	if _, err := src.Exec(ctx, `UPDATE accounts SET owner = 'five-first' WHERE id = 5`); err != nil {
		t.Fatalf("update 5a: %v", err)
	}
	if _, err := src.Exec(ctx, `UPDATE accounts SET owner = 'five-second' WHERE id = 5`); err != nil {
		t.Fatalf("update 5b: %v", err)
	}
	// Case 5: insert beyond initial scan upper bound
	if _, err := src.Exec(ctx, `INSERT INTO accounts (id, owner, balance_cents) VALUES (100, 'beyond-bound', 10000)`); err != nil {
		t.Fatalf("insert 100: %v", err)
	}
	src.Close(context.Background())

	// Wait for backfill and CDC to settle.
	dst, err := itest.DestConn(ctx)
	if err != nil {
		t.Fatalf("dest conn: %v", err)
	}
	itest.CloseOnCleanup(t, "destination connection", dst)

	var completed int64 = -1
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

	// Wait a bit more for trailing CDC.
	time.Sleep(3 * time.Second)

	// Verify final state matches source.
	expected := map[int64]struct {
		owner   string
		balance int64
		exists  bool
	}{
		-10:               {"neg-ten", -1000, true},
		-5:                {"neg-five", -500, true},
		0:                 {"zero", 0, true},
		1:                 {"one-updated", 100, true},
		2:                 {"", 0, false},
		3:                 {"three-reborn", 3000, true},
		4:                 {"four", 400, true},
		5:                 {"five-second", 500, true},
		6:                 {"six", 600, true},
		7:                 {"seven", 700, true},
		8:                 {"eight", 800, true},
		9:                 {"nine", 900, true},
		10:                {"ten", 1000, true},
		100:               {"beyond-bound", 10000, true},
		math.MaxInt64 - 1: {"max-minus-one", math.MaxInt64 - 1, true},
	}

	for id, exp := range expected {
		var owner string
		var balance int64
		err := dst.QueryRow(ctx, `SELECT owner, balance_cents FROM accounts WHERE id = $1`, id).Scan(&owner, &balance)
		if exp.exists {
			if err != nil {
				t.Fatalf("expected row %d to exist: %v", id, err)
			}
			if owner != exp.owner || balance != exp.balance {
				t.Fatalf("row %d mismatch: expected (%s,%d), got (%s,%d)", id, exp.owner, exp.balance, owner, balance)
			}
		} else {
			if err == nil {
				t.Fatalf("expected row %d to be deleted, got (%s,%d)", id, owner, balance)
			}
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

	fmt.Println("Phase 6 hard cases verified")
}

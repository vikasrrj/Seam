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
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

// TestPhase0_CDCFlow proves source changes reach the destination via Kafka,
// transition by transition, and that the durable Kafka checkpoint advances.
//
// The scenario is structured so the initial backfill can never explain the
// observed writes: source rows 1..3 are seeded before the job is created (they
// arrive via the scan path), and the CDC mutations use id=5, which is outside
// the [1..3] backfill manifest after the manifest is exhausted. Every change to
// id=5 at the destination must therefore have flowed capture → Kafka → sink.
//
// Each step uses bounded polling with failure diagnostics that report the last
// observed state; nothing relies on an unconditional sleep.
func TestPhase0_CDCFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	if err := itest.ResetTables(ctx); err != nil {
		t.Fatalf("reset tables: %v", err)
	}

	// Seed the source before capture and the job exist, so rows 1..3 are
	// reachable only through the backfill scan.
	src, err := itest.SourceConn(ctx)
	if err != nil {
		t.Fatalf("source conn: %v", err)
	}
	itest.CloseOnCleanup(t, "source connection", src)
	for id := int64(1); id <= 3; id++ {
		if _, err := src.Exec(ctx, `INSERT INTO accounts (id, owner, balance_cents) VALUES ($1::bigint, 'seed-' || $1::bigint::text, $1::bigint * 100)`, id); err != nil {
			t.Fatalf("seed row %d: %v", id, err)
		}
	}

	// Start the capture reader.
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

	// Start the seam reconciler. There is no durable chunk store, so it runs
	// the legacy scan loop and then CDC-only mode.
	jobCfg := model.JobConfig{
		JobID:             itest.JobID("phase0"),
		SourceDSN:         itest.SourceDSN(),
		SourceReplDSN:     itest.SourceReplDSN(),
		SourceSlot:        "seam_itest_slot",
		SourcePublication: "seam_pub",
		DestDSN:           itest.DestDSN(),
		KafkaBrokers:      itest.KafkaBrokers(),
		KafkaTopic:        itest.KafkaTopic(),
		ChunkSize:         100,
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
	if upperBound != 3 {
		t.Fatalf("upper bound = %d, want 3 (seeded ids 1..3)", upperBound)
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

	dst, err := itest.DestConn(ctx)
	if err != nil {
		t.Fatalf("dest conn: %v", err)
	}
	itest.CloseOnCleanup(t, "destination connection", dst)

	// Baseline: the seeded rows reach the destination through the backfill.
	// This proves capture, the marker stream, and the sink are all live before
	// any CDC mutation is issued.
	waitForRow(ctx, t, dst, 1, "seed-1", 100, "initial backfill")
	waitForRow(ctx, t, dst, 3, "seed-3", 300, "initial backfill")

	// Capture both persistence frontiers before mutating the source. The
	// replication slot proves capture acknowledged WAL; seam_checkpoints proves
	// the destination transaction durably advanced the Kafka consumer offset.
	baselineLSN := waitSlotLSN(ctx, t)
	baselineCheckpoint, err := cpStore.LoadCheckpoint(ctx, jobCfg.JobID)
	if err != nil {
		t.Fatalf("load baseline checkpoint: %v", err)
	}
	if baselineCheckpoint == nil {
		t.Fatal("baseline checkpoint disappeared")
	}

	// Transition 1: insert. The exact row and values must appear.
	if _, err := src.Exec(ctx, `INSERT INTO accounts (id, owner, balance_cents) VALUES (5, 'vik', 100)`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	waitForRow(ctx, t, dst, 5, "vik", 100, "insert via CDC")

	// Transition 2: update. The updated values must appear.
	if _, err := src.Exec(ctx, `UPDATE accounts SET owner = 'vikas', balance_cents = 250 WHERE id = 5`); err != nil {
		t.Fatalf("update: %v", err)
	}
	waitForRow(ctx, t, dst, 5, "vikas", 250, "update via CDC")

	// Transition 3: delete. The row must disappear, and only that row.
	if _, err := src.Exec(ctx, `DELETE FROM accounts WHERE id = 5`); err != nil {
		t.Fatalf("delete: %v", err)
	}
	waitForRowGone(ctx, t, dst, 5, "delete via CDC")
	// The delete must not have disturbed the backfilled rows.
	waitForRow(ctx, t, dst, 2, "seed-2", 200, "seeded row after delete")

	// Both durable frontiers must advance past the baselines captured before
	// the mutations. Destination row contents alone do not prove the offset was
	// committed atomically with those mutations.
	advancedOffset := waitForCheckpointAdvance(ctx, t, cpStore, jobCfg.JobID, baselineCheckpoint.NextKafkaOffset)
	t.Logf("phase0: durable Kafka checkpoint advanced %d -> %d", baselineCheckpoint.NextKafkaOffset, advancedOffset)
	waitForLSNAdvance(ctx, t, baselineLSN)
	t.Logf("phase0: slot confirmed_flush_lsn advanced from %s", baselineLSN)

	recCancel()
	select {
	case err := <-recErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("reconciler error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reconciler did not stop")
	}

	// Force the capture connection closed so the reader goroutine exits.
	_ = reader.Close()
	select {
	case err := <-capErr:
		if err != nil {
			t.Fatalf("capture reader error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("capture reader did not stop")
	}

	fmt.Println("Phase 0 CDC flow verified")
}

func waitForCheckpointAdvance(ctx context.Context, t *testing.T, store *checkpoint.Store, jobID string, baseline int64) int64 {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	last := baseline
	for {
		cp, err := store.LoadCheckpoint(ctx, jobID)
		if err != nil {
			t.Fatalf("load checkpoint for %s: %v", jobID, err)
		}
		if cp == nil {
			t.Fatalf("checkpoint for %s disappeared", jobID)
		}
		last = cp.NextKafkaOffset
		if last > baseline {
			return last
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for durable Kafka checkpoint: job=%s baseline=%d last=%d", jobID, baseline, last)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// waitForRow polls until the destination row has exactly the wanted values.
// On timeout it reports the last observed state.
func waitForRow(ctx context.Context, t *testing.T, dst *pgx.Conn, id int64, wantOwner string, wantBalance int64, what string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var owner string
	var balance int64
	var lastErr error
	for {
		err := dst.QueryRow(ctx, `SELECT owner, balance_cents FROM accounts WHERE id = $1`, id).Scan(&owner, &balance)
		if err == nil && owner == wantOwner && balance == wantBalance {
			return
		}
		if err == nil {
			lastErr = errors.New("row present with unexpected values")
		} else if !errors.Is(err, pgx.ErrNoRows) {
			lastErr = err
		}
		if time.Now().After(deadline) {
			if errors.Is(lastErr, pgx.ErrNoRows) || lastErr == nil {
				t.Fatalf("timeout waiting for %s: destination row %d not present; want (%q, %d)", what, id, wantOwner, wantBalance)
			}
			t.Fatalf("timeout waiting for %s: destination row %d last observed (%q, %d) want (%q, %d); scan error %v",
				what, id, owner, balance, wantOwner, wantBalance, lastErr)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// waitForRowGone polls until the destination row disappears.
func waitForRowGone(ctx context.Context, t *testing.T, dst *pgx.Conn, id int64, what string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var count int64
		if err := dst.QueryRow(ctx, `SELECT COUNT(*) FROM accounts WHERE id = $1`, id).Scan(&count); err != nil {
			t.Fatalf("count destination row %d: %v", id, err)
		}
		if count == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s: destination row %d still present (count=%d)", what, id, count)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// slotFlushLSN reads the capture slot's durable confirmed flush position.
func slotFlushLSN(ctx context.Context, t *testing.T) (pglogrepl.LSN, error) {
	t.Helper()
	src, err := itest.SourceConn(ctx)
	if err != nil {
		return 0, err
	}
	defer src.Close(context.Background())
	var lsnText string
	if err := src.QueryRow(ctx, `SELECT COALESCE(confirmed_flush_lsn, restart_lsn)::text FROM pg_replication_slots WHERE slot_name = 'seam_itest_slot'`).Scan(&lsnText); err != nil {
		return 0, fmt.Errorf("read slot lsn: %w", err)
	}
	return pglogrepl.ParseLSN(lsnText)
}

// waitSlotLSN returns the current slot flush LSN once the slot exists.
func waitSlotLSN(ctx context.Context, t *testing.T) pglogrepl.LSN {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		lsn, err := slotFlushLSN(ctx, t)
		if err == nil {
			if lsn == 0 {
				t.Fatalf("slot confirmed_flush_lsn is zero before mutation")
			}
			return lsn
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout reading slot lsn: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// waitForLSNAdvance polls until the slot's confirmed flush LSN strictly
// exceeds the baseline, proving the checkpoint advanced past our mutations.
func waitForLSNAdvance(ctx context.Context, t *testing.T, baseline pglogrepl.LSN) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		current, err := slotFlushLSN(ctx, t)
		if err != nil {
			t.Fatalf("read slot lsn while waiting for advance: %v", err)
		}
		if current > baseline {
			t.Logf("phase0: checkpoint advanced %s -> %s", baseline, current)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout: durable checkpoint stayed at %s; expected > %s", current, baseline)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

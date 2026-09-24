//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"example.com/seam/integration/itest"
	"example.com/seam/internal/capture"
	"example.com/seam/internal/checkpoint"
	"example.com/seam/internal/kafka"
	"example.com/seam/internal/marker"
	"example.com/seam/internal/model"
	"example.com/seam/internal/reconcile"
	"example.com/seam/internal/recovery"
	"example.com/seam/internal/scan"
	"example.com/seam/internal/sink"
)

// TestChaos_CrashRestart verifies source == destination after concurrent random
// mutations and repeated reconciler crashes/restarts.
func TestChaos_CrashRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	if err := itest.ResetTables(ctx); err != nil {
		t.Fatalf("reset tables: %v", err)
	}

	src, err := itest.SourceConn(ctx)
	if err != nil {
		t.Fatalf("source conn: %v", err)
	}
	// Seed with a small table so there is data to mutate.
	for i := int64(1); i <= 20; i++ {
		if _, err := src.Exec(ctx,
			`INSERT INTO accounts (id, owner, balance_cents) VALUES ($1, $2, $3)`,
			i, fmt.Sprintf("owner-%d", i), i*100); err != nil {
			t.Fatalf("seed insert: %v", err)
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
		JobID:             itest.JobID("chaos"),
		SourceDSN:         itest.SourceDSN(),
		SourceReplDSN:     itest.SourceReplDSN(),
		SourceSlot:        "seam_itest_slot",
		SourcePublication: "seam_pub",
		DestDSN:           itest.DestDSN(),
		KafkaBrokers:      itest.KafkaBrokers(),
		KafkaTopic:        itest.KafkaTopic(),
		ChunkSize:         7,
		Workers:           1,
		WorkerID:          "chaos-worker",
		LeaseDuration:     10 * time.Second,
		HeartbeatInterval: 2 * time.Second,
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
	// The job is durable state: recovery and the restart loop below expect a
	// sealed chunk manifest. Discovery must run before any reconciler starts,
	// mirroring the production --start-fresh path.
	if err := cpStore.DiscoverAndCreateChunks(ctx, cp.JobID, cp.Attempt, itest.SourceDSN(), chunkReader.Schema(), upperBound, jobCfg.ChunkSize); err != nil {
		t.Fatalf("discover chunks: %v", err)
	}

	// startReconciler starts the reconciler from the given checkpoint and returns
	// a cancel func + error channel.
	startReconciler := func(ctx context.Context, checkpoint *model.Checkpoint) (context.CancelFunc, <-chan error) {
		recCtx, recCancel := context.WithCancel(ctx)
		consumer, err := kafka.NewConsumer(itest.KafkaBrokers(), itest.KafkaTopic(), checkpoint.NextKafkaOffset, func(data []byte) (model.Change, error) {
			var codec capture.JSONCodec
			return codec.Decode(data)
		})
		if err != nil {
			t.Fatalf("create consumer at offset %d: %v", checkpoint.NextKafkaOffset, err)
		}
		rec := reconcile.New(reconcile.Config{
			JobConfig:       jobCfg,
			Checkpoint:      checkpoint,
			Consumer:        consumer,
			CheckpointStore: cpStore,
			ChunkStore:      cpStore,
			MarkerStore:     marker.NewStore(itest.SourceDSN()),
			Scanner:         chunkReader,
			Sink:            mutator,
			SourceSchema:    chunkReader.Schema(),
		})
		recErr := make(chan error, 1)
		go func() {
			if err := rec.Run(recCtx); err != nil && !errors.Is(err, context.Canceled) {
				recErr <- err
			}
			close(recErr)
			consumer.Close()
		}()
		return recCancel, recErr
	}

	// Reconciler restart loop. The bounded channel avoids racing on a channel
	// variable while still letting the writer request a crash of the current
	// instance. A second request waits until the loop accepted the first.
	restartCh := make(chan struct{}, 1)
	recErrCh := make(chan error, 1)
	loopCtx, loopCancel := context.WithCancel(ctx)
	go func() {
		for {
			// Mirror production restart behavior: verify continuity and bump the
			// attempt when a previous chunk was left unfinished.
			result, err := recovery.Recover(loopCtx, cpStore, jobCfg.JobID, jobCfg.SourceDSN)
			if err != nil {
				recErrCh <- fmt.Errorf("recover: %w", err)
				return
			}
			latest := result.Checkpoint
			if latest == nil {
				recErrCh <- fmt.Errorf("checkpoint missing")
				return
			}
			recCtx, recCancel := context.WithCancel(loopCtx)
			cancel, ch := startReconciler(recCtx, latest)
			select {
			case err := <-ch:
				recCancel()
				_ = cancel
				if err != nil && !errors.Is(err, context.Canceled) {
					recErrCh <- err
					return
				}
				if loopCtx.Err() != nil {
					recErrCh <- nil
					return
				}
				// Exited after cancel: restart.
			case <-restartCh:
				recCancel()
				if err := <-ch; err != nil && !errors.Is(err, context.Canceled) {
					recErrCh <- err
					return
				}
				_ = cancel
				// Shutdown drained; restart immediately.
			case <-loopCtx.Done():
				recCancel()
				<-ch
				_ = cancel
				recErrCh <- nil
				return
			}
		}
	}()

	// Chaos: a fixed seed and operation count make failures replayable. Crashes
	// are requested at exact operation boundaries rather than wall-clock times.
	const mutationSeed int64 = 20260924
	const mutationCount = 600
	var maxID int64 = 20
	mutErr := make(chan error, 1)
	go func() {
		rng := rand.New(rand.NewSource(mutationSeed))
		src, err := itest.SourceConn(ctx)
		if err != nil {
			mutErr <- err
			return
		}
		defer src.Close(context.Background())

		for mutations := 0; mutations < mutationCount; mutations++ {
			op := rng.Intn(4)
			switch op {
			case 0: // insert
				maxID++
				if _, err := src.Exec(ctx,
					`INSERT INTO accounts (id, owner, balance_cents) VALUES ($1, $2, $3)`,
					maxID, fmt.Sprintf("chaos-%d", maxID), rng.Int63n(10000)); err != nil {
					mutErr <- fmt.Errorf("insert %d: %w", maxID, err)
					return
				}
			case 1: // update existing
				id := rng.Int63n(maxID) + 1
				if _, err := src.Exec(ctx,
					`UPDATE accounts SET balance_cents = $1 WHERE id = $2`,
					rng.Int63n(10000), id); err != nil {
					mutErr <- fmt.Errorf("update %d: %w", id, err)
					return
				}
			case 2: // delete
				id := rng.Int63n(maxID) + 1
				if _, err := src.Exec(ctx, `DELETE FROM accounts WHERE id = $1`, id); err != nil {
					mutErr <- fmt.Errorf("delete %d: %w", id, err)
					return
				}
			case 3: // update owner
				id := rng.Int63n(maxID) + 1
				if _, err := src.Exec(ctx,
					`UPDATE accounts SET owner = $1 WHERE id = $2`,
					fmt.Sprintf("mutated-%d-%d", id, mutations), id); err != nil {
					mutErr <- fmt.Errorf("update owner %d: %w", id, err)
					return
				}
			}
			done := mutations + 1
			if done == 150 || done == 300 || done == 450 {
				select {
				case restartCh <- struct{}{}:
				case <-ctx.Done():
					mutErr <- ctx.Err()
					return
				}
			}
			if done%25 == 0 {
				time.Sleep(time.Millisecond)
			}
		}
		mutErr <- nil
	}()

	// Wait for the fixed workload, then put a unique marker after it in source
	// commit order. Observing the destination checkpoint beyond this barrier is
	// a precise convergence boundary; Kafka end offset alone could move later.
	if err := <-mutErr; err != nil {
		t.Fatalf("mutation error: %v", err)
	}
	brokerEnd, err := kafka.EndOffset(ctx, itest.KafkaBrokers(), itest.KafkaTopic())
	if err != nil {
		t.Fatalf("read Kafka end before final barrier: %v", err)
	}
	markers := marker.NewStore(itest.SourceDSN())
	barrierID, err := markers.WriteBarrier(ctx, jobCfg.JobID, "chaos-final-barrier")
	if err != nil {
		t.Fatalf("write final source barrier: %v", err)
	}
	barrierCtx, cancelBarrier := context.WithTimeout(ctx, 60*time.Second)
	barrierOffset, err := kafka.WaitForBarrier(barrierCtx, itest.KafkaBrokers(), itest.KafkaTopic(), barrierID, brokerEnd, func(data []byte) (model.Change, error) {
		var codec capture.JSONCodec
		return codec.Decode(data)
	})
	cancelBarrier()
	if err != nil {
		t.Fatalf("wait for final source barrier in Kafka: %v", err)
	}

	// Wait for reconciler to catch up to final source state.
	srcCheck, err := itest.SourceConn(ctx)
	if err != nil {
		t.Fatalf("source check conn: %v", err)
	}
	itest.CloseOnCleanup(t, "source check connection", srcCheck)

	dst, err := itest.DestConn(ctx)
	if err != nil {
		t.Fatalf("dest conn: %v", err)
	}
	itest.CloseOnCleanup(t, "destination connection", dst)

	var sourceCount, destCount int
	converged := false
	catchupDeadline := time.Now().Add(90 * time.Second)
	for !converged && time.Now().Before(catchupDeadline) {
		if err := srcCheck.QueryRow(ctx, `SELECT COUNT(*) FROM accounts`).Scan(&sourceCount); err != nil {
			t.Fatalf("count source: %v", err)
		}
		if err := dst.QueryRow(ctx, `SELECT COUNT(*) FROM accounts`).Scan(&destCount); err != nil {
			t.Fatalf("count dest: %v", err)
		}
		latest, err := cpStore.LoadCheckpoint(ctx, jobCfg.JobID)
		if err != nil {
			t.Fatalf("load checkpoint during catch-up: %v", err)
		}
		if latest != nil && latest.CompletedThrough >= latest.ScanUpperBound && latest.NextKafkaOffset >= barrierOffset && sourceCount == destCount {
			if err := verifyExact(ctx); err == nil {
				converged = true
			}
		}
		if converged {
			converged = true
			break
		}
		select {
		case recErr := <-recErrCh:
			if recErr != nil {
				t.Fatalf("reconciler exited during catch-up: %v", recErr)
			}
			t.Fatal("reconciler loop exited before catch-up")
		case <-time.After(200 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if !converged {
		var completed, nextOffset, applied int64
		_ = dst.QueryRow(ctx, `SELECT completed_through_id, next_kafka_offset FROM seam_checkpoints WHERE job_id = $1`, jobCfg.JobID).Scan(&completed, &nextOffset)
		_ = dst.QueryRow(ctx, `SELECT COUNT(*) FROM seam_applied_txs WHERE job_id = $1`, jobCfg.JobID).Scan(&applied)
		select {
		case recErr := <-recErrCh:
			if recErr != nil {
				t.Fatalf("catch-up failed (reconciler error %q): source=%d dest=%d completed_through=%d next_offset=%d applied_txs=%d",
					recErr, sourceCount, destCount, completed, nextOffset, applied)
			}
		default:
		}
		t.Fatalf("catch-up failed: seed=%d mutations=%d source=%d dest=%d completed_through=%d next_offset=%d barrier_offset=%d applied_txs=%d",
			mutationSeed, mutationCount, sourceCount, destCount, completed, nextOffset, barrierOffset, applied)
	}

	// Stop reconciler loop gracefully.
	loopCancel()
	select {
	case err := <-recErrCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("reconciler error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("reconciler loop did not stop")
	}

	// Final exact verification: every source row matches destination, and counts match.
	if err := verifyExact(ctx); err != nil {
		t.Fatalf("final verification failed: %v", err)
	}

	t.Logf("chaos test passed: seed=%d mutations=%d source rows=%d dest rows=%d barrier=%d", mutationSeed, mutationCount, sourceCount, destCount, barrierOffset)
}

func verifyExact(ctx context.Context) error {
	src, err := itest.SourceConn(ctx)
	if err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	defer src.Close(context.Background())

	dst, err := itest.DestConn(ctx)
	if err != nil {
		return fmt.Errorf("connect dest: %w", err)
	}
	defer dst.Close(context.Background())

	var srcCount, dstCount int
	if err := src.QueryRow(ctx, `SELECT COUNT(*) FROM accounts`).Scan(&srcCount); err != nil {
		return err
	}
	if err := dst.QueryRow(ctx, `SELECT COUNT(*) FROM accounts`).Scan(&dstCount); err != nil {
		return err
	}
	if srcCount != dstCount {
		return fmt.Errorf("row count mismatch: source=%d dest=%d", srcCount, dstCount)
	}

	rows, err := src.Query(ctx, `SELECT id, owner, balance_cents FROM accounts ORDER BY id`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id, bal int64
		var owner string
		if err := rows.Scan(&id, &owner, &bal); err != nil {
			return err
		}
		var dstOwner string
		var dstBal int64
		err := dst.QueryRow(ctx, `SELECT owner, balance_cents FROM accounts WHERE id = $1`, id).Scan(&dstOwner, &dstBal)
		if err != nil {
			return fmt.Errorf("dest missing row %d: %w", id, err)
		}
		if dstOwner != owner || dstBal != bal {
			return fmt.Errorf("row %d mismatch: source=(%s,%d) dest=(%s,%d)", id, owner, bal, dstOwner, dstBal)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Source count matched dest count and every source row matched, so the tables
	// are identical.
	return nil
}

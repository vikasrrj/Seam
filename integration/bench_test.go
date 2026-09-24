//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"example.com/seam/integration/itest"
	"example.com/seam/internal/capture"
	"example.com/seam/internal/checkpoint"
	"example.com/seam/internal/kafka"
	"example.com/seam/internal/marker"
	"example.com/seam/internal/model"
	"example.com/seam/internal/reconcile"
	resourcecontrol "example.com/seam/internal/resource"
	"example.com/seam/internal/scan"
	"example.com/seam/internal/sink"
)

// The backfill benchmark measures source barrier through durable scan
// completion for a fixed 10,000-row workload. Seeding, capture startup, table
// setup, and the final exact-content verification are all outside the timer.
//
// What is inside the timer (one complete backfill cycle):
//   - Kafka end offset, source LOW-marker write, and waiting for the barrier
//   - source upper-bound read
//   - durable job creation and chunk discovery
//   - reconciler run until the durable checkpoint passes the scan upper bound
//
// One worker and four workers are SEPARATE benchmark cases so a runner can
// execute each in a FRESH benchmark process and alternate their order
// (counterbalanced). Run with -benchtime=1x and use
// scripts/bench-counterbalanced.sh to collect and summarize samples.
func BenchmarkBackfillWorkers1(b *testing.B) {
	benchBackfill(b, backfillBenchConfig{workers: 1, chunkSize: 1000, rows: 10000, ownerBytes: 16})
}
func BenchmarkBackfillWorkers4(b *testing.B) {
	benchBackfill(b, backfillBenchConfig{workers: 4, chunkSize: 1000, rows: 10000, ownerBytes: 16})
}

// BenchmarkBackfillConfigured is the fresh-process entry point used by
// scripts/bench-matrix.sh. Environment variables make each matrix cell an
// explicit independent process rather than stateful benchmark subtests.
func BenchmarkBackfillConfigured(b *testing.B) {
	benchBackfill(b, backfillBenchConfig{
		workers:    benchEnvInt(b, "SEAM_BENCH_WORKERS", 1),
		chunkSize:  benchEnvInt(b, "SEAM_BENCH_CHUNK_SIZE", 1000),
		rows:       benchEnvInt(b, "SEAM_BENCH_ROWS", 10000),
		ownerBytes: benchEnvInt(b, "SEAM_BENCH_OWNER_BYTES", 16),
	})
}

type backfillBenchConfig struct {
	workers, chunkSize, rows, ownerBytes int
}

func benchEnvInt(b *testing.B, name string, fallback int) int {
	b.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < 1 {
		b.Fatalf("%s must be a positive integer, got %q", name, value)
	}
	return n
}

func benchBackfill(b *testing.B, bench backfillBenchConfig) {
	// Go's benchmark runner calls StartTimer before invoking the benchmark
	// function (see testing.(*B).runN), so the timer is RUNNING at entry. Stop
	// it before any setup: everything below (reset, seeding, capture startup)
	// must stay outside the measurement.
	b.StopTimer()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Seeding is outside the timer.
	if err := itest.ResetTables(ctx); err != nil {
		b.Fatal(err)
	}
	src, err := itest.SourceConn(ctx)
	if err != nil {
		b.Fatal(err)
	}
	_, err = src.Exec(ctx, `INSERT INTO accounts(id, owner, balance_cents)
		SELECT id, repeat('x', $2) || id::text, id * 100 FROM generate_series(1, $1::bigint) AS g(id)`, bench.rows, bench.ownerBytes)
	src.Close(context.Background())
	if err != nil {
		b.Fatal(err)
	}

	store := checkpoint.NewStore(itest.DestDSN())
	if err := store.EnsureTables(ctx); err != nil {
		b.Fatal(err)
	}
	markers := marker.NewStore(itest.SourceDSN())
	if err := markers.EnsureTable(ctx); err != nil {
		b.Fatal(err)
	}
	systemID, err := capture.SourceSystemID(ctx, itest.SourceDSN())
	if err != nil {
		b.Fatal(err)
	}
	topicID, err := kafka.TopicIdentity(ctx, itest.KafkaBrokers(), itest.KafkaTopic())
	if err != nil {
		b.Fatal(err)
	}
	reader, err := capture.StartReader(ctx, capture.ReaderConfig{
		SQLDSN: itest.SourceDSN(), ReplicationDSN: itest.SourceReplDSN(),
		Slot: "seam_itest_slot", Publication: "seam_pub", KafkaBrokers: itest.KafkaBrokers(),
		KafkaTopic: itest.KafkaTopic(), Generation: "gen:0",
	})
	if err != nil {
		b.Fatal(err)
	}
	readerCtx, stopReader := context.WithCancel(ctx)
	readerDone := make(chan error, 1)
	go func() { readerDone <- reader.Run(readerCtx) }()
	defer func() {
		stopReader()
		reader.Close()
		select {
		case err := <-readerDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				b.Errorf("capture: %v", err)
			}
		case <-time.After(5 * time.Second):
			b.Error("capture did not stop")
		}
	}()
	permitPoll := 10 * time.Millisecond
	scanPermits, err := resourcecontrol.NewPostgresAdvisoryPool(itest.SourceDSN(), resourcecontrol.SourceScanClass, bench.workers, permitPoll)
	if err != nil {
		b.Fatal(err)
	}
	defer scanPermits.Close()
	destPermits, err := resourcecontrol.NewPostgresAdvisoryPool(itest.DestDSN(), resourcecontrol.DestinationTxClass, 8, permitPoll)
	if err != nil {
		b.Fatal(err)
	}
	defer destPermits.Close()

	var total time.Duration
	var discoveryTotal time.Duration
	var reconcileTotal time.Duration
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dst, err := itest.DestConn(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := dst.Exec(ctx, `TRUNCATE accounts, seam_jobs, seam_checkpoints, seam_applied_txs, seam_chunks, seam_candidates, seam_promotions, seam_cutover_gates`); err != nil {
			b.Fatal(err)
		}
		jobID := fmt.Sprintf("bench-%d-%d-%d-%d", bench.workers, bench.chunkSize, time.Now().UnixNano(), i)
		cfg := model.JobConfig{
			JobID: jobID, SourceDSN: itest.SourceDSN(), SourceReplDSN: itest.SourceReplDSN(),
			SourceSlot: "seam_itest_slot", SourcePublication: "seam_pub", SourceSystemID: systemID,
			DestDSN: itest.DestDSN(), DestTable: "accounts", KafkaBrokers: itest.KafkaBrokers(),
			KafkaTopic: itest.KafkaTopic(), KafkaTopicID: topicID, ChunkSize: bench.chunkSize,
			Workers: bench.workers, WorkerID: jobID, LeaseDuration: 30 * time.Second,
			MaxInMemoryCandidates: bench.chunkSize, MaxRecordsPerBatch: 100, HeartbeatInterval: 10 * time.Second,
		}
		b.StartTimer()
		started := time.Now()
		end, err := kafka.EndOffset(ctx, itest.KafkaBrokers(), itest.KafkaTopic())
		if err != nil {
			b.Fatal(err)
		}
		barrierID, err := markers.WriteBarrier(ctx, jobID, "gen:0:attempt:0")
		if err != nil {
			b.Fatal(err)
		}
		start, err := kafka.WaitForBarrier(ctx, itest.KafkaBrokers(), itest.KafkaTopic(), barrierID, end, benchDecode)
		if err != nil {
			b.Fatal(err)
		}
		chunkReader, err := scan.NewChunkReader(ctx, itest.SourceDSN())
		if err != nil {
			b.Fatal(err)
		}
		upper, err := chunkReader.UpperBound(ctx)
		if err != nil {
			b.Fatal(err)
		}
		mutator, err := sink.NewMutatorFor("accounts", chunkReader.Schema())
		if err != nil {
			b.Fatal(err)
		}
		cp, err := store.CreateJobAt(ctx, cfg, chunkReader.Schema(), upper, start)
		if err != nil {
			b.Fatal(err)
		}
		discoveryStarted := time.Now()
		if err := store.DiscoverAndCreateChunks(ctx, jobID, cp.Attempt, itest.SourceDSN(), chunkReader.Schema(), upper, cfg.ChunkSize); err != nil {
			b.Fatal(err)
		}
		discoveryTotal += time.Since(discoveryStarted)
		consumer, err := kafka.NewConsumer(itest.KafkaBrokers(), itest.KafkaTopic(), start, benchDecode, kafka.WithExpectedTopicID(topicID))
		if err != nil {
			b.Fatal(err)
		}
		recCtx, stopRec := context.WithCancel(ctx)
		recDone := make(chan error, 1)
		resources, err := resourcecontrol.New(resourcecontrol.Config{
			MaxSourceScans: bench.workers, MaxDestTx: 8, MaxCDCLag: 10_000,
			PollInterval: time.Second, ScanPermits: scanPermits, DestPermits: destPermits,
			EndOffset: func(callCtx context.Context) (int64, error) {
				return kafka.EndOffset(callCtx, itest.KafkaBrokers(), itest.KafkaTopic())
			},
		}, start)
		if err != nil {
			b.Fatal(err)
		}
		rec := reconcile.New(reconcile.Config{JobConfig: cfg, Checkpoint: cp, Consumer: consumer,
			CheckpointStore: store, ChunkStore: store, MarkerStore: markers,
			Scanner: chunkReader, Sink: mutator, SourceSchema: chunkReader.Schema(), Resources: resources})
		reconcileStarted := time.Now()
		go func() { recDone <- rec.Run(recCtx) }()
		for {
			current, err := store.LoadCheckpoint(ctx, jobID)
			if err != nil {
				b.Fatal(err)
			}
			if current != nil && current.CompletedThrough >= upper {
				break
			}
			select {
			case err := <-recDone:
				b.Fatalf("reconciler exited before completion: %v", err)
			case <-ctx.Done():
				b.Fatal(ctx.Err())
			case <-time.After(50 * time.Millisecond):
			}
		}
		reconcileTotal += time.Since(reconcileStarted)
		total += time.Since(started)
		b.StopTimer()
		stopRec()
		consumer.Close()
		select {
		case err := <-recDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				b.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			b.Fatal("reconciler did not stop")
		}
		dst.Close(context.Background())

		// Exact-content verification OUTSIDE the timer: an ordered merge of the
		// full source and destination tables (count and every row value), not
		// just a row count.
		verifyExactContents(b, ctx)
	}
	b.ReportMetric(float64(bench.rows*b.N)/total.Seconds(), "rows/s")
	b.ReportMetric((float64((bench.ownerBytes+16)*bench.rows*b.N)/(1<<20))/total.Seconds(), "MiB/s")
	b.ReportMetric(float64(discoveryTotal.Milliseconds())/float64(b.N), "discover-ms/op")
	b.ReportMetric(float64(reconcileTotal.Milliseconds())/float64(b.N), "reconcile-ms/op")
}

// verifyExactContents compares source and destination row-for-row in key order
// after the benchmark, outside the timer. It detects missing rows, extra rows,
// duplicates, reordered rows, and changed column values — not just a row count.
func verifyExactContents(b *testing.B, ctx context.Context) {
	b.Helper()
	src, err := itest.SourceConn(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer src.Close(context.Background())
	dst, err := itest.DestConn(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer dst.Close(context.Background())

	srcRows, err := src.Query(ctx, `SELECT id, owner, balance_cents FROM accounts ORDER BY id`)
	if err != nil {
		b.Fatal(err)
	}
	defer srcRows.Close()
	dstRows, err := dst.Query(ctx, `SELECT id, owner, balance_cents FROM accounts ORDER BY id`)
	if err != nil {
		b.Fatal(err)
	}
	defer dstRows.Close()

	type accountRow struct {
		id      int64
		owner   string
		balance int64
	}
	var matched int
	for {
		srcNext, dstNext := srcRows.Next(), dstRows.Next()
		if srcNext != dstNext {
			b.Fatalf("row count mismatch after %d matched rows (source exhausted=%v destination exhausted=%v): duplicates or missing rows",
				matched, !srcNext, !dstNext)
		}
		if !srcNext {
			break
		}
		var s, d accountRow
		if err := srcRows.Scan(&s.id, &s.owner, &s.balance); err != nil {
			b.Fatalf("scan source row at position %d: %v", matched, err)
		}
		if err := dstRows.Scan(&d.id, &d.owner, &d.balance); err != nil {
			b.Fatalf("scan destination row at position %d: %v", matched, err)
		}
		if s != d {
			b.Fatalf("row at position %d differs: source (%d,%q,%d) destination (%d,%q,%d)",
				matched, s.id, s.owner, s.balance, d.id, d.owner, d.balance)
		}
		matched++
	}
	if err := srcRows.Err(); err != nil {
		b.Fatalf("source scan: %v", err)
	}
	if err := dstRows.Err(); err != nil {
		b.Fatalf("destination scan: %v", err)
	}
}

func benchDecode(data []byte) (model.Change, error) {
	var codec capture.JSONCodec
	return codec.Decode(data)
}

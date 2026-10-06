//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
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

// BenchmarkCDCTransaction measures one committed source transaction from the
// application write through its durable destination checkpoint. Setup,
// backfill completion, and exact-content verification are outside the timer.
func BenchmarkCDCTransactionWorkers1(b *testing.B) { benchCDCTransaction(b, 1, 1000) }
func BenchmarkCDCTransactionWorkers4(b *testing.B) { benchCDCTransaction(b, 4, 1000) }

func benchCDCTransaction(b *testing.B, workers, rowsPerTransaction int) {
	b.StopTimer()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if err := itest.ResetTables(ctx); err != nil {
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
		_ = reader.Close()
		select {
		case runErr := <-readerDone:
			if runErr != nil && !errors.Is(runErr, context.Canceled) {
				b.Errorf("capture: %v", runErr)
			}
		case <-time.After(5 * time.Second):
			b.Error("capture did not stop")
		}
	}()

	endBeforeBarrier, err := kafka.EndOffset(ctx, itest.KafkaBrokers(), itest.KafkaTopic())
	if err != nil {
		b.Fatal(err)
	}
	barrierID, err := markers.WriteBarrier(ctx, "cdc-benchmark", "gen:0:attempt:0")
	if err != nil {
		b.Fatal(err)
	}
	startOffset, err := kafka.WaitForBarrier(ctx, itest.KafkaBrokers(), itest.KafkaTopic(), barrierID, endBeforeBarrier, benchDecode)
	if err != nil {
		b.Fatal(err)
	}
	chunkReader, err := scan.NewChunkReader(ctx, itest.SourceDSN())
	if err != nil {
		b.Fatal(err)
	}
	mutator, err := sink.NewMutatorFor("accounts", chunkReader.Schema())
	if err != nil {
		b.Fatal(err)
	}
	jobID := fmt.Sprintf("cdc-bench-%d-%d", workers, time.Now().UnixNano())
	cfg := model.JobConfig{
		JobID: jobID, SourceDSN: itest.SourceDSN(), SourceReplDSN: itest.SourceReplDSN(),
		SourceSlot: "seam_itest_slot", SourcePublication: "seam_pub", SourceSystemID: systemID,
		DestDSN: itest.DestDSN(), DestTable: "accounts", KafkaBrokers: itest.KafkaBrokers(),
		KafkaTopic: itest.KafkaTopic(), KafkaTopicID: topicID, ChunkSize: 2500, Workers: workers,
		WorkerID: jobID, LeaseDuration: 30 * time.Second, MaxInMemoryCandidates: 2500,
		MaxRecordsPerBatch: 100, HeartbeatInterval: 10 * time.Second,
	}
	cp, err := store.CreateJobAt(ctx, cfg, chunkReader.Schema(), 0, startOffset)
	if err != nil {
		b.Fatal(err)
	}
	if err := store.DiscoverAndCreateChunks(ctx, jobID, cp.Attempt, itest.SourceDSN(), chunkReader.Schema(), 0, cfg.ChunkSize); err != nil {
		b.Fatal(err)
	}
	consumer, err := kafka.NewConsumer(itest.KafkaBrokers(), itest.KafkaTopic(), startOffset, benchDecode, kafka.WithExpectedTopicID(topicID))
	if err != nil {
		b.Fatal(err)
	}
	defer consumer.Close()
	resources, err := resourcecontrol.New(resourcecontrol.Config{
		MaxSourceScans: workers, MaxDestTx: 8, MaxCDCLag: 10_000, PollInterval: time.Second,
		EndOffset: func(callCtx context.Context) (int64, error) {
			return kafka.EndOffset(callCtx, itest.KafkaBrokers(), itest.KafkaTopic())
		},
	}, startOffset)
	if err != nil {
		b.Fatal(err)
	}

	var profile *benchProfile
	recCfg := reconcile.Config{
		JobConfig: cfg, Checkpoint: cp, Consumer: consumer,
		CheckpointStore: store, ChunkStore: store, MarkerStore: markers,
		Scanner: chunkReader, Sink: mutator, SourceSchema: chunkReader.Schema(), Resources: resources,
	}
	if os.Getenv("SEAM_BENCH_PROFILE") == "1" {
		profile = newBenchProfile()
		profiledStore := &profiledStore{base: store, profile: profile}
		recCfg.Consumer = &profiledConsumer{base: consumer, profile: profile}
		recCfg.CheckpointStore = profiledStore
		recCfg.ChunkStore = profiledStore
		recCfg.Sink = &profiledSink{base: mutator, profile: profile}
		recCfg.Resources = &profiledResources{base: resources, profile: profile}
	}
	rec := reconcile.New(recCfg)
	recCtx, stopRec := context.WithCancel(ctx)
	recDone := make(chan error, 1)
	go func() { recDone <- rec.Run(recCtx) }()
	defer func() {
		stopRec()
		consumer.Close()
		select {
		case runErr := <-recDone:
			if runErr != nil && !errors.Is(runErr, context.Canceled) {
				b.Errorf("reconciler: %v", runErr)
			}
		case <-time.After(5 * time.Second):
			b.Error("reconciler did not stop")
		}
	}()

	// Wait until the empty historical window and both of its marker
	// transactions are durably applied. Otherwise an advancing setup marker can
	// be mistaken for the measured CDC transaction.
	waitForCDCReady(b, ctx, store, jobID)
	src, err := itest.SourceConn(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer src.Close(context.Background())
	baselineEnd, err := kafka.EndOffset(ctx, itest.KafkaBrokers(), itest.KafkaTopic())
	if err != nil {
		b.Fatal(err)
	}
	observer, err := kafka.NewConsumer(itest.KafkaBrokers(), itest.KafkaTopic(), baselineEnd, benchDecode, kafka.WithExpectedTopicID(topicID))
	if err != nil {
		b.Fatal(err)
	}
	defer observer.Close()
	var sourceBaselineWALLSN, destinationBaselineWALLSN string
	if profile != nil {
		if sourceBaselineWALLSN, err = resetPostgresProfile(ctx, itest.SourceDSN()); err != nil {
			b.Fatal(err)
		}
		if destinationBaselineWALLSN, err = resetPostgresProfile(ctx, itest.DestDSN()); err != nil {
			b.Fatal(err)
		}
		profile.reset()
	}

	var appWrite, captureToKafka, kafkaToApply, endToEnd time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		observed := make(chan struct {
			finalOffset int64
			err         error
		}, 1)
		go func() {
			for {
				transactions, pollErr := observer.PollTransactions(ctx)
				if pollErr != nil {
					observed <- struct {
						finalOffset int64
						err         error
					}{err: pollErr}
					return
				}
				if len(transactions) == 0 {
					continue
				}
				finalOffset := transactions[len(transactions)-1].FinalOffset
				for _, transaction := range transactions {
					transaction.Close()
				}
				observed <- struct {
					finalOffset int64
					err         error
				}{finalOffset: finalOffset}
				return
			}
		}()
		firstID := int64(iteration*rowsPerTransaction + 1)
		lastID := firstID + int64(rowsPerTransaction) - 1
		started := time.Now()
		if _, err := src.Exec(ctx, `INSERT INTO accounts(id, owner, balance_cents)
			SELECT id, repeat('x', 16) || id::text, id * 100
			FROM generate_series($1::bigint, $2::bigint) AS g(id)`, firstID, lastID); err != nil {
			b.Fatal(err)
		}
		committed := time.Now()
		appWrite += committed.Sub(started)
		observation := <-observed
		if observation.err != nil {
			b.Fatal(observation.err)
		}
		capturedEnd := observation.finalOffset + 1
		captured := time.Now()
		captureToKafka += captured.Sub(committed)
		waitForCheckpointOffset(b, ctx, store, jobID, capturedEnd)
		applied := time.Now()
		kafkaToApply += applied.Sub(captured)
		endToEnd += applied.Sub(started)
	}
	b.StopTimer()
	b.ReportMetric(float64(appWrite.Microseconds())/1000/float64(b.N), "app-write-ms/op")
	b.ReportMetric(float64(captureToKafka.Microseconds())/1000/float64(b.N), "capture-to-kafka-ms/op")
	b.ReportMetric(float64(kafkaToApply.Microseconds())/1000/float64(b.N), "kafka-to-apply-ms/op")
	b.ReportMetric(float64(endToEnd.Microseconds())/1000/float64(b.N), "end-to-end-ms/op")
	b.ReportMetric(float64(rowsPerTransaction*b.N)/endToEnd.Seconds(), "rows/s")
	if profile != nil {
		profile.report(b.N, endToEnd)
		sourceStats, readErr := readPostgresProfile(ctx, itest.SourceDSN(), sourceBaselineWALLSN)
		if readErr != nil {
			b.Fatal(readErr)
		}
		destinationStats, readErr := readPostgresProfile(ctx, itest.DestDSN(), destinationBaselineWALLSN)
		if readErr != nil {
			b.Fatal(readErr)
		}
		printPostgresProfile("source-cdc", sourceStats)
		printPostgresProfile("destination-cdc", destinationStats)
	}

	dst, err := itest.DestConn(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer dst.Close(context.Background())
	var count int
	if err := dst.QueryRow(ctx, `SELECT count(*) FROM accounts`).Scan(&count); err != nil {
		b.Fatal(err)
	}
	if want := rowsPerTransaction * b.N; count != want {
		b.Fatalf("destination rows = %d, want %d", count, want)
	}
}

func waitForCheckpointOffset(b *testing.B, ctx context.Context, store *checkpoint.Store, jobID string, target int64) {
	b.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		cp, err := store.LoadCheckpoint(ctx, jobID)
		if err != nil {
			b.Fatal(err)
		}
		if cp != nil && cp.NextKafkaOffset >= target {
			return
		}
		if time.Now().After(deadline) {
			b.Fatalf("checkpoint did not reach Kafka offset %d", target)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForCDCReady(b *testing.B, ctx context.Context, store *checkpoint.Store, jobID string) {
	b.Helper()
	deadline := time.Now().Add(30 * time.Second)
	stable := 0
	for {
		end, err := kafka.EndOffset(ctx, itest.KafkaBrokers(), itest.KafkaTopic())
		if err != nil {
			b.Fatal(err)
		}
		cp, err := store.LoadCheckpoint(ctx, jobID)
		if err != nil {
			b.Fatal(err)
		}
		if cp != nil && cp.CompletedThrough >= 0 && cp.NextKafkaOffset >= end {
			stable++
			if stable == 3 {
				return
			}
		} else {
			stable = 0
		}
		if time.Now().After(deadline) {
			b.Fatal("reconciler did not settle into CDC-only mode")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

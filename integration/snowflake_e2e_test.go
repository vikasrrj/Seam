//go:build integration && snowflake_integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"example.com/seam/integration/itest"
	"example.com/seam/internal/capture"
	"example.com/seam/internal/kafka"
	"example.com/seam/internal/marker"
	"example.com/seam/internal/model"
	"example.com/seam/internal/scan"
	"example.com/seam/internal/schema"
	seamsnowflake "example.com/seam/internal/snowflake"
	"example.com/seam/internal/snowpromotion"
	"example.com/seam/internal/snowreconcile"
	_ "github.com/snowflakedb/gosnowflake/v2"
)

// TestSnowflakeOnlineBackfillCrashRecovery proves the complete path rather
// than isolated SQL helpers: PostgreSQL logical decoding -> Kafka -> fenced
// Snowflake sink, concurrent online backfill, sink takeover, worker restart,
// stable-snapshot validation, idempotent view promotion, and exact row match.
func TestSnowflakeOnlineBackfillCrashRecovery(t *testing.T) {
	dsn := os.Getenv("SNOWFLAKE_DSN")
	database := os.Getenv("SNOWFLAKE_DATABASE")
	if dsn == "" || database == "" {
		t.Skip("SNOWFLAKE_DSN and SNOWFLAKE_DATABASE are required")
	}
	if !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*$`).MatchString(database) {
		t.Fatalf("invalid SNOWFLAKE_DATABASE identifier %q", database)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	if err := itest.ResetTables(ctx); err != nil {
		t.Fatal(err)
	}
	source, err := itest.SourceConn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	itest.CloseOnCleanup(t, "Snowflake E2E source", source)
	if _, err := source.Exec(ctx, `INSERT INTO accounts(id, owner, balance_cents)
		SELECT id, 'seed-' || id::text, id * 100 FROM generate_series(1, 500) AS g(id)`); err != nil {
		t.Fatal(err)
	}
	descriptor, err := schema.LoadFromConn(ctx, source, "public", "accounts")
	if err != nil {
		t.Fatal(err)
	}

	reader, err := capture.StartReader(ctx, capture.ReaderConfig{
		SQLDSN: itest.SourceDSN(), ReplicationDSN: itest.SourceReplDSN(),
		Slot: "seam_itest_slot", Publication: "seam_pub", Table: "accounts",
		KafkaBrokers: itest.KafkaBrokers(), KafkaTopic: itest.KafkaTopic(), Generation: "gen:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	readerCtx, stopReader := context.WithCancel(ctx)
	readerDone := make(chan error, 1)
	go func() { readerDone <- reader.Run(readerCtx) }()
	t.Cleanup(func() {
		stopReader()
		_ = reader.Close()
		select {
		case err := <-readerDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("capture reader: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("capture reader did not stop")
		}
	})

	topicID, err := kafka.TopicIdentity(ctx, itest.KafkaBrokers(), itest.KafkaTopic())
	if err != nil {
		t.Fatal(err)
	}
	earliest, err := kafka.EarliestOffset(ctx, itest.KafkaBrokers(), itest.KafkaTopic())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	suffix := fmt.Sprintf("%X", time.Now().UnixNano())
	snowCfg := seamsnowflake.Config{
		Database: database, Schema: "SEAM_E2E_" + suffix,
		InternalSchema: "SEAM_E2E_INTERNAL_" + suffix,
		LiveTable:      "ACCOUNTS", StreamID: "e2e-" + suffix,
		TopicID: topicID, Partition: 0,
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		for _, schemaName := range []string{snowCfg.Schema, snowCfg.InternalSchema} {
			statement := `DROP SCHEMA IF EXISTS "` + strings.ToUpper(database) + `"."` + strings.ToUpper(schemaName) + `" CASCADE`
			if _, err := db.ExecContext(cleanupCtx, statement); err != nil {
				t.Errorf("drop Snowflake E2E schema %s: %v", schemaName, err)
			}
		}
	})
	store, err := seamsnowflake.NewStore(db, snowCfg, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureObjects(ctx, earliest); err != nil {
		t.Fatal(err)
	}

	const sinkLease = 30 * time.Second
	const chunkLease = 30 * time.Second
	sink1, err := startTestSnowflakeSink(ctx, store, topicID, "sink-before-crash", sinkLease)
	if err != nil {
		t.Fatal(err)
	}
	markers := marker.NewStore(itest.SourceDSN())
	if err := markers.EnsureTable(ctx); err != nil {
		t.Fatal(err)
	}
	scanner, err := scan.NewChunkReader(ctx, itest.SourceDSN())
	if err != nil {
		t.Fatal(err)
	}
	if err := scanner.SetLimits(1000, 32<<20); err != nil {
		t.Fatal(err)
	}

	jobID := "snowflake-e2e-" + suffix
	attempt := "attempt-1"
	coordinatorCfg := snowreconcile.Config{
		JobID: jobID, Attempt: attempt, ShadowTable: "ACCOUNTS_SHADOW",
		ChunkSize: 50, Workers: 1, WorkerID: "worker-before-crash",
		// Snowflake control-plane calls can take several seconds. Keep the
		// backfill lease long enough that a healthy heartbeat is not fenced by
		// ordinary warehouse latency. Thirty seconds still keeps intentional
		// takeover and recovery bounded inside the test timeout.
		Lease: chunkLease, Heartbeat: 5 * time.Second, Poll: 100 * time.Millisecond,
	}
	coordinator, err := snowreconcile.New(coordinatorCfg, store, scanner, markers)
	if err != nil {
		t.Fatal(err)
	}
	firstCoordinatorCtx, stopFirstCoordinator := context.WithCancel(ctx)
	firstCoordinatorDone := make(chan error, 1)
	go func() { firstCoordinatorDone <- coordinator.Run(firstCoordinatorCtx) }()
	waitSnowflakeJobState(ctx, t, store, jobID, seamsnowflake.BackfillRunning)

	// Changes happen after the shadow route is active. Some are consumed before
	// the crash and some accumulate in Kafka while no sink owns the lease.
	if _, err := source.Exec(ctx, `UPDATE accounts SET owner = 'updated-before-crash', balance_cents = 777 WHERE id BETWEEN 1 AND 40`); err != nil {
		t.Fatal(err)
	}
	sink1.crash()
	if err := sink1.wait(); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(ctx, `DELETE FROM accounts WHERE id BETWEEN 41 AND 60`); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(ctx, `INSERT INTO accounts(id, owner, balance_cents)
		SELECT id, 'inserted-while-sink-down', id * 10 FROM generate_series(1001, 1040) AS g(id)`); err != nil {
		t.Fatal(err)
	}

	// A crashed owner is not released. Takeover is possible only after expiry,
	// and the new epoch fences any old apply transaction that wakes up later.
	if err := waitForSinkLeaseExpiry(ctx, store, "sink-after-crash", sinkLease); err != nil {
		t.Fatal(err)
	}
	sink2, err := startTestSnowflakeSink(ctx, store, topicID, "sink-after-crash", sinkLease)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sink2.stopAndRelease(store); err != nil {
			t.Errorf("second Snowflake sink: %v", err)
		}
	})

	waitForCompletedChunk(ctx, t, store, jobID)
	stopFirstCoordinator()
	if err := <-firstCoordinatorDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("first coordinator: %v", err)
	}
	coordinatorCfg.Workers = 4
	coordinatorCfg.WorkerID = "worker-after-crash"
	coordinator, err = snowreconcile.New(coordinatorCfg, store, scanner, markers)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Run(ctx); err != nil {
		t.Fatal(err)
	}

	promotion, err := snowpromotion.New(snowpromotion.Config{
		SourceDSN: itest.SourceDSN(), JobID: jobID, Attempt: attempt,
		ValidationTable: "ACCOUNTS_VALIDATION", KafkaBrokers: itest.KafkaBrokers(),
		KafkaTopic: itest.KafkaTopic(), KafkaTopicID: topicID,
		PollInterval: 100 * time.Millisecond, MaxWritePause: 45 * time.Second,
		LockTimeout: 5 * time.Second, MaxDeltaKeys: 10_000, MaxPollRecords: 100,
	}, store, markers, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if err := promotion.Validate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(ctx, `UPDATE accounts SET owner = 'changed-after-validation', balance_cents = 999 WHERE id BETWEEN 61 AND 80`); err != nil {
		t.Fatal(err)
	}
	if err := promotion.Promote(ctx); err != nil {
		t.Fatal(err)
	}
	if err := promotion.Promote(ctx); err != nil {
		t.Fatalf("idempotent promotion retry: %v", err)
	}

	sourceRows, err := scanner.ReadChunk(ctx, math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	sourceRows, err = seamsnowflake.NormalizeSourceRows(descriptor, sourceRows)
	if err != nil {
		t.Fatal(err)
	}
	destinationRows, err := store.ReadRows(ctx, snowCfg.LiveTable, math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	assertExactRows(t, descriptor, sourceRows, destinationRows)
	t.Logf("exact Snowflake match after sink and worker recovery: %d rows", len(sourceRows))
}

type runningSnowflakeSink struct {
	cancel context.CancelCauseFunc
	done   chan error
	lease  *seamsnowflake.SinkLease
}

func startTestSnowflakeSink(parent context.Context, store *seamsnowflake.Store, topicID, owner string, duration time.Duration) (*runningSnowflakeSink, error) {
	lease, err := store.AcquireSinkLease(parent, owner, duration)
	if err != nil {
		return nil, err
	}
	next, err := store.NextOffset(parent)
	if err != nil {
		return nil, err
	}
	consumer, err := kafka.NewConsumer(itest.KafkaBrokers(), itest.KafkaTopic(), next, snowflakeDecode, kafka.WithExpectedTopicID(topicID), kafka.WithMaxPollRecords(100))
	if err != nil {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = store.ReleaseSinkLease(releaseCtx, lease)
		return nil, err
	}
	ctx, cancel := context.WithCancelCause(parent)
	running := &runningSnowflakeSink{cancel: cancel, done: make(chan error, 1), lease: lease}
	go func() {
		renewDone := make(chan struct{})
		go func() {
			defer close(renewDone)
			ticker := time.NewTicker(duration / 3)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err := store.RenewSinkLease(ctx, lease, duration); err != nil {
						cancel(err)
						return
					}
				}
			}
		}()
		defer func() {
			consumer.Close()
			cancel(context.Canceled)
			<-renewDone
		}()
		for {
			transactions, err := consumer.PollTransactions(ctx)
			if err != nil {
				if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
					running.done <- cause
				} else {
					running.done <- nil
				}
				return
			}
			for index, transaction := range transactions {
				err := store.ApplyTransaction(ctx, lease, transaction)
				transaction.Close()
				if err != nil {
					for _, remainder := range transactions[index+1:] {
						remainder.Close()
					}
					running.done <- err
					return
				}
			}
		}
	}()
	return running, nil
}

func (sink *runningSnowflakeSink) crash()      { sink.cancel(context.Canceled) }
func (sink *runningSnowflakeSink) wait() error { return <-sink.done }

func (sink *runningSnowflakeSink) stopAndRelease(store *seamsnowflake.Store) error {
	sink.cancel(context.Canceled)
	runErr := sink.wait()
	if errors.Is(runErr, context.Canceled) {
		runErr = nil
	}
	releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	releaseErr := store.ReleaseSinkLease(releaseCtx, sink.lease)
	return errors.Join(runErr, releaseErr)
}

func waitForSinkLeaseExpiry(ctx context.Context, store *seamsnowflake.Store, owner string, duration time.Duration) error {
	deadline := time.Now().Add(2 * duration)
	for {
		lease, err := store.AcquireSinkLease(ctx, owner, duration)
		if err == nil {
			return store.ReleaseSinkLease(ctx, lease)
		}
		var held *seamsnowflake.LeaseHeldError
		if !errors.As(err, &held) {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("sink lease did not expire: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func waitSnowflakeJobState(ctx context.Context, t *testing.T, store *seamsnowflake.Store, jobID string, state seamsnowflake.BackfillState) {
	t.Helper()
	for {
		job, err := store.LoadBackfill(ctx, jobID)
		if err == nil && job.State == state {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for job %s: last job=%+v err=%v", state, job, err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func waitForCompletedChunk(ctx context.Context, t *testing.T, store *seamsnowflake.Store, jobID string) {
	t.Helper()
	for {
		job, err := store.LoadBackfill(ctx, jobID)
		if err == nil && job.CompletedChunks > 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for completed chunk: job=%+v err=%v", job, err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func assertExactRows(t *testing.T, descriptor *schema.Schema, source, destination []model.Row) {
	t.Helper()
	if len(source) != len(destination) {
		t.Fatalf("row count mismatch: source=%d Snowflake=%d", len(source), len(destination))
	}
	for rowIndex := range source {
		for columnIndex := range source[rowIndex].Values {
			if !source[rowIndex].Values[columnIndex].Equal(destination[rowIndex].Values[columnIndex]) {
				t.Fatalf("row %d column %s mismatch: source=%+v Snowflake=%+v", rowIndex, descriptor.Columns[columnIndex].Name, source[rowIndex].Values[columnIndex], destination[rowIndex].Values[columnIndex])
			}
		}
	}
}

func snowflakeDecode(data []byte) (model.Change, error) {
	var codec capture.JSONCodec
	return codec.Decode(data)
}

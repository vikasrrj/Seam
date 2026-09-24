// Package promotion performs a fenced PostgreSQL shadow-table cutover.
package promotion

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"example.com/seam/internal/capture"
	"example.com/seam/internal/checkpoint"
	"example.com/seam/internal/kafka"
	"example.com/seam/internal/marker"
	"example.com/seam/internal/model"
	"example.com/seam/internal/schema"
	"example.com/seam/internal/transport"
	"github.com/jackc/pgx/v5"
)

type Config struct {
	SourceDSN, DestDSN     string
	KafkaBrokers           []string
	KafkaTopic             string
	LiveJobID, ShadowJobID string
	// MaxWritePause independently bounds each period after a source table lock:
	// validation-snapshot capture, and final barrier catch-up/validation/swap.
	// Zero inherits the caller's deadline.
	MaxWritePause time.Duration
	// MaxDeltaKeys bounds the changed-key validation set between the stable
	// validation snapshot and final cutover. Exceeding it aborts safely.
	MaxDeltaKeys int
}

type Result struct {
	BarrierOffset         int64
	RetiredTable          string
	AlreadyPromoted       bool
	FenceDuration         time.Duration
	SnapshotFenceDuration time.Duration
}

var errGatePassed = errors.New("job passed cutover gate")

// Promote holds source account writes after a captured barrier, waits for
// both independent destination frontiers, proves exact shadow equality, and
// swaps physical table names in one PostgreSQL transaction. A timeout aborts
// without changing the public table. This is a short write pause only when
// catch-up and equality validation fit the caller's deadline.
func Promote(ctx context.Context, cfg Config) (Result, error) {
	if cfg.LiveJobID == "" || cfg.ShadowJobID == "" || cfg.LiveJobID == cfg.ShadowJobID {
		return Result{}, fmt.Errorf("distinct live and shadow job IDs are required")
	}
	store := checkpoint.NewStore(cfg.DestDSN)
	if err := store.EnsureTables(ctx); err != nil {
		return Result{}, err
	}
	if prior, ok, err := priorPromotion(ctx, cfg.DestDSN, cfg.LiveJobID, cfg.ShadowJobID); err != nil {
		return Result{}, err
	} else if ok {
		return prior, nil
	}
	live, err := store.LoadJobRecord(ctx, cfg.LiveJobID)
	if err != nil {
		return Result{}, err
	}
	shadow, err := store.LoadJobRecord(ctx, cfg.ShadowJobID)
	if err != nil {
		return Result{}, err
	}
	if live.Config.DestTable != "accounts" || shadow.Config.DestTable != "accounts_shadow" {
		return Result{}, fmt.Errorf("promotion requires live accounts and shadow accounts_shadow jobs")
	}
	if live.SourceSchema == nil || shadow.SourceSchema == nil {
		return Result{}, fmt.Errorf("live and shadow jobs must record a durable source schema descriptor")
	}
	if live.SourceSchema.Fingerprint != shadow.SourceSchema.Fingerprint ||
		(live.SourceSchemaFingerprint != "" && live.SourceSchemaFingerprint != live.SourceSchema.Fingerprint) ||
		(shadow.SourceSchemaFingerprint != "" && shadow.SourceSchemaFingerprint != shadow.SourceSchema.Fingerprint) {
		return Result{}, fmt.Errorf("live and shadow jobs pin different source schema epochs; promotion refuses schema drift")
	}
	if live.SourceDSNHash == "" || live.SourceDSNHash != checkpoint.DSNFingerprint(cfg.SourceDSN) || shadow.SourceDSNHash != live.SourceDSNHash {
		return Result{}, fmt.Errorf("live and shadow source connection identities differ")
	}
	if live.Config.SourceSystemID == "" || live.Config.SourceSystemID != shadow.Config.SourceSystemID {
		return Result{}, fmt.Errorf("live and shadow source system identities differ")
	}
	systemID, err := capture.SourceSystemID(ctx, cfg.SourceDSN)
	if err != nil {
		return Result{}, err
	}
	if systemID != live.Config.SourceSystemID {
		return Result{}, fmt.Errorf("source system identifier changed")
	}
	if live.Config.KafkaTopic != cfg.KafkaTopic || shadow.Config.KafkaTopic != cfg.KafkaTopic ||
		live.Config.KafkaTopicID == "" || live.Config.KafkaTopicID != shadow.Config.KafkaTopicID {
		return Result{}, fmt.Errorf("live and shadow Kafka stream identities differ")
	}
	topicID, err := kafka.TopicIdentity(ctx, cfg.KafkaBrokers, cfg.KafkaTopic)
	if err != nil {
		return Result{}, err
	}
	if topicID != live.Config.KafkaTopicID {
		return Result{}, fmt.Errorf("Kafka topic identity changed")
	}
	preflight, err := store.Conn(ctx)
	if err != nil {
		return Result{}, err
	}
	preflightErr := checkSwapContract(ctx, preflight, live.SourceSchema, live.SchemaFingerprint, shadow.SchemaFingerprint)
	preflight.Close(context.Background())
	if preflightErr != nil {
		return Result{}, preflightErr
	}
	sealed, err := store.DiscoveryComplete(ctx, cfg.ShadowJobID)
	if err != nil || !sealed {
		return Result{}, fmt.Errorf("shadow discovery is not sealed: %w", err)
	}
	// Drain both generations to a fixed broker frontier before blocking source
	// writes. Continuous traffic may append after this sample, but the remaining
	// fenced work is then only that bounded suffix plus the barrier itself.
	preFenceEnd, err := kafka.EndOffset(ctx, cfg.KafkaBrokers, cfg.KafkaTopic)
	if err != nil {
		return Result{}, err
	}
	earliest, err := kafka.EarliestOffset(ctx, cfg.KafkaBrokers, cfg.KafkaTopic)
	if err != nil {
		return Result{}, err
	}
	if err := waitForFrontiers(ctx, store, cfg, preFenceEnd, earliest); err != nil {
		return Result{}, fmt.Errorf("pre-fence catch-up: %w", err)
	}
	jobs := []string{cfg.LiveJobID, cfg.ShadowJobID}
	if _, err := convergeExactPrefix(ctx, store, cfg); err != nil {
		return Result{}, fmt.Errorf("establish exact pre-fence prefix: %w", err)
	}
	gated := true
	defer func() {
		if gated {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = store.ClearCutoverGates(cleanupCtx, jobs...)
		}
	}()
	if cfg.MaxDeltaKeys <= 0 {
		cfg.MaxDeltaKeys = 100_000
	}
	markers := marker.NewStore(cfg.SourceDSN)
	if err := markers.EnsureTable(ctx); err != nil {
		return Result{}, err
	}

	// First short fence: establish an exact stream prefix and an MVCC source
	// snapshot at that prefix. Once the snapshot is established, source writes
	// resume while the expensive full comparison runs.
	snapshotFenceConn, err := transport.ConnectPostgres(ctx, cfg.SourceDSN)
	if err != nil {
		return Result{}, err
	}
	snapshotFenceTx, err := snapshotFenceConn.Begin(ctx)
	if err != nil {
		snapshotFenceConn.Close(context.Background())
		return Result{}, err
	}
	if _, err := snapshotFenceTx.Exec(ctx, `SET LOCAL lock_timeout = '5s'`); err != nil {
		snapshotFenceTx.Rollback(context.Background())
		snapshotFenceConn.Close(context.Background())
		return Result{}, err
	}
	if _, err := snapshotFenceTx.Exec(ctx, `LOCK TABLE public.accounts IN SHARE MODE`); err != nil {
		snapshotFenceTx.Rollback(context.Background())
		snapshotFenceConn.Close(context.Background())
		return Result{}, fmt.Errorf("acquire source validation-snapshot fence: %w", err)
	}
	snapshotFenceStart := time.Now()
	snapshotFenceCtx := ctx
	cancelSnapshotFence := func() {}
	if cfg.MaxWritePause > 0 {
		snapshotFenceCtx, cancelSnapshotFence = context.WithTimeout(ctx, cfg.MaxWritePause)
	}
	defer cancelSnapshotFence()
	snapshotBrokerEnd, err := kafka.EndOffset(snapshotFenceCtx, cfg.KafkaBrokers, cfg.KafkaTopic)
	if err != nil {
		snapshotFenceTx.Rollback(context.Background())
		snapshotFenceConn.Close(context.Background())
		return Result{}, err
	}
	snapshotMarker, err := markers.WriteBarrier(snapshotFenceCtx, cfg.ShadowJobID, fmt.Sprintf("validation:%d", time.Now().UnixNano()))
	if err != nil {
		snapshotFenceTx.Rollback(context.Background())
		snapshotFenceConn.Close(context.Background())
		return Result{}, err
	}
	validationPrefix, err := kafka.WaitForBarrier(snapshotFenceCtx, cfg.KafkaBrokers, cfg.KafkaTopic, snapshotMarker, snapshotBrokerEnd, decodeChange)
	if err != nil {
		snapshotFenceTx.Rollback(context.Background())
		snapshotFenceConn.Close(context.Background())
		return Result{}, err
	}
	if err := store.SetCutoverGates(snapshotFenceCtx, jobs, validationPrefix); err != nil {
		snapshotFenceTx.Rollback(context.Background())
		snapshotFenceConn.Close(context.Background())
		return Result{}, err
	}
	if err := waitForExactFrontiers(snapshotFenceCtx, store, cfg, validationPrefix, earliest); err != nil {
		snapshotFenceTx.Rollback(context.Background())
		snapshotFenceConn.Close(context.Background())
		return Result{}, fmt.Errorf("wait for validation prefix %d: %w", validationPrefix, err)
	}
	snapshotConn, err := transport.ConnectPostgres(snapshotFenceCtx, cfg.SourceDSN)
	if err != nil {
		snapshotFenceTx.Rollback(context.Background())
		snapshotFenceConn.Close(context.Background())
		return Result{}, err
	}
	snapshotTx, err := snapshotConn.BeginTx(snapshotFenceCtx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		snapshotConn.Close(context.Background())
		snapshotFenceTx.Rollback(context.Background())
		snapshotFenceConn.Close(context.Background())
		return Result{}, err
	}
	var snapshotID string
	if err := snapshotTx.QueryRow(snapshotFenceCtx, `SELECT txid_current_snapshot()::text`).Scan(&snapshotID); err != nil {
		snapshotTx.Rollback(context.Background())
		snapshotConn.Close(context.Background())
		snapshotFenceTx.Rollback(context.Background())
		snapshotFenceConn.Close(context.Background())
		return Result{}, err
	}
	if err := snapshotFenceTx.Commit(snapshotFenceCtx); err != nil {
		snapshotTx.Rollback(context.Background())
		snapshotConn.Close(context.Background())
		snapshotFenceConn.Close(context.Background())
		return Result{}, err
	}
	snapshotFenceDuration := time.Since(snapshotFenceStart)
	cancelSnapshotFence()
	snapshotFenceConn.Close(context.Background())
	validationTx, err := store.Begin(ctx)
	if err != nil {
		snapshotTx.Rollback(context.Background())
		snapshotConn.Close(context.Background())
		return Result{}, err
	}
	if _, err := validationTx.Exec(ctx, `LOCK TABLE public.accounts_shadow IN SHARE MODE`); err == nil {
		err = compareShadow(ctx, snapshotTx, validationTx)
	}
	_ = validationTx.Rollback(context.Background())
	_ = snapshotTx.Rollback(context.Background())
	snapshotConn.Close(context.Background())
	if err != nil {
		return Result{}, fmt.Errorf("shadow differs from source at stable validation prefix %d: %w", validationPrefix, err)
	}

	// Final short fence: advance both jobs to the barrier and validate only
	// keys changed since the already-proven MVCC snapshot.
	source, err := transport.ConnectPostgres(ctx, cfg.SourceDSN)
	if err != nil {
		return Result{}, err
	}
	defer source.Close(context.Background())
	sourceTx, err := source.Begin(ctx)
	if err != nil {
		return Result{}, err
	}
	defer sourceTx.Rollback(context.Background())
	if _, err := sourceTx.Exec(ctx, `SET LOCAL lock_timeout = '5s'`); err != nil {
		return Result{}, err
	}
	if _, err := sourceTx.Exec(ctx, `LOCK TABLE public.accounts IN SHARE MODE`); err != nil {
		return Result{}, fmt.Errorf("acquire source cutover fence: %w", err)
	}
	fenceStarted := time.Now()
	fenceCtx := ctx
	cancelFence := func() {}
	if cfg.MaxWritePause > 0 {
		fenceCtx, cancelFence = context.WithTimeout(ctx, cfg.MaxWritePause)
	}
	defer cancelFence()
	brokerEnd, err := kafka.EndOffset(fenceCtx, cfg.KafkaBrokers, cfg.KafkaTopic)
	if err != nil {
		return Result{}, err
	}
	barrierID, err := markers.WriteBarrier(fenceCtx, cfg.ShadowJobID, fmt.Sprintf("promotion:%d", time.Now().UnixNano()))
	if err != nil {
		return Result{}, err
	}
	barrier, err := kafka.WaitForBarrier(fenceCtx, cfg.KafkaBrokers, cfg.KafkaTopic, barrierID, brokerEnd, decodeChange)
	if err != nil {
		return Result{}, err
	}
	if err := store.SetCutoverGates(fenceCtx, jobs, barrier); err != nil {
		return Result{}, fmt.Errorf("advance exact cutover gate to barrier %d: %w", barrier, err)
	}
	if err := waitForExactFrontiers(fenceCtx, store, cfg, barrier, earliest); err != nil {
		return Result{}, fmt.Errorf("wait for both exact frontiers at barrier %d: %w", barrier, err)
	}
	if currentTopicID, err := kafka.TopicIdentity(fenceCtx, cfg.KafkaBrokers, cfg.KafkaTopic); err != nil {
		return Result{}, err
	} else if currentTopicID != topicID {
		return Result{}, fmt.Errorf("Kafka topic identity changed during promotion")
	}
	changedKeys, err := collectChangedKeys(fenceCtx, cfg, live.SourceSchema, validationPrefix, barrier)
	if err != nil {
		return Result{}, err
	}
	result, err := commitPromotion(fenceCtx, cfg, barrier, func(ctx context.Context, destTx pgx.Tx) error {
		if err := checkSwapContract(ctx, destTx, live.SourceSchema, live.SchemaFingerprint, shadow.SchemaFingerprint); err != nil {
			return err
		}
		if err := compareChangedKeys(ctx, sourceTx, destTx, changedKeys); err != nil {
			return fmt.Errorf("shadow differs from source delta at barrier %d: %w", barrier, err)
		}
		return nil
	}, func(ctx context.Context, destTx pgx.Tx) error {
		return checkSwapContract(ctx, destTx, live.SourceSchema, live.SchemaFingerprint, shadow.SchemaFingerprint)
	})
	if err != nil {
		return Result{}, err
	}
	result.FenceDuration = time.Since(fenceStarted)
	result.SnapshotFenceDuration = snapshotFenceDuration
	if err := store.ClearCutoverGates(fenceCtx, jobs...); err != nil {
		return Result{}, fmt.Errorf("release cutover gates: %w", err)
	}
	gated = false
	return result, nil
}

func convergeExactPrefix(ctx context.Context, store *checkpoint.Store, cfg Config) (int64, error) {
	jobs := []string{cfg.LiveJobID, cfg.ShadowJobID}
	for {
		live, err := store.LoadCheckpoint(ctx, cfg.LiveJobID)
		if err != nil {
			return 0, err
		}
		shadow, err := store.LoadCheckpoint(ctx, cfg.ShadowJobID)
		if err != nil {
			return 0, err
		}
		if live == nil || shadow == nil || !live.Active || !shadow.Active {
			return 0, fmt.Errorf("live or shadow job is inactive or missing")
		}
		target := live.NextKafkaOffset
		if shadow.NextKafkaOffset > target {
			target = shadow.NextKafkaOffset
		}
		if err := store.SetCutoverGates(ctx, jobs, target); err != nil {
			return 0, err
		}
		if err := waitForExactFrontiers(ctx, store, cfg, target, 0); err == nil {
			return target, nil
		} else if !errors.Is(err, errGatePassed) {
			return 0, err
		}
		// One job may have committed a transaction already in flight when the
		// first gate became visible. Re-read both durable boundaries and move
		// the gate forward; once observed, a gated job cannot pass it again.
	}
}

func waitForExactFrontiers(ctx context.Context, store *checkpoint.Store, cfg Config, target, earliest int64) error {
	for {
		live, err := store.LoadCheckpoint(ctx, cfg.LiveJobID)
		if err != nil {
			return err
		}
		shadow, err := store.LoadCheckpoint(ctx, cfg.ShadowJobID)
		if err != nil {
			return err
		}
		if live == nil || shadow == nil || !live.Active || !shadow.Active {
			return fmt.Errorf("live or shadow job is inactive or missing")
		}
		if live.NextKafkaOffset < earliest || shadow.NextKafkaOffset < earliest {
			return fmt.Errorf("live or shadow checkpoint fell behind Kafka retention start %d", earliest)
		}
		if live.NextKafkaOffset > target || shadow.NextKafkaOffset > target {
			return fmt.Errorf("%w %d (live=%d shadow=%d)", errGatePassed, target, live.NextKafkaOffset, shadow.NextKafkaOffset)
		}
		if live.NextKafkaOffset == target && shadow.NextKafkaOffset == target &&
			live.CompletedThrough >= live.ScanUpperBound && shadow.CompletedThrough >= shadow.ScanUpperBound {
			incomplete, err := store.HasIncompleteChunks(ctx, cfg.ShadowJobID, shadow.Attempt)
			if err != nil {
				return err
			}
			if !incomplete {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func waitForFrontiers(ctx context.Context, store *checkpoint.Store, cfg Config, target, earliest int64) error {
	for {
		liveCP, err := store.LoadCheckpoint(ctx, cfg.LiveJobID)
		if err != nil {
			return err
		}
		shadowCP, err := store.LoadCheckpoint(ctx, cfg.ShadowJobID)
		if err != nil {
			return err
		}
		if liveCP == nil || shadowCP == nil || !liveCP.Active || !shadowCP.Active {
			return fmt.Errorf("live or shadow job is inactive or missing")
		}
		if liveCP.NextKafkaOffset < earliest || shadowCP.NextKafkaOffset < earliest {
			return fmt.Errorf("live or shadow checkpoint fell behind Kafka retention start %d", earliest)
		}
		if liveCP.CompletedThrough >= liveCP.ScanUpperBound && shadowCP.CompletedThrough >= shadowCP.ScanUpperBound &&
			liveCP.NextKafkaOffset >= target && shadowCP.NextKafkaOffset >= target {
			incomplete, err := store.HasIncompleteChunks(ctx, cfg.ShadowJobID, shadowCP.Attempt)
			if err != nil {
				return err
			}
			if !incomplete {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func decodeChange(data []byte) (model.Change, error) {
	var codec capture.JSONCodec
	return codec.Decode(data)
}

type swapQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func checkSwapContract(ctx context.Context, conn swapQuerier, source *schema.Schema, liveFP, shadowFP string) error {
	a, err := checkpoint.FingerprintFor(ctx, conn, "accounts", source)
	if err != nil {
		return err
	}
	b, err := checkpoint.FingerprintFor(ctx, conn, "accounts_shadow", source)
	if err != nil {
		return err
	}
	if a != liveFP || b != shadowFP || a != b {
		return fmt.Errorf("live and shadow table schemas differ from each other or their pinned fingerprints")
	}
	var dependencies int
	if err := conn.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM pg_constraint WHERE confrelid = 'public.accounts'::regclass) +
			(SELECT COUNT(*) FROM pg_trigger WHERE tgrelid = 'public.accounts'::regclass AND NOT tgisinternal) +
			(SELECT COUNT(*) FROM pg_depend d JOIN pg_rewrite r ON d.objid = r.oid
			 WHERE d.refobjid = 'public.accounts'::regclass AND r.ev_class != 'public.accounts'::regclass)
	`).Scan(&dependencies); err != nil {
		return err
	}
	if dependencies != 0 {
		return fmt.Errorf("public.accounts has foreign-key, trigger, or view dependencies that a rename would leave on the retired table")
	}
	var shadowTriggers int
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM pg_trigger WHERE tgrelid = 'public.accounts_shadow'::regclass AND NOT tgisinternal`).Scan(&shadowTriggers); err != nil {
		return err
	}
	if shadowTriggers != 0 {
		return fmt.Errorf("shadow table has user triggers outside the supported swap contract")
	}
	var liveACL, shadowACL *string
	var liveOwner, shadowOwner uint32
	var liveRLS, shadowRLS bool
	if err := conn.QueryRow(ctx, `SELECT relacl::text, relowner, relrowsecurity OR relforcerowsecurity FROM pg_class WHERE oid = 'public.accounts'::regclass`).Scan(&liveACL, &liveOwner, &liveRLS); err != nil {
		return err
	}
	if err := conn.QueryRow(ctx, `SELECT relacl::text, relowner, relrowsecurity OR relforcerowsecurity FROM pg_class WHERE oid = 'public.accounts_shadow'::regclass`).Scan(&shadowACL, &shadowOwner, &shadowRLS); err != nil {
		return err
	}
	if liveOwner != shadowOwner || liveRLS || shadowRLS || (liveACL == nil) != (shadowACL == nil) || liveACL != nil && *liveACL != *shadowACL {
		return fmt.Errorf("live and shadow table ownership, grants, or row-security policy differ")
	}
	return nil
}

func compareShadow(ctx context.Context, source, dest pgx.Tx) error {
	sourceRows, err := source.Query(ctx, `SELECT id, owner, balance_cents FROM public.accounts ORDER BY id`)
	if err != nil {
		return err
	}
	defer sourceRows.Close()
	shadowRows, err := dest.Query(ctx, `SELECT id, owner, balance_cents FROM public.accounts_shadow ORDER BY id`)
	if err != nil {
		return err
	}
	defer shadowRows.Close()
	var count int64
	for {
		hasSource, hasShadow := sourceRows.Next(), shadowRows.Next()
		if !hasSource || !hasShadow {
			if err := sourceRows.Err(); err != nil {
				return err
			}
			if err := shadowRows.Err(); err != nil {
				return err
			}
			if hasSource != hasShadow {
				return fmt.Errorf("row count differs after %d equal rows", count)
			}
			return nil
		}
		var sourceID, sourceBalance, shadowID, shadowBalance int64
		var sourceOwner, shadowOwner string
		if err := sourceRows.Scan(&sourceID, &sourceOwner, &sourceBalance); err != nil {
			return err
		}
		if err := shadowRows.Scan(&shadowID, &shadowOwner, &shadowBalance); err != nil {
			return err
		}
		if sourceID != shadowID || sourceOwner != shadowOwner || sourceBalance != shadowBalance {
			return fmt.Errorf("first mismatch at row %d: source=(%d,%q,%d), shadow=(%d,%q,%d)",
				count, sourceID, sourceOwner, sourceBalance, shadowID, shadowOwner, shadowBalance)
		}
		count++
	}
}

func collectChangedKeys(ctx context.Context, cfg Config, source *schema.Schema, startOffset, targetOffset int64) ([]int64, error) {
	if targetOffset < startOffset {
		return nil, fmt.Errorf("validation offset regressed from %d to %d", startOffset, targetOffset)
	}
	if targetOffset == startOffset {
		return nil, nil
	}
	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, startOffset, decodeChange, kafka.WithMaxPollRecords(100))
	if err != nil {
		return nil, err
	}
	defer consumer.Close()
	keys := make(map[int64]struct{})
	position := startOffset
	for position < targetOffset {
		transactions, err := consumer.PollTransactions(ctx)
		if err != nil {
			return nil, err
		}
		if transactions == nil {
			return nil, fmt.Errorf("Kafka consumer stopped at %d before validation target %d", position, targetOffset)
		}
		for index, transaction := range transactions {
			next := transaction.FinalOffset + 1
			if next > targetOffset {
				closeTransactions(transactions[index:])
				return nil, fmt.Errorf("validation target %d is not a source transaction boundary; next transaction ends at %d", targetOffset, next)
			}
			if next > position {
				if err := transaction.Walk(func(records []kafka.Record) error {
					for i := range records {
						record := &records[i]
						if record.Change.Row == nil {
							continue
						}
						key, err := source.KeyFromChange(&record.Change)
						if err != nil {
							return fmt.Errorf("promotion delta row at offset %d: %w", record.Offset, err)
						}
						keys[key] = struct{}{}
						if len(keys) > cfg.MaxDeltaKeys {
							return fmt.Errorf("promotion delta exceeds %d distinct keys; retry with a larger limit or lower write traffic", cfg.MaxDeltaKeys)
						}
					}
					return nil
				}); err != nil {
					closeTransactions(transactions[index:])
					return nil, err
				}
				position = next
			}
			transaction.Close()
			if position == targetOffset {
				closeTransactions(transactions[index+1:])
				result := make([]int64, 0, len(keys))
				for key := range keys {
					result = append(result, key)
				}
				sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
				return result, nil
			}
		}
	}
	return nil, fmt.Errorf("validation stopped at %d before target %d", position, targetOffset)
}

func closeTransactions(transactions []*kafka.Transaction) {
	for _, transaction := range transactions {
		transaction.Close()
	}
}

type validationRow struct {
	owner   string
	balance int64
}

func compareChangedKeys(ctx context.Context, source, dest pgx.Tx, keys []int64) error {
	const batchSize = 1000
	for start := 0; start < len(keys); start += batchSize {
		end := start + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[start:end]
		sourceRows, err := loadValidationRows(ctx, source, "public.accounts", batch)
		if err != nil {
			return err
		}
		shadowRows, err := loadValidationRows(ctx, dest, "public.accounts_shadow", batch)
		if err != nil {
			return err
		}
		for _, id := range batch {
			sourceRow, sourceOK := sourceRows[id]
			shadowRow, shadowOK := shadowRows[id]
			if sourceOK != shadowOK || sourceOK && sourceRow != shadowRow {
				return fmt.Errorf("changed key %d differs: source=(present=%v,%q,%d) shadow=(present=%v,%q,%d)",
					id, sourceOK, sourceRow.owner, sourceRow.balance, shadowOK, shadowRow.owner, shadowRow.balance)
			}
		}
	}
	return nil
}

func loadValidationRows(ctx context.Context, tx pgx.Tx, table string, keys []int64) (map[int64]validationRow, error) {
	rows, err := tx.Query(ctx, fmt.Sprintf(`SELECT id, owner, balance_cents FROM %s WHERE id = ANY($1::bigint[])`, table), keys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[int64]validationRow, len(keys))
	for rows.Next() {
		var id int64
		var row validationRow
		if err := rows.Scan(&id, &row.owner, &row.balance); err != nil {
			return nil, err
		}
		result[id] = row
	}
	return result, rows.Err()
}

func priorPromotion(ctx context.Context, destDSN, liveJobID, shadowJobID string) (Result, bool, error) {
	conn, err := transport.ConnectPostgres(ctx, destDSN)
	if err != nil {
		return Result{}, false, err
	}
	defer conn.Close(context.Background())
	var result Result
	var storedLive string
	var activeOID uint32
	err = conn.QueryRow(ctx, `SELECT live_job_id, barrier_offset, retired_table, active_table_oid FROM seam_promotions WHERE shadow_job_id = $1`, shadowJobID).Scan(&storedLive, &result.BarrierOffset, &result.RetiredTable, &activeOID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Result{}, false, nil
	}
	if err != nil {
		return Result{}, false, err
	}
	if storedLive != liveJobID {
		return Result{}, false, fmt.Errorf("shadow job %q was promoted with live job %q", shadowJobID, storedLive)
	}
	var currentOID uint32
	if err := conn.QueryRow(ctx, `SELECT 'public.accounts'::regclass::oid`).Scan(&currentOID); err != nil {
		return Result{}, false, err
	}
	if currentOID != activeOID {
		return Result{}, false, fmt.Errorf("active public.accounts table changed since promotion")
	}
	result.AlreadyPromoted = true
	return result, true, nil
}

func commitPromotion(ctx context.Context, cfg Config, barrier int64, validate, finalCheck func(context.Context, pgx.Tx) error) (Result, error) {
	conn, err := transport.ConnectPostgres(ctx, cfg.DestDSN)
	if err != nil {
		return Result{}, err
	}
	defer conn.Close(context.Background())
	tx, err := conn.Begin(ctx)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '5s'`); err != nil {
		return Result{}, err
	}
	var epoch int64
	if err := tx.QueryRow(ctx, `SELECT epoch FROM seam_route_fence WHERE id = TRUE FOR UPDATE`).Scan(&epoch); err != nil {
		return Result{}, err
	}
	var prior Result
	var storedLive string
	err = tx.QueryRow(ctx, `SELECT live_job_id, barrier_offset, retired_table FROM seam_promotions WHERE shadow_job_id = $1`, cfg.ShadowJobID).Scan(&storedLive, &prior.BarrierOffset, &prior.RetiredTable)
	if err == nil {
		if storedLive != cfg.LiveJobID {
			return Result{}, fmt.Errorf("shadow job was promoted with a different live job")
		}
		prior.AlreadyPromoted = true
		return prior, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Result{}, err
	}
	if _, err := tx.Exec(ctx, `LOCK TABLE public.accounts, public.accounts_shadow IN SHARE MODE`); err != nil {
		return Result{}, fmt.Errorf("acquire destination validation locks: %w", err)
	}
	// SHARE locks preserve ordinary reads while excluding direct destination
	// writes and DDL. The route fence separately excludes SEAM writers.
	if validate != nil {
		if err := validate(ctx, tx); err != nil {
			return Result{}, err
		}
	}
	if _, err := tx.Exec(ctx, `LOCK TABLE public.accounts, public.accounts_shadow IN ACCESS EXCLUSIVE MODE`); err != nil {
		return Result{}, fmt.Errorf("acquire destination table swap locks: %w", err)
	}
	if finalCheck != nil {
		if err := finalCheck(ctx, tx); err != nil {
			return Result{}, err
		}
	}
	for _, jobID := range []string{cfg.LiveJobID, cfg.ShadowJobID} {
		var offset, completed, upper int64
		var active bool
		if err := tx.QueryRow(ctx, `
			SELECT next_kafka_offset, completed_through_id, scan_upper_bound, active
			FROM seam_checkpoints WHERE job_id = $1 FOR UPDATE`, jobID).Scan(&offset, &completed, &upper, &active); err != nil {
			return Result{}, err
		}
		if !active || offset != barrier || completed < upper {
			return Result{}, fmt.Errorf("job %q moved away from cutover prefix %d", jobID, barrier)
		}
	}
	retired := fmt.Sprintf("accounts_retired_%d", time.Now().UnixNano())
	if _, err := tx.Exec(ctx, `UPDATE seam_checkpoints SET active = FALSE, updated_at = NOW() WHERE job_id = $1`, cfg.ShadowJobID); err != nil {
		return Result{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE seam_route_fence SET epoch = epoch + 1 WHERE id = TRUE`); err != nil {
		return Result{}, err
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`ALTER TABLE public.accounts RENAME TO %s`, retired)); err != nil {
		return Result{}, err
	}
	if _, err := tx.Exec(ctx, `ALTER TABLE public.accounts_shadow RENAME TO accounts`); err != nil {
		return Result{}, err
	}
	// PostgreSQL table renames do not give the primary-key constraints and
	// backing indexes the names needed by a subsequent accounts_shadow table.
	// Free both standard names inside this same atomic cutover.
	if err := renamePrimaryKey(ctx, tx, retired, retired+"_pkey"); err != nil {
		return Result{}, err
	}
	if err := renamePrimaryKey(ctx, tx, "accounts", "accounts_pkey"); err != nil {
		return Result{}, err
	}
	var activeOID uint32
	if err := tx.QueryRow(ctx, `SELECT 'public.accounts'::regclass::oid`).Scan(&activeOID); err != nil {
		return Result{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO seam_promotions(shadow_job_id, live_job_id, barrier_offset, retired_table, active_table_oid)
		VALUES($1, $2, $3, $4, $5)`, cfg.ShadowJobID, cfg.LiveJobID, barrier, retired, activeOID); err != nil {
		return Result{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		if prior, ok, readErr := priorPromotion(context.Background(), cfg.DestDSN, cfg.LiveJobID, cfg.ShadowJobID); readErr == nil && ok {
			return prior, nil
		}
		return Result{}, fmt.Errorf("commit destination promotion: %w", err)
	}
	return Result{BarrierOffset: barrier, RetiredTable: retired}, nil
}

func renamePrimaryKey(ctx context.Context, tx pgx.Tx, table, target string) error {
	var current string
	if err := tx.QueryRow(ctx, `SELECT conname FROM pg_constraint
		WHERE conrelid = $1::regclass AND contype = 'p'`, "public."+table).Scan(&current); err != nil {
		return fmt.Errorf("read %s primary key: %w", table, err)
	}
	if current == target {
		return nil
	}
	statement := fmt.Sprintf(`ALTER TABLE %s RENAME CONSTRAINT %s TO %s`,
		pgx.Identifier{"public", table}.Sanitize(), pgx.Identifier{current}.Sanitize(), pgx.Identifier{target}.Sanitize())
	if _, err := tx.Exec(ctx, statement); err != nil {
		return fmt.Errorf("rename %s primary key to %s: %w", table, target, err)
	}
	return nil
}

// Package snowpromotion validates and promotes a Snowflake shadow while the
// PostgreSQL source remains writable except for two bounded fence intervals.
package snowpromotion

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"time"

	"example.com/seam/internal/capture"
	"example.com/seam/internal/kafka"
	"example.com/seam/internal/marker"
	"example.com/seam/internal/model"
	"example.com/seam/internal/schema"
	seamsnowflake "example.com/seam/internal/snowflake"
	"example.com/seam/internal/transport"
	"github.com/jackc/pgx/v5"
)

type Warehouse interface {
	NextOffset(context.Context) (int64, error)
	WaitForMarker(context.Context, string, time.Duration) error
	MarkerPosition(context.Context, string) (int64, uint64, error)
	PrepareValidationClone(context.Context, string, string, string) (*seamsnowflake.BackfillJob, error)
	MarkValidated(context.Context, string) error
	FailBackfill(context.Context, string, error) error
	LoadBackfill(context.Context, string) (*seamsnowflake.BackfillJob, error)
	ReadRows(context.Context, string, int64, int64) ([]model.Row, error)
	ReadKeys(context.Context, string, []int64) ([]model.Row, error)
	PromoteView(context.Context, string, string) error
}

type Config struct {
	SourceDSN       string
	JobID           string
	Attempt         string
	ValidationTable string
	KafkaBrokers    []string
	KafkaTopic      string
	KafkaTopicID    string
	PollInterval    time.Duration
	MaxWritePause   time.Duration
	LockTimeout     time.Duration
	MaxDeltaKeys    int
	MaxPollRecords  int
}

type Manager struct {
	cfg        Config
	store      Warehouse
	markers    *marker.Store
	descriptor *schema.Schema
}

func New(cfg Config, store Warehouse, markers *marker.Store, descriptor *schema.Schema) (*Manager, error) {
	if cfg.SourceDSN == "" || cfg.JobID == "" || cfg.Attempt == "" || cfg.ValidationTable == "" || cfg.KafkaTopic == "" || cfg.KafkaTopicID == "" {
		return nil, fmt.Errorf("source, job, attempt, validation table, and Kafka stream identity are required")
	}
	if len(cfg.KafkaBrokers) == 0 || store == nil || markers == nil || descriptor == nil || descriptor.PKColumn() == nil {
		return nil, fmt.Errorf("promotion dependencies are incomplete")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 250 * time.Millisecond
	}
	if cfg.MaxWritePause <= 0 {
		cfg.MaxWritePause = 30 * time.Second
	}
	if cfg.LockTimeout <= 0 {
		cfg.LockTimeout = 5 * time.Second
	}
	if cfg.MaxDeltaKeys <= 0 {
		cfg.MaxDeltaKeys = 100_000
	}
	if cfg.MaxPollRecords <= 0 {
		cfg.MaxPollRecords = 100
	}
	return &Manager{cfg: cfg, store: store, markers: markers, descriptor: descriptor}, nil
}

// Validate captures an exported PostgreSQL snapshot and a zero-copy Snowflake
// clone at the same ordered marker. The source lock lasts only through marker
// catch-up, clone creation, and snapshot import; the full range comparison
// runs after writes resume.
func (manager *Manager) Validate(ctx context.Context) error {
	job, err := manager.store.LoadBackfill(ctx, manager.cfg.JobID)
	if err != nil {
		return err
	}
	switch job.State {
	case seamsnowflake.BackfillReady, seamsnowflake.BackfillPromoting, seamsnowflake.BackfillCompleted:
		return nil
	case seamsnowflake.BackfillReadyToVerify, seamsnowflake.BackfillVerifying:
		// A verifying job restarts from a new exported source snapshot and marker.
		// The old PostgreSQL snapshot cannot survive the crashed connection.
	default:
		return fmt.Errorf("Snowflake backfill %q is %s, expected %s or %s", manager.cfg.JobID, job.State, seamsnowflake.BackfillReadyToVerify, seamsnowflake.BackfillVerifying)
	}
	if err := manager.validateTopic(ctx); err != nil {
		return err
	}
	if err := manager.preDrain(ctx); err != nil {
		return err
	}
	exporterConn, exporterTx, err := manager.beginFence(ctx)
	if err != nil {
		return err
	}
	defer exporterConn.Close(context.Background())
	defer exporterTx.Rollback(context.Background())
	fenceCtx, cancelFence := context.WithTimeout(ctx, manager.cfg.MaxWritePause)
	defer cancelFence()
	var snapshotID string
	if err := exporterTx.QueryRow(fenceCtx, "SELECT pg_export_snapshot()").Scan(&snapshotID); err != nil {
		return fmt.Errorf("export source validation snapshot: %w", err)
	}
	markerID, err := manager.markers.WriteBarrier(fenceCtx, manager.cfg.JobID, manager.cfg.Attempt+":validation")
	if err != nil {
		return err
	}
	if err := manager.store.WaitForMarker(fenceCtx, markerID, manager.cfg.PollInterval); err != nil {
		return fmt.Errorf("wait for validation marker: %w", err)
	}
	job, err = manager.store.PrepareValidationClone(fenceCtx, manager.cfg.JobID, markerID, manager.cfg.ValidationTable)
	if err != nil {
		return err
	}
	validationConn, validationTx, err := manager.importSnapshot(fenceCtx, snapshotID)
	if err != nil {
		return err
	}
	defer validationConn.Close(context.Background())
	defer validationTx.Rollback(context.Background())
	if err := exporterTx.Commit(fenceCtx); err != nil {
		return fmt.Errorf("release source validation fence: %w", err)
	}
	cancelFence()
	if err := compareRanges(ctx, manager.descriptor, validationTx, manager.store, job.ValidationTable, job.Spec.Chunks); err != nil {
		validationErr := fmt.Errorf("exact Snowflake shadow validation failed: %w", err)
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelCleanup()
		if cleanupErr := manager.store.FailBackfill(cleanupCtx, manager.cfg.JobID, validationErr); cleanupErr != nil {
			return errors.Join(validationErr, fmt.Errorf("deactivate failed Snowflake shadow: %w", cleanupErr))
		}
		return validationErr
	}
	if err := manager.store.MarkValidated(ctx, manager.cfg.JobID); err != nil {
		return err
	}
	return nil
}

// Promote validates only keys changed after the full clone validation, under
// a final bounded source fence, then atomically repoints the public Snowflake
// view. Unchanged keys retain the proof established by Validate.
func (manager *Manager) Promote(ctx context.Context) error {
	if err := manager.validateTopic(ctx); err != nil {
		return err
	}
	job, err := manager.store.LoadBackfill(ctx, manager.cfg.JobID)
	if err != nil {
		return err
	}
	if job.State == seamsnowflake.BackfillCompleted {
		return nil
	}
	if job.State == seamsnowflake.BackfillPromoting {
		// Promotion intent is written before the idempotent view DDL. Both CDC
		// routes remain active until metadata convergence, so the shadow stays
		// current even when a crashed process released the source fence.
		return manager.store.PromoteView(ctx, manager.cfg.JobID, job.PromotionMarkerID)
	}
	if job.State != seamsnowflake.BackfillReady {
		return fmt.Errorf("Snowflake backfill %q is %s, expected %s", manager.cfg.JobID, job.State, seamsnowflake.BackfillReady)
	}
	if !job.ValidationReady || job.ValidationMarkerID == "" || job.ValidationTable == "" {
		return fmt.Errorf("Snowflake backfill %q has no durable validation proof", manager.cfg.JobID)
	}
	earliest, err := kafka.EarliestOffset(ctx, manager.cfg.KafkaBrokers, manager.cfg.KafkaTopic)
	if err != nil {
		return err
	}
	start := job.ValidationOffset + 1
	if start < earliest {
		return fmt.Errorf("validation boundary %d is before Kafka earliest retained offset %d; delta proof is impossible", start, earliest)
	}
	preFenceEnd, err := kafka.EndOffset(ctx, manager.cfg.KafkaBrokers, manager.cfg.KafkaTopic)
	if err != nil {
		return err
	}
	if err := manager.waitFrontier(ctx, preFenceEnd); err != nil {
		return err
	}
	changed := make(map[int64]struct{})
	next, err := manager.collectChangedKeys(ctx, start, preFenceEnd, changed)
	if err != nil {
		return err
	}

	fenceConn, fenceTx, err := manager.beginFence(ctx)
	if err != nil {
		return err
	}
	defer fenceConn.Close(context.Background())
	defer fenceTx.Rollback(context.Background())
	fenceCtx, cancelFence := context.WithTimeout(ctx, manager.cfg.MaxWritePause)
	defer cancelFence()
	markerID, err := manager.markers.WriteBarrier(fenceCtx, manager.cfg.JobID, manager.cfg.Attempt+":promotion")
	if err != nil {
		return err
	}
	if err := manager.store.WaitForMarker(fenceCtx, markerID, manager.cfg.PollInterval); err != nil {
		return fmt.Errorf("wait for promotion marker: %w", err)
	}
	finalOffset, _, err := manager.store.MarkerPosition(fenceCtx, markerID)
	if err != nil {
		return err
	}
	if next <= finalOffset {
		if _, err := manager.collectChangedKeys(fenceCtx, next, finalOffset+1, changed); err != nil {
			return err
		}
	}
	keys := sortedKeys(changed)
	if err := compareKeys(fenceCtx, manager.descriptor, fenceTx, manager.store, job.Spec.ShadowTable, keys); err != nil {
		return fmt.Errorf("final Snowflake delta validation failed: %w", err)
	}
	if err := manager.store.PromoteView(fenceCtx, manager.cfg.JobID, markerID); err != nil {
		return err
	}
	if err := fenceTx.Commit(fenceCtx); err != nil {
		return fmt.Errorf("release source promotion fence: %w", err)
	}
	return nil
}

func (manager *Manager) beginFence(ctx context.Context) (*pgx.Conn, pgx.Tx, error) {
	conn, err := transport.ConnectPostgres(ctx, manager.cfg.SourceDSN)
	if err != nil {
		return nil, nil, err
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		conn.Close(context.Background())
		return nil, nil, err
	}
	lockMilliseconds := manager.cfg.LockTimeout.Milliseconds()
	if lockMilliseconds <= 0 {
		lockMilliseconds = 1
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", lockMilliseconds)); err != nil {
		tx.Rollback(context.Background())
		conn.Close(context.Background())
		return nil, nil, err
	}
	if _, err := tx.Exec(ctx, "LOCK TABLE "+manager.descriptor.TableQualified()+" IN SHARE MODE"); err != nil {
		tx.Rollback(context.Background())
		conn.Close(context.Background())
		return nil, nil, fmt.Errorf("acquire source write fence: %w", err)
	}
	return conn, tx, nil
}

var snapshotPattern = regexp.MustCompile(`^[0-9A-Fa-f-]+$`)

func (manager *Manager) importSnapshot(ctx context.Context, snapshotID string) (*pgx.Conn, pgx.Tx, error) {
	if !snapshotPattern.MatchString(snapshotID) {
		return nil, nil, fmt.Errorf("PostgreSQL returned invalid snapshot identifier %q", snapshotID)
	}
	conn, err := transport.ConnectPostgres(ctx, manager.cfg.SourceDSN)
	if err != nil {
		return nil, nil, err
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		conn.Close(context.Background())
		return nil, nil, err
	}
	if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT '"+snapshotID+"'"); err != nil {
		tx.Rollback(context.Background())
		conn.Close(context.Background())
		return nil, nil, fmt.Errorf("import source validation snapshot: %w", err)
	}
	return conn, tx, nil
}

func (manager *Manager) preDrain(ctx context.Context) error {
	end, err := kafka.EndOffset(ctx, manager.cfg.KafkaBrokers, manager.cfg.KafkaTopic)
	if err != nil {
		return err
	}
	return manager.waitFrontier(ctx, end)
}

func (manager *Manager) waitFrontier(ctx context.Context, target int64) error {
	ticker := time.NewTicker(manager.cfg.PollInterval)
	defer ticker.Stop()
	for {
		next, err := manager.store.NextOffset(ctx)
		if err != nil {
			return err
		}
		if next >= target {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (manager *Manager) validateTopic(ctx context.Context) error {
	actual, err := kafka.TopicIdentity(ctx, manager.cfg.KafkaBrokers, manager.cfg.KafkaTopic)
	if err != nil {
		return err
	}
	if actual != manager.cfg.KafkaTopicID {
		return fmt.Errorf("Kafka topic identity changed from %s to %s; validation proof is invalid", manager.cfg.KafkaTopicID, actual)
	}
	return nil
}

func (manager *Manager) collectChangedKeys(ctx context.Context, start, targetNext int64, changed map[int64]struct{}) (int64, error) {
	if start >= targetNext {
		return start, nil
	}
	consumer, err := kafka.NewConsumer(manager.cfg.KafkaBrokers, manager.cfg.KafkaTopic, start, decodeChange,
		kafka.WithExpectedTopicID(manager.cfg.KafkaTopicID), kafka.WithMaxPollRecords(manager.cfg.MaxPollRecords))
	if err != nil {
		return start, err
	}
	defer consumer.Close()
	next := start
	for next < targetNext {
		transactions, err := consumer.PollTransactions(ctx)
		if err != nil {
			return next, err
		}
		if len(transactions) == 0 {
			if ctx.Err() != nil {
				return next, ctx.Err()
			}
			continue
		}
		for index, transaction := range transactions {
			err := transaction.Walk(func(records []kafka.Record) error {
				for _, record := range records {
					if record.Change.Row == nil {
						continue
					}
					key, err := manager.descriptor.KeyFromChange(&record.Change)
					if err != nil {
						return err
					}
					changed[key] = struct{}{}
					if len(changed) > manager.cfg.MaxDeltaKeys {
						return fmt.Errorf("changed-key set exceeded configured limit %d; run validation again closer to promotion", manager.cfg.MaxDeltaKeys)
					}
				}
				return nil
			})
			next = transaction.FinalOffset + 1
			transaction.Close()
			if err != nil {
				for _, remainder := range transactions[index+1:] {
					remainder.Close()
				}
				return next, err
			}
			if next >= targetNext {
				for _, remainder := range transactions[index+1:] {
					remainder.Close()
				}
				return next, nil
			}
		}
	}
	return next, nil
}

func compareRanges(ctx context.Context, descriptor *schema.Schema, source pgx.Tx, warehouse Warehouse, table string, ranges []model.ChunkRange) error {
	for _, keyRange := range ranges {
		sourceRows, err := descriptor.ScanRowsFrom(ctx, source, keyRange.Min, keyRange.Max)
		if err != nil {
			return err
		}
		sourceRows, err = seamsnowflake.NormalizeSourceRows(descriptor, sourceRows)
		if err != nil {
			return err
		}
		destinationRows, err := warehouse.ReadRows(ctx, table, keyRange.Min, keyRange.Max)
		if err != nil {
			return err
		}
		if err := compareRows(descriptor, sourceRows, destinationRows); err != nil {
			return fmt.Errorf("range %s: %w", keyRange, err)
		}
	}
	return nil
}

func compareKeys(ctx context.Context, descriptor *schema.Schema, source pgx.Tx, warehouse Warehouse, table string, keys []int64) error {
	const batchSize = 1000
	for start := 0; start < len(keys); start += batchSize {
		end := start + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		sourceRows, err := descriptor.ScanKeysFrom(ctx, source, keys[start:end])
		if err != nil {
			return err
		}
		sourceRows, err = seamsnowflake.NormalizeSourceRows(descriptor, sourceRows)
		if err != nil {
			return err
		}
		destinationRows, err := warehouse.ReadKeys(ctx, table, keys[start:end])
		if err != nil {
			return err
		}
		if err := compareRows(descriptor, sourceRows, destinationRows); err != nil {
			return fmt.Errorf("changed-key batch beginning at %d: %w", keys[start], err)
		}
	}
	return nil
}

func compareRows(descriptor *schema.Schema, source, destination []model.Row) error {
	if len(source) != len(destination) {
		return fmt.Errorf("row count differs: source=%d destination=%d", len(source), len(destination))
	}
	for rowIndex := range source {
		sourceKey, err := descriptor.KeyFromRow(&source[rowIndex])
		if err != nil {
			return err
		}
		destinationKey, err := descriptor.KeyFromRow(&destination[rowIndex])
		if err != nil {
			return err
		}
		if sourceKey != destinationKey {
			return fmt.Errorf("ordered primary key differs: source=%d destination=%d", sourceKey, destinationKey)
		}
		if len(source[rowIndex].Values) != len(destination[rowIndex].Values) {
			return fmt.Errorf("key %d column count differs", sourceKey)
		}
		for columnIndex := range source[rowIndex].Values {
			if !source[rowIndex].Values[columnIndex].Equal(destination[rowIndex].Values[columnIndex]) {
				return fmt.Errorf("key %d column %q differs", sourceKey, descriptor.Columns[columnIndex].Name)
			}
		}
	}
	return nil
}

func sortedKeys(changed map[int64]struct{}) []int64 {
	keys := make([]int64, 0, len(changed))
	for key := range changed {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func decodeChange(data []byte) (model.Change, error) {
	var codec capture.JSONCodec
	return codec.Decode(data)
}

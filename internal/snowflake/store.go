package snowflake

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"strconv"
	"strings"
	"time"

	"example.com/seam/internal/kafka"
	"example.com/seam/internal/model"
	"example.com/seam/internal/schema"
)

// Store owns the authoritative Snowflake-side apply frontier. The same
// transaction that mutates target rows records APPLIED_TRANSACTIONS and
// OFFSETS; a crash before COMMIT applies neither, and a crash afterwards is
// recognized as a replay even when a PostgreSQL control checkpoint lagged.
type Store struct {
	db     *sql.DB
	cfg    Config
	schema *schema.Schema
}

type Route struct {
	ID    string
	Table string
}

const (
	stageBatchRows  = 500
	stageBatchBytes = 4 << 20
)

type appliedTransaction struct {
	firstOffset int64
	finalOffset int64
	eventCount  int
	fingerprint string
}

type replayAction uint8

const (
	replayApply replayAction = iota
	replayAdvanceOnly
	replayAlreadyPast
)

func NewStore(db *sql.DB, cfg Config, source *schema.Schema) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("nil Snowflake database")
	}
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if err := ValidateSourceSchema(source); err != nil {
		return nil, err
	}
	return &Store{db: db, cfg: cfg, schema: source}, nil
}

// EnsureObjects creates the data-plane metadata and initial live route.
// Snowflake DDL auto-commits, so this function is deliberately separate from
// ApplyTransaction and must finish before consumption starts.
func (s *Store) EnsureObjects(ctx context.Context, initialOffset int64) error {
	physicalLive := s.cfg.livePhysicalTable()
	if !identifierPattern.MatchString(physicalLive) {
		return fmt.Errorf("derived Snowflake live physical table %q is invalid", physicalLive)
	}
	targetSQL, err := createTargetSQL(s.cfg, s.schema, physicalLive)
	if err != nil {
		return err
	}
	statements := []string{
		"CREATE SCHEMA IF NOT EXISTS " + quoteIdentifier(s.cfg.Database) + "." + quoteIdentifier(s.cfg.Schema),
		"CREATE SCHEMA IF NOT EXISTS " + quoteIdentifier(s.cfg.Database) + "." + quoteIdentifier(s.cfg.InternalSchema),
		targetSQL,
		createLiveViewSQL(s.cfg, s.schema, physicalLive, false),
		"CREATE TRANSIENT TABLE IF NOT EXISTS " + s.cfg.internal("CDC_STAGE") + " (BATCH_ID VARCHAR NOT NULL, SOURCE_TX VARCHAR NOT NULL, SEQUENCE NUMBER(38,0) NOT NULL, OP VARCHAR NOT NULL, PK NUMBER(38,0) NOT NULL, SOURCE_LSN NUMBER(20,0) NOT NULL, PAYLOAD VARIANT)",
		"CREATE TABLE IF NOT EXISTS " + s.cfg.internal("ROUTES") + " (STREAM_ID VARCHAR NOT NULL, ROUTE_ID VARCHAR NOT NULL, TARGET_TABLE VARCHAR NOT NULL, ACTIVE BOOLEAN NOT NULL, CREATED_AT TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP())",
		"CREATE TABLE IF NOT EXISTS " + s.cfg.internal("KEY_CLOCKS") + " (STREAM_ID VARCHAR NOT NULL, ROUTE_ID VARCHAR NOT NULL, PK NUMBER(38,0) NOT NULL, SOURCE_LSN NUMBER(20,0) NOT NULL, SOURCE_SEQUENCE NUMBER(38,0) NOT NULL, DELETED BOOLEAN NOT NULL, UPDATED_AT TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP())",
		"CREATE TABLE IF NOT EXISTS " + s.cfg.internal("APPLIED_TRANSACTIONS") + " (STREAM_ID VARCHAR NOT NULL, SOURCE_TX VARCHAR NOT NULL, FIRST_OFFSET NUMBER(38,0) NOT NULL, FINAL_OFFSET NUMBER(38,0) NOT NULL, EVENT_COUNT NUMBER(38,0) NOT NULL, TX_FINGERPRINT VARCHAR NOT NULL, COMMITTED_AT TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP())",
		"CREATE TABLE IF NOT EXISTS " + s.cfg.internal("OFFSETS") + " (STREAM_ID VARCHAR NOT NULL, TOPIC_ID VARCHAR NOT NULL, PARTITION NUMBER(10,0) NOT NULL, NEXT_OFFSET NUMBER(38,0) NOT NULL, UPDATED_AT TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP())",
		"CREATE TABLE IF NOT EXISTS " + s.cfg.internal("SINK_LEASES") + " (STREAM_ID VARCHAR NOT NULL, OWNER_ID VARCHAR NOT NULL, OWNER_EPOCH NUMBER(38,0) NOT NULL, LEASE_EXPIRES TIMESTAMP_LTZ NOT NULL, UPDATED_AT TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP())",
		"CREATE TABLE IF NOT EXISTS " + s.cfg.internal("MARKERS") + " (STREAM_ID VARCHAR NOT NULL, MARKER_ID VARCHAR NOT NULL, KIND VARCHAR NOT NULL, JOB_ID VARCHAR NOT NULL, ATTEMPT VARCHAR NOT NULL, CHUNK_MIN NUMBER(38,0), CHUNK_MAX NUMBER(38,0), SOURCE_TX VARCHAR NOT NULL, FINAL_OFFSET NUMBER(38,0) NOT NULL, SOURCE_LSN NUMBER(20,0) NOT NULL, COMMITTED_AT TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP())",
		"CREATE TABLE IF NOT EXISTS " + s.cfg.internal("BACKFILL_JOBS") + " (STREAM_ID VARCHAR NOT NULL, JOB_ID VARCHAR NOT NULL, ATTEMPT VARCHAR NOT NULL, STATE VARCHAR NOT NULL, SHADOW_ROUTE_ID VARCHAR NOT NULL, SHADOW_TABLE VARCHAR NOT NULL, SCAN_UPPER_BOUND NUMBER(38,0) NOT NULL, START_OFFSET NUMBER(38,0) NOT NULL, TOTAL_CHUNKS NUMBER(38,0) NOT NULL, COMPLETED_CHUNKS NUMBER(38,0) NOT NULL DEFAULT 0, VALIDATION_TABLE VARCHAR, VALIDATION_MARKER_ID VARCHAR, VALIDATION_OFFSET NUMBER(38,0), VALIDATION_READY BOOLEAN NOT NULL DEFAULT FALSE, VALIDATED_AT TIMESTAMP_LTZ, PROMOTION_MARKER_ID VARCHAR, PROMOTION_OFFSET NUMBER(38,0), ERROR_MESSAGE VARCHAR, CREATED_AT TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(), UPDATED_AT TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP())",
		"CREATE TABLE IF NOT EXISTS " + s.cfg.internal("BACKFILL_CHUNKS") + " (STREAM_ID VARCHAR NOT NULL, JOB_ID VARCHAR NOT NULL, ATTEMPT VARCHAR NOT NULL, CHUNK_MIN NUMBER(38,0) NOT NULL, CHUNK_MAX NUMBER(38,0) NOT NULL, STATE VARCHAR NOT NULL, LEASE_OWNER VARCHAR, LEASE_TOKEN NUMBER(38,0) NOT NULL DEFAULT 0, LEASE_EXPIRES TIMESTAMP_LTZ, LOW_MARKER_ID VARCHAR, HIGH_MARKER_ID VARCHAR, LOW_LSN NUMBER(20,0), ROWS_SCANNED NUMBER(38,0) NOT NULL DEFAULT 0, ROWS_APPLIED NUMBER(38,0) NOT NULL DEFAULT 0, ERROR_MESSAGE VARCHAR, CREATED_AT TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(), UPDATED_AT TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP())",
		"CREATE TABLE IF NOT EXISTS " + s.cfg.internal("BACKFILL_FILES") + " (STREAM_ID VARCHAR NOT NULL, JOB_ID VARCHAR NOT NULL, ATTEMPT VARCHAR NOT NULL, CHUNK_MIN NUMBER(38,0) NOT NULL, LEASE_TOKEN NUMBER(38,0) NOT NULL, FILE_ID VARCHAR NOT NULL, STAGE_PATH VARCHAR NOT NULL, CONTENT_SHA256 VARCHAR NOT NULL, ROW_COUNT NUMBER(38,0) NOT NULL, BYTE_COUNT NUMBER(38,0) NOT NULL, STATE VARCHAR NOT NULL, CREATED_AT TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(), UPDATED_AT TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP())",
		"CREATE TRANSIENT TABLE IF NOT EXISTS " + s.cfg.internal("SNAPSHOT_STAGE") + " (STREAM_ID VARCHAR NOT NULL, JOB_ID VARCHAR NOT NULL, ATTEMPT VARCHAR NOT NULL, CHUNK_MIN NUMBER(38,0) NOT NULL, SEQUENCE NUMBER(38,0) NOT NULL, PK NUMBER(38,0) NOT NULL, PAYLOAD VARIANT NOT NULL)",
	}
	if s.cfg.SnapshotLoader == SnapshotLoaderBulk {
		statements = append(statements, "CREATE STAGE IF NOT EXISTS "+s.cfg.internal("SNAPSHOT_FILES")+" FILE_FORMAT = (TYPE = JSON COMPRESSION = GZIP)")
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("initialize Snowflake objects: %w", err)
		}
	}
	// These columns make an object created by the pre-release Snowflake draft
	// fail closed instead of silently using its offset-based replay semantics.
	// Snowflake can reject ADD COLUMN IF NOT EXISTS when a DEFAULT is present
	// and the column already exists, so inspect INFORMATION_SCHEMA first.
	migrations := []struct {
		table, column, definition string
	}{
		{"KEY_CLOCKS", "STREAM_ID", "VARCHAR"},
		{"KEY_CLOCKS", "SOURCE_SEQUENCE", "NUMBER(38,0)"},
		{"APPLIED_TRANSACTIONS", "EVENT_COUNT", "NUMBER(38,0)"},
		{"APPLIED_TRANSACTIONS", "TX_FINGERPRINT", "VARCHAR"},
		{"BACKFILL_JOBS", "VALIDATION_TABLE", "VARCHAR"},
		{"BACKFILL_JOBS", "VALIDATION_MARKER_ID", "VARCHAR"},
		{"BACKFILL_JOBS", "VALIDATION_OFFSET", "NUMBER(38,0)"},
		{"BACKFILL_JOBS", "VALIDATION_READY", "BOOLEAN DEFAULT FALSE"},
		{"BACKFILL_JOBS", "VALIDATED_AT", "TIMESTAMP_LTZ"},
		{"BACKFILL_JOBS", "PROMOTION_MARKER_ID", "VARCHAR"},
		{"BACKFILL_JOBS", "PROMOTION_OFFSET", "NUMBER(38,0)"},
	}
	for _, migration := range migrations {
		if err := s.ensureColumn(ctx, migration.table, migration.column, migration.definition); err != nil {
			return fmt.Errorf("migrate Snowflake objects: %w", err)
		}
	}
	if _, err := s.db.ExecContext(ctx, "UPDATE "+s.cfg.internal("BACKFILL_JOBS")+" SET VALIDATION_READY = FALSE WHERE VALIDATION_READY IS NULL"); err != nil {
		return fmt.Errorf("migrate Snowflake validation state: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, "MERGE INTO "+s.cfg.internal("ROUTES")+" D USING (SELECT ? AS STREAM_ID, 'live' AS ROUTE_ID, ? AS TARGET_TABLE) S ON D.STREAM_ID = S.STREAM_ID AND D.ROUTE_ID = S.ROUTE_ID WHEN NOT MATCHED THEN INSERT (STREAM_ID, ROUTE_ID, TARGET_TABLE, ACTIVE) VALUES (S.STREAM_ID, S.ROUTE_ID, S.TARGET_TABLE, TRUE)", s.cfg.StreamID, strings.ToUpper(physicalLive)); err != nil {
		return fmt.Errorf("initialize Snowflake live route: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, "MERGE INTO "+s.cfg.internal("OFFSETS")+" D USING (SELECT ? AS STREAM_ID, ? AS TOPIC_ID, ? AS PARTITION, ? AS NEXT_OFFSET) S ON D.STREAM_ID = S.STREAM_ID WHEN NOT MATCHED THEN INSERT (STREAM_ID, TOPIC_ID, PARTITION, NEXT_OFFSET) VALUES (S.STREAM_ID, S.TOPIC_ID, S.PARTITION, S.NEXT_OFFSET)", s.cfg.StreamID, s.cfg.TopicID, s.cfg.Partition, initialOffset); err != nil {
		return fmt.Errorf("initialize Snowflake stream offset: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, "MERGE INTO "+s.cfg.internal("SINK_LEASES")+" D USING (SELECT ? AS STREAM_ID) S ON D.STREAM_ID = S.STREAM_ID WHEN NOT MATCHED THEN INSERT (STREAM_ID, OWNER_ID, OWNER_EPOCH, LEASE_EXPIRES) VALUES (S.STREAM_ID, '', 0, TO_TIMESTAMP_LTZ(0))", s.cfg.StreamID); err != nil {
		return fmt.Errorf("initialize Snowflake sink lease: %w", err)
	}
	return nil
}

func (s *Store) ensureColumn(ctx context.Context, table, column, definition string) error {
	var count int
	query := "SELECT COUNT(*) FROM " + quoteIdentifier(s.cfg.Database) + ".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?"
	if err := s.db.QueryRowContext(ctx, query, strings.ToUpper(s.cfg.InternalSchema), strings.ToUpper(table), strings.ToUpper(column)).Scan(&count); err != nil {
		return fmt.Errorf("inspect %s.%s: %w", table, column, err)
	}
	if count > 1 {
		return fmt.Errorf("information schema returned %d copies of %s.%s", count, table, column)
	}
	if count == 1 {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, "ALTER TABLE "+s.cfg.internal(table)+" ADD COLUMN "+quoteIdentifier(column)+" "+definition); err != nil {
		return fmt.Errorf("add %s.%s: %w", table, column, err)
	}
	return nil
}

func (s *Store) NextOffset(ctx context.Context) (int64, error) {
	var topicID string
	var partition int32
	var next int64
	err := s.db.QueryRowContext(ctx, "SELECT TOPIC_ID, PARTITION, NEXT_OFFSET FROM "+s.cfg.internal("OFFSETS")+" WHERE STREAM_ID = ?", s.cfg.StreamID).Scan(&topicID, &partition, &next)
	if err != nil {
		return 0, fmt.Errorf("read Snowflake stream offset: %w", err)
	}
	if topicID != s.cfg.TopicID || partition != s.cfg.Partition {
		return 0, fmt.Errorf("Snowflake stream identity is topic %q partition %d, configured as topic %q partition %d; resnapshot required", topicID, partition, s.cfg.TopicID, s.cfg.Partition)
	}
	return next, nil
}

func (s *Store) ApplyTransaction(ctx context.Context, lease *SinkLease, transaction *kafka.Transaction) (retErr error) {
	if lease == nil {
		return fmt.Errorf("Snowflake apply requires a sink lease")
	}
	if transaction == nil {
		return fmt.Errorf("nil Kafka transaction")
	}
	if transaction.FirstOffset < 0 || transaction.FinalOffset < transaction.FirstOffset {
		return fmt.Errorf("invalid Kafka transaction offset range %d..%d", transaction.FirstOffset, transaction.FinalOffset)
	}
	fingerprint, eventCount, lsn, err := s.validateAndFingerprint(transaction)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Snowflake apply transaction: %w", err)
	}
	defer func() {
		if retErr != nil {
			_ = tx.Rollback()
		}
	}()

	sourceTx := transaction.Source.String()
	var applied *appliedTransaction
	var previous appliedTransaction
	err = tx.QueryRowContext(ctx, "SELECT FIRST_OFFSET, FINAL_OFFSET, EVENT_COUNT, TX_FINGERPRINT FROM "+s.cfg.internal("APPLIED_TRANSACTIONS")+" WHERE STREAM_ID = ? AND SOURCE_TX = ?", s.cfg.StreamID, sourceTx).Scan(&previous.firstOffset, &previous.finalOffset, &previous.eventCount, &previous.fingerprint)
	if err == nil {
		applied = &previous
	} else if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check applied Snowflake transaction: %w", err)
	}
	var nextOffset int64
	if err := tx.QueryRowContext(ctx, "SELECT NEXT_OFFSET FROM "+s.cfg.internal("OFFSETS")+" WHERE STREAM_ID = ?", s.cfg.StreamID).Scan(&nextOffset); err != nil {
		return fmt.Errorf("read Snowflake apply frontier: %w", err)
	}
	action, err := classifyReplay(nextOffset, transaction, eventCount, fingerprint, applied)
	if err != nil {
		return err
	}
	if action == replayAlreadyPast {
		return tx.Rollback()
	}
	if action == replayAdvanceOnly {
		if err := s.fenceSinkLease(ctx, tx, lease); err != nil {
			return err
		}
		if err := advanceOffset(ctx, tx, s.cfg, transaction.FirstOffset, transaction.FinalOffset+1); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit Snowflake replay frontier: %w", err)
		}
		return nil
	}
	batchID := s.cfg.StreamID + ":" + sourceTx
	if err := s.stageTransaction(ctx, tx, transaction, batchID, sourceTx, lsn); err != nil {
		return err
	}

	routes, err := activeRoutes(ctx, tx, s.cfg)
	if err != nil {
		return err
	}
	if len(routes) == 0 {
		return fmt.Errorf("Snowflake stream %q has no active target routes", s.cfg.StreamID)
	}
	for _, route := range routes {
		if !identifierPattern.MatchString(route.Table) {
			return fmt.Errorf("route %q contains invalid target table %q", route.ID, route.Table)
		}
		mergeSQL, err := mergeTargetSQL(s.cfg, s.schema, route.Table)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, mergeSQL, batchID, s.cfg.StreamID, route.ID); err != nil {
			return fmt.Errorf("merge Snowflake route %q: %w", route.ID, err)
		}
		if _, err := tx.ExecContext(ctx, mergeClocksSQL(s.cfg), batchID, s.cfg.StreamID, route.ID, s.cfg.StreamID, route.ID); err != nil {
			return fmt.Errorf("advance Snowflake key clocks for route %q: %w", route.ID, err)
		}
	}
	// Touch the lease row after all potentially long staging and MERGE work.
	// A takeover updates the same row, so either this transaction proves its
	// still-live epoch before committing, or every data effect rolls back.
	if err := s.fenceSinkLease(ctx, tx, lease); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+s.cfg.internal("APPLIED_TRANSACTIONS")+" (STREAM_ID, SOURCE_TX, FIRST_OFFSET, FINAL_OFFSET, EVENT_COUNT, TX_FINGERPRINT) VALUES (?, ?, ?, ?, ?, ?)", s.cfg.StreamID, sourceTx, transaction.FirstOffset, transaction.FinalOffset, eventCount, fingerprint); err != nil {
		return fmt.Errorf("record applied Snowflake transaction: %w", err)
	}
	if err := advanceOffset(ctx, tx, s.cfg, transaction.FirstOffset, transaction.FinalOffset+1); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM "+s.cfg.internal("CDC_STAGE")+" WHERE BATCH_ID = ?", batchID); err != nil {
		return fmt.Errorf("clear Snowflake CDC stage: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Snowflake apply transaction: %w", err)
	}
	return nil
}

func classifyReplay(frontier int64, transaction *kafka.Transaction, eventCount int, fingerprint string, applied *appliedTransaction) (replayAction, error) {
	if transaction == nil {
		return replayApply, fmt.Errorf("nil Kafka transaction")
	}
	if applied != nil {
		if applied.eventCount <= 0 || applied.fingerprint == "" {
			return replayApply, fmt.Errorf("source transaction %s has a legacy Snowflake ledger entry without a content identity; resnapshot required", transaction.Source)
		}
		if applied.eventCount != eventCount || applied.fingerprint != fingerprint {
			return replayApply, fmt.Errorf("source transaction %s replay content differs from its Snowflake ledger entry", transaction.Source)
		}
		if transaction.FinalOffset < frontier {
			return replayAlreadyPast, nil
		}
		if transaction.FirstOffset == frontier {
			return replayAdvanceOnly, nil
		}
		if transaction.FirstOffset > frontier {
			return replayApply, fmt.Errorf("Kafka offset gap: transaction begins at %d, Snowflake expects %d", transaction.FirstOffset, frontier)
		}
		return replayApply, fmt.Errorf("Snowflake frontier %d falls inside replayed transaction offsets %d..%d", frontier, transaction.FirstOffset, transaction.FinalOffset)
	}
	if transaction.FirstOffset == frontier {
		return replayApply, nil
	}
	if transaction.FirstOffset > frontier {
		return replayApply, fmt.Errorf("Kafka offset gap: transaction begins at %d, Snowflake expects %d", transaction.FirstOffset, frontier)
	}
	if transaction.FinalOffset < frontier {
		return replayApply, fmt.Errorf("Kafka transaction offsets %d..%d are behind Snowflake frontier %d but have no source-transaction ledger entry", transaction.FirstOffset, transaction.FinalOffset, frontier)
	}
	return replayApply, fmt.Errorf("Snowflake frontier %d falls inside unapplied transaction offsets %d..%d", frontier, transaction.FirstOffset, transaction.FinalOffset)
}

func advanceOffset(ctx context.Context, tx *sql.Tx, cfg Config, from, to int64) error {
	result, err := tx.ExecContext(ctx, "UPDATE "+cfg.internal("OFFSETS")+" SET NEXT_OFFSET = ?, UPDATED_AT = CURRENT_TIMESTAMP() WHERE STREAM_ID = ? AND NEXT_OFFSET = ?", to, cfg.StreamID, from)
	if err != nil {
		return fmt.Errorf("advance Snowflake apply frontier: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("read Snowflake apply frontier result: %w", err)
	} else if affected != 1 {
		return fmt.Errorf("Snowflake apply frontier compare-and-set affected %d rows, expected 1", affected)
	}
	return nil
}

// validateAndFingerprint reads a replayable transaction without materializing
// it. The fingerprint is independent of Kafka offsets, which are allowed to
// change when capture republishes a PostgreSQL transaction after recovery.
func (s *Store) validateAndFingerprint(transaction *kafka.Transaction) (string, int, uint64, error) {
	lsn, err := parseLSN(transaction.Source.LSN)
	if err != nil {
		return "", 0, 0, err
	}
	h := sha256.New()
	writeFingerprintString(h, "seam-snowflake-transaction-v1")
	writeSourceFingerprint(h, transaction.Source)
	eventCount := 0
	err = transaction.Walk(func(records []kafka.Record) error {
		for _, record := range records {
			change := record.Change
			if err := s.validateChange(transaction.Source, &change); err != nil {
				return err
			}
			writeChangeFingerprint(h, &change)
			eventCount++
		}
		return nil
	})
	if err != nil {
		return "", 0, 0, err
	}
	if eventCount != transaction.TotalCount || eventCount <= 0 {
		return "", 0, 0, fmt.Errorf("transaction %s contains %d events, envelope declares %d", transaction.Source, eventCount, transaction.TotalCount)
	}
	return hex.EncodeToString(h.Sum(nil)), eventCount, lsn, nil
}

func (s *Store) validateChange(source model.SourceTx, change *model.Change) error {
	if change == nil {
		return fmt.Errorf("nil change")
	}
	if change.Source != source {
		return fmt.Errorf("change source %s differs from envelope source %s", change.Source, source)
	}
	if (change.Row == nil) == (change.Marker == nil) {
		return fmt.Errorf("transaction %s change must contain exactly one of row or marker", source)
	}
	if change.Marker != nil {
		if change.Marker.ID == "" || change.Marker.JobID == "" || change.Marker.Attempt == "" {
			return fmt.Errorf("transaction %s contains an incomplete marker", source)
		}
		return nil
	}
	if change.SchemaID != s.schema.Fingerprint {
		return fmt.Errorf("source schema epoch changed from %s to %s; resnapshot required", s.schema.Fingerprint, change.SchemaID)
	}
	if change.Op != model.OpInsert && change.Op != model.OpUpdate && change.Op != model.OpDelete {
		return fmt.Errorf("transaction %s contains unsupported operation %q", source, change.Op)
	}
	_, _, err := encodeRow(s.schema, change.Row)
	return err
}

type stagedChange struct {
	sequence int64
	op       model.Operation
	pk       int64
	payload  string
}

func (s *Store) stageTransaction(ctx context.Context, tx *sql.Tx, transaction *kafka.Transaction, batchID, sourceTx string, lsn uint64) error {
	sequence := int64(0)
	batch := make([]stagedChange, 0, stageBatchRows)
	batchBytes := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		query, args := stageInsertSQL(s.cfg, batchID, sourceTx, lsn, batch)
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("stage %d Snowflake changes: %w", len(batch), err)
		}
		batch = batch[:0]
		batchBytes = 0
		return nil
	}
	err := transaction.Walk(func(records []kafka.Record) error {
		for _, record := range records {
			change := record.Change
			if change.Marker != nil {
				if err := flush(); err != nil {
					return err
				}
				if err := insertMarker(ctx, tx, s.cfg, sourceTx, transaction.FinalOffset, lsn, change.Marker); err != nil {
					return err
				}
				sequence++
				continue
			}
			pk, payload, err := encodeRow(s.schema, change.Row)
			if err != nil {
				return err
			}
			rowBytes := len(payload) + len(sourceTx) + 96
			if rowBytes > stageBatchBytes {
				return fmt.Errorf("Snowflake change for key %d is %d bytes; stage batch limit is %d", pk, rowBytes, stageBatchBytes)
			}
			if len(batch) == stageBatchRows || (len(batch) > 0 && batchBytes+rowBytes > stageBatchBytes) {
				if err := flush(); err != nil {
					return err
				}
			}
			batch = append(batch, stagedChange{sequence: sequence, op: change.Op, pk: pk, payload: payload})
			batchBytes += rowBytes
			sequence++
		}
		return nil
	})
	if err != nil {
		return err
	}
	return flush()
}

func stageInsertSQL(cfg Config, batchID, sourceTx string, lsn uint64, changes []stagedChange) (string, []any) {
	var query strings.Builder
	query.WriteString("INSERT INTO ")
	query.WriteString(cfg.internal("CDC_STAGE"))
	query.WriteString(" (BATCH_ID, SOURCE_TX, SEQUENCE, OP, PK, SOURCE_LSN, PAYLOAD) ")
	args := make([]any, 0, len(changes)*7)
	for i, change := range changes {
		if i > 0 {
			query.WriteString(" UNION ALL ")
		}
		query.WriteString("SELECT ?, ?, ?, ?, ?, ?, PARSE_JSON(?)")
		args = append(args, batchID, sourceTx, change.sequence, string(change.op), change.pk, strconv.FormatUint(lsn, 10), change.payload)
	}
	return query.String(), args
}

func writeSourceFingerprint(h hash.Hash, source model.SourceTx) {
	writeFingerprintString(h, source.SystemID)
	writeFingerprintString(h, source.Generation)
	writeFingerprintString(h, source.LSN)
	var xid [4]byte
	binary.BigEndian.PutUint32(xid[:], source.XID)
	_, _ = h.Write(xid[:])
}

func writeChangeFingerprint(h hash.Hash, change *model.Change) {
	writeFingerprintString(h, string(change.Op))
	writeFingerprintString(h, change.SchemaID)
	if change.Marker != nil {
		_, _ = h.Write([]byte{1})
		writeFingerprintString(h, change.Marker.ID)
		writeFingerprintString(h, string(change.Marker.Kind))
		writeFingerprintString(h, change.Marker.JobID)
		writeFingerprintString(h, change.Marker.Attempt)
		var bounds [16]byte
		binary.BigEndian.PutUint64(bounds[:8], uint64(change.Marker.ChunkMin))
		binary.BigEndian.PutUint64(bounds[8:], uint64(change.Marker.ChunkMax))
		_, _ = h.Write(bounds[:])
		return
	}
	_, _ = h.Write([]byte{0})
	for _, value := range change.Row.Values {
		_, _ = h.Write([]byte{byte(value.Kind)})
		switch value.Kind {
		case model.ValueInt64:
			var integer [8]byte
			binary.BigEndian.PutUint64(integer[:], uint64(value.Int))
			_, _ = h.Write(integer[:])
		case model.ValueText:
			writeFingerprintString(h, value.Text)
		}
	}
}

func writeFingerprintString(h hash.Hash, value string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = h.Write(size[:])
	_, _ = h.Write([]byte(value))
}

func activeRoutes(ctx context.Context, tx *sql.Tx, cfg Config) ([]Route, error) {
	rows, err := tx.QueryContext(ctx, "SELECT ROUTE_ID, TARGET_TABLE FROM "+cfg.internal("ROUTES")+" WHERE STREAM_ID = ? AND ACTIVE = TRUE ORDER BY ROUTE_ID", cfg.StreamID)
	if err != nil {
		return nil, fmt.Errorf("read Snowflake routes: %w", err)
	}
	defer rows.Close()
	var routes []Route
	for rows.Next() {
		var route Route
		if err := rows.Scan(&route.ID, &route.Table); err != nil {
			return nil, fmt.Errorf("scan Snowflake route: %w", err)
		}
		routes = append(routes, route)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate Snowflake routes: %w", err)
	}
	return routes, nil
}

func insertMarker(ctx context.Context, tx *sql.Tx, cfg Config, sourceTx string, finalOffset int64, lsn uint64, marker *model.Marker) error {
	_, err := tx.ExecContext(ctx, "INSERT INTO "+cfg.internal("MARKERS")+" (STREAM_ID, MARKER_ID, KIND, JOB_ID, ATTEMPT, CHUNK_MIN, CHUNK_MAX, SOURCE_TX, FINAL_OFFSET, SOURCE_LSN) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", cfg.StreamID, marker.ID, string(marker.Kind), marker.JobID, marker.Attempt, marker.ChunkMin, marker.ChunkMax, sourceTx, finalOffset, strconv.FormatUint(lsn, 10))
	if err != nil {
		return fmt.Errorf("record Snowflake marker %q: %w", marker.ID, err)
	}
	return nil
}

func encodeRow(source *schema.Schema, row *model.Row) (int64, string, error) {
	if row == nil || len(row.Values) != len(source.Columns) {
		return 0, "", fmt.Errorf("row has %d values, schema has %d columns", valueCount(row), len(source.Columns))
	}
	payload := make(map[string]any, len(source.Columns))
	var pk int64
	for index, column := range source.Columns {
		value := row.Values[index]
		if column.PrimaryKey {
			if value.Kind != model.ValueInt64 {
				return 0, "", fmt.Errorf("primary key %q is not an int64", column.Name)
			}
			pk = value.Int
		}
		switch value.Kind {
		case model.ValueNull:
			payload[column.Name] = nil
		case model.ValueInt64:
			// JSON numbers lose precision in common decoders. A canonical string
			// is converted explicitly by generated Snowflake SQL.
			payload[column.Name] = strconv.FormatInt(value.Int, 10)
		case model.ValueText:
			payload[column.Name] = value.Text
		default:
			return 0, "", fmt.Errorf("column %q has unknown value kind %d", column.Name, value.Kind)
		}
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return 0, "", fmt.Errorf("encode Snowflake row payload: %w", err)
	}
	return pk, string(encoded), nil
}

func valueCount(row *model.Row) int {
	if row == nil {
		return 0
	}
	return len(row.Values)
}

func parseLSN(value string) (uint64, error) {
	parts := strings.Split(value, "/")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid PostgreSQL LSN %q", value)
	}
	high, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid PostgreSQL LSN %q: %w", value, err)
	}
	low, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid PostgreSQL LSN %q: %w", value, err)
	}
	return high<<32 | low, nil
}

// MarkerCommitted reports whether the sink has durably processed a marker.
// The backfill coordinator uses this as the CDC catch-up boundary.
func (s *Store) MarkerCommitted(ctx context.Context, markerID string) (bool, error) {
	var one int
	err := s.db.QueryRowContext(ctx, "SELECT 1 FROM "+s.cfg.internal("MARKERS")+" WHERE STREAM_ID = ? AND MARKER_ID = ? LIMIT 1", s.cfg.StreamID, markerID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check Snowflake marker %q: %w", markerID, err)
	}
	return true, nil
}

// WaitForMarker waits for the ordered sink to cross a source marker. It is a
// polling control-plane operation; target correctness does not depend on the
// polling interval.
func (s *Store) WaitForMarker(ctx context.Context, markerID string, interval time.Duration) error {
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		committed, err := s.MarkerCommitted(ctx, markerID)
		if err != nil {
			return err
		}
		if committed {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Package checkpoint persists Seam's durable progress state in destination PostgreSQL.
package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"example.com/seam/internal/model"
	"example.com/seam/internal/scan"
	"example.com/seam/internal/transport"
	"github.com/jackc/pgx/v5"
)

// Store owns the destination checkpoint tables.
type Store struct {
	dsn string
}

func NewStore(dsn string) *Store {
	return &Store{dsn: dsn}
}

// Conn opens a connection to the destination database.
func (s *Store) Conn(ctx context.Context) (*pgx.Conn, error) {
	return transport.ConnectPostgres(ctx, s.dsn)
}

func (s *Store) conn(ctx context.Context) (*pgx.Conn, error) {
	return s.Conn(ctx)
}

// Begin starts a transaction and returns a pgx.Tx that closes the underlying
// connection on commit or rollback. This lets callers treat Begin as the only
// connection primitive they need.
func (s *Store) Begin(ctx context.Context) (pgx.Tx, error) {
	conn, err := s.Conn(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		conn.Close(context.Background())
		return nil, err
	}
	return &connTx{Tx: tx, conn: conn}, nil
}

type connTx struct {
	pgx.Tx
	conn *pgx.Conn
}

func (t *connTx) Commit(ctx context.Context) error {
	err := t.Tx.Commit(ctx)
	t.conn.Close(context.Background())
	return err
}

func (t *connTx) Rollback(ctx context.Context) error {
	err := t.Tx.Rollback(ctx)
	t.conn.Close(context.Background())
	return err
}

// EnsureTables creates the checkpoint schema if it does not exist.
func (s *Store) EnsureTables(ctx context.Context) error {
	conn, err := s.conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS seam_jobs (
			job_id TEXT PRIMARY KEY,
			source_dsn TEXT NOT NULL,
			source_slot TEXT NOT NULL,
			source_publication TEXT NOT NULL,
			table_schema TEXT NOT NULL,
			table_name TEXT NOT NULL,
			kafka_topic TEXT NOT NULL,
			generation TEXT NOT NULL,
			scan_upper_bound BIGINT NOT NULL,
			schema_fingerprint TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);

		CREATE TABLE IF NOT EXISTS seam_checkpoints (
			job_id TEXT PRIMARY KEY REFERENCES seam_jobs(job_id),
			generation TEXT NOT NULL,
			attempt TEXT NOT NULL,
			scan_upper_bound BIGINT NOT NULL,
			completed_through_id BIGINT NOT NULL DEFAULT -1,
			next_kafka_offset BIGINT NOT NULL DEFAULT 0,
			last_applied_lsn TEXT,
			active BOOLEAN NOT NULL DEFAULT TRUE,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);

		CREATE TABLE IF NOT EXISTS seam_applied_txs (
			job_id TEXT NOT NULL,
			generation TEXT NOT NULL,
			source_lsn TEXT NOT NULL,
			source_xid TEXT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (job_id, generation, source_lsn)
		);

		CREATE TABLE IF NOT EXISTS seam_chunks (
			job_id TEXT NOT NULL REFERENCES seam_jobs(job_id) ON DELETE CASCADE,
			chunk_min_id BIGINT NOT NULL,
			chunk_max_id BIGINT NOT NULL,
			attempt TEXT NOT NULL,
			status TEXT NOT NULL,
			worker_id TEXT,
			lease_start TIMESTAMPTZ,
			lease_expiry TIMESTAMPTZ,
			heartbeat_at TIMESTAMPTZ,
			low_offset BIGINT,
			high_offset BIGINT,
			low_lsn TEXT,
			high_lsn TEXT,
			rows_scanned BIGINT NOT NULL DEFAULT 0,
			rows_applied BIGINT NOT NULL DEFAULT 0,
			error_message TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (job_id, chunk_min_id, chunk_max_id, attempt)
		);

		CREATE INDEX IF NOT EXISTS seam_chunks_status_idx ON seam_chunks(job_id, status, updated_at);
	`)
	return err
}

// Fingerprint returns a deterministic validation token for the destination
// accounts table schema. Empty string means any schema is accepted.
func Fingerprint(ctx context.Context, conn *pgx.Conn) (string, error) {
	rows, err := conn.Query(ctx, `
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'accounts'
		ORDER BY ordinal_position`)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var cols []map[string]string
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return "", err
		}
		cols = append(cols, map[string]string{"name": name, "type": typ})
	}
	b, _ := json.Marshal(cols)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])[:16], nil
}

// CreateJob inserts a new job and checkpoint. It returns the checkpoint.
func (s *Store) CreateJob(ctx context.Context, cfg model.JobConfig, upperBound int64) (*model.Checkpoint, error) {
	conn, err := s.conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())

	fp, err := Fingerprint(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("fingerprint destination schema: %w", err)
	}

	generation := "gen:0"
	attempt := "gen:0:attempt:0"
	checkpoint := &model.Checkpoint{
		JobID:            cfg.JobID,
		Generation:       generation,
		Attempt:          attempt,
		ScanUpperBound:   upperBound,
		CompletedThrough: math.MinInt64,
		NextKafkaOffset:  0,
		Active:           true,
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		INSERT INTO seam_jobs (job_id, source_dsn, source_slot, source_publication, table_schema, table_name, kafka_topic, generation, scan_upper_bound, schema_fingerprint)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		cfg.JobID, cfg.SourceDSN, cfg.SourceSlot, cfg.SourcePublication, "public", "accounts", cfg.KafkaTopic, generation, upperBound, fp); err != nil {
		return nil, fmt.Errorf("insert job: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO seam_checkpoints (job_id, generation, attempt, scan_upper_bound, completed_through_id, next_kafka_offset, active)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		cfg.JobID, generation, attempt, upperBound, checkpoint.CompletedThrough, checkpoint.NextKafkaOffset, checkpoint.Active); err != nil {
		return nil, fmt.Errorf("insert checkpoint: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return checkpoint, nil
}

// LoadCheckpoint returns the current checkpoint for a job, or nil if not found.
func (s *Store) LoadCheckpoint(ctx context.Context, jobID string) (*model.Checkpoint, error) {
	conn, err := s.conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())

	var cp model.Checkpoint
	var lastLSN *string
	err = conn.QueryRow(ctx, `
		SELECT job_id, generation, attempt, scan_upper_bound, completed_through_id, next_kafka_offset, last_applied_lsn, active
		FROM seam_checkpoints WHERE job_id = $1`, jobID).Scan(
		&cp.JobID, &cp.Generation, &cp.Attempt, &cp.ScanUpperBound, &cp.CompletedThrough, &cp.NextKafkaOffset, &lastLSN, &cp.Active,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if lastLSN != nil {
		cp.LastAppliedLSN = *lastLSN
	}
	return &cp, nil
}

// JobRecord is the persisted job metadata.
type JobRecord struct {
	Config            model.JobConfig
	Generation        string
	ScanUpperBound    int64
	SchemaFingerprint string
}

// LoadJobRecord returns the persisted configuration and generation for a job.
func (s *Store) LoadJobRecord(ctx context.Context, jobID string) (*JobRecord, error) {
	conn, err := s.conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())

	var rec JobRecord
	var schema, table string
	if err := conn.QueryRow(ctx, `
		SELECT job_id, source_dsn, source_slot, source_publication, table_schema, table_name, kafka_topic, generation, scan_upper_bound, schema_fingerprint
		FROM seam_jobs WHERE job_id = $1`, jobID).Scan(
		&rec.Config.JobID, &rec.Config.SourceDSN, &rec.Config.SourceSlot, &rec.Config.SourcePublication, &schema, &table, &rec.Config.KafkaTopic, &rec.Generation, &rec.ScanUpperBound, &rec.SchemaFingerprint,
	); err != nil {
		return nil, err
	}
	return &rec, nil
}

// IsApplied returns true if the source transaction has already been applied.
func (s *Store) IsApplied(ctx context.Context, tx pgx.Tx, jobID, generation, lsn string) (bool, error) {
	var exists bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM seam_applied_txs WHERE job_id = $1 AND generation = $2 AND source_lsn = $3)`,
		jobID, generation, lsn).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// MarkApplied records a source transaction as applied.
func (s *Store) MarkApplied(ctx context.Context, tx pgx.Tx, jobID, generation, lsn string, xid uint32) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO seam_applied_txs (job_id, generation, source_lsn, source_xid) VALUES ($1, $2, $3, $4)
		ON CONFLICT (job_id, generation, source_lsn) DO NOTHING`,
		jobID, generation, lsn, fmt.Sprintf("%d", xid))
	return err
}

// UpdateCheckpoint writes the checkpoint inside the caller's transaction.
func (s *Store) UpdateCheckpoint(ctx context.Context, tx pgx.Tx, cp *model.Checkpoint) error {
	_, err := tx.Exec(ctx, `
		UPDATE seam_checkpoints SET
			generation = $2,
			attempt = $3,
			scan_upper_bound = $4,
			completed_through_id = $5,
			next_kafka_offset = $6,
			last_applied_lsn = $7,
			active = $8,
			updated_at = NOW()
		WHERE job_id = $1`,
		cp.JobID, cp.Generation, cp.Attempt, cp.ScanUpperBound, cp.CompletedThrough, cp.NextKafkaOffset, cp.LastAppliedLSN, cp.Active)
	return err
}

// CreateChunk inserts a pending chunk for a job.
func (s *Store) CreateChunk(ctx context.Context, chunk *model.Chunk) error {
	conn, err := s.conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(ctx, `
		INSERT INTO seam_chunks (
			job_id, chunk_min_id, chunk_max_id, attempt, status,
			worker_id, lease_start, lease_expiry, heartbeat_at,
			low_offset, high_offset, low_lsn, high_lsn,
			rows_scanned, rows_applied, error_message
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		ON CONFLICT (job_id, chunk_min_id, chunk_max_id, attempt) DO NOTHING`,
		chunk.JobID, chunk.ChunkMinID, chunk.ChunkMaxID, chunk.Attempt, chunk.Status,
		chunk.WorkerID, chunk.LeaseStart, chunk.LeaseExpiry, chunk.HeartbeatAt,
		chunk.LowOffset, chunk.HighOffset, chunk.LowLSN, chunk.HighLSN,
		chunk.RowsScanned, chunk.RowsApplied, chunk.ErrorMessage)
	return err
}

// LeaseChunk atomically claims the oldest pending chunk for a worker.
// It returns nil, nil if no pending chunk is available.
func (s *Store) LeaseChunk(ctx context.Context, jobID, workerID string, leaseDuration time.Duration) (*model.Chunk, error) {
	conn, err := s.conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())

	now := time.Now()
	expiry := now.Add(leaseDuration)
	var chunk model.Chunk
	var leaseStart, leaseExpiry, heartbeatAt *time.Time
	var lowOffset, highOffset *int64
	var lowLSN, highLSN *string
	err = conn.QueryRow(ctx, `
		UPDATE seam_chunks SET
			status = $1,
			worker_id = $2,
			lease_start = $3,
			lease_expiry = $4,
			heartbeat_at = $5,
			updated_at = NOW()
		WHERE ctid = (
			SELECT ctid FROM seam_chunks
			WHERE job_id = $6 AND status = $7
			ORDER BY chunk_min_id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING
			job_id, chunk_min_id, chunk_max_id, attempt, status,
			worker_id, lease_start, lease_expiry, heartbeat_at,
			low_offset, high_offset, low_lsn, high_lsn,
			rows_scanned, rows_applied, error_message,
			created_at, updated_at`,
		model.ChunkLeased, workerID, now, expiry, now,
		jobID, model.ChunkPending,
	).Scan(
		&chunk.JobID, &chunk.ChunkMinID, &chunk.ChunkMaxID, &chunk.Attempt, &chunk.Status,
		&chunk.WorkerID, &leaseStart, &leaseExpiry, &heartbeatAt,
		&lowOffset, &highOffset, &lowLSN, &highLSN,
		&chunk.RowsScanned, &chunk.RowsApplied, &chunk.ErrorMessage,
		&chunk.CreatedAt, &chunk.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	chunk.LeaseStart = leaseStart
	chunk.LeaseExpiry = leaseExpiry
	chunk.HeartbeatAt = heartbeatAt
	chunk.LowOffset = lowOffset
	chunk.HighOffset = highOffset
	chunk.LowLSN = lowLSN
	chunk.HighLSN = highLSN
	return &chunk, nil
}

// UpdateChunk writes the full chunk state inside the caller's transaction.
func (s *Store) UpdateChunk(ctx context.Context, tx pgx.Tx, chunk *model.Chunk) error {
	_, err := tx.Exec(ctx, `
		UPDATE seam_chunks SET
			status = $5,
			worker_id = $6,
			lease_start = $7,
			lease_expiry = $8,
			heartbeat_at = $9,
			low_offset = $10,
			high_offset = $11,
			low_lsn = $12,
			high_lsn = $13,
			rows_scanned = $14,
			rows_applied = $15,
			error_message = $16,
			updated_at = NOW()
		WHERE job_id = $1 AND chunk_min_id = $2 AND chunk_max_id = $3 AND attempt = $4`,
		chunk.JobID, chunk.ChunkMinID, chunk.ChunkMaxID, chunk.Attempt, chunk.Status,
		chunk.WorkerID, chunk.LeaseStart, chunk.LeaseExpiry, chunk.HeartbeatAt,
		chunk.LowOffset, chunk.HighOffset, chunk.LowLSN, chunk.HighLSN,
		chunk.RowsScanned, chunk.RowsApplied, chunk.ErrorMessage)
	return err
}

// LoadChunks returns all chunks for a job, optionally filtered by status.
func (s *Store) LoadChunks(ctx context.Context, jobID string, statuses ...model.ChunkState) ([]model.Chunk, error) {
	conn, err := s.conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())

	query := `SELECT
		job_id, chunk_min_id, chunk_max_id, attempt, status,
		worker_id, lease_start, lease_expiry, heartbeat_at,
		low_offset, high_offset, low_lsn, high_lsn,
		rows_scanned, rows_applied, error_message,
		created_at, updated_at
	FROM seam_chunks WHERE job_id = $1`
	args := []any{jobID}
	if len(statuses) > 0 {
		query += ` AND status = ANY($2)`
		statusStrings := make([]string, len(statuses))
		for i, st := range statuses {
			statusStrings[i] = string(st)
		}
		args = append(args, statusStrings)
	}
	query += ` ORDER BY chunk_min_id`

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanChunks(rows)
}

// LoadIncompleteChunks returns chunks that are not completed.
func (s *Store) LoadIncompleteChunks(ctx context.Context, jobID string) ([]model.Chunk, error) {
	conn, err := s.conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())

	rows, err := conn.Query(ctx, `
		SELECT
			job_id, chunk_min_id, chunk_max_id, attempt, status,
			worker_id, lease_start, lease_expiry, heartbeat_at,
			low_offset, high_offset, low_lsn, high_lsn,
			rows_scanned, rows_applied, error_message,
			created_at, updated_at
		FROM seam_chunks
		WHERE job_id = $1 AND status != $2
		ORDER BY chunk_min_id`, jobID, model.ChunkCompleted)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanChunks(rows)
}

// ReleaseExpiredLeases moves leased chunks back to pending when their lease
// has expired and heartbeat is stale.
func (s *Store) ReleaseExpiredLeases(ctx context.Context, jobID string, before time.Time) (int64, error) {
	conn, err := s.conn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close(context.Background())

	tag, err := conn.Exec(ctx, `
		UPDATE seam_chunks SET
			status = $1,
			worker_id = NULL,
			lease_start = NULL,
			lease_expiry = NULL,
			heartbeat_at = NULL,
			updated_at = NOW()
		WHERE job_id = $2 AND status = $3 AND lease_expiry < $4`,
		model.ChunkPending, jobID, model.ChunkLeased, before)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// HeartbeatChunk updates the heartbeat timestamp for a leased chunk.
func (s *Store) HeartbeatChunk(ctx context.Context, chunk *model.Chunk) error {
	conn, err := s.conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	now := time.Now()
	_, err = conn.Exec(ctx, `
		UPDATE seam_chunks SET heartbeat_at = $1, updated_at = NOW()
		WHERE job_id = $2 AND chunk_min_id = $3 AND chunk_max_id = $4 AND attempt = $5`,
		now, chunk.JobID, chunk.ChunkMinID, chunk.ChunkMaxID, chunk.Attempt)
	return err
}

func scanChunks(rows pgx.Rows) ([]model.Chunk, error) {
	var chunks []model.Chunk
	for rows.Next() {
		var chunk model.Chunk
		var leaseStart, leaseExpiry, heartbeatAt *time.Time
		var lowOffset, highOffset *int64
		var lowLSN, highLSN *string
		if err := rows.Scan(
			&chunk.JobID, &chunk.ChunkMinID, &chunk.ChunkMaxID, &chunk.Attempt, &chunk.Status,
			&chunk.WorkerID, &leaseStart, &leaseExpiry, &heartbeatAt,
			&lowOffset, &highOffset, &lowLSN, &highLSN,
			&chunk.RowsScanned, &chunk.RowsApplied, &chunk.ErrorMessage,
			&chunk.CreatedAt, &chunk.UpdatedAt,
		); err != nil {
			return nil, err
		}
		chunk.LeaseStart = leaseStart
		chunk.LeaseExpiry = leaseExpiry
		chunk.HeartbeatAt = heartbeatAt
		chunk.LowOffset = lowOffset
		chunk.HighOffset = highOffset
		chunk.LowLSN = lowLSN
		chunk.HighLSN = highLSN
		chunks = append(chunks, chunk)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return chunks, nil
}

// DiscoverAndCreateChunks scans the source table and persists chunk boundaries
// for the job. It is idempotent: existing chunks are left untouched.
func (s *Store) DiscoverAndCreateChunks(ctx context.Context, jobID, attempt string, sourceDSN string, upperBound int64, chunkSize int) error {
	return s.DiscoverAndCreateChunksFor(ctx, jobID, attempt, sourceDSN, scan.DefaultChunkReaderTable, scan.DefaultChunkReaderKey, upperBound, chunkSize)
}

// DiscoverAndCreateChunksFor is DiscoverAndCreateChunks with an explicit
// source table and key column; see scan.NewChunkReaderFor for identifier rules.
func (s *Store) DiscoverAndCreateChunksFor(ctx context.Context, jobID, attempt string, sourceDSN, sourceTable, sourceKey string, upperBound int64, chunkSize int) error {
	reader, err := scan.NewChunkReaderFor(sourceDSN, sourceTable, sourceKey)
	if err != nil {
		return fmt.Errorf("discover chunks: %w", err)
	}
	completedThrough := int64(math.MinInt64)

	for {
		chunk, ok, err := reader.NextChunk(ctx, completedThrough, upperBound, chunkSize)
		if err != nil {
			return fmt.Errorf("discover chunk: %w", err)
		}
		if !ok {
			break
		}
		c := &model.Chunk{
			JobID:      jobID,
			ChunkMinID: chunk.Min,
			ChunkMaxID: chunk.Max,
			Attempt:    attempt,
			Status:     model.ChunkPending,
		}
		if err := s.CreateChunk(ctx, c); err != nil {
			return fmt.Errorf("create chunk %s: %w", chunk, err)
		}
		completedThrough = chunk.Max
	}
	return nil
}

// EnsureDestinationTable creates the destination accounts table if needed.
func (s *Store) EnsureDestinationTable(ctx context.Context) error {
	conn, err := s.conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS accounts (
			id BIGINT PRIMARY KEY,
			owner TEXT NOT NULL,
			balance_cents BIGINT NOT NULL
		)`)
	return err
}

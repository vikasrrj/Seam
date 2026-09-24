// Package checkpoint persists Seam's durable progress state in destination PostgreSQL.
package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"example.com/seam/internal/model"
	"example.com/seam/internal/scan"
	"example.com/seam/internal/schema"
	"example.com/seam/internal/transport"
	"github.com/jackc/pgx/v5"
)

// Store owns the destination checkpoint tables.
type Store struct {
	dsn       string
	gateMu    sync.Mutex
	gateCache map[string]cutoverGateCache
}

type cutoverGateCache struct {
	target *int64
	readAt time.Time
}

func NewStore(dsn string) *Store {
	return &Store{dsn: dsn, gateCache: make(map[string]cutoverGateCache)}
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
	// Serialize every destination write transaction with a promotion. The
	// relation name is resolved only after this shared lock is acquired.
	var epoch int64
	if err := tx.QueryRow(ctx, `SELECT epoch FROM seam_route_fence WHERE id = TRUE FOR SHARE`).Scan(&epoch); err != nil {
		tx.Rollback(ctx)
		conn.Close(context.Background())
		return nil, fmt.Errorf("acquire destination routing fence: %w", err)
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
			source_dsn_sha256 TEXT NOT NULL DEFAULT '',
			source_slot TEXT NOT NULL,
			source_publication TEXT NOT NULL,
			source_system_id TEXT NOT NULL DEFAULT '',
			table_schema TEXT NOT NULL,
			table_name TEXT NOT NULL,
			destination_table TEXT NOT NULL DEFAULT 'accounts',
			kafka_topic TEXT NOT NULL,
			kafka_topic_id TEXT NOT NULL DEFAULT '',
			generation TEXT NOT NULL,
			scan_upper_bound BIGINT NOT NULL,
			discovery_cursor BIGINT,
			discovery_complete BOOLEAN NOT NULL DEFAULT FALSE,
			schema_fingerprint TEXT NOT NULL,
			source_schema JSONB NOT NULL DEFAULT '{}',
			source_schema_fingerprint TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);

		CREATE TABLE IF NOT EXISTS seam_checkpoints (
			job_id TEXT PRIMARY KEY REFERENCES seam_jobs(job_id),
			generation TEXT NOT NULL,
			attempt TEXT NOT NULL,
			owner_id TEXT,
			owner_epoch BIGINT NOT NULL DEFAULT 0,
			owner_lease_expiry TIMESTAMPTZ,
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
			lease_token BIGINT NOT NULL DEFAULT 0,
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
		CREATE TABLE IF NOT EXISTS seam_candidates (
			job_id TEXT NOT NULL REFERENCES seam_jobs(job_id) ON DELETE CASCADE,
			attempt TEXT NOT NULL,
			chunk_min_id BIGINT NOT NULL,
			chunk_max_id BIGINT NOT NULL,
			id BIGINT NOT NULL,
			payload JSONB NOT NULL,
			evicted BOOLEAN NOT NULL DEFAULT FALSE,
			PRIMARY KEY (job_id, attempt, chunk_min_id, id)
		);
		CREATE INDEX IF NOT EXISTS seam_candidates_survivors_idx
			ON seam_candidates(job_id, attempt, chunk_min_id) WHERE evicted = FALSE;
		-- Migrate pre-Phase-1 candidates (fixed owner/balance_cents columns) to
		-- the generic canonical-text payload. The row migration is conditional
		-- in Go: the legacy columns only exist on databases created before
		-- Phase 1. The payload column itself is additive so both shapes coexist
		-- until the migration runs.
		ALTER TABLE seam_candidates ADD COLUMN IF NOT EXISTS payload JSONB;
		CREATE TABLE IF NOT EXISTS seam_route_fence (
			id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
			epoch BIGINT NOT NULL DEFAULT 0
		);
		INSERT INTO seam_route_fence (id, epoch) VALUES (TRUE, 0) ON CONFLICT (id) DO NOTHING;
		CREATE TABLE IF NOT EXISTS seam_promotions (
			shadow_job_id TEXT PRIMARY KEY REFERENCES seam_jobs(job_id),
			live_job_id TEXT NOT NULL REFERENCES seam_jobs(job_id),
			barrier_offset BIGINT NOT NULL,
			retired_table TEXT NOT NULL,
			active_table_oid OID NOT NULL,
			promoted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE TABLE IF NOT EXISTS seam_cutover_gates (
			job_id TEXT PRIMARY KEY REFERENCES seam_jobs(job_id) ON DELETE CASCADE,
			target_offset BIGINT NOT NULL CHECK (target_offset >= 0),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
		);
		ALTER TABLE seam_jobs ADD COLUMN IF NOT EXISTS discovery_cursor BIGINT;
		ALTER TABLE seam_jobs ADD COLUMN IF NOT EXISTS discovery_complete BOOLEAN NOT NULL DEFAULT FALSE;
		ALTER TABLE seam_jobs ADD COLUMN IF NOT EXISTS kafka_topic_id TEXT NOT NULL DEFAULT '';
		ALTER TABLE seam_jobs ADD COLUMN IF NOT EXISTS source_system_id TEXT NOT NULL DEFAULT '';
		ALTER TABLE seam_jobs ADD COLUMN IF NOT EXISTS source_dsn_sha256 TEXT NOT NULL DEFAULT '';
		ALTER TABLE seam_jobs ADD COLUMN IF NOT EXISTS destination_table TEXT NOT NULL DEFAULT 'accounts';
		ALTER TABLE seam_jobs ADD COLUMN IF NOT EXISTS source_schema JSONB NOT NULL DEFAULT '{}';
		ALTER TABLE seam_jobs ADD COLUMN IF NOT EXISTS source_schema_fingerprint TEXT NOT NULL DEFAULT '';
		ALTER TABLE seam_chunks ADD COLUMN IF NOT EXISTS lease_token BIGINT NOT NULL DEFAULT 0;
		ALTER TABLE seam_checkpoints ADD COLUMN IF NOT EXISTS owner_id TEXT;
		ALTER TABLE seam_checkpoints ADD COLUMN IF NOT EXISTS owner_epoch BIGINT NOT NULL DEFAULT 0;
		ALTER TABLE seam_checkpoints ADD COLUMN IF NOT EXISTS owner_lease_expiry TIMESTAMPTZ;
	`)
	if err != nil {
		return err
	}
	return migrateLegacyCandidates(ctx, conn)
}

// migrateLegacyCandidates converts pre-Phase-1 seam_candidates rows (fixed
// owner/balance_cents columns) to the generic canonical-text payload. Fresh
// databases never have the legacy columns and skip the migration entirely.
func migrateLegacyCandidates(ctx context.Context, conn *pgx.Conn) error {
	var hasLegacy bool
	if err := conn.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = 'seam_candidates' AND column_name = 'owner'
		)`).Scan(&hasLegacy); err != nil {
		return fmt.Errorf("inspect seam_candidates columns: %w", err)
	}
	if hasLegacy {
		// A legacy row becomes a one-entry canonical-text payload object keyed
		// by the accounts columns, preserving its evicted state.
		if _, err := conn.Exec(ctx, `
			UPDATE seam_candidates
			SET payload = jsonb_build_array(jsonb_build_object(
				'id', id::text, 'owner', owner::text, 'balance_cents', balance_cents::text))
			WHERE payload IS NULL`); err != nil {
			return fmt.Errorf("migrate legacy seam_candidates payload: %w", err)
		}
		if _, err := conn.Exec(ctx, `ALTER TABLE seam_candidates ALTER COLUMN payload SET NOT NULL`); err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, `ALTER TABLE seam_candidates DROP COLUMN IF EXISTS owner`); err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, `ALTER TABLE seam_candidates DROP COLUMN IF EXISTS balance_cents`); err != nil {
			return err
		}
		return nil
	}
	_, err := conn.Exec(ctx, `ALTER TABLE seam_candidates ALTER COLUMN payload SET NOT NULL`)
	return err
}

// SetCutoverGates atomically asks multiple jobs to stop after the same Kafka
// transaction boundary. Reconcilers may finish a transaction already in
// flight; the promotion coordinator re-reads both checkpoints and advances
// the gate to the maximum observed boundary until they converge.
func (s *Store) SetCutoverGates(ctx context.Context, jobIDs []string, target int64) error {
	if target < 0 || len(jobIDs) == 0 {
		return fmt.Errorf("invalid cutover gate target or empty job set")
	}
	conn, err := s.conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, jobID := range jobIDs {
		tag, err := tx.Exec(ctx, `
			INSERT INTO seam_cutover_gates(job_id, target_offset, updated_at)
			SELECT $1, $2, clock_timestamp() FROM seam_checkpoints WHERE job_id = $1 AND active = TRUE
			ON CONFLICT(job_id) DO UPDATE SET target_offset = EXCLUDED.target_offset, updated_at = EXCLUDED.updated_at`, jobID, target)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("cannot gate inactive or missing job %q", jobID)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.gateMu.Lock()
	for _, jobID := range jobIDs {
		value := target
		s.gateCache[jobID] = cutoverGateCache{target: &value, readAt: time.Now()}
	}
	s.gateMu.Unlock()
	return nil
}

func (s *Store) ClearCutoverGates(ctx context.Context, jobIDs ...string) error {
	if len(jobIDs) == 0 {
		return nil
	}
	conn, err := s.conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, `DELETE FROM seam_cutover_gates WHERE job_id = ANY($1::text[])`, jobIDs); err != nil {
		return err
	}
	s.gateMu.Lock()
	for _, jobID := range jobIDs {
		delete(s.gateCache, jobID)
	}
	s.gateMu.Unlock()
	return nil
}

// CutoverTarget reads through a short cache. This limits the normal CDC path
// to at most ten metadata reads per second. Promotion accounts for a source
// transaction already in flight when a new gate becomes visible.
func (s *Store) CutoverTarget(ctx context.Context, jobID string) (*int64, error) {
	s.gateMu.Lock()
	entry, ok := s.gateCache[jobID]
	if ok && time.Since(entry.readAt) < 100*time.Millisecond {
		s.gateMu.Unlock()
		return cloneInt64(entry.target), nil
	}
	s.gateMu.Unlock()
	conn, err := s.conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	var target int64
	err = conn.QueryRow(ctx, `SELECT target_offset FROM seam_cutover_gates WHERE job_id = $1`, jobID).Scan(&target)
	var value *int64
	if errors.Is(err, pgx.ErrNoRows) {
		err = nil
	} else if err == nil {
		value = &target
	}
	if err != nil {
		return nil, err
	}
	s.gateMu.Lock()
	s.gateCache[jobID] = cutoverGateCache{target: cloneInt64(value), readAt: time.Now()}
	s.gateMu.Unlock()
	return value, nil
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

// FingerprintFor validates that the destination table is column-isomorphic to
// the job's durable source descriptor and additionally holds every
// promotion-safety property Seam requires, then returns the deterministic
// fingerprint of the source schema as the destination's validation token.
func FingerprintFor(ctx context.Context, conn schema.Querier, table string, source *schema.Schema) (string, error) {
	if source == nil {
		return "", fmt.Errorf("source schema descriptor is nil")
	}
	if err := schema.ValidateIsomorphicDestination(ctx, conn, source, table); err != nil {
		return "", fmt.Errorf("destination table %q is not isomorphic to the job's source schema: %w", table, err)
	}
	key := source.PKColumn().Name
	var kind, persistence string
	var indexes, primaryIndexes, keyPrimary, otherConstraints, userTriggers int
	var partition, rowSecurity bool
	if err := conn.QueryRow(ctx, `
		SELECT c.relkind::text, c.relpersistence::text, c.relispartition, c.relrowsecurity OR c.relforcerowsecurity,
			(SELECT COUNT(*) FROM pg_index WHERE indrelid = c.oid),
			(SELECT COUNT(*) FROM pg_index WHERE indrelid = c.oid AND indisprimary),
			(SELECT COUNT(*) FROM pg_constraint p JOIN pg_attribute a
			 ON a.attrelid = p.conrelid AND a.attname = $2
			 WHERE p.conrelid = c.oid AND p.contype = 'p'
			 AND array_length(p.conkey, 1) = 1 AND p.conkey[1] = a.attnum),
			(SELECT COUNT(*) FROM pg_constraint p WHERE p.conrelid = c.oid AND p.contype <> 'p')
			,(SELECT COUNT(*) FROM pg_trigger t WHERE t.tgrelid = c.oid AND NOT t.tgisinternal)
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public' AND c.relname = $1`, table, key).Scan(
		&kind, &persistence, &partition, &rowSecurity, &indexes, &primaryIndexes, &keyPrimary, &otherConstraints, &userTriggers); err != nil {
		return "", err
	}
	if kind != "r" || persistence != "p" || partition || rowSecurity || indexes != 1 || primaryIndexes != 1 || keyPrimary != 1 || otherConstraints != 0 || userTriggers != 0 {
		return "", fmt.Errorf("destination table %q requires a logged ordinary table, exactly one primary key, and no other indexes, constraints, row security, or user triggers", table)
	}
	return source.Fingerprint, nil
}

// CreateJob inserts a new job and checkpoint. It returns the checkpoint.
func (s *Store) CreateJob(ctx context.Context, cfg model.JobConfig, sourceSchema *schema.Schema, upperBound int64) (*model.Checkpoint, error) {
	return s.CreateJobAt(ctx, cfg, sourceSchema, upperBound, 0)
}

// CreateJobAt pins the first complete Kafka transaction after a source
// barrier. The caller must observe that barrier in the broker before scanning.
func (s *Store) CreateJobAt(ctx context.Context, cfg model.JobConfig, sourceSchema *schema.Schema, upperBound, startOffset int64) (*model.Checkpoint, error) {
	if startOffset < 0 {
		return nil, fmt.Errorf("negative start offset %d", startOffset)
	}
	if sourceSchema == nil {
		return nil, fmt.Errorf("source schema descriptor is nil")
	}
	if cfg.DestTable == "" {
		cfg.DestTable = "accounts"
	}
	conn, err := s.conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())

	fp, err := FingerprintFor(ctx, conn, cfg.DestTable, sourceSchema)
	if err != nil {
		return nil, fmt.Errorf("fingerprint destination schema: %w", err)
	}
	sourceSchemaJSON, err := json.Marshal(sourceSchema)
	if err != nil {
		return nil, fmt.Errorf("encode source schema descriptor: %w", err)
	}

	generation := "gen:0"
	attempt := "gen:0:attempt:0"
	checkpoint := &model.Checkpoint{
		JobID:            cfg.JobID,
		Generation:       generation,
		Attempt:          attempt,
		ScanUpperBound:   upperBound,
		CompletedThrough: math.MinInt64,
		NextKafkaOffset:  startOffset,
		Active:           true,
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	// The global route row serializes fresh table reservations with other
	// creators and promotion. Two independent jobs must never write the same
	// physical table at once, even if both observed it empty beforehand.
	var epoch int64
	if err := tx.QueryRow(ctx, `SELECT epoch FROM seam_route_fence WHERE id = TRUE FOR UPDATE`).Scan(&epoch); err != nil {
		return nil, fmt.Errorf("reserve destination route: %w", err)
	}
	var existingJob string
	err = tx.QueryRow(ctx, `
		SELECT j.job_id FROM seam_jobs j JOIN seam_checkpoints cp USING (job_id)
		WHERE j.destination_table = $1 AND cp.active = TRUE LIMIT 1`, cfg.DestTable).Scan(&existingJob)
	if err == nil {
		return nil, fmt.Errorf("destination %q is already owned by active job %q", cfg.DestTable, existingJob)
	}
	if err != pgx.ErrNoRows {
		return nil, fmt.Errorf("check destination route owner: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO seam_jobs (job_id, source_dsn, source_dsn_sha256, source_slot, source_publication, source_system_id, table_schema, table_name, destination_table, kafka_topic, kafka_topic_id, generation, scan_upper_bound, schema_fingerprint, source_schema, source_schema_fingerprint)
		VALUES ($1, '', $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`,
		cfg.JobID, DSNFingerprint(cfg.SourceDSN), cfg.SourceSlot, cfg.SourcePublication, cfg.SourceSystemID, sourceSchema.Namespace, sourceSchema.Table, cfg.DestTable, cfg.KafkaTopic, cfg.KafkaTopicID, generation, upperBound, fp, sourceSchemaJSON, sourceSchema.Fingerprint); err != nil {
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
		SELECT job_id, generation, attempt, COALESCE(owner_id, ''), owner_epoch, owner_lease_expiry,
		       scan_upper_bound, completed_through_id, next_kafka_offset, last_applied_lsn, active
		FROM seam_checkpoints WHERE job_id = $1`, jobID).Scan(
		&cp.JobID, &cp.Generation, &cp.Attempt, &cp.OwnerID, &cp.OwnerEpoch, &cp.OwnerLeaseExpiry,
		&cp.ScanUpperBound, &cp.CompletedThrough, &cp.NextKafkaOffset, &lastLSN, &cp.Active,
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

// AcquireLeadership claims exclusive process ownership for a job and bumps
// its fencing epoch. A live, unexpired owner cannot be displaced. Once a
// lease expires, exactly one contender can advance the epoch and every write
// from the old epoch is rejected by AssertLeadership/UpdateCheckpoint.
func (s *Store) AcquireLeadership(ctx context.Context, jobID, ownerID string, leaseDuration time.Duration) (*model.Checkpoint, error) {
	if ownerID == "" {
		return nil, fmt.Errorf("leadership owner id is empty")
	}
	if leaseDuration <= 0 {
		return nil, fmt.Errorf("leadership lease duration must be positive")
	}
	conn, err := s.conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())

	var cp model.Checkpoint
	var lastLSN *string
	err = conn.QueryRow(ctx, `
		UPDATE seam_checkpoints SET
			owner_id = $2,
			owner_epoch = owner_epoch + 1,
			owner_lease_expiry = clock_timestamp() + make_interval(secs => $3::double precision),
			updated_at = NOW()
		WHERE job_id = $1 AND active = TRUE
		  AND (owner_id IS NULL OR owner_lease_expiry IS NULL OR owner_lease_expiry <= clock_timestamp())
		RETURNING job_id, generation, attempt, owner_id, owner_epoch, owner_lease_expiry,
		          scan_upper_bound, completed_through_id, next_kafka_offset, last_applied_lsn, active`,
		jobID, ownerID, leaseDuration.Seconds()).Scan(
		&cp.JobID, &cp.Generation, &cp.Attempt, &cp.OwnerID, &cp.OwnerEpoch, &cp.OwnerLeaseExpiry,
		&cp.ScanUpperBound, &cp.CompletedThrough, &cp.NextKafkaOffset, &lastLSN, &cp.Active,
	)
	if err == pgx.ErrNoRows {
		var currentOwner *string
		var currentEpoch int64
		var expiry *time.Time
		var active bool
		loadErr := conn.QueryRow(ctx, `
			SELECT owner_id, owner_epoch, owner_lease_expiry, active
			FROM seam_checkpoints WHERE job_id = $1`, jobID).Scan(&currentOwner, &currentEpoch, &expiry, &active)
		if loadErr == pgx.ErrNoRows {
			return nil, fmt.Errorf("job %q not found", jobID)
		}
		if loadErr != nil {
			return nil, loadErr
		}
		if !active {
			return nil, fmt.Errorf("job %q is inactive", jobID)
		}
		owner := "<none>"
		if currentOwner != nil {
			owner = *currentOwner
		}
		return nil, fmt.Errorf("job %q leadership is held by %q at epoch %d until %v", jobID, owner, currentEpoch, expiry)
	}
	if err != nil {
		return nil, fmt.Errorf("acquire job leadership: %w", err)
	}
	if lastLSN != nil {
		cp.LastAppliedLSN = *lastLSN
	}
	return &cp, nil
}

// RenewLeadership extends a lease only for the current owner and epoch. An
// expired lease cannot be resurrected, even if no contender has taken over.
func (s *Store) RenewLeadership(ctx context.Context, cp *model.Checkpoint, leaseDuration time.Duration) error {
	if cp == nil || cp.OwnerID == "" || cp.OwnerEpoch <= 0 {
		return fmt.Errorf("cannot renew unowned checkpoint")
	}
	if leaseDuration <= 0 {
		return fmt.Errorf("leadership lease duration must be positive")
	}
	conn, err := s.conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	var expiry time.Time
	err = conn.QueryRow(ctx, `
		UPDATE seam_checkpoints SET
			owner_lease_expiry = clock_timestamp() + make_interval(secs => $4::double precision),
			updated_at = NOW()
		WHERE job_id = $1 AND owner_id = $2 AND owner_epoch = $3
		  AND owner_lease_expiry > clock_timestamp() AND active = TRUE
		RETURNING owner_lease_expiry`, cp.JobID, cp.OwnerID, cp.OwnerEpoch, leaseDuration.Seconds()).Scan(&expiry)
	if err == pgx.ErrNoRows {
		return fmt.Errorf("job %q leadership epoch %d is expired or fenced", cp.JobID, cp.OwnerEpoch)
	}
	if err != nil {
		return err
	}
	cp.OwnerLeaseExpiry = &expiry
	return nil
}

// ReleaseLeadership gives up a live lease so a cleanly stopped process does
// not force its successor to wait for expiry. The epoch remains monotonic.
func (s *Store) ReleaseLeadership(ctx context.Context, cp *model.Checkpoint) error {
	if cp == nil || cp.OwnerID == "" || cp.OwnerEpoch <= 0 {
		return nil
	}
	conn, err := s.conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	tag, err := conn.Exec(ctx, `
		UPDATE seam_checkpoints SET owner_id = NULL, owner_lease_expiry = NULL, updated_at = NOW()
		WHERE job_id = $1 AND owner_id = $2 AND owner_epoch = $3`, cp.JobID, cp.OwnerID, cp.OwnerEpoch)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("job %q leadership epoch %d is no longer owned", cp.JobID, cp.OwnerEpoch)
	}
	return nil
}

// AssertLeadership locks and validates the job's ownership row inside a
// destination transaction. Because takeover updates the same row, either this
// transaction finishes before takeover or the stale transaction fails before
// it mutates destination data.
func (s *Store) AssertLeadership(ctx context.Context, tx pgx.Tx, cp *model.Checkpoint) error {
	if cp == nil {
		return fmt.Errorf("nil checkpoint ownership")
	}
	var ownerID *string
	var epoch int64
	var leaseValid bool
	var active bool
	if err := tx.QueryRow(ctx, `
		SELECT owner_id, owner_epoch,
		       owner_lease_expiry IS NOT NULL AND owner_lease_expiry > clock_timestamp(), active
		FROM seam_checkpoints WHERE job_id = $1 FOR SHARE`, cp.JobID).Scan(&ownerID, &epoch, &leaseValid, &active); err != nil {
		return fmt.Errorf("read job leadership: %w", err)
	}
	// Epoch zero is retained only for direct legacy/unit harnesses. Production
	// acquires a positive epoch before discovery or reconciliation.
	if cp.OwnerEpoch == 0 && cp.OwnerID == "" && epoch == 0 && ownerID == nil && active {
		return nil
	}
	if !active || ownerID == nil || *ownerID != cp.OwnerID || epoch != cp.OwnerEpoch || !leaseValid {
		return fmt.Errorf("job %q leadership %q epoch %d is expired or fenced", cp.JobID, cp.OwnerID, cp.OwnerEpoch)
	}
	return nil
}

// JobRecord is the persisted job metadata.
type JobRecord struct {
	Config            model.JobConfig
	Generation        string
	ScanUpperBound    int64
	SchemaFingerprint string
	SourceDSNHash     string
	// SourceSchema is the durable descriptor of the source table, pinned (and
	// epoch-validated) on this job record.
	SourceSchema *schema.Schema
	// SourceSchemaFingerprint echoes the epoch fingerprint for cheap checks
	// without deserializing the descriptor.
	SourceSchemaFingerprint string
}

// DSNFingerprint prevents accidental source reconfiguration without storing
// the connection string (and its embedded credentials) in destination state.
func DSNFingerprint(dsn string) string {
	sum := sha256.Sum256([]byte(dsn))
	return hex.EncodeToString(sum[:])
}

// LoadJobRecord returns the persisted configuration and generation for a job,
// deserializing the durable source schema descriptor.
func (s *Store) LoadJobRecord(ctx context.Context, jobID string) (*JobRecord, error) {
	conn, err := s.conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())

	var rec JobRecord
	var schemaJSON []byte
	if err := conn.QueryRow(ctx, `
		SELECT job_id, source_dsn_sha256, source_slot, source_publication, source_system_id, destination_table, kafka_topic, kafka_topic_id, generation, scan_upper_bound, schema_fingerprint, source_schema, source_schema_fingerprint
		FROM seam_jobs WHERE job_id = $1`, jobID).Scan(
		&rec.Config.JobID, &rec.SourceDSNHash, &rec.Config.SourceSlot, &rec.Config.SourcePublication, &rec.Config.SourceSystemID, &rec.Config.DestTable, &rec.Config.KafkaTopic, &rec.Config.KafkaTopicID, &rec.Generation, &rec.ScanUpperBound, &rec.SchemaFingerprint, &schemaJSON, &rec.SourceSchemaFingerprint,
	); err != nil {
		return nil, err
	}
	if len(schemaJSON) > 0 {
		var descriptor schema.Schema
		if err := json.Unmarshal(schemaJSON, &descriptor); err != nil {
			return nil, fmt.Errorf("decode job %q source schema descriptor: %w", jobID, err)
		}
		rec.SourceSchema = &descriptor
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
func (s *Store) UpdateCheckpoint(ctx context.Context, tx pgx.Tx, cp *model.Checkpoint, expectedOffset int64) error {
	tag, err := tx.Exec(ctx, `
		UPDATE seam_checkpoints SET
			scan_upper_bound = $4,
			completed_through_id = $5,
			next_kafka_offset = $6,
			last_applied_lsn = $7,
			active = $8,
			updated_at = NOW()
		WHERE job_id = $1 AND generation = $2 AND attempt = $3 AND active = TRUE
		  AND next_kafka_offset = $9 AND next_kafka_offset <= $6
		  AND (($10 = 0 AND $11 = '' AND owner_epoch = 0 AND owner_id IS NULL)
		       OR (owner_epoch = $10 AND owner_id = $11 AND owner_lease_expiry > clock_timestamp()))`,
		cp.JobID, cp.Generation, cp.Attempt, cp.ScanUpperBound, cp.CompletedThrough, cp.NextKafkaOffset, cp.LastAppliedLSN, cp.Active, expectedOffset,
		cp.OwnerEpoch, cp.OwnerID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("checkpoint %q changed by another process or would regress", cp.JobID)
	}
	return nil
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

// LeaseChunk atomically claims the next chunk for a worker: it selects the
// oldest chunk of the given attempt that is either pending or has an expired
// lease (a previous worker that stopped heartbeating). It returns nil, nil if
// no such chunk is available.
func (s *Store) LeaseChunk(ctx context.Context, jobID, attempt, workerID string, leaseDuration time.Duration) (*model.Chunk, error) {
	return s.LeaseChunkOwned(ctx, jobID, attempt, workerID, "", 0, leaseDuration)
}

// LeaseChunkOwned leases work only while the supplied job leadership epoch is
// current. The zero epoch is kept for direct legacy test/admin callers whose
// jobs have never acquired process leadership.
func (s *Store) LeaseChunkOwned(ctx context.Context, jobID, attempt, workerID, ownerID string, ownerEpoch int64, leaseDuration time.Duration) (*model.Chunk, error) {
	conn, err := s.conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())

	var chunk model.Chunk
	var leaseStart, leaseExpiry, heartbeatAt *time.Time
	var lowOffset, highOffset *int64
	var lowLSN, highLSN *string
	var leaseWorkerID, errorMessage *string
	err = conn.QueryRow(ctx, `
		UPDATE seam_chunks SET
			status = $1,
			worker_id = $2,
			lease_token = lease_token + 1,
			lease_start = NOW(),
			lease_expiry = NOW() + make_interval(secs => $3::double precision),
			heartbeat_at = NOW(),
			updated_at = NOW()
		WHERE ctid = (
			SELECT ctid FROM seam_chunks
			WHERE job_id = $4
			  AND attempt = $5
			  AND EXISTS (
				SELECT 1 FROM seam_checkpoints cp
				WHERE cp.job_id = seam_chunks.job_id AND cp.attempt = $5 AND cp.active = TRUE
				  AND (($8 = 0 AND $9 = '' AND cp.owner_epoch = 0 AND cp.owner_id IS NULL)
				       OR (cp.owner_epoch = $8 AND cp.owner_id = $9 AND cp.owner_lease_expiry > clock_timestamp()))
			  )
			  AND (
				status = $6
				OR (status = $7 AND lease_expiry < NOW())
			  )
			ORDER BY chunk_min_id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING
			job_id, chunk_min_id, chunk_max_id, attempt, status,
			worker_id, lease_token, lease_start, lease_expiry, heartbeat_at,
			low_offset, high_offset, low_lsn, high_lsn,
			rows_scanned, rows_applied, error_message,
			created_at, updated_at`,
		model.ChunkLeased, workerID, leaseDuration.Seconds(),
		jobID, attempt, model.ChunkPending, model.ChunkLeased, ownerEpoch, ownerID,
	).Scan(
		&chunk.JobID, &chunk.ChunkMinID, &chunk.ChunkMaxID, &chunk.Attempt, &chunk.Status,
		&leaseWorkerID, &chunk.LeaseToken, &leaseStart, &leaseExpiry, &heartbeatAt,
		&lowOffset, &highOffset, &lowLSN, &highLSN,
		&chunk.RowsScanned, &chunk.RowsApplied, &errorMessage,
		&chunk.CreatedAt, &chunk.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if leaseWorkerID != nil {
		chunk.WorkerID = *leaseWorkerID
	}
	if errorMessage != nil {
		chunk.ErrorMessage = *errorMessage
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
// A chunk may only be mutated by the worker that currently owns its lease:
// the statement is a compare-and-swap on worker_id, so a worker whose lease a
// sibling already reassigned cannot overwrite the row (it fails instead).
func (s *Store) UpdateChunk(ctx context.Context, tx pgx.Tx, chunk *model.Chunk) error {
	tag, err := tx.Exec(ctx, `
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
		WHERE job_id = $1 AND chunk_min_id = $2 AND chunk_max_id = $3 AND attempt = $4
		  AND worker_id = $6 AND lease_token = $17
		  AND EXISTS (SELECT 1 FROM seam_checkpoints cp WHERE cp.job_id = seam_chunks.job_id AND cp.attempt = $4 AND cp.active = TRUE FOR SHARE)`,
		chunk.JobID, chunk.ChunkMinID, chunk.ChunkMaxID, chunk.Attempt, chunk.Status,
		chunk.WorkerID, chunk.LeaseStart, chunk.LeaseExpiry, chunk.HeartbeatAt,
		chunk.LowOffset, chunk.HighOffset, chunk.LowLSN, chunk.HighLSN,
		chunk.RowsScanned, chunk.RowsApplied, chunk.ErrorMessage, chunk.LeaseToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("chunk %s (%s) is no longer owned by worker %q", chunk.Range(), chunk.Attempt, chunk.WorkerID)
	}
	return nil
}

// StageCandidates persists one snapshot window under the generic schema
// descriptor. A prior CDC touch may already have inserted an evicted tombstone;
// the upsert fills in the canonical-text payload while preserving that
// tombstone.
func (s *Store) StageCandidates(ctx context.Context, tx pgx.Tx, chunk *model.Chunk, candidates []model.Row, descriptor *schema.Schema) error {
	if descriptor == nil {
		return fmt.Errorf("stage candidates: nil schema descriptor")
	}
	const batchRows = 1000
	for start := 0; start < len(candidates); start += batchRows {
		end := start + batchRows
		if end > len(candidates) {
			end = len(candidates)
		}
		ids := make([]int64, 0, end-start)
		payloads := make([]json.RawMessage, 0, end-start)
		for _, row := range candidates[start:end] {
			key, err := descriptor.KeyFromRow(&row)
			if err != nil {
				return fmt.Errorf("stage candidate: %w", err)
			}
			if key < chunk.ChunkMinID || key > chunk.ChunkMaxID {
				return fmt.Errorf("candidate id %d is outside chunk %s", key, chunk.Range())
			}
			obj, err := candidatePayload(descriptor, &row)
			if err != nil {
				return fmt.Errorf("encode candidate id %d payload: %w", key, err)
			}
			ids = append(ids, key)
			payloads = append(payloads, obj)
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO seam_candidates (job_id, attempt, chunk_min_id, chunk_max_id, id, payload, evicted)
			SELECT $1, $2, $3, $4, id, payload, FALSE
			FROM unnest($5::bigint[], $6::jsonb[]) AS batch(id, payload)
			ON CONFLICT (job_id, attempt, chunk_min_id, id) DO UPDATE
			SET payload = EXCLUDED.payload,
			    chunk_max_id = EXCLUDED.chunk_max_id,
			    evicted = seam_candidates.evicted OR EXCLUDED.evicted`,
			chunk.JobID, chunk.Attempt, chunk.ChunkMinID, chunk.ChunkMaxID, ids, payloads)
		if err != nil {
			return fmt.Errorf("stage %d candidates for chunk %s: %w", end-start, chunk.Range(), err)
		}
	}
	return nil
}

// candidatePayload renders one candidate as a canonical-text JSON object keyed
// by column name: integer cells as decimal text, every other cell as its exact
// canonical text, SQL NULL as JSON null. PostgreSQL's ::text casts invert the
// object when the sink applies it, so no value is ever lossy-converted.
func candidatePayload(descriptor *schema.Schema, row *model.Row) (json.RawMessage, error) {
	obj := make(map[string]any, len(descriptor.Columns))
	for i, col := range descriptor.Columns {
		if len(row.Values) <= i {
			return nil, fmt.Errorf("row has %d values; schema expects %d", len(row.Values), len(descriptor.Columns))
		}
		value := row.Values[i]
		switch value.Kind {
		case model.ValueNull:
			obj[col.Name] = nil
		case model.ValueInt64:
			obj[col.Name] = strconv.FormatInt(value.Int, 10)
		case model.ValueText:
			obj[col.Name] = value.Text
		default:
			return nil, fmt.Errorf("column %q has unsupported value kind %d", col.Name, value.Kind)
		}
	}
	payload, err := json.Marshal([]any{obj})
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// EvictCandidates durably records keys touched by CDC. It also works before
// the snapshot worker arrives by inserting tombstones which StageCandidates
// later preserves.
func (s *Store) EvictCandidates(ctx context.Context, tx pgx.Tx, chunk *model.Chunk, keys []int64) error {
	if len(keys) == 0 {
		return nil
	}
	filtered := make([]int64, 0, len(keys))
	seen := make(map[int64]struct{}, len(keys))
	for _, key := range keys {
		if key < chunk.ChunkMinID || key > chunk.ChunkMaxID {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		filtered = append(filtered, key)
	}
	if len(filtered) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO seam_candidates (job_id, attempt, chunk_min_id, chunk_max_id, id, evicted)
		SELECT $1, $2, $3, $4, id, TRUE FROM unnest($5::bigint[]) AS touched(id)
		ON CONFLICT (job_id, attempt, chunk_min_id, id) DO UPDATE SET evicted = TRUE`,
		chunk.JobID, chunk.Attempt, chunk.ChunkMinID, chunk.ChunkMaxID, filtered)
	if err != nil {
		return fmt.Errorf("evict candidates for chunk %s: %w", chunk.Range(), err)
	}
	return nil
}

func (s *Store) ClearCandidates(ctx context.Context, tx pgx.Tx, chunk *model.Chunk) error {
	_, err := tx.Exec(ctx, `DELETE FROM seam_candidates WHERE job_id = $1 AND attempt = $2 AND chunk_min_id = $3`, chunk.JobID, chunk.Attempt, chunk.ChunkMinID)
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
		worker_id, lease_token, lease_start, lease_expiry, heartbeat_at,
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

// HasIncompleteChunks checks for unfinished work without materializing the
// whole manifest. Promotion and recovery need existence, not every row.
func (s *Store) HasIncompleteChunks(ctx context.Context, jobID, attempt string) (bool, error) {
	conn, err := s.conn(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close(context.Background())
	var incomplete bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS(
		SELECT 1 FROM seam_chunks WHERE job_id = $1 AND attempt = $2 AND status != $3
	)`, jobID, attempt, model.ChunkCompleted).Scan(&incomplete); err != nil {
		return false, err
	}
	return incomplete, nil
}

// BeginAttempt atomically fences the previous attempt and reschedules its
// unfinished chunks. An old worker may retain memory, but its chunk and
// checkpoint writes will no longer match the active attempt.
func (s *Store) BeginAttempt(ctx context.Context, jobID, oldAttempt, newAttempt string, expectedOffset int64) error {
	cp, err := s.LoadCheckpoint(ctx, jobID)
	if err != nil {
		return err
	}
	if cp == nil {
		return fmt.Errorf("job %q not found", jobID)
	}
	// This compatibility entry point is used by direct test/admin callers. A
	// production owner calls BeginAttemptOwned with its positive fencing epoch.
	cp.OwnerID = ""
	cp.OwnerEpoch = 0
	return s.BeginAttemptOwned(ctx, cp, oldAttempt, newAttempt, expectedOffset)
}

// BeginAttemptOwned fences the previous attempt while requiring the caller's
// current leadership epoch in the same transaction.
func (s *Store) BeginAttemptOwned(ctx context.Context, cp *model.Checkpoint, oldAttempt, newAttempt string, expectedOffset int64) error {
	conn, err := s.conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := s.AssertLeadership(ctx, tx, cp); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE seam_checkpoints SET attempt = $3, updated_at = NOW()
		WHERE job_id = $1 AND attempt = $2 AND next_kafka_offset = $4 AND active = TRUE`,
		cp.JobID, oldAttempt, newAttempt, expectedOffset)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("job %q attempt changed concurrently", cp.JobID)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO seam_chunks (
			job_id, chunk_min_id, chunk_max_id, attempt, status,
			worker_id, lease_start, lease_expiry, heartbeat_at,
			low_offset, high_offset, low_lsn, high_lsn,
			rows_scanned, rows_applied, error_message
		)
		SELECT job_id, chunk_min_id, chunk_max_id, $3, $4,
			NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, NULL
		FROM seam_chunks
		WHERE job_id = $1 AND attempt = $2 AND status != $5
		ON CONFLICT (job_id, chunk_min_id, chunk_max_id, attempt) DO NOTHING`,
		cp.JobID, oldAttempt, newAttempt, model.ChunkPending, model.ChunkCompleted)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
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
		WHERE job_id = $2 AND status = ANY($3)
		  AND attempt = (SELECT attempt FROM seam_checkpoints WHERE job_id = $2)
		  AND lease_expiry < $4`,
		model.ChunkPending, jobID, []string{string(model.ChunkLeased), string(model.ChunkScanning), string(model.ChunkReconciling), string(model.ChunkCommitting)}, before)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// HeartbeatChunk renews the owner's lease on a leased chunk: it refreshes the
// heartbeat timestamp and rolls the lease expiry forward by leaseDuration. Only
// the worker that owns the lease may renew it; renewing a chunk that was
// already reassigned fails.
func (s *Store) HeartbeatChunk(ctx context.Context, chunk *model.Chunk, leaseDuration time.Duration) error {
	return s.HeartbeatChunkOwned(ctx, chunk, "", 0, leaseDuration)
}

// HeartbeatChunkOwned renews a chunk only while both its worker lease token
// and the process leadership epoch remain current.
func (s *Store) HeartbeatChunkOwned(ctx context.Context, chunk *model.Chunk, ownerID string, ownerEpoch int64, leaseDuration time.Duration) error {
	conn, err := s.conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	tag, err := conn.Exec(ctx, `
		UPDATE seam_chunks SET heartbeat_at = NOW(), lease_expiry = NOW() + make_interval(secs => $1::double precision), updated_at = NOW()
		WHERE job_id = $2 AND chunk_min_id = $3 AND chunk_max_id = $4 AND attempt = $5
		  AND worker_id = $6 AND lease_token = $7
		  AND EXISTS (
			SELECT 1 FROM seam_checkpoints cp
			WHERE cp.job_id = seam_chunks.job_id AND cp.attempt = $5 AND cp.active = TRUE
			  AND (($8 = 0 AND $9 = '' AND cp.owner_epoch = 0 AND cp.owner_id IS NULL)
			       OR (cp.owner_epoch = $8 AND cp.owner_id = $9 AND cp.owner_lease_expiry > clock_timestamp()))
		  )`,
		leaseDuration.Seconds(), chunk.JobID, chunk.ChunkMinID, chunk.ChunkMaxID, chunk.Attempt, chunk.WorkerID, chunk.LeaseToken, ownerEpoch, ownerID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("chunk %s (%s) is no longer owned by worker %q", chunk.Range(), chunk.Attempt, chunk.WorkerID)
	}
	return nil
}

func scanChunks(rows pgx.Rows) ([]model.Chunk, error) {
	var chunks []model.Chunk
	for rows.Next() {
		var chunk model.Chunk
		var leaseStart, leaseExpiry, heartbeatAt *time.Time
		var lowOffset, highOffset *int64
		var lowLSN, highLSN *string
		var workerID, errorMessage *string
		if err := rows.Scan(
			&chunk.JobID, &chunk.ChunkMinID, &chunk.ChunkMaxID, &chunk.Attempt, &chunk.Status,
			&workerID, &chunk.LeaseToken, &leaseStart, &leaseExpiry, &heartbeatAt,
			&lowOffset, &highOffset, &lowLSN, &highLSN,
			&chunk.RowsScanned, &chunk.RowsApplied, &errorMessage,
			&chunk.CreatedAt, &chunk.UpdatedAt,
		); err != nil {
			return nil, err
		}
		chunk.LeaseStart = leaseStart
		if workerID != nil {
			chunk.WorkerID = *workerID
		}
		if errorMessage != nil {
			chunk.ErrorMessage = *errorMessage
		}
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

// DiscoverAndCreateChunks scans the source table and persists a gap-free
// manifest under the job's durable schema descriptor.
func (s *Store) DiscoverAndCreateChunks(ctx context.Context, jobID, attempt string, sourceDSN string, descriptor *schema.Schema, upperBound int64, chunkSize int) error {
	return s.discoverAndCreateChunksFor(ctx, nil, jobID, attempt, sourceDSN, descriptor, upperBound, chunkSize)
}

// DiscoverAndCreateChunksFor is DiscoverAndCreateChunks with an explicit
// descriptor; see scan.NewChunkReaderFor for identifier rules.
func (s *Store) DiscoverAndCreateChunksFor(ctx context.Context, jobID, attempt string, sourceDSN string, descriptor *schema.Schema, upperBound int64, chunkSize int) error {
	return s.discoverAndCreateChunksFor(ctx, nil, jobID, attempt, sourceDSN, descriptor, upperBound, chunkSize)
}

// DiscoverAndCreateChunksForOwned persists each discovered range only while
// cp remains the current leadership epoch. Source reads can finish after a
// takeover, but their results cannot enter the durable manifest.
func (s *Store) DiscoverAndCreateChunksForOwned(ctx context.Context, cp *model.Checkpoint, sourceDSN string, descriptor *schema.Schema, upperBound int64, chunkSize int) error {
	if cp == nil {
		return fmt.Errorf("discover chunks: nil checkpoint ownership")
	}
	return s.discoverAndCreateChunksFor(ctx, cp, cp.JobID, cp.Attempt, sourceDSN, descriptor, upperBound, chunkSize)
}

func (s *Store) discoverAndCreateChunksFor(ctx context.Context, cp *model.Checkpoint, jobID, attempt string, sourceDSN string, descriptor *schema.Schema, upperBound int64, chunkSize int) error {
	if chunkSize < 1 {
		return fmt.Errorf("discover chunks: chunk size must be positive")
	}
	reader, err := scan.NewChunkReaderFor(ctx, sourceDSN, descriptor.Table)
	if err != nil {
		return fmt.Errorf("discover chunks: %w", err)
	}
	conn, err := s.conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	for {
		var cursor *int64
		var complete bool
		var storedUpper int64
		var activeAttempt string
		err := conn.QueryRow(ctx, `
			SELECT j.discovery_cursor, j.discovery_complete, j.scan_upper_bound, cp.attempt
			FROM seam_jobs j JOIN seam_checkpoints cp USING (job_id)
			WHERE j.job_id = $1`, jobID).Scan(&cursor, &complete, &storedUpper, &activeAttempt)
		if err != nil {
			return fmt.Errorf("load discovery frontier: %w", err)
		}
		if storedUpper != upperBound || activeAttempt != attempt {
			return fmt.Errorf("discovery job %q upper bound or attempt changed", jobID)
		}
		if complete {
			return nil
		}
		if cursor == nil {
			var existing bool
			if err := conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM seam_chunks WHERE job_id = $1)`, jobID).Scan(&existing); err != nil {
				return err
			}
			if existing {
				return fmt.Errorf("job %q has chunks but no discovery cursor; legacy manifest cannot be proved complete", jobID)
			}
		}
		chunk, ok, err := reader.NextChunkFrom(ctx, cursor, upperBound, chunkSize)
		if err != nil {
			return fmt.Errorf("discover chunk: %w", err)
		}
		logical, err := discoveryRange(cursor, chunk, ok, upperBound)
		if err != nil {
			return err
		}
		logicalMin, maxID := logical.Min, logical.Max
		sealed := !ok || maxID == upperBound
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		var currentCursor *int64
		var currentComplete bool
		var currentAttempt string
		err = tx.QueryRow(ctx, `
			SELECT j.discovery_cursor, j.discovery_complete, cp.attempt
			FROM seam_jobs j JOIN seam_checkpoints cp USING (job_id)
			WHERE j.job_id = $1 FOR UPDATE OF j, cp`, jobID).Scan(&currentCursor, &currentComplete, &currentAttempt)
		if err != nil {
			tx.Rollback(ctx)
			return err
		}
		if cp != nil {
			if err := s.AssertLeadership(ctx, tx, cp); err != nil {
				tx.Rollback(ctx)
				return err
			}
		}
		if currentComplete || currentAttempt != attempt || !equalCursor(currentCursor, cursor) {
			tx.Rollback(ctx)
			continue
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO seam_chunks (job_id, chunk_min_id, chunk_max_id, attempt, status)
			VALUES ($1, $2, $3, $4, $5)`, jobID, logicalMin, maxID, attempt, model.ChunkPending)
		if err == nil {
			_, err = tx.Exec(ctx, `UPDATE seam_jobs SET discovery_cursor = $2, discovery_complete = $3 WHERE job_id = $1`, jobID, maxID, sealed)
		}
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("persist discovery range [%d,%d]: %w", logicalMin, maxID, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		if sealed {
			return nil
		}
	}
}

// discoveryRange expands each sparse page to the end of the previous page.
// A terminal page covers keys deleted while discovery was running. CDC owns
// inserts and updates that occur after the source barrier.
func discoveryRange(cursor *int64, page model.ChunkRange, found bool, upperBound int64) (model.ChunkRange, error) {
	minID := int64(math.MinInt64)
	if cursor != nil {
		if *cursor == math.MaxInt64 {
			return model.ChunkRange{}, fmt.Errorf("discovery cursor at maximum without a seal")
		}
		minID = *cursor + 1
	}
	maxID := upperBound
	if found {
		maxID = page.Max
		if page.Min < minID || page.Min > page.Max {
			return model.ChunkRange{}, fmt.Errorf("invalid source page %s after %d", page, minID)
		}
	}
	if maxID < minID || maxID > upperBound {
		return model.ChunkRange{}, fmt.Errorf("invalid discovery range [%d,%d] for upper bound %d", minID, maxID, upperBound)
	}
	return model.ChunkRange{Min: minID, Max: maxID}, nil
}

func equalCursor(a, b *int64) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

// DiscoveryComplete is a durable seal; callers must not infer completion from
// an empty lease queue or from the last existing source key.
func (s *Store) DiscoveryComplete(ctx context.Context, jobID string) (bool, error) {
	conn, err := s.conn(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close(context.Background())
	var complete bool
	var upperBound int64
	if err := conn.QueryRow(ctx, `SELECT discovery_complete, scan_upper_bound FROM seam_jobs WHERE job_id = $1`, jobID).Scan(&complete, &upperBound); err != nil {
		return false, err
	}
	if !complete {
		return false, nil
	}
	rows, err := conn.Query(ctx, `
		SELECT DISTINCT chunk_min_id, chunk_max_id FROM seam_chunks
		WHERE job_id = $1 ORDER BY chunk_min_id, chunk_max_id`, jobID)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	var ranges []model.ChunkRange
	for rows.Next() {
		var range_ model.ChunkRange
		if err := rows.Scan(&range_.Min, &range_.Max); err != nil {
			return false, err
		}
		ranges = append(ranges, range_)
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if err := validateManifestRanges(ranges, upperBound); err != nil {
		return false, fmt.Errorf("job %q has invalid sealed chunk coverage: %w", jobID, err)
	}
	return complete, nil
}

func validateManifestRanges(ranges []model.ChunkRange, upperBound int64) error {
	if len(ranges) == 0 || ranges[0].Min != math.MinInt64 {
		return fmt.Errorf("manifest does not start at minimum int64")
	}
	for i, range_ := range ranges {
		if range_.Min > range_.Max {
			return fmt.Errorf("invalid chunk range %s", range_)
		}
		if i > 0 {
			prior := ranges[i-1]
			if prior.Max == math.MaxInt64 || range_.Min != prior.Max+1 {
				return fmt.Errorf("gap or overlap between %s and %s", prior, range_)
			}
		}
	}
	if ranges[len(ranges)-1].Max != upperBound {
		return fmt.Errorf("manifest ends at %d, expected %d", ranges[len(ranges)-1].Max, upperBound)
	}
	return nil
}

// EnsureDestinationTable creates the destination accounts table if needed.
func (s *Store) EnsureDestinationTable(ctx context.Context) error {
	return s.EnsureDestinationTableFor(ctx, "accounts")
}

func (s *Store) EnsureDestinationTableFor(ctx context.Context, table string) error {
	if table != "accounts" && table != "accounts_shadow" {
		return fmt.Errorf("unsupported destination table %q", table)
	}
	conn, err := s.conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	_, err = conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGINT PRIMARY KEY,
			owner TEXT NOT NULL,
			balance_cents BIGINT NOT NULL
		)`, table))
	return err
}

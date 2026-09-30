package snowflake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"example.com/seam/internal/model"
)

// BackfillState is the durable Snowflake-side lifecycle. Transitions are
// monotonic; crash recovery resumes from persisted chunk state rather than
// inferring progress from target rows.
type BackfillState string

const (
	BackfillDiscovering   BackfillState = "discovering"
	BackfillRunning       BackfillState = "running"
	BackfillReadyToVerify BackfillState = "ready_to_verify"
	BackfillVerifying     BackfillState = "verifying"
	BackfillReady         BackfillState = "ready"
	BackfillPromoting     BackfillState = "promoting"
	BackfillCompleted     BackfillState = "completed"
	BackfillFailed        BackfillState = "failed"
)

type BackfillSpec struct {
	JobID          string
	Attempt        string
	ShadowTable    string
	ScanUpperBound int64
	Chunks         []model.ChunkRange
}

type BackfillJob struct {
	Spec               BackfillSpec
	State              BackfillState
	ShadowRouteID      string
	StartOffset        int64
	CompletedChunks    int
	ValidationTable    string
	ValidationMarkerID string
	ValidationOffset   int64
	ValidationReady    bool
	PromotionMarkerID  string
	PromotionOffset    int64
	ErrorMessage       string
}

type ChunkLease struct {
	StreamID   string
	JobID      string
	Attempt    string
	Range      model.ChunkRange
	WorkerID   string
	LeaseToken int64
	ExpiresAt  time.Time
}

func (spec BackfillSpec) validate(cfg Config) error {
	if err := spec.validateIdentity(cfg); err != nil {
		return err
	}
	if len(spec.Chunks) == 0 {
		return fmt.Errorf("Snowflake backfill manifest must contain at least one sealed range")
	}
	expectedMin := int64(math.MinInt64)
	for index, chunk := range spec.Chunks {
		if chunk.Min != expectedMin || chunk.Max < chunk.Min || chunk.Max > spec.ScanUpperBound {
			return fmt.Errorf("Snowflake backfill manifest is not gap-free at chunk %d: got %s, expected minimum %d through upper bound %d", index, chunk, expectedMin, spec.ScanUpperBound)
		}
		if chunk.Max == math.MaxInt64 {
			if index != len(spec.Chunks)-1 {
				return fmt.Errorf("Snowflake backfill manifest continues after maximum key")
			}
		} else {
			expectedMin = chunk.Max + 1
		}
	}
	if spec.Chunks[len(spec.Chunks)-1].Max != spec.ScanUpperBound {
		return fmt.Errorf("Snowflake backfill manifest ends at %d, scan upper bound is %d", spec.Chunks[len(spec.Chunks)-1].Max, spec.ScanUpperBound)
	}
	return nil
}

func (spec BackfillSpec) validateIdentity(cfg Config) error {
	if strings.TrimSpace(spec.JobID) == "" || strings.TrimSpace(spec.Attempt) == "" {
		return fmt.Errorf("Snowflake backfill job ID and attempt are required")
	}
	if !identifierPattern.MatchString(spec.ShadowTable) {
		return fmt.Errorf("invalid Snowflake shadow table identifier %q", spec.ShadowTable)
	}
	if strings.EqualFold(spec.ShadowTable, cfg.LiveTable) {
		return fmt.Errorf("Snowflake shadow table must differ from live table %q", cfg.LiveTable)
	}
	return nil
}

func shadowRouteID(jobID, attempt string) string { return "shadow:" + jobID + ":" + attempt }

// PrepareBackfill creates a new empty typed shadow and activates its CDC route
// before the source upper bound is sampled. This ordering closes the
// insert-above-upper-bound race: every source change after preparation either
// appears in a later scan or is delivered to the shadow through CDC.
func (s *Store) PrepareBackfill(ctx context.Context, jobID, attempt, shadowTable string) (*BackfillJob, error) {
	spec := BackfillSpec{JobID: jobID, Attempt: attempt, ShadowTable: shadowTable}
	if err := spec.validateIdentity(s.cfg); err != nil {
		return nil, err
	}
	if existing, err := s.LoadBackfill(ctx, spec.JobID); err == nil {
		if existing.Spec.JobID == spec.JobID && existing.Spec.Attempt == spec.Attempt && strings.EqualFold(existing.Spec.ShadowTable, spec.ShadowTable) {
			return existing, nil
		}
		return nil, fmt.Errorf("Snowflake backfill job %q already exists with different immutable configuration", spec.JobID)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	var active int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+s.cfg.internal("BACKFILL_JOBS")+" WHERE STREAM_ID = ? AND STATE NOT IN (?, ?)", s.cfg.StreamID, string(BackfillCompleted), string(BackfillFailed)).Scan(&active); err != nil {
		return nil, fmt.Errorf("check active Snowflake backfills: %w", err)
	}
	if active != 0 {
		return nil, fmt.Errorf("Snowflake stream %q already has %d active backfill job(s)", s.cfg.StreamID, active)
	}
	createSQL, err := createShadowTableSQL(s.cfg, s.schema, spec.ShadowTable)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(ctx, createSQL); err != nil {
		return nil, fmt.Errorf("create Snowflake shadow table %q (SEAM refuses to reuse an existing table): %w", spec.ShadowTable, err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin Snowflake backfill creation: %w", err)
	}
	defer tx.Rollback()
	var startOffset int64
	if err := tx.QueryRowContext(ctx, "SELECT NEXT_OFFSET FROM "+s.cfg.internal("OFFSETS")+" WHERE STREAM_ID = ?", s.cfg.StreamID).Scan(&startOffset); err != nil {
		return nil, fmt.Errorf("read Snowflake frontier for backfill: %w", err)
	}
	routeID := shadowRouteID(spec.JobID, spec.Attempt)
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+s.cfg.internal("BACKFILL_JOBS")+" (STREAM_ID, JOB_ID, ATTEMPT, STATE, SHADOW_ROUTE_ID, SHADOW_TABLE, SCAN_UPPER_BOUND, START_OFFSET, TOTAL_CHUNKS, COMPLETED_CHUNKS) VALUES (?, ?, ?, ?, ?, ?, 0, ?, 0, 0)", s.cfg.StreamID, spec.JobID, spec.Attempt, string(BackfillDiscovering), routeID, strings.ToUpper(spec.ShadowTable), startOffset); err != nil {
		return nil, fmt.Errorf("insert Snowflake backfill job: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+s.cfg.internal("ROUTES")+" (STREAM_ID, ROUTE_ID, TARGET_TABLE, ACTIVE) VALUES (?, ?, ?, TRUE)", s.cfg.StreamID, routeID, strings.ToUpper(spec.ShadowTable)); err != nil {
		return nil, fmt.Errorf("activate Snowflake shadow CDC route: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit Snowflake backfill creation: %w", err)
	}
	return &BackfillJob{Spec: spec, State: BackfillDiscovering, ShadowRouteID: routeID, StartOffset: startOffset}, nil
}

// SealBackfillManifest publishes every logical range and changes the job to
// running in one transaction. An empty lease queue is meaningful only after
// this seal; a crash while discovering cannot masquerade as completion.
func (s *Store) SealBackfillManifest(ctx context.Context, spec BackfillSpec) (*BackfillJob, error) {
	if err := spec.validate(s.cfg); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var state BackfillState
	var attempt, shadowTable, routeID string
	var startOffset int64
	if err := tx.QueryRowContext(ctx, "SELECT ATTEMPT, STATE, SHADOW_TABLE, SHADOW_ROUTE_ID, START_OFFSET FROM "+s.cfg.internal("BACKFILL_JOBS")+" WHERE STREAM_ID = ? AND JOB_ID = ?", s.cfg.StreamID, spec.JobID).Scan(&attempt, &state, &shadowTable, &routeID, &startOffset); err != nil {
		return nil, fmt.Errorf("load prepared Snowflake backfill: %w", err)
	}
	if attempt != spec.Attempt || !strings.EqualFold(shadowTable, spec.ShadowTable) {
		return nil, fmt.Errorf("Snowflake backfill %q immutable identity changed while discovering", spec.JobID)
	}
	if state != BackfillDiscovering {
		return nil, fmt.Errorf("Snowflake backfill %q is %s; only a discovering job can seal its manifest", spec.JobID, state)
	}
	for start := 0; start < len(spec.Chunks); start += stageBatchRows {
		end := start + stageBatchRows
		if end > len(spec.Chunks) {
			end = len(spec.Chunks)
		}
		query, args := chunkManifestInsertSQL(s.cfg, spec, spec.Chunks[start:end])
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return nil, fmt.Errorf("insert Snowflake chunk manifest: %w", err)
		}
	}
	result, err := tx.ExecContext(ctx, "UPDATE "+s.cfg.internal("BACKFILL_JOBS")+" SET STATE = ?, SCAN_UPPER_BOUND = ?, TOTAL_CHUNKS = ?, UPDATED_AT = CURRENT_TIMESTAMP() WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND STATE = ?", string(BackfillRunning), spec.ScanUpperBound, len(spec.Chunks), s.cfg.StreamID, spec.JobID, spec.Attempt, string(BackfillDiscovering))
	if err != nil {
		return nil, err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return nil, fmt.Errorf("seal Snowflake backfill %q manifest: job was concurrently changed", spec.JobID)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &BackfillJob{Spec: spec, State: BackfillRunning, ShadowRouteID: routeID, StartOffset: startOffset}, nil
}

// CreateBackfill is a convenience for a manifest discovered under an external
// source fence. Normal online coordinators must call PrepareBackfill, then
// sample/discover the source, then SealBackfillManifest.
func (s *Store) CreateBackfill(ctx context.Context, spec BackfillSpec) (*BackfillJob, error) {
	if _, err := s.PrepareBackfill(ctx, spec.JobID, spec.Attempt, spec.ShadowTable); err != nil {
		return nil, err
	}
	return s.SealBackfillManifest(ctx, spec)
}

func sameBackfill(left, right BackfillSpec) bool {
	if left.JobID != right.JobID || left.Attempt != right.Attempt || !strings.EqualFold(left.ShadowTable, right.ShadowTable) || left.ScanUpperBound != right.ScanUpperBound || len(left.Chunks) != len(right.Chunks) {
		return false
	}
	for i := range left.Chunks {
		if left.Chunks[i] != right.Chunks[i] {
			return false
		}
	}
	return true
}

func chunkManifestInsertSQL(cfg Config, spec BackfillSpec, chunks []model.ChunkRange) (string, []any) {
	var query strings.Builder
	query.WriteString("INSERT INTO ")
	query.WriteString(cfg.internal("BACKFILL_CHUNKS"))
	query.WriteString(" (STREAM_ID, JOB_ID, ATTEMPT, CHUNK_MIN, CHUNK_MAX, STATE) ")
	args := make([]any, 0, len(chunks)*6)
	for i, chunk := range chunks {
		if i > 0 {
			query.WriteString(" UNION ALL ")
		}
		query.WriteString("SELECT ?, ?, ?, ?, ?, ?")
		args = append(args, cfg.StreamID, spec.JobID, spec.Attempt, chunk.Min, chunk.Max, string(model.ChunkPending))
	}
	return query.String(), args
}

func (s *Store) LoadBackfill(ctx context.Context, jobID string) (*BackfillJob, error) {
	var job BackfillJob
	var errorMessage *string
	var validationTable, validationMarker, promotionMarker *string
	var validationOffset, promotionOffset *int64
	var total int
	err := s.db.QueryRowContext(ctx, "SELECT ATTEMPT, STATE, SHADOW_ROUTE_ID, SHADOW_TABLE, SCAN_UPPER_BOUND, START_OFFSET, TOTAL_CHUNKS, COMPLETED_CHUNKS, VALIDATION_TABLE, VALIDATION_MARKER_ID, VALIDATION_OFFSET, VALIDATION_READY, PROMOTION_MARKER_ID, PROMOTION_OFFSET, ERROR_MESSAGE FROM "+s.cfg.internal("BACKFILL_JOBS")+" WHERE STREAM_ID = ? AND JOB_ID = ?", s.cfg.StreamID, jobID).Scan(
		&job.Spec.Attempt, &job.State, &job.ShadowRouteID, &job.Spec.ShadowTable, &job.Spec.ScanUpperBound, &job.StartOffset, &total, &job.CompletedChunks, &validationTable, &validationMarker, &validationOffset, &job.ValidationReady, &promotionMarker, &promotionOffset, &errorMessage)
	if err != nil {
		return nil, err
	}
	job.Spec.JobID = jobID
	if errorMessage != nil {
		job.ErrorMessage = *errorMessage
	}
	if validationTable != nil {
		job.ValidationTable = *validationTable
	}
	if validationMarker != nil {
		job.ValidationMarkerID = *validationMarker
	}
	if validationOffset != nil {
		job.ValidationOffset = *validationOffset
	}
	if promotionMarker != nil {
		job.PromotionMarkerID = *promotionMarker
	}
	if promotionOffset != nil {
		job.PromotionOffset = *promotionOffset
	}
	rows, err := s.db.QueryContext(ctx, "SELECT CHUNK_MIN, CHUNK_MAX FROM "+s.cfg.internal("BACKFILL_CHUNKS")+" WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? ORDER BY CHUNK_MIN", s.cfg.StreamID, jobID, job.Spec.Attempt)
	if err != nil {
		return nil, fmt.Errorf("load Snowflake backfill chunks: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var chunk model.ChunkRange
		if err := rows.Scan(&chunk.Min, &chunk.Max); err != nil {
			return nil, err
		}
		job.Spec.Chunks = append(job.Spec.Chunks, chunk)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(job.Spec.Chunks) != total {
		return nil, fmt.Errorf("Snowflake backfill %q manifest has %d chunks, job records %d", jobID, len(job.Spec.Chunks), total)
	}
	if job.State == BackfillDiscovering {
		if err := job.Spec.validateIdentity(s.cfg); err != nil {
			return nil, fmt.Errorf("Snowflake backfill %q has an invalid durable identity: %w", jobID, err)
		}
		return &job, nil
	}
	if err := job.Spec.validate(s.cfg); err != nil {
		return nil, fmt.Errorf("Snowflake backfill %q has an invalid durable manifest: %w", jobID, err)
	}
	return &job, nil
}

// LeaseChunk uses a token compare-and-set. Snowflake does not expose a normal
// row-locking worker queue, so selection may race; only one contender can
// advance the observed token and the losers retry.
func (s *Store) LeaseChunk(ctx context.Context, jobID, attempt, workerID string, duration time.Duration) (*ChunkLease, error) {
	if jobID == "" || attempt == "" || workerID == "" || duration <= 0 {
		return nil, fmt.Errorf("job, attempt, worker, and positive lease duration are required")
	}
	for retries := 0; retries < 8; retries++ {
		var chunk model.ChunkRange
		var token int64
		err := s.db.QueryRowContext(ctx, "SELECT CHUNK_MIN, CHUNK_MAX, LEASE_TOKEN FROM "+s.cfg.internal("BACKFILL_CHUNKS")+" WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND (STATE = ? OR (STATE IN (?, ?, ?) AND LEASE_EXPIRES <= CURRENT_TIMESTAMP())) ORDER BY CHUNK_MIN LIMIT 1", s.cfg.StreamID, jobID, attempt, string(model.ChunkPending), string(model.ChunkLeased), string(model.ChunkScanning), string(model.ChunkCommitting)).Scan(&chunk.Min, &chunk.Max, &token)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("select Snowflake chunk lease candidate: %w", err)
		}
		result, err := s.db.ExecContext(ctx, "UPDATE "+s.cfg.internal("BACKFILL_CHUNKS")+" SET STATE = ?, LEASE_OWNER = ?, LEASE_TOKEN = LEASE_TOKEN + 1, LEASE_EXPIRES = DATEADD('millisecond', ?, CURRENT_TIMESTAMP()), LOW_MARKER_ID = NULL, HIGH_MARKER_ID = NULL, LOW_LSN = NULL, ROWS_SCANNED = 0, ROWS_APPLIED = 0, ERROR_MESSAGE = NULL, UPDATED_AT = CURRENT_TIMESTAMP() WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND CHUNK_MIN = ? AND LEASE_TOKEN = ? AND (STATE = ? OR LEASE_EXPIRES <= CURRENT_TIMESTAMP())", string(model.ChunkLeased), workerID, duration.Milliseconds(), s.cfg.StreamID, jobID, attempt, chunk.Min, token, string(model.ChunkPending))
		if err != nil {
			return nil, fmt.Errorf("claim Snowflake chunk: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return nil, err
		}
		if affected == 1 {
			return &ChunkLease{StreamID: s.cfg.StreamID, JobID: jobID, Attempt: attempt, Range: chunk, WorkerID: workerID, LeaseToken: token + 1, ExpiresAt: time.Now().Add(duration)}, nil
		}
	}
	return nil, fmt.Errorf("Snowflake chunk lease contention did not converge after 8 attempts")
}

func (s *Store) RenewChunkLease(ctx context.Context, lease *ChunkLease, duration time.Duration) error {
	if err := validateLease(lease, s.cfg.StreamID); err != nil {
		return err
	}
	if duration <= 0 {
		return fmt.Errorf("lease duration must be positive")
	}
	result, err := s.db.ExecContext(ctx, "UPDATE "+s.cfg.internal("BACKFILL_CHUNKS")+" SET LEASE_EXPIRES = DATEADD('millisecond', ?, CURRENT_TIMESTAMP()), UPDATED_AT = CURRENT_TIMESTAMP() WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND CHUNK_MIN = ? AND LEASE_OWNER = ? AND LEASE_TOKEN = ? AND LEASE_EXPIRES > CURRENT_TIMESTAMP() AND STATE IN (?, ?, ?)", duration.Milliseconds(), s.cfg.StreamID, lease.JobID, lease.Attempt, lease.Range.Min, lease.WorkerID, lease.LeaseToken, string(model.ChunkLeased), string(model.ChunkScanning), string(model.ChunkCommitting))
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil || affected != 1 {
		return fmt.Errorf("Snowflake chunk %s lease token %d is expired or fenced", lease.Range, lease.LeaseToken)
	}
	lease.ExpiresAt = time.Now().Add(duration)
	return nil
}

func validateLease(lease *ChunkLease, streamID string) error {
	if lease == nil || lease.StreamID != streamID || lease.JobID == "" || lease.Attempt == "" || lease.WorkerID == "" || lease.LeaseToken <= 0 || lease.Range.Max < lease.Range.Min {
		return fmt.Errorf("invalid Snowflake chunk lease")
	}
	return nil
}

// BeginChunkScan records the LOW marker and resets any disposable snapshot
// stage from a crashed prior owner of this logical range.
func (s *Store) BeginChunkScan(ctx context.Context, lease *ChunkLease, lowMarkerID string) error {
	if err := validateLease(lease, s.cfg.StreamID); err != nil {
		return err
	}
	if lowMarkerID == "" {
		return fmt.Errorf("LOW marker ID is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "DELETE FROM "+s.cfg.internal("SNAPSHOT_STAGE")+" WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND CHUNK_MIN = ?", s.cfg.StreamID, lease.JobID, lease.Attempt, lease.Range.Min); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, "UPDATE "+s.cfg.internal("BACKFILL_CHUNKS")+" SET STATE = ?, LOW_MARKER_ID = ?, HIGH_MARKER_ID = NULL, LOW_LSN = NULL, ROWS_SCANNED = 0, ROWS_APPLIED = 0, UPDATED_AT = CURRENT_TIMESTAMP() WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND CHUNK_MIN = ? AND CHUNK_MAX = ? AND LEASE_OWNER = ? AND LEASE_TOKEN = ? AND LEASE_EXPIRES > CURRENT_TIMESTAMP() AND STATE = ?", string(model.ChunkScanning), lowMarkerID, s.cfg.StreamID, lease.JobID, lease.Attempt, lease.Range.Min, lease.Range.Max, lease.WorkerID, lease.LeaseToken, string(model.ChunkLeased))
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("begin Snowflake chunk %s scan: lease is expired or fenced", lease.Range)
	}
	return tx.Commit()
}

// StageSnapshot replaces one chunk's disposable candidate set in one DML
// transaction. Batches bound statement size; a partial failure rolls back the
// entire replacement and leaves the chunk resumable.
func (s *Store) StageSnapshot(ctx context.Context, lease *ChunkLease, rows []model.Row) error {
	if err := validateLease(lease, s.cfg.StreamID); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var state string
	if err := tx.QueryRowContext(ctx, "SELECT STATE FROM "+s.cfg.internal("BACKFILL_CHUNKS")+" WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND CHUNK_MIN = ? AND CHUNK_MAX = ? AND LEASE_OWNER = ? AND LEASE_TOKEN = ? AND LEASE_EXPIRES > CURRENT_TIMESTAMP()", s.cfg.StreamID, lease.JobID, lease.Attempt, lease.Range.Min, lease.Range.Max, lease.WorkerID, lease.LeaseToken).Scan(&state); err != nil {
		return fmt.Errorf("validate Snowflake snapshot lease: %w", err)
	}
	if state != string(model.ChunkScanning) {
		return fmt.Errorf("Snowflake chunk %s is %s, expected scanning", lease.Range, state)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM "+s.cfg.internal("SNAPSHOT_STAGE")+" WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND CHUNK_MIN = ?", s.cfg.StreamID, lease.JobID, lease.Attempt, lease.Range.Min); err != nil {
		return err
	}
	type candidate struct {
		sequence int
		pk       int64
		payload  string
	}
	batch := make([]candidate, 0, stageBatchRows)
	bytes := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		var query strings.Builder
		query.WriteString("INSERT INTO ")
		query.WriteString(s.cfg.internal("SNAPSHOT_STAGE"))
		query.WriteString(" (STREAM_ID, JOB_ID, ATTEMPT, CHUNK_MIN, SEQUENCE, PK, PAYLOAD) ")
		args := make([]any, 0, len(batch)*7)
		for index, row := range batch {
			if index > 0 {
				query.WriteString(" UNION ALL ")
			}
			query.WriteString("SELECT ?, ?, ?, ?, ?, ?, PARSE_JSON(?)")
			args = append(args, s.cfg.StreamID, lease.JobID, lease.Attempt, lease.Range.Min, row.sequence, row.pk, row.payload)
		}
		if _, err := tx.ExecContext(ctx, query.String(), args...); err != nil {
			return fmt.Errorf("stage %d Snowflake snapshot rows: %w", len(batch), err)
		}
		batch = batch[:0]
		bytes = 0
		return nil
	}
	for sequence := range rows {
		pk, payload, err := encodeRow(s.schema, &rows[sequence])
		if err != nil {
			return err
		}
		if pk < lease.Range.Min || pk > lease.Range.Max {
			return fmt.Errorf("snapshot key %d is outside leased chunk %s", pk, lease.Range)
		}
		rowBytes := len(payload) + 64
		if rowBytes > stageBatchBytes {
			return fmt.Errorf("snapshot row %d exceeds %d-byte stage limit", pk, stageBatchBytes)
		}
		if len(batch) == stageBatchRows || (len(batch) > 0 && bytes+rowBytes > stageBatchBytes) {
			if err := flush(); err != nil {
				return err
			}
		}
		batch = append(batch, candidate{sequence: sequence, pk: pk, payload: payload})
		bytes += rowBytes
	}
	if err := flush(); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, "UPDATE "+s.cfg.internal("BACKFILL_CHUNKS")+" SET ROWS_SCANNED = ?, UPDATED_AT = CURRENT_TIMESTAMP() WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND CHUNK_MIN = ? AND LEASE_OWNER = ? AND LEASE_TOKEN = ? AND LEASE_EXPIRES > CURRENT_TIMESTAMP() AND STATE = ?", len(rows), s.cfg.StreamID, lease.JobID, lease.Attempt, lease.Range.Min, lease.WorkerID, lease.LeaseToken, string(model.ChunkScanning))
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("stage Snowflake chunk %s: lease is expired or fenced", lease.Range)
	}
	return tx.Commit()
}

func (s *Store) SealChunkScan(ctx context.Context, lease *ChunkLease, highMarkerID string) error {
	if err := validateLease(lease, s.cfg.StreamID); err != nil {
		return err
	}
	if highMarkerID == "" {
		return fmt.Errorf("HIGH marker ID is required")
	}
	result, err := s.db.ExecContext(ctx, "UPDATE "+s.cfg.internal("BACKFILL_CHUNKS")+" SET STATE = ?, HIGH_MARKER_ID = ?, UPDATED_AT = CURRENT_TIMESTAMP() WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND CHUNK_MIN = ? AND CHUNK_MAX = ? AND LEASE_OWNER = ? AND LEASE_TOKEN = ? AND LEASE_EXPIRES > CURRENT_TIMESTAMP() AND STATE = ? AND LOW_MARKER_ID IS NOT NULL", string(model.ChunkCommitting), highMarkerID, s.cfg.StreamID, lease.JobID, lease.Attempt, lease.Range.Min, lease.Range.Max, lease.WorkerID, lease.LeaseToken, string(model.ChunkScanning))
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("seal Snowflake chunk %s: lease is expired, fenced, or not scanning", lease.Range)
	}
	return nil
}

// FinalizeChunk waits for callers to observe HIGH in the ordered CDC sink,
// then atomically merges candidates, completes the chunk, and clears staging.
// Any key with a CDC clock newer than LOW is excluded, including tombstones.
func (s *Store) FinalizeChunk(ctx context.Context, lease *ChunkLease) error {
	if err := validateLease(lease, s.cfg.StreamID); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var lowMarkerID, highMarkerID, routeID, shadowTable string
	var rowsScanned int64
	err = tx.QueryRowContext(ctx, "SELECT C.LOW_MARKER_ID, C.HIGH_MARKER_ID, C.ROWS_SCANNED, J.SHADOW_ROUTE_ID, J.SHADOW_TABLE FROM "+s.cfg.internal("BACKFILL_CHUNKS")+" C JOIN "+s.cfg.internal("BACKFILL_JOBS")+" J ON J.STREAM_ID = C.STREAM_ID AND J.JOB_ID = C.JOB_ID AND J.ATTEMPT = C.ATTEMPT WHERE C.STREAM_ID = ? AND C.JOB_ID = ? AND C.ATTEMPT = ? AND C.CHUNK_MIN = ? AND C.CHUNK_MAX = ? AND C.LEASE_OWNER = ? AND C.LEASE_TOKEN = ? AND C.LEASE_EXPIRES > CURRENT_TIMESTAMP() AND C.STATE = ?", s.cfg.StreamID, lease.JobID, lease.Attempt, lease.Range.Min, lease.Range.Max, lease.WorkerID, lease.LeaseToken, string(model.ChunkCommitting)).Scan(&lowMarkerID, &highMarkerID, &rowsScanned, &routeID, &shadowTable)
	if err != nil {
		return fmt.Errorf("validate Snowflake chunk finalization lease: %w", err)
	}
	var lowLSNText string
	if err := tx.QueryRowContext(ctx, "SELECT SOURCE_LSN FROM "+s.cfg.internal("MARKERS")+" WHERE STREAM_ID = ? AND MARKER_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND CHUNK_MIN = ? AND CHUNK_MAX = ?", s.cfg.StreamID, lowMarkerID, lease.JobID, lease.Attempt, lease.Range.Min, lease.Range.Max).Scan(&lowLSNText); err != nil {
		return fmt.Errorf("LOW marker %q is not durably applied: %w", lowMarkerID, err)
	}
	lowLSN, err := strconv.ParseUint(lowLSNText, 10, 64)
	if err != nil {
		return fmt.Errorf("LOW marker %q has invalid source LSN %q: %w", lowMarkerID, lowLSNText, err)
	}
	var highSeen int
	if err := tx.QueryRowContext(ctx, "SELECT 1 FROM "+s.cfg.internal("MARKERS")+" WHERE STREAM_ID = ? AND MARKER_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND CHUNK_MIN = ? AND CHUNK_MAX = ?", s.cfg.StreamID, highMarkerID, lease.JobID, lease.Attempt, lease.Range.Min, lease.Range.Max).Scan(&highSeen); err != nil {
		return fmt.Errorf("HIGH marker %q is not durably applied: %w", highMarkerID, err)
	}
	mergeSQL, err := mergeSnapshotSQL(s.cfg, s.schema, shadowTable)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, mergeSQL, s.cfg.StreamID, routeID, s.cfg.StreamID, lease.JobID, lease.Attempt, lease.Range.Min, strconv.FormatUint(lowLSN, 10))
	if err != nil {
		return fmt.Errorf("merge Snowflake snapshot chunk %s: %w", lease.Range, err)
	}
	rowsApplied, err := result.RowsAffected()
	if err != nil {
		return err
	}
	result, err = tx.ExecContext(ctx, "UPDATE "+s.cfg.internal("BACKFILL_CHUNKS")+" SET STATE = ?, LOW_LSN = ?, ROWS_APPLIED = ?, LEASE_EXPIRES = NULL, UPDATED_AT = CURRENT_TIMESTAMP() WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND CHUNK_MIN = ? AND LEASE_OWNER = ? AND LEASE_TOKEN = ? AND LEASE_EXPIRES > CURRENT_TIMESTAMP() AND STATE = ?", string(model.ChunkCompleted), strconv.FormatUint(lowLSN, 10), rowsApplied, s.cfg.StreamID, lease.JobID, lease.Attempt, lease.Range.Min, lease.WorkerID, lease.LeaseToken, string(model.ChunkCommitting))
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("finalize Snowflake chunk %s: lease was fenced during merge", lease.Range)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM "+s.cfg.internal("SNAPSHOT_STAGE")+" WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND CHUNK_MIN = ?", s.cfg.StreamID, lease.JobID, lease.Attempt, lease.Range.Min); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE "+s.cfg.internal("BACKFILL_JOBS")+" SET COMPLETED_CHUNKS = (SELECT COUNT(*) FROM "+s.cfg.internal("BACKFILL_CHUNKS")+" WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND STATE = ?), STATE = IFF((SELECT COUNT(*) FROM "+s.cfg.internal("BACKFILL_CHUNKS")+" WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND STATE <> ?) = 0, ?, STATE), UPDATED_AT = CURRENT_TIMESTAMP() WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND STATE = ?", s.cfg.StreamID, lease.JobID, lease.Attempt, string(model.ChunkCompleted), s.cfg.StreamID, lease.JobID, lease.Attempt, string(model.ChunkCompleted), string(BackfillReadyToVerify), s.cfg.StreamID, lease.JobID, lease.Attempt, string(BackfillRunning)); err != nil {
		return err
	}
	_ = rowsScanned // persisted separately from warehouse MERGE's affected-row semantics
	return tx.Commit()
}

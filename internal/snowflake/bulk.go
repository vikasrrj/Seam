package snowflake

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"example.com/seam/internal/model"
)

const (
	bulkFilePrepared = "prepared"
)

type snapshotFile struct {
	ID             string
	ContentSHA256  string
	LocalPath      string
	LocalDirectory string
	StageDirectory string
	StagePath      string
	Rows           int
	Bytes          int64
}

type snapshotFileRow struct {
	StreamID string          `json:"stream_id"`
	JobID    string          `json:"job_id"`
	Attempt  string          `json:"attempt"`
	ChunkMin string          `json:"chunk_min"`
	Sequence int             `json:"sequence"`
	PK       string          `json:"pk"`
	Payload  json.RawMessage `json:"payload"`
}

func (s *Store) stageSnapshotBulk(ctx context.Context, lease *ChunkLease, rows []model.Row) error {
	if err := validateLease(lease, s.cfg.StreamID); err != nil {
		return err
	}
	if len(rows) == 0 {
		return s.stageEmptySnapshot(ctx, lease)
	}
	finishFile := startPhase(ctx, "snapshot-file-encode-gzip-write")
	file, err := s.writeSnapshotFile(lease, rows)
	finishFile()
	if err != nil {
		return err
	}
	defer os.RemoveAll(file.LocalDirectory)

	if err := s.prepareSnapshotFile(ctx, lease, file); err != nil {
		return err
	}
	if err := s.uploadSnapshotFile(ctx, lease, file); err != nil {
		return err
	}
	return s.copySnapshotFile(ctx, lease, file)
}

func (s *Store) writeSnapshotFile(lease *ChunkLease, rows []model.Row) (_ *snapshotFile, retErr error) {
	directory, err := os.MkdirTemp(s.cfg.BulkTempDir, "seam-snowflake-")
	if err != nil {
		return nil, fmt.Errorf("create Snowflake bulk temporary directory: %w", err)
	}
	defer func() {
		if retErr != nil {
			_ = os.RemoveAll(directory)
		}
	}()

	path := filepath.Join(directory, "snapshot.json.gz")
	output, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create Snowflake snapshot file: %w", err)
	}
	contentHash := sha256.New()
	compressed := gzip.NewWriter(io.MultiWriter(output, contentHash))
	compressed.Header.ModTime = time.Unix(0, 0)
	compressed.Header.OS = 255
	encoder := json.NewEncoder(compressed)
	encoder.SetEscapeHTML(false)

	for sequence := range rows {
		pk, payload, err := encodeRow(s.schema, &rows[sequence])
		if err != nil {
			_ = compressed.Close()
			_ = output.Close()
			return nil, err
		}
		if pk < lease.Range.Min || pk > lease.Range.Max {
			_ = compressed.Close()
			_ = output.Close()
			return nil, fmt.Errorf("snapshot key %d is outside leased chunk %s", pk, lease.Range)
		}
		if !json.Valid([]byte(payload)) {
			_ = compressed.Close()
			_ = output.Close()
			return nil, fmt.Errorf("snapshot key %d produced invalid JSON", pk)
		}
		row := snapshotFileRow{
			StreamID: s.cfg.StreamID,
			JobID:    lease.JobID,
			Attempt:  lease.Attempt,
			ChunkMin: strconv.FormatInt(lease.Range.Min, 10),
			Sequence: sequence,
			PK:       strconv.FormatInt(pk, 10),
			Payload:  json.RawMessage(payload),
		}
		if err := encoder.Encode(row); err != nil {
			_ = compressed.Close()
			_ = output.Close()
			return nil, fmt.Errorf("encode Snowflake snapshot row %d: %w", pk, err)
		}
	}
	if err := compressed.Close(); err != nil {
		_ = output.Close()
		return nil, fmt.Errorf("finish Snowflake snapshot compression: %w", err)
	}
	if err := output.Close(); err != nil {
		return nil, fmt.Errorf("close Snowflake snapshot file: %w", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	contentSHA := hex.EncodeToString(contentHash.Sum(nil))
	identity := sha256.Sum256([]byte(strings.Join([]string{
		s.cfg.StreamID,
		lease.JobID,
		lease.Attempt,
		strconv.FormatInt(lease.Range.Min, 10),
		strconv.FormatInt(lease.Range.Max, 10),
		strconv.FormatInt(lease.LeaseToken, 10),
		contentSHA,
	}, "\x00")))
	fileID := hex.EncodeToString(identity[:])
	stageDirectory := "@" + s.cfg.internal("SNAPSHOT_FILES") + "/" + fileID
	return &snapshotFile{
		ID:             fileID,
		ContentSHA256:  contentSHA,
		LocalPath:      path,
		LocalDirectory: directory,
		StageDirectory: stageDirectory,
		StagePath:      stageDirectory + "/snapshot.json.gz",
		Rows:           len(rows),
		Bytes:          info.Size(),
	}, nil
}

func (s *Store) prepareSnapshotFile(ctx context.Context, lease *ChunkLease, file *snapshotFile) error {
	defer startPhase(ctx, "snapshot-manifest-prepare")()
	query := "MERGE INTO " + s.cfg.internal("BACKFILL_FILES") + " D USING (SELECT ? AS STREAM_ID, ? AS JOB_ID, ? AS ATTEMPT, ? AS CHUNK_MIN, ? AS LEASE_TOKEN, ? AS FILE_ID, ? AS STAGE_PATH, ? AS CONTENT_SHA256, ? AS ROW_COUNT, ? AS BYTE_COUNT, ? AS STATE WHERE EXISTS (SELECT 1 FROM " + s.cfg.internal("BACKFILL_CHUNKS") + " C WHERE C.STREAM_ID = ? AND C.JOB_ID = ? AND C.ATTEMPT = ? AND C.CHUNK_MIN = ? AND C.CHUNK_MAX = ? AND C.LEASE_OWNER = ? AND C.LEASE_TOKEN = ? AND C.LEASE_EXPIRES > CURRENT_TIMESTAMP() AND C.STATE = ?)) S ON D.STREAM_ID = S.STREAM_ID AND D.JOB_ID = S.JOB_ID AND D.ATTEMPT = S.ATTEMPT AND D.CHUNK_MIN = S.CHUNK_MIN AND D.LEASE_TOKEN = S.LEASE_TOKEN AND D.FILE_ID = S.FILE_ID WHEN MATCHED THEN UPDATE SET STAGE_PATH = S.STAGE_PATH, CONTENT_SHA256 = S.CONTENT_SHA256, ROW_COUNT = S.ROW_COUNT, BYTE_COUNT = S.BYTE_COUNT, STATE = S.STATE, UPDATED_AT = CURRENT_TIMESTAMP() WHEN NOT MATCHED THEN INSERT (STREAM_ID, JOB_ID, ATTEMPT, CHUNK_MIN, LEASE_TOKEN, FILE_ID, STAGE_PATH, CONTENT_SHA256, ROW_COUNT, BYTE_COUNT, STATE) VALUES (S.STREAM_ID, S.JOB_ID, S.ATTEMPT, S.CHUNK_MIN, S.LEASE_TOKEN, S.FILE_ID, S.STAGE_PATH, S.CONTENT_SHA256, S.ROW_COUNT, S.BYTE_COUNT, S.STATE)"
	result, err := s.db.ExecContext(ctx, query,
		s.cfg.StreamID, lease.JobID, lease.Attempt, lease.Range.Min, lease.LeaseToken,
		file.ID, file.StagePath, file.ContentSHA256, file.Rows, file.Bytes, bulkFilePrepared,
		s.cfg.StreamID, lease.JobID, lease.Attempt, lease.Range.Min, lease.Range.Max,
		lease.WorkerID, lease.LeaseToken, string(model.ChunkScanning))
	if err != nil {
		return fmt.Errorf("record prepared Snowflake snapshot file: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("prepare Snowflake chunk %s: lease expired or was fenced", lease.Range)
	}
	return nil
}

func (s *Store) uploadSnapshotFile(ctx context.Context, lease *ChunkLease, file *snapshotFile) error {
	defer startPhase(ctx, "snapshot-upload")()
	absolute, err := filepath.Abs(file.LocalPath)
	if err != nil {
		return err
	}
	fileURI := "file://" + filepath.ToSlash(absolute)
	query := "PUT '" + strings.ReplaceAll(fileURI, "'", "''") + "' " + file.StageDirectory + " AUTO_COMPRESS = FALSE OVERWRITE = TRUE PARALLEL = " + strconv.Itoa(s.cfg.UploadParallel)
	finishPut := startPhase(ctx, "snapshot-put-submit")
	result, err := s.db.QueryContext(ctx, query)
	finishPut()
	if err != nil {
		return fmt.Errorf("upload Snowflake snapshot file: %w", err)
	}
	var uploaded int
	for result.Next() {
		var source, target, sourceSize, targetSize, sourceCompression, targetCompression, status, message sql.NullString
		if err := result.Scan(&source, &target, &sourceSize, &targetSize, &sourceCompression, &targetCompression, &status, &message); err != nil {
			result.Close()
			return fmt.Errorf("read Snowflake PUT result: %w", err)
		}
		if status.String != "UPLOADED" && status.String != "SKIPPED" {
			result.Close()
			return fmt.Errorf("upload Snowflake snapshot file: status %q: %s", status.String, message.String)
		}
		uploaded++
	}
	if err := result.Err(); err != nil {
		result.Close()
		return fmt.Errorf("read Snowflake PUT result: %w", err)
	}
	if err := result.Close(); err != nil {
		return fmt.Errorf("close Snowflake PUT result: %w", err)
	}
	if uploaded != 1 {
		return fmt.Errorf("upload Snowflake snapshot file returned %d results, expected 1", uploaded)
	}
	// Keep the durable manifest in prepared. COPY and chunk progress commit
	// atomically, so a crash after PUT safely repeats the same content-addressed
	// OVERWRITE upload without another manifest state transition.
	return nil
}

func (s *Store) stageEmptySnapshot(ctx context.Context, lease *ChunkLease) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := validateSnapshotLease(ctx, tx, s.cfg, lease); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM "+s.cfg.internal("SNAPSHOT_STAGE")+" WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND CHUNK_MIN = ?", s.cfg.StreamID, lease.JobID, lease.Attempt, lease.Range.Min); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, "UPDATE "+s.cfg.internal("BACKFILL_CHUNKS")+" SET ROWS_SCANNED = 0, UPDATED_AT = CURRENT_TIMESTAMP() WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND CHUNK_MIN = ? AND LEASE_OWNER = ? AND LEASE_TOKEN = ? AND LEASE_EXPIRES > CURRENT_TIMESTAMP() AND STATE = ?", s.cfg.StreamID, lease.JobID, lease.Attempt, lease.Range.Min, lease.WorkerID, lease.LeaseToken, string(model.ChunkScanning))
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("stage empty Snowflake chunk %s: lease expired or was fenced", lease.Range)
	}
	return tx.Commit()
}

func (s *Store) copySnapshotFile(ctx context.Context, lease *ChunkLease, file *snapshotFile) error {
	defer startPhase(ctx, "snapshot-copy-transaction")()
	finishBegin := startPhase(ctx, "snapshot-copy-begin")
	tx, err := s.db.BeginTx(ctx, nil)
	finishBegin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	finishLease := startPhase(ctx, "snapshot-copy-lease-read")
	leaseErr := validateSnapshotLease(ctx, tx, s.cfg, lease)
	finishLease()
	if leaseErr != nil {
		return leaseErr
	}
	finishDelete := startPhase(ctx, "snapshot-copy-stage-delete")
	_, deleteErr := tx.ExecContext(ctx, "DELETE FROM "+s.cfg.internal("SNAPSHOT_STAGE")+" WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND CHUNK_MIN = ?", s.cfg.StreamID, lease.JobID, lease.Attempt, lease.Range.Min)
	finishDelete()
	if deleteErr != nil {
		return deleteErr
	}
	finishCopy := startPhase(ctx, "snapshot-copy-statement")
	loaded, copyErr := copySnapshotRows(ctx, tx, bulkCopySQL(s.cfg, file.StageDirectory))
	finishCopy()
	if copyErr != nil {
		return fmt.Errorf("copy Snowflake snapshot file: %w", copyErr)
	}
	if loaded != int64(file.Rows) {
		return fmt.Errorf("Snowflake snapshot file loaded %d rows, expected %d", loaded, file.Rows)
	}
	finishProgress := startPhase(ctx, "snapshot-copy-progress-update")
	result, err := tx.ExecContext(ctx, "UPDATE "+s.cfg.internal("BACKFILL_CHUNKS")+" SET ROWS_SCANNED = ?, UPDATED_AT = CURRENT_TIMESTAMP() WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND CHUNK_MIN = ? AND LEASE_OWNER = ? AND LEASE_TOKEN = ? AND LEASE_EXPIRES > CURRENT_TIMESTAMP() AND STATE = ?", file.Rows, s.cfg.StreamID, lease.JobID, lease.Attempt, lease.Range.Min, lease.WorkerID, lease.LeaseToken, string(model.ChunkScanning))
	finishProgress()
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("copy Snowflake chunk %s: lease expired or was fenced", lease.Range)
	}
	finishCommit := startPhase(ctx, "snapshot-copy-commit")
	commitErr := tx.Commit()
	finishCommit()
	return commitErr
}

// copySnapshotRows reads Snowflake's COPY result instead of issuing a second
// COUNT query. The result's rows_loaded value is the warehouse's authoritative
// count for the exact staged file and remains inside the surrounding atomic
// transaction.
func copySnapshotRows(ctx context.Context, tx *sql.Tx, query string) (int64, error) {
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	loadedColumn := -1
	for index, column := range columns {
		if strings.EqualFold(column, "rows_loaded") {
			loadedColumn = index
			break
		}
	}
	if loadedColumn < 0 {
		return 0, fmt.Errorf("COPY result has no rows_loaded column")
	}
	values := make([]any, len(columns))
	destinations := make([]any, len(columns))
	for index := range values {
		destinations[index] = &values[index]
	}
	var loaded int64
	for rows.Next() {
		if err := rows.Scan(destinations...); err != nil {
			return 0, err
		}
		value := values[loadedColumn]
		if bytes, ok := value.([]byte); ok {
			value = string(bytes)
		}
		count, err := strconv.ParseInt(fmt.Sprint(value), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse COPY rows_loaded value %q: %w", value, err)
		}
		loaded += count
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return loaded, nil
}

func validateSnapshotLease(ctx context.Context, tx *sql.Tx, cfg Config, lease *ChunkLease) error {
	var state string
	err := tx.QueryRowContext(ctx, "SELECT STATE FROM "+cfg.internal("BACKFILL_CHUNKS")+" WHERE STREAM_ID = ? AND JOB_ID = ? AND ATTEMPT = ? AND CHUNK_MIN = ? AND CHUNK_MAX = ? AND LEASE_OWNER = ? AND LEASE_TOKEN = ? AND LEASE_EXPIRES > CURRENT_TIMESTAMP()", cfg.StreamID, lease.JobID, lease.Attempt, lease.Range.Min, lease.Range.Max, lease.WorkerID, lease.LeaseToken).Scan(&state)
	if err != nil {
		return fmt.Errorf("validate Snowflake snapshot lease: %w", err)
	}
	if state != string(model.ChunkScanning) {
		return fmt.Errorf("Snowflake chunk %s is %s, expected scanning", lease.Range, state)
	}
	return nil
}

func bulkCopySQL(cfg Config, stageDirectory string) string {
	return "COPY INTO " + cfg.internal("SNAPSHOT_STAGE") + " (STREAM_ID, JOB_ID, ATTEMPT, CHUNK_MIN, SEQUENCE, PK, PAYLOAD) FROM (SELECT $1:stream_id::VARCHAR, $1:job_id::VARCHAR, $1:attempt::VARCHAR, TO_NUMBER($1:chunk_min::VARCHAR), TO_NUMBER($1:sequence), TO_NUMBER($1:pk::VARCHAR), $1:payload FROM " + stageDirectory + ") FILE_FORMAT = (TYPE = JSON COMPRESSION = GZIP) ON_ERROR = ABORT_STATEMENT FORCE = TRUE"
}

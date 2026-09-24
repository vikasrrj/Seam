package capture

import (
	"context"
	"fmt"

	"example.com/seam/internal/transport"
	"github.com/jackc/pgx/v5"
)

// captureLeadership is a session-scoped source lease. PostgreSQL releases the
// advisory lock when this connection dies, so takeover does not depend on a
// wall-clock timeout. The durable epoch makes every takeover observable.
type captureLeadership struct {
	conn    *pgx.Conn
	slot    string
	ownerID string
	epoch   int64
}

func acquireCaptureLeadership(ctx context.Context, cfg ReaderConfig, systemID, topicID string) (*captureLeadership, error) {
	conn, err := transport.ConnectPostgres(ctx, cfg.SQLDSN)
	if err != nil {
		return nil, fmt.Errorf("connect capture control: %w", err)
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			conn.Close(context.Background())
		}
	}()
	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS seam_capture_owners (
			slot_name TEXT PRIMARY KEY,
			owner_id TEXT NOT NULL,
			owner_epoch BIGINT NOT NULL,
			backend_pid INTEGER NOT NULL,
			source_system_id TEXT NOT NULL,
			generation TEXT NOT NULL,
			publication TEXT NOT NULL,
			kafka_topic_id TEXT NOT NULL,
			acquired_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
			heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
		)`); err != nil {
		return nil, fmt.Errorf("ensure capture ownership table: %w", err)
	}
	var acquired bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtextextended('seam-capture:' || $1, 0))`, cfg.Slot).Scan(&acquired); err != nil {
		return nil, fmt.Errorf("acquire capture advisory lock: %w", err)
	}
	if !acquired {
		return nil, fmt.Errorf("capture slot %q already has a live owner", cfg.Slot)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var priorSystemID, priorGeneration, priorPublication, priorTopicID string
	var priorEpoch int64
	err = tx.QueryRow(ctx, `
		SELECT source_system_id, generation, publication, kafka_topic_id, owner_epoch
		FROM seam_capture_owners WHERE slot_name = $1 FOR UPDATE`, cfg.Slot).Scan(
		&priorSystemID, &priorGeneration, &priorPublication, &priorTopicID, &priorEpoch)
	if err != nil && err != pgx.ErrNoRows {
		return nil, fmt.Errorf("load capture ownership: %w", err)
	}
	if err == nil && (priorSystemID != systemID || priorGeneration != cfg.Generation || priorPublication != cfg.Publication || priorTopicID != topicID) {
		return nil, fmt.Errorf("capture slot %q identity changed (system/generation/publication/topic); create a new generation and slot instead of taking over", cfg.Slot)
	}
	epoch := priorEpoch + 1
	if _, err := tx.Exec(ctx, `
		INSERT INTO seam_capture_owners
			(slot_name, owner_id, owner_epoch, backend_pid, source_system_id, generation, publication, kafka_topic_id, acquired_at, heartbeat_at)
		VALUES ($1, $2, $3, pg_backend_pid(), $4, $5, $6, $7, clock_timestamp(), clock_timestamp())
		ON CONFLICT (slot_name) DO UPDATE SET
			owner_id = EXCLUDED.owner_id,
			owner_epoch = EXCLUDED.owner_epoch,
			backend_pid = EXCLUDED.backend_pid,
			acquired_at = EXCLUDED.acquired_at,
			heartbeat_at = EXCLUDED.heartbeat_at`,
		cfg.Slot, cfg.OwnerID, epoch, systemID, cfg.Generation, cfg.Publication, topicID); err != nil {
		return nil, fmt.Errorf("persist capture ownership: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit capture ownership: %w", err)
	}
	closeOnError = false
	return &captureLeadership{conn: conn, slot: cfg.Slot, ownerID: cfg.OwnerID, epoch: epoch}, nil
}

func (l *captureLeadership) assert(ctx context.Context, heartbeat bool) error {
	if l == nil || l.conn == nil {
		return fmt.Errorf("capture leadership is not held")
	}
	var owned bool
	if err := l.conn.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM seam_capture_owners
			WHERE slot_name = $1 AND owner_id = $2 AND owner_epoch = $3
			  AND backend_pid = pg_backend_pid()
		)`, l.slot, l.ownerID, l.epoch).Scan(&owned); err != nil {
		return fmt.Errorf("verify capture leadership: %w", err)
	}
	if !owned {
		return fmt.Errorf("capture leadership for slot %q epoch %d was fenced", l.slot, l.epoch)
	}
	if heartbeat {
		tag, err := l.conn.Exec(ctx, `
			UPDATE seam_capture_owners SET heartbeat_at = clock_timestamp()
			WHERE slot_name = $1 AND owner_id = $2 AND owner_epoch = $3 AND backend_pid = pg_backend_pid()`,
			l.slot, l.ownerID, l.epoch)
		if err != nil {
			return fmt.Errorf("heartbeat capture leadership: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("capture leadership for slot %q epoch %d was fenced", l.slot, l.epoch)
		}
	}
	return nil
}

func (l *captureLeadership) close() {
	if l != nil && l.conn != nil {
		_ = l.conn.Close(context.Background())
		l.conn = nil
	}
}

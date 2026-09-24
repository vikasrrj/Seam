// Package marker writes LOW/HIGH control markers to the source database.
package marker

import (
	"context"
	"fmt"
	"time"

	"example.com/seam/internal/model"
	"example.com/seam/internal/retry"
	"example.com/seam/internal/transport"
	"github.com/jackc/pgx/v5"
)

// Store manages the seam_marker table in the source database.
type Store struct {
	dsn string
}

func NewStore(dsn string) *Store {
	return &Store{dsn: dsn}
}

func (s *Store) conn(ctx context.Context) (*pgx.Conn, error) {
	return transport.ConnectPostgres(ctx, s.dsn)
}

// EnsureTable creates the marker table if it does not exist.
func (s *Store) EnsureTable(ctx context.Context) error {
	conn, err := s.conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS seam_marker (
			id TEXT PRIMARY KEY,
			job_id TEXT NOT NULL,
			attempt TEXT NOT NULL,
			kind TEXT NOT NULL,
			chunk_min_id BIGINT NOT NULL,
			chunk_max_id BIGINT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`)
	return err
}

// WriteLow inserts a LOW marker and returns its ID.
func (s *Store) WriteLow(ctx context.Context, jobID, attempt string, chunk model.ChunkRange) (string, error) {
	return s.write(ctx, "low", jobID, attempt, chunk)
}

// WriteHigh inserts a HIGH marker and returns its ID.
func (s *Store) WriteHigh(ctx context.Context, jobID, attempt string, chunk model.ChunkRange) (string, error) {
	return s.write(ctx, "high", jobID, attempt, chunk)
}

// WriteBarrier creates a unique source transaction used to prove that the
// capture slot and Kafka stream were connected before a new scan begins.
func (s *Store) WriteBarrier(ctx context.Context, jobID, attempt string) (string, error) {
	return s.write(ctx, "barrier", jobID, attempt, model.ChunkRange{})
}

func (s *Store) write(ctx context.Context, kind, jobID, attempt string, chunk model.ChunkRange) (string, error) {
	id := fmt.Sprintf("%s:%s:%s:%d:%d:%d", jobID, attempt, kind, chunk.Min, chunk.Max, time.Now().UnixNano())
	_, err := retry.Do(ctx, retry.DefaultConfig(), func() (struct{}, error) {
		conn, err := s.conn(ctx)
		if err != nil {
			return struct{}{}, err
		}
		defer conn.Close(context.Background())
		_, err = conn.Exec(ctx,
			`INSERT INTO seam_marker (id, job_id, attempt, kind, chunk_min_id, chunk_max_id) VALUES ($1, $2, $3, $4, $5, $6)`,
			id, jobID, attempt, kind, chunk.Min, chunk.Max)
		return struct{}{}, err
	}, retry.IsRetryablePG)
	if err != nil {
		return "", fmt.Errorf("insert %s marker: %w", kind, err)
	}
	return id, nil
}

// MaxCommittedID returns the largest account id that is considered fully
// backfilled. It is used only by helpers/tests, not by the reconciler.
func (s *Store) MaxCommittedID(ctx context.Context) (int64, error) {
	conn, err := s.conn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close(context.Background())
	var max *int64
	if err := conn.QueryRow(ctx, `SELECT MAX(chunk_max_id) FROM seam_marker WHERE kind = 'high'`).Scan(&max); err != nil {
		return 0, err
	}
	if max == nil {
		return -1, nil
	}
	return *max, nil
}

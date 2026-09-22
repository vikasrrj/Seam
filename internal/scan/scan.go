// Package scan reads bounded primary-key chunks from the source accounts table.
package scan

import (
	"context"
	"fmt"

	"example.com/seam/internal/model"
	"example.com/seam/internal/retry"
	"example.com/seam/internal/transport"
	"github.com/jackc/pgx/v5"
)

// ChunkReader performs keyset pagination over the source accounts table.
type ChunkReader struct {
	dsn string
}

func NewChunkReader(dsn string) *ChunkReader {
	return &ChunkReader{dsn: dsn}
}

func (r *ChunkReader) conn(ctx context.Context) (*pgx.Conn, error) {
	return transport.ConnectPostgres(ctx, r.dsn)
}

// UpperBound returns the maximum id in the source table at backfill start.
func (r *ChunkReader) UpperBound(ctx context.Context) (int64, error) {
	return retry.Do(ctx, retry.DefaultConfig(), func() (int64, error) {
		conn, err := r.conn(ctx)
		if err != nil {
			return 0, err
		}
		defer conn.Close(context.Background())
		var max *int64
		if err := conn.QueryRow(ctx, `SELECT MAX(id) FROM accounts`).Scan(&max); err != nil {
			return 0, err
		}
		if max == nil {
			return 0, nil
		}
		return *max, nil
	}, retry.IsRetryablePG)
}

// chunkResult carries the return values for NextChunk through retry.Do.
type chunkResult struct {
	chunk model.ChunkRange
	ok    bool
}

// NextChunk returns the inclusive primary-key interval of the next chunk using
// keyset pagination. It finds the smallest chunk_size rows with completedThrough
// < id <= upperBound; rows beyond the backfill upper bound are delivered via
// CDC only and must never be chunked. If no rows remain, it returns false.
func (r *ChunkReader) NextChunk(ctx context.Context, completedThrough, upperBound int64, chunkSize int) (model.ChunkRange, bool, error) {
	res, err := retry.Do(ctx, retry.DefaultConfig(), func() (chunkResult, error) {
		conn, err := r.conn(ctx)
		if err != nil {
			return chunkResult{}, err
		}
		defer conn.Close(context.Background())

		var minID, maxID *int64
		if err := conn.QueryRow(ctx, `
			SELECT MIN(id), MAX(id) FROM (
				SELECT id FROM accounts WHERE id > $1 AND id <= $2 ORDER BY id LIMIT $3
			) sub`, completedThrough, upperBound, chunkSize).Scan(&minID, &maxID); err != nil {
			return chunkResult{}, fmt.Errorf("find next chunk after %d: %w", completedThrough, err)
		}
		if minID == nil || maxID == nil {
			return chunkResult{}, nil
		}
		return chunkResult{chunk: model.ChunkRange{Min: *minID, Max: *maxID}, ok: true}, nil
	}, retry.IsRetryablePG)
	if err != nil {
		return model.ChunkRange{}, false, err
	}
	return res.chunk, res.ok, nil
}

// ReadChunk reads all rows with id in [minID, maxID] inclusive.
func (r *ChunkReader) ReadChunk(ctx context.Context, minID, maxID int64) ([]model.Account, error) {
	return retry.Do(ctx, retry.DefaultConfig(), func() ([]model.Account, error) {
		conn, err := r.conn(ctx)
		if err != nil {
			return nil, err
		}
		defer conn.Close(context.Background())

		rows, err := conn.Query(ctx,
			`SELECT id, owner, balance_cents FROM accounts WHERE id >= $1 AND id <= $2 ORDER BY id`,
			minID, maxID)
		if err != nil {
			return nil, fmt.Errorf("query chunk [%d,%d]: %w", minID, maxID, err)
		}
		defer rows.Close()

		var result []model.Account
		for rows.Next() {
			var account model.Account
			if err := rows.Scan(&account.ID, &account.Owner, &account.BalanceCents); err != nil {
				return nil, fmt.Errorf("scan row: %w", err)
			}
			result = append(result, account)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return result, nil
	}, retry.IsRetryablePG)
}

// NextChunk is the legacy helper that computes the next interval without
// querying the database. It is kept for callers that do not need sparse-key
// handling. Ranges never exceed the backfill upper bound and the arithmetic is
// overflow-safe even when a chunk reaches math.MaxInt64.
func NextChunk(completedThrough int64, chunkSize int, upperBound int64) (model.ChunkRange, bool) {
	if completedThrough >= upperBound {
		return model.ChunkRange{}, false
	}
	minID := completedThrough + 1
	maxID := minID + int64(chunkSize) - 1
	if maxID > upperBound || maxID < minID {
		maxID = upperBound
	}
	return model.ChunkRange{Min: minID, Max: maxID}, true
}

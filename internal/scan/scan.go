// Package scan reads bounded primary-key chunks from the source accounts table.
package scan

import (
	"context"
	"fmt"
	"regexp"

	"example.com/seam/internal/model"
	"example.com/seam/internal/retry"
	"example.com/seam/internal/transport"
	"github.com/jackc/pgx/v5"
)

// identPattern matches PostgreSQL identifiers Seam is willing to interpolate
// into SQL. Anything else is rejected to keep generated SQL injection-safe.
var identPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// DefaultChunkReaderTable is the source table the built-in readers scan.
const DefaultChunkReaderTable = "accounts"

// DefaultChunkReaderKey is the primary-key column used for keyset pagination.
const DefaultChunkReaderKey = "id"

// ChunkReader performs keyset pagination over the source table. It never uses
// OFFSET: every page is anchored on the strictly-greater-than predicate of the
// previous page's last key, which keeps page cost independent of table size.
type ChunkReader struct {
	dsn       string
	table     string
	keyColumn string
}

// NewChunkReader returns a reader over the default accounts table keyed by id.
func NewChunkReader(dsn string) *ChunkReader {
	r, err := NewChunkReaderFor(dsn, DefaultChunkReaderTable, DefaultChunkReaderKey)
	if err != nil {
		// Defaults are validated constants; this cannot fail.
		panic(fmt.Sprintf("scan: invalid default reader config: %v", err))
	}
	return r
}

// NewChunkReaderFor returns a keyset reader over an arbitrary table and key
// column. table and keyColumn must be plain identifiers (letters, digits,
// underscores); anything else is rejected before a connection is made.
func NewChunkReaderFor(dsn, table, keyColumn string) (*ChunkReader, error) {
	if !identPattern.MatchString(table) {
		return nil, fmt.Errorf("scan: invalid table name %q", table)
	}
	if !identPattern.MatchString(keyColumn) {
		return nil, fmt.Errorf("scan: invalid key column %q", keyColumn)
	}
	return &ChunkReader{dsn: dsn, table: table, keyColumn: keyColumn}, nil
}

func (r *ChunkReader) conn(ctx context.Context) (*pgx.Conn, error) {
	return transport.ConnectPostgres(ctx, r.dsn)
}

// UpperBound returns the maximum key value in the source table at backfill
// start. An empty table yields 0, which makes the backfill trivially complete.
func (r *ChunkReader) UpperBound(ctx context.Context) (int64, error) {
	return retry.Do(ctx, retry.DefaultConfig(), func() (int64, error) {
		conn, err := r.conn(ctx)
		if err != nil {
			return 0, err
		}
		defer conn.Close(context.Background())
		var max *int64
		if err := conn.QueryRow(ctx, upperBoundQuery(r.table, r.keyColumn)).Scan(&max); err != nil {
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
		if err := conn.QueryRow(ctx,
			nextChunkQuery(r.table, r.keyColumn, chunkSize),
			completedThrough, upperBound).Scan(&minID, &maxID); err != nil {
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

// ReadChunk reads all rows with key in [minID, maxID] inclusive.
func (r *ChunkReader) ReadChunk(ctx context.Context, minID, maxID int64) ([]model.Account, error) {
	return retry.Do(ctx, retry.DefaultConfig(), func() ([]model.Account, error) {
		conn, err := r.conn(ctx)
		if err != nil {
			return nil, err
		}
		defer conn.Close(context.Background())

		rows, err := conn.Query(ctx, readChunkQuery(r.table, r.keyColumn), minID, maxID)
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

// upperBoundQuery builds the keyset upper-bound query for a table/key pair.
func upperBoundQuery(table, key string) string {
	return fmt.Sprintf(`SELECT MAX(%s) FROM %s`, key, table)
}

// nextChunkQuery builds the next-page keyset query: the caller then binds
// (lastKeyInclusive, upperBound). The LIMIT is pinned to the configured chunk
// size; page cost does not grow with completedThrough because the query is
// anchored on a strictly-greater-than predicate rather than OFFSET.
func nextChunkQuery(table, key string, chunkSize int) string {
	return fmt.Sprintf(
		`SELECT MIN(%[2]s), MAX(%[2]s) FROM (SELECT %[2]s FROM %[1]s WHERE %[2]s > $1 AND %[2]s <= $2 ORDER BY %[2]s LIMIT %[3]d) sub`,
		table, key, chunkSize)
}

// readChunkQuery builds the in-chunk read query for a table/key pair.
func readChunkQuery(table, key string) string {
	return fmt.Sprintf(
		`SELECT id, owner, balance_cents FROM %[1]s WHERE %[2]s >= $1 AND %[2]s <= $2 ORDER BY %[2]s`,
		table, key)
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

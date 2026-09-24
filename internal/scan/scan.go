// Package scan reads bounded primary-key chunks from a source table.
package scan

import (
	"context"
	"fmt"

	"example.com/seam/internal/model"
	"example.com/seam/internal/retry"
	"example.com/seam/internal/schema"
	"example.com/seam/internal/transport"
	"github.com/jackc/pgx/v5"
)

// DefaultChunkReaderTable is the source table the built-in readers scan when
// none is configured.
const DefaultChunkReaderTable = "accounts"

// ChunkReader performs keyset pagination over a source table, keyed by the
// primary key the durable schema descriptor pins. It never uses OFFSET: every
// page is anchored on the strictly-greater-than predicate of the previous
// page's last key, which keeps page cost independent of table size.
type ChunkReader struct {
	dsn      string
	schema   *schema.Schema
	key      string
	table    string
	maxRows  int
	maxBytes int64
}

// SetLimits bounds one in-flight scan before it has accumulated a full chunk.
// The byte estimate includes row overhead plus canonical cell text.
func (r *ChunkReader) SetLimits(maxRows int, maxBytes int64) error {
	if maxRows < 1 || maxBytes < 1 {
		return fmt.Errorf("scan: candidate limits must be positive")
	}
	r.maxRows, r.maxBytes = maxRows, maxBytes
	return nil
}

// NewChunkReader returns a reader over the default accounts table. The durable
// descriptor is loaded eagerly so the chunking queries and column lists are
// proven against the catalog before the job starts.
func NewChunkReader(ctx context.Context, dsn string) (*ChunkReader, error) {
	return NewChunkReaderFor(ctx, dsn, DefaultChunkReaderTable)
}

// NewChunkReaderFor returns a keyset reader over the named table. The table's
// schema is loaded from the source catalog and validated against the supported
// type set; anything else fails closed before a connection is made.
func NewChunkReaderFor(ctx context.Context, dsn, table string) (*ChunkReader, error) {
	descriptor, err := schema.Load(ctx, dsn, "public", table)
	if err != nil {
		return nil, fmt.Errorf("scan: load schema for public.%s: %w", table, err)
	}
	pk := descriptor.PKColumn()
	if pk == nil {
		return nil, fmt.Errorf("scan: public.%s has no primary key", table)
	}
	return &ChunkReader{
		dsn:    dsn,
		schema: descriptor,
		key:    pk.Name,
		table:  table,
	}, nil
}

// Schema exposes the validated source descriptor, so callers can verify rows
// against the same fingerprint the job recorded.
func (r *ChunkReader) Schema() *schema.Schema { return r.schema }

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
		if err := conn.QueryRow(ctx, upperBoundQuery(r.schema.TableQualified(), r.key)).Scan(&max); err != nil {
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
	return r.NextChunkFrom(ctx, &completedThrough, upperBound, chunkSize)
}

// NextChunkFrom accepts a nil cursor for the first page. A numeric sentinel
// would exclude a real row whose key is math.MinInt64.
func (r *ChunkReader) NextChunkFrom(ctx context.Context, cursor *int64, upperBound int64, chunkSize int) (model.ChunkRange, bool, error) {
	if chunkSize < 1 {
		return model.ChunkRange{}, false, fmt.Errorf("scan: chunk size must be positive")
	}
	if cursor != nil && *cursor >= upperBound {
		return model.ChunkRange{}, false, nil
	}
	table := r.schema.TableQualified()
	res, err := retry.Do(ctx, retry.DefaultConfig(), func() (chunkResult, error) {
		conn, err := r.conn(ctx)
		if err != nil {
			return chunkResult{}, err
		}
		defer conn.Close(context.Background())

		var minID, maxID *int64
		query := firstChunkQuery(table, r.key, chunkSize)
		args := []any{upperBound}
		if cursor != nil {
			query = nextChunkQuery(table, r.key, chunkSize)
			args = []any{*cursor, upperBound}
		}
		if err := conn.QueryRow(ctx, query, args...).Scan(&minID, &maxID); err != nil {
			return chunkResult{}, fmt.Errorf("find next chunk after %v: %w", cursor, err)
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

func firstChunkQuery(table, key string, chunkSize int) string {
	return fmt.Sprintf(
		`SELECT MIN(%[2]s), MAX(%[2]s) FROM (SELECT %[2]s FROM %[1]s WHERE %[2]s <= $1 ORDER BY %[2]s LIMIT %[3]d) sub`,
		table, key, chunkSize)
}

// ReadChunk reads all rows with key in [minID, maxID] inclusive as canonical
// text under a UTC session, and returns them as positional model.Row values.
func (r *ChunkReader) ReadChunk(ctx context.Context, minID, maxID int64) ([]model.Row, error) {
	return retry.Do(ctx, retry.DefaultConfig(), func() ([]model.Row, error) {
		conn, err := r.conn(ctx)
		if err != nil {
			return nil, err
		}
		defer conn.Close(context.Background())

		rows, err := r.schema.ScanRows(ctx, conn, minID, maxID)
		if err != nil {
			return nil, fmt.Errorf("read chunk [%d,%d]: %w", minID, maxID, err)
		}
		if r.maxRows > 0 && len(rows) > r.maxRows {
			return nil, fmt.Errorf("chunk [%d,%d] exceeds %d-row scan limit", minID, maxID, r.maxRows)
		}
		if r.maxBytes > 0 {
			var estimated int64
			for _, row := range rows {
				estimated += 96
				for _, v := range row.Values {
					if v.Kind == model.ValueText {
						estimated += int64(len(v.Text))
					}
				}
			}
			if estimated > r.maxBytes {
				return nil, fmt.Errorf("chunk [%d,%d] exceeds %d-byte scan budget", minID, maxID, r.maxBytes)
			}
		}
		return rows, nil
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

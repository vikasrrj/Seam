// Package sink applies row changes to the destination table described by the
// job's durable schema descriptor.
package sink

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"example.com/seam/internal/model"
	"example.com/seam/internal/schema"
	"github.com/jackc/pgx/v5"
)

// Mutator applies a model.Change to the destination table inside a single
// transaction. It does not commit.
type Mutator struct {
	table  string
	schema *schema.Schema
}

const candidateBatchRows = 1000
const candidateBatchBytes = 4 << 20

// NewMutatorFor routes one job to its physical destination table. The table
// must be a plain identifier because it is interpolated into SQL; the schema
// descriptor provides all column names, types, and the conflict key.
func NewMutatorFor(table string, dataSchema *schema.Schema) (*Mutator, error) {
	if dataSchema == nil {
		return nil, fmt.Errorf("sink: nil source schema descriptor")
	}
	if err := schema.ValidateIdentifier(table); err != nil {
		return nil, fmt.Errorf("unsupported destination table %q", table)
	}
	return &Mutator{table: table, schema: dataSchema}, nil
}

// Schema exposes the descriptor the sink applies rows against.
func (m *Mutator) Schema() *schema.Schema { return m.schema }

// Apply applies one generic row change: inserts and updates upsert the full
// canonical row, deletes key the primary key only.
func (m *Mutator) Apply(ctx context.Context, tx pgx.Tx, change model.Change) error {
	switch change.Op {
	case model.OpInsert, model.OpUpdate:
		if change.Row == nil {
			return fmt.Errorf("insert/update change has no row payload")
		}
		if err := m.upsert(ctx, tx, change.Row); err != nil {
			return err
		}
		return nil
	case model.OpDelete:
		if change.Row == nil {
			return fmt.Errorf("delete change has no row payload")
		}
		key, err := m.schema.KeyFromRow(change.Row)
		if err != nil {
			return fmt.Errorf("delete: %w", err)
		}
		return m.delete(ctx, tx, key)
	default:
		return fmt.Errorf("unsupported operation %q", change.Op)
	}
}

// ApplyBatch collapses repeated writes to the same key within one source
// transaction and applies the final key states with bounded set-based SQL.
// This preserves PostgreSQL commit visibility while removing one destination
// round trip per row. The caller still owns the encompassing transaction and
// couples it to the CDC checkpoint.
func (m *Mutator) ApplyBatch(ctx context.Context, tx pgx.Tx, changes []model.Change) error {
	if len(changes) == 0 {
		return nil
	}
	last := make(map[int64]model.Change, len(changes))
	order := make([]int64, 0, len(changes))
	for _, change := range changes {
		if change.Row == nil {
			return fmt.Errorf("change has no row payload")
		}
		if change.Op != model.OpInsert && change.Op != model.OpUpdate && change.Op != model.OpDelete {
			return fmt.Errorf("unsupported operation %q", change.Op)
		}
		key, err := m.schema.KeyFromChange(&change)
		if err != nil {
			return err
		}
		if _, exists := last[key]; !exists {
			order = append(order, key)
		}
		last[key] = change
	}

	deletes := make([]int64, 0, len(last))
	upserts := make([]model.Row, 0, len(last))
	for _, key := range order {
		change := last[key]
		if change.Op == model.OpDelete {
			deletes = append(deletes, key)
		} else {
			upserts = append(upserts, *change.Row)
		}
	}
	for start := 0; start < len(deletes); start += candidateBatchRows {
		end := start + candidateBatchRows
		if end > len(deletes) {
			end = len(deletes)
		}
		key := m.schema.PKColumn().Name
		if _, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE %s = ANY($1::bigint[])`, m.table, key), deletes[start:end]); err != nil {
			return fmt.Errorf("delete %d CDC keys: %w", end-start, err)
		}
	}
	return m.WriteCandidates(ctx, tx, upserts)
}

// bindRow renders one full row as canonical text parameters: a string per
// cell (or nil for SQL NULL). Integer cells are re-rendered from their typed
// int64 form; every other cell is already canonical text.
func (m *Mutator) bindRow(row *model.Row) ([]any, error) {
	if row == nil {
		return nil, fmt.Errorf("nil row")
	}
	if len(row.Values) != len(m.schema.Columns) {
		return nil, fmt.Errorf("row has %d values; schema expects %d", len(row.Values), len(m.schema.Columns))
	}
	bind := make([]any, len(row.Values))
	for i, col := range m.schema.Columns {
		value := row.Values[i]
		switch value.Kind {
		case model.ValueNull:
			bind[i] = nil
		case model.ValueInt64:
			bind[i] = strconv.FormatInt(value.Int, 10)
		case model.ValueText:
			bind[i] = value.Text
		default:
			return nil, fmt.Errorf("row column %q has unsupported value kind %d", col.Name, value.Kind)
		}
	}
	return bind, nil
}

// nonKeyUpdates renders the ON CONFLICT DO UPDATE assignment list over every
// non-primary-key column.
func (m *Mutator) nonKeyUpdates() string {
	pk := m.schema.PKColumn().Name
	var updates []string
	for _, col := range m.schema.Columns {
		if col.Name == pk {
			continue
		}
		updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", col.Name, col.Name))
	}
	return strings.Join(updates, ", ")
}

func (m *Mutator) upsert(ctx context.Context, tx pgx.Tx, row *model.Row) error {
	bind, err := m.bindRow(row)
	if err != nil {
		return err
	}
	pk := m.schema.PKColumn().Name
	key, err := m.schema.KeyFromRow(row)
	if err != nil {
		return err
	}
	cols := m.schema.Columns
	args := m.schema.CanonicalApplyList(cols, 1)
	query := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s`,
		m.table, m.schema.CanonicalColumnList(), args, pk, m.nonKeyUpdates())
	if _, err := tx.Exec(ctx, query, bind...); err != nil {
		return fmt.Errorf("upsert id=%d: %w", key, err)
	}
	return nil
}

func (m *Mutator) delete(ctx context.Context, tx pgx.Tx, id int64) error {
	key := m.schema.PKColumn().Name
	_, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE %s = $1`, m.table, key), id)
	if err != nil {
		return fmt.Errorf("delete id=%d: %w", id, err)
	}
	return nil
}

// WriteCandidates inserts surviving snapshot candidates in one batch. Every
// survivor has passed LOW/HIGH collision eviction. Keep batches bounded by
// both rows and bytes so a large window does not create an unbounded SQL
// message. Each statement remains inside the caller's destination transaction
// with chunk state and checkpoint.
func (m *Mutator) WriteCandidates(ctx context.Context, tx pgx.Tx, candidates []model.Row) error {
	if len(candidates) == 0 {
		return nil
	}
	cols := m.schema.Columns
	pk := m.schema.PKColumn().Name
	selectList := make([]string, len(cols))
	for i, col := range cols {
		if col.Name == pk {
			selectList[i] = "batch." + pk
			continue
		}
		selectList[i] = fmt.Sprintf("(batch.%s)::%s", col.Name, col.TypeName)
	}
	unnestArgs := make([]string, len(cols))
	for i, col := range cols {
		if col.Name == pk {
			unnestArgs[i] = fmt.Sprintf("$%d::bigint[]", i+1)
		} else {
			unnestArgs[i] = fmt.Sprintf("$%d::text[]", i+1)
		}
	}
	batchCols := make([]string, len(cols))
	for i, col := range cols {
		batchCols[i] = col.Name
	}
	core := fmt.Sprintf(
		`INSERT INTO %s (%s) SELECT %s FROM unnest(%s) AS batch(%s) ON CONFLICT (%s) DO UPDATE SET %s`,
		m.table, m.schema.CanonicalColumnList(), strings.Join(selectList, ", "),
		strings.Join(unnestArgs, ", "), strings.Join(batchCols, ", "), pk, m.nonKeyUpdates())

	for start := 0; start < len(candidates); {
		ids := make([]int64, 0, candidateBatchRows)
		cellArrays := make([][]*string, len(cols))
		bytes := 0
		for start < len(candidates) && len(ids) < candidateBatchRows {
			row := candidates[start]
			if len(row.Values) != len(cols) {
				return fmt.Errorf("candidate row has %d values; schema expects %d", len(row.Values), len(cols))
			}
			key, err := m.schema.KeyFromRow(&row)
			if err != nil {
				return err
			}
			rowBytes := 0
			for i, col := range cols {
				value := row.Values[i]
				if col.Name == pk {
					continue // the key travels in the bigint[] column
				}
				switch value.Kind {
				case model.ValueNull:
					cellArrays[i] = append(cellArrays[i], nil)
				case model.ValueInt64:
					rowBytes += 16
					text := strconv.FormatInt(value.Int, 10)
					cellArrays[i] = append(cellArrays[i], &text)
				case model.ValueText:
					rowBytes += len(value.Text)
					text := value.Text
					cellArrays[i] = append(cellArrays[i], &text)
				default:
					return fmt.Errorf("candidate id=%d column %q has unsupported value kind %d", key, col.Name, value.Kind)
				}
			}
			if rowBytes > candidateBatchBytes {
				return fmt.Errorf("candidate id=%d exceeds %d-byte batch limit", key, candidateBatchBytes)
			}
			if len(ids) > 0 && bytes+rowBytes > candidateBatchBytes {
				break
			}
			ids = append(ids, key)
			bytes += rowBytes
			start++
		}
		args := make([]any, len(cols))
		for i, col := range cols {
			if col.Name == pk {
				args[i] = ids
			} else {
				args[i] = cellArrays[i]
			}
		}
		if _, err := tx.Exec(ctx, core, args...); err != nil {
			return fmt.Errorf("write %d candidates starting at id=%d: %w", len(ids), ids[0], err)
		}
	}
	return nil
}

// WriteStagedCandidates copies the surviving rows of one durable candidate
// window into the destination with one set-based statement. Staged payloads
// are canonical-text JSON objects keyed by column name.
func (m *Mutator) WriteStagedCandidates(ctx context.Context, tx pgx.Tx, chunk *model.Chunk) (int64, error) {
	cols := m.schema.Columns
	pk := m.schema.PKColumn().Name
	selectList := make([]string, len(cols))
	for i, col := range cols {
		selectList[i] = fmt.Sprintf("(elem->>'%s')::%s", col.Name, col.TypeName)
	}
	query := fmt.Sprintf(`
		INSERT INTO %s (%s)
		SELECT %s
		FROM seam_candidates c, jsonb_array_elements(c.payload) AS elem
		WHERE c.job_id = $1 AND c.attempt = $2 AND c.chunk_min_id = $3 AND c.evicted = FALSE
		ON CONFLICT (%s) DO UPDATE SET %s`,
		m.table, m.schema.CanonicalColumnList(), strings.Join(selectList, ", "), pk, m.nonKeyUpdates())
	tag, err := tx.Exec(ctx, query, chunk.JobID, chunk.Attempt, chunk.ChunkMinID)
	if err != nil {
		return 0, fmt.Errorf("write staged candidates for chunk %s: %w", chunk.Range(), err)
	}
	return tag.RowsAffected(), nil
}

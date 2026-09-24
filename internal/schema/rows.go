package schema

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"example.com/seam/internal/model"
	"github.com/jackc/pgx/v5"
)

// KeyFromRow extracts the int64 message primary key from a positional row.
func (s *Schema) KeyFromRow(row *model.Row) (int64, error) {
	if row == nil {
		return 0, fmt.Errorf("row is nil")
	}
	if s.PKOrdinal < 1 || s.PKOrdinal > len(row.Values) {
		return 0, fmt.Errorf("primary key ordinal %d out of range for %d-value row", s.PKOrdinal, len(row.Values))
	}
	v := row.Values[s.PKOrdinal-1]
	if v.Kind != model.ValueInt64 && v.Kind != model.ValueNull {
		return 0, fmt.Errorf("primary key value is not typed int64 (kind %d)", v.Kind)
	}
	if v.Kind == model.ValueNull {
		return 0, fmt.Errorf("primary key value is NULL")
	}
	return v.Int, nil
}

// KeyFromChange extracts the message primary key from a row change.
func (s *Schema) KeyFromChange(change *model.Change) (int64, error) {
	if change == nil || change.Row == nil {
		return 0, fmt.Errorf("change has no row payload")
	}
	return s.KeyFromRow(change.Row)
}

// BindValue converts one canonical cell into a pgx bindable parameter. The
// parameter is always sent as text (the caller renders ($n::text)::type), so
// the only codec involved is the text codec and PostgreSQL's own input
// functions perform the exact cast. NULL binds as nil.
func (s *Schema) BindValue(col Column, v model.Value) any {
	if v.IsNull() {
		return nil
	}
	if s.IsIntegerColumn(col) {
		return v.Int
	}
	return v.Text
}

// IsIntegerColumn reports whether the column is carried as a typed int64.
func (s *Schema) IsIntegerColumn(col Column) bool {
	return integerTypeOIDs[col.TypeOID] && col.TypeOID != 0
}

// CanonicalColumnList renders "col1, col2, ..." for SELECT/INSERT columns.
func (s *Schema) CanonicalColumnList() string {
	names := make([]string, len(s.Columns))
	for i, col := range s.Columns {
		names[i] = col.Name
	}
	return strings.Join(names, ", ")
}

// CanonicalScanList renders "(col1)::text, (col2)::text, ..." so a scan
// returns the canonical text form of every column under the connection's
// session timezone. Callers must SET TIME ZONE 'UTC' on the session first so
// timestamptz values are deterministic and byte-identical to the capture side.
func (s *Schema) CanonicalScanList() string {
	parts := make([]string, len(s.Columns))
	for i, col := range s.Columns {
		parts[i] = fmt.Sprintf("(%s)::text", col.Name)
	}
	return strings.Join(parts, ", ")
}

// CanonicalApplyList renders "($1::text)::type1, ($2::text)::type2, ...".
// Every value travels as canonical text and is cast by PostgreSQL's own type
// input function; no Go codec ever reformats a value.
func (s *Schema) CanonicalApplyList(cols []Column, firstParam int) string {
	parts := make([]string, len(cols))
	for i := range cols {
		parts[i] = fmt.Sprintf("($%d::text)::%s", firstParam+i, cols[i].TypeName)
	}
	return strings.Join(parts, ", ")
}

// TableQualified renders the schema-qualified quoted table name.
func (s *Schema) TableQualified() string {
	return fmt.Sprintf("%s.%s", s.Namespace, s.Table)
}

// ScanRows reads the given key range from the source table as canonical text,
// under a UTC session. It returns one positional model.Row per source row
// ordered by the primary key. The primary key and the integer column family
// are parsed into typed int64 values; every other column stays in its exact
// canonical text form.
func (s *Schema) ScanRows(ctx context.Context, conn *pgx.Conn, minID, maxID int64) ([]model.Row, error) {
	if _, err := conn.Exec(ctx, "SET TIME ZONE 'UTC'"); err != nil {
		return nil, fmt.Errorf("set UTC session for canonical scan: %w", err)
	}
	key := s.PKColumn().Name
	query := fmt.Sprintf(`SELECT %s FROM %s WHERE %s >= $1 AND %s <= $2 ORDER BY %s`,
		s.CanonicalScanList(), s.TableQualified(), key, key, key)
	rows, err := conn.Query(ctx, query, minID, maxID)
	if err != nil {
		return nil, fmt.Errorf("scan %s [%d,%d]: %w", s.TableQualified(), minID, maxID, err)
	}
	defer rows.Close()
	var result []model.Row
	for rows.Next() {
		row, err := s.scanRow(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

type rowScanner interface {
	Scan(...any) error
}

func (s *Schema) scanRow(row rowScanner) (model.Row, error) {
	values := make([]model.Value, len(s.Columns))
	// Scan into *string (NULL yields nil) for every column; the canonical
	// ::text cast guarantees a text-format scan result.
	dest := make([]*string, len(s.Columns))
	args := make([]any, len(s.Columns))
	for i := range dest {
		args[i] = &dest[i]
	}
	if err := row.Scan(args...); err != nil {
		return model.Row{}, err
	}
	for i, col := range s.Columns {
		if dest[i] == nil {
			values[i] = model.NullValue()
			continue
		}
		if s.IsIntegerColumn(col) {
			n, err := strconv.ParseInt(*dest[i], 10, 64)
			if err != nil {
				return model.Row{}, fmt.Errorf("column %q canonical text %q is not an integer: %w", col.Name, *dest[i], err)
			}
			values[i] = model.Int64Value(n)
			continue
		}
		values[i] = model.TextValue(*dest[i])
	}
	return model.Row{Values: values}, nil
}

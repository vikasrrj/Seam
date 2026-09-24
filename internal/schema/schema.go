// Package schema models the durable, stable-until-generation PostgreSQL table
// shape Seam is willing to replicate. A Schema is loaded once from the source
// catalog, validated against the supported type contract, given a
// deterministic fingerprint, and pinned to jobs and transaction envelopes.
// Neither the capture decoder, the source scanner, the destination sink, nor
// promotion ever assumes the fixed accounts shape; every column position and
// cast is derived from this descriptor.
//
// Stability contract: one schema per job generation. Any change to the
// published columns, their order, their types, their nullability, or the
// primary key changes the fingerprint, and every consumer fails closed
// rather than applying rows to a schema it did not pin.
package schema

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"example.com/seam/internal/transport"
	"github.com/jackc/pgx/v5"
)

// identPattern matches PostgreSQL identifiers Seam is willing to interpolate
// into SQL. Anything else is rejected to keep generated SQL injection-safe.
var identPattern = identRE()

// Column is one ordered column of the replicated table.
type Column struct {
	// Ordinal is the 1-based position in CREATE TABLE / pg_attribute order.
	Ordinal int    `json:"ordinal"`
	Name    string `json:"name"`
	// TypeName is the canonical pg_type.typname (for example "bigint",
	// "text", "numeric", "timestamptz", "bytea").
	TypeName string `json:"type_name"`
	// TypeOID is the PostgreSQL type OID.
	TypeOID uint32 `json:"type_oid"`
	// Nullable is true when the column may hold NULL.
	Nullable bool `json:"nullable"`
	// PrimaryKey is true for the single BIGINT primary-key column used for
	// chunking. Exactly one column must have it.
	PrimaryKey bool `json:"primary_key"`
}

// Schema is the durable descriptor of one replicated table.
type Schema struct {
	// Namespace and Table are the quoted-identifier-safe schema/table names.
	Namespace string   `json:"namespace"`
	Table     string   `json:"table"`
	Columns   []Column `json:"columns"`
	// ReplicaIdentity is the pg_class.relreplident value ("f" = FULL is the
	// only supported value; it is required to reconstruct unchanged TOAST
	// columns on update).
	ReplicaIdentity string `json:"replica_identity"`
	// PKOrdinal is the 1-based ordinal of the single BIGINT chunking key.
	PKOrdinal int `json:"pk_ordinal"`
	// Fingerprint is the deterministic schema epoch; jobs and envelopes carry
	// it so schema drift fails closed everywhere.
	Fingerprint string `json:"fingerprint"`
}

// PKColumn returns the primary-key column.
func (s *Schema) PKColumn() *Column {
	if s == nil || s.PKOrdinal < 1 || s.PKOrdinal > len(s.Columns) {
		return nil
	}
	return &s.Columns[s.PKOrdinal-1]
}

// SQLTable renders the schema-qualified table for interpolation. Namespace
// and Table have already been validated as bare identifiers.
func (s *Schema) SQLTable() string {
	return fmt.Sprintf("%s.%s", s.Namespace, s.Table)
}

// ColumnNames returns the ordered bare column names.
func (s *Schema) ColumnNames() []string {
	names := make([]string, len(s.Columns))
	for i, col := range s.Columns {
		names[i] = col.Name
	}
	return names
}

// StringList is a helper for error messages.
func (s *Schema) StringList() string {
	parts := make([]string, len(s.Columns))
	for i, col := range s.Columns {
		parts[i] = fmt.Sprintf("%s %s%s", col.Name, col.TypeName, nullSuffix(col))
	}
	return strings.Join(parts, ", ")
}

func nullSuffix(col Column) string {
	if col.Nullable {
		return " NULL"
	}
	return " NOT NULL"
}

// SupportedTypes is the closed set of PostgreSQL type OIDs Seam can
// replicate exactly. Values travel as canonical text (the exact inversion of
// PostgreSQL's own type input/output functions) and are bound back through
// explicit ::type casts, so no lossy string conversion ever happens. The
// value kind carried in model.Value is derived from this map.
//
// int2/int4/int8, bool, text/varchar/bpchar/char, bytea, numeric,
// float4/float8, timestamp/timestamptz, date, time, timetz, interval, uuid,
// json, and jsonb all round-trip exactly through their canonical text form.
// Everything else fails closed at schema load, before any row is touched.
var SupportedTypes = map[uint32]string{
	16:   "bool",
	17:   "bytea",
	18:   "char",
	20:   "int8",
	21:   "int2",
	23:   "int4",
	25:   "text",
	114:  "json",
	700:  "float4",
	701:  "float8",
	1042: "bpchar",
	1043: "varchar",
	1082: "date",
	1083: "time",
	1114: "timestamp",
	1184: "timestamptz",
	1186: "interval",
	1266: "timetz",
	1700: "numeric",
	2950: "uuid",
	3802: "jsonb",
}

// integerTypeOIDs are the types whose canonical text form is a plain integer
// and whose Go representation is int64 (the message-primary-key set).
var integerTypeOIDs = map[uint32]bool{20: true, 21: true, 23: true}

// IsIntegerType reports whether the type is carried as a typed int64 value.
func IsIntegerType(oid uint32) bool { return integerTypeOIDs[oid] }

// IsTimeType reports whether the type must be parsed to a Go time.Time when
// binding parameters (so PostgreSQL receives a typed timestamp, never a
// session-dependent text form).
func IsTimeType(oid uint32) bool { return oid == 1114 || oid == 1184 }

// IsByteaType reports the bytea type.
func IsByteaType(oid uint32) bool { return oid == 17 }

// IsBoolType reports the bool type.
func IsBoolType(oid uint32) bool { return oid == 16 }

// Load reads and validates the schema for one table from the PostgreSQL
// catalog. It returns an error when the table is absent, has no single
// BIGINT primary key, publishes an unsupported type, or is not using
// REPLICA IDENTITY FULL.
func Load(ctx context.Context, dsn, namespace, table string) (*Schema, error) {
	if err := ValidateIdentifier(namespace); err != nil {
		return nil, fmt.Errorf("schema namespace %q: %w", namespace, err)
	}
	if err := ValidateIdentifier(table); err != nil {
		return nil, fmt.Errorf("schema table %q: %w", table, err)
	}
	conn, err := transport.ConnectPostgres(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect for schema catalog: %w", err)
	}
	defer conn.Close(context.Background())
	descriptor, err := LoadFromConn(ctx, conn, namespace, table)
	if err != nil {
		return nil, err
	}
	return descriptor, nil
}

// LoadFromConn reads the descriptor using an existing connection, requiring
// REPLICA IDENTITY FULL (a source-CDC property).
func LoadFromConn(ctx context.Context, conn *pgx.Conn, namespace, table string) (*Schema, error) {
	return loadFromConn(ctx, conn, namespace, table, true)
}

// LoadDestFromConn reads a destination-column descriptor without requiring
// REPLICA IDENTITY FULL. Seam writes full rows to destinations, so the
// destination's replica identity is irrelevant and must not fail validation.
func LoadDestFromConn(ctx context.Context, conn Querier, namespace, table string) (*Schema, error) {
	return loadFromConn(ctx, conn, namespace, table, false)
}

func loadFromConn(ctx context.Context, conn Querier, namespace, table string, requireReplicaIdentityFull bool) (*Schema, error) {
	var replicaIdentity string
	if err := conn.QueryRow(ctx, `
		SELECT relreplident::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2`, namespace, table).Scan(&replicaIdentity); err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("source %s.%s does not exist", namespace, table)
		}
		return nil, fmt.Errorf("read replica identity of %s.%s: %w", namespace, table, err)
	}
	if requireReplicaIdentityFull && replicaIdentity != "f" {
		return nil, fmt.Errorf("%s.%s must use REPLICA IDENTITY FULL so unchanged TOAST values can be reconstructed", namespace, table)
	}

	rows, err := conn.Query(ctx, `
		SELECT a.attnum, a.attname::text, t.typname::text, a.atttypid::oid, a.attnotnull
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_type t ON t.oid = a.atttypid
		WHERE n.nspname = $1 AND c.relname = $2
		  AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum`, namespace, table)
	if err != nil {
		return nil, fmt.Errorf("read columns of %s.%s: %w", namespace, table, err)
	}
	defer rows.Close()
	var columns []Column
	for rows.Next() {
		var col Column
		if err := rows.Scan(&col.Ordinal, &col.Name, &col.TypeName, &col.TypeOID, &col.Nullable); err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if len(columns) == 0 {
		return nil, fmt.Errorf("%s.%s has no columns", namespace, table)
	}

	// Resolve the primary key constraint.
	var pkName string
	pkCols, err := primaryKeyColumns(ctx, conn, namespace, table)
	if err != nil {
		return nil, err
	}
	switch len(pkCols) {
	case 0:
		return nil, fmt.Errorf("%s.%s has no primary key; a single BIGINT primary key is required", namespace, table)
	case 1:
		pkName = pkCols[0]
	default:
		return nil, fmt.Errorf("%s.%s has a %d-column primary key; exactly one BIGINT primary-key column is required", namespace, table, len(pkCols))
	}
	var pkOrdinal int
	foundPK := false
	for i := range columns {
		if columns[i].Name == pkName {
			columns[i].PrimaryKey = true
			pkOrdinal = columns[i].Ordinal
			foundPK = true
		}
		typeName, supported := SupportedTypes[columns[i].TypeOID]
		if !supported {
			return nil, fmt.Errorf("%s.%s column %q has unsupported type %q (OID %d); supported types are: %s",
				namespace, table, columns[i].Name, columns[i].TypeName, columns[i].TypeOID, supportedTypeList())
		}
		if columns[i].TypeName != typeName {
			return nil, fmt.Errorf("%s.%s column %q type %q (OID %d) does not match catalog type name %q",
				namespace, table, columns[i].Name, columns[i].TypeName, columns[i].TypeOID, typeName)
		}
	}
	if !foundPK {
		return nil, fmt.Errorf("primary key column %q is absent from %s.%s", pkName, namespace, table)
	}
	pk := columns[pkOrdinal-1]
	if pk.TypeOID != 20 { // int8
		return nil, fmt.Errorf("%s.%s primary key %q must be BIGINT for ordered chunking, got %s",
			namespace, table, pk.Name, pk.TypeName)
	}
	if pk.Nullable {
		return nil, fmt.Errorf("%s.%s primary key %q cannot be nullable", namespace, table, pk.Name)
	}

	descriptor := &Schema{
		Namespace:       namespace,
		Table:           table,
		Columns:         columns,
		ReplicaIdentity: replicaIdentity,
		PKOrdinal:       pkOrdinal,
	}
	descriptor.Fingerprint = descriptor.FingerprintFor()
	return descriptor, nil
}

// FingerprintFor computes the same deterministic fingerprint as Load without
// re-reading the catalog, from an already-validated descriptor.
func (s *Schema) FingerprintFor() string {
	wrapper := struct {
		Namespace       string   `json:"namespace"`
		Table           string   `json:"table"`
		ReplicaIdentity string   `json:"replica_identity"`
		Columns         []Column `json:"columns"`
	}{
		Namespace:       s.Namespace,
		Table:           s.Table,
		ReplicaIdentity: s.ReplicaIdentity,
		Columns:         s.Columns,
	}
	b, _ := json.Marshal(wrapper)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])[:16]
}

// Querier is the minimal pgx surface Seam's descriptor and fingerprint
// queries need. Both *pgx.Conn and pgx.Tx implement it, so the same
// validation code runs on dedicated connections and inside destination
// transactions.
type Querier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// ValidateIsomorphicDestination checks that a destination table has exactly
// the source descriptor's columns (same names, order, types, and
// nullability) and a single BIGINT primary key on the same column. Seam
// applies rows positionally through the source descriptor, so a destination
// that differs is a fail-closed contract violation.
func ValidateIsomorphicDestination(ctx context.Context, conn Querier, source *Schema, destTable string) error {
	if err := ValidateIdentifier(destTable); err != nil {
		return fmt.Errorf("destination table %q: %w", destTable, err)
	}
	dst, err := LoadDestFromConn(ctx, conn, "public", destTable)
	if err != nil {
		return fmt.Errorf("destination %s.%s: %w", "public", destTable, err)
	}
	// Destination replica identity is irrelevant: Seam writes full rows.
	if len(dst.Columns) != len(source.Columns) {
		return fmt.Errorf("destination %s.%s has %d columns; source %s has %d",
			"public", destTable, len(dst.Columns), source.SQLTable(), len(source.Columns))
	}
	for i := range source.Columns {
		a, b := source.Columns[i], dst.Columns[i]
		if a.Name != b.Name || a.TypeOID != b.TypeOID || a.TypeName != b.TypeName || a.Nullable != b.Nullable {
			return fmt.Errorf("destination %s.%s column %d is (%s %s nullable=%v); source is (%s %s nullable=%v)",
				"public", destTable, i+1, b.Name, b.TypeName, b.Nullable, a.Name, a.TypeName, a.Nullable)
		}
	}
	if dst.PKOrdinal != source.PKOrdinal {
		return fmt.Errorf("destination %s.%s primary key is at ordinal %d; source %s is at %d",
			"public", destTable, dst.PKOrdinal, source.SQLTable(), source.PKOrdinal)
	}
	return nil
}

// primaryKeyColumns returns the ordered column names of the table's primary
// key. Multi-column keys are returned in constraint order so callers can
// reject them.
func primaryKeyColumns(ctx context.Context, conn Querier, namespace, table string) ([]string, error) {
	rows, err := conn.Query(ctx, `
		SELECT a.attname::text
		FROM pg_constraint con
		JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE
		JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
		WHERE con.conrelid = ($1 || '.' || $2)::regclass AND con.contype = 'p'
		ORDER BY k.ord`, namespace, table)
	if err != nil {
		return nil, fmt.Errorf("read primary key of %s.%s: %w", namespace, table, err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// ValidateIdentifier enforces the narrow, injection-safe identifier contract.
func ValidateIdentifier(identifier string) error {
	if !identPattern.MatchString(identifier) {
		return fmt.Errorf("invalid PostgreSQL identifier %q", identifier)
	}
	return nil
}

func supportedTypeList() string {
	names := make([]string, 0, len(SupportedTypes))
	for _, name := range SupportedTypes {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

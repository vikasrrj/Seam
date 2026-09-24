package sink

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"example.com/seam/internal/model"
	"example.com/seam/internal/schema"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fakeTx is a minimal pgx.Tx stub that records executed SQL.
type fakeTx struct{ execs []execCall }

type execCall struct {
	sql  string
	args []any
}

func (t *fakeTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	t.execs = append(t.execs, execCall{sql: sql, args: arguments})
	return pgconn.CommandTag{}, nil
}

func (t *fakeTx) Commit(ctx context.Context) error   { return nil }
func (t *fakeTx) Rollback(ctx context.Context) error { return nil }
func (t *fakeTx) Begin(ctx context.Context) (pgx.Tx, error) {
	return nil, fmt.Errorf("not implemented")
}
func (t *fakeTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}
func (t *fakeTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults { return nil }
func (t *fakeTx) LargeObjects() pgx.LargeObjects                               { return pgx.LargeObjects{} }
func (t *fakeTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return nil, fmt.Errorf("not implemented")
}
func (t *fakeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return nil, nil
}
func (t *fakeTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row { return nil }
func (t *fakeTx) Conn() *pgx.Conn                                               { return nil }

func testAccountsSchema() *schema.Schema {
	s := &schema.Schema{
		Namespace:       "public",
		Table:           "accounts",
		ReplicaIdentity: "f",
		PKOrdinal:       1,
		Columns: []schema.Column{
			{Ordinal: 1, Name: "id", TypeName: "int8", TypeOID: 20, PrimaryKey: true},
			{Ordinal: 2, Name: "owner", TypeName: "text", TypeOID: 25},
			{Ordinal: 3, Name: "balance_cents", TypeName: "int8", TypeOID: 20},
		},
	}
	s.Fingerprint = s.FingerprintFor()
	return s
}

func mustMutator(t *testing.T, table string) *Mutator {
	t.Helper()
	m, err := NewMutatorFor(table, testAccountsSchema())
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func accountRow(id int64, owner string, balance int64) *model.Row {
	return &model.Row{Values: []model.Value{
		model.Int64Value(id),
		model.TextValue(owner),
		model.Int64Value(balance),
	}}
}

func TestMutator_UpsertIsIdempotent(t *testing.T) {
	m := mustMutator(t, "accounts")
	tx := &fakeTx{}
	row := accountRow(1, "alice", 100)

	for i := 0; i < 3; i++ {
		if err := m.upsert(context.Background(), tx, row); err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
	}
	if len(tx.execs) != 3 {
		t.Fatalf("expected 3 upserts, got %d", len(tx.execs))
	}
	for _, c := range tx.execs {
		if c.sql == "" {
			t.Fatal("expected non-empty SQL")
		}
		if !strings.Contains(c.sql, "INSERT INTO accounts") {
			t.Fatalf("upsert targeted a wrong table: %q", c.sql)
		}
	}
}

func TestMutator_DeleteByID(t *testing.T) {
	m := mustMutator(t, "accounts")
	tx := &fakeTx{}
	if err := m.delete(context.Background(), tx, 42); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if len(tx.execs) != 1 {
		t.Fatalf("expected 1 delete, got %d", len(tx.execs))
	}
	if !strings.Contains(tx.execs[0].sql, "DELETE FROM accounts") {
		t.Fatalf("delete targeted a wrong table: %q", tx.execs[0].sql)
	}
}

func TestMutator_WriteCandidates(t *testing.T) {
	m := mustMutator(t, "accounts")
	tx := &fakeTx{}
	candidates := []model.Row{
		*accountRow(1, "a", 100),
		*accountRow(2, "b", 200),
	}
	if err := m.WriteCandidates(context.Background(), tx, candidates); err != nil {
		t.Fatalf("write candidates: %v", err)
	}
	if len(tx.execs) != 1 {
		t.Fatalf("expected one set-based upsert, got %d statements", len(tx.execs))
	}
	ids, ok := tx.execs[0].args[0].([]int64)
	if !ok || len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("wrong batched IDs: %#v", tx.execs[0].args[0])
	}
	// Non-null text cells travel as *string arrays bound through unnest, one
	// array per column, position-aligned with the schema.
	owners, ok := tx.execs[0].args[1].([]*string)
	if !ok || len(owners) != 2 || owners[0] == nil || *owners[0] != "a" || *owners[1] != "b" {
		t.Fatalf("wrong owner cell array: %#v", tx.execs[0].args[1])
	}
}

func TestMutator_WriteCandidatesBoundsBatches(t *testing.T) {
	m := mustMutator(t, "accounts")
	tx := &fakeTx{}
	candidates := make([]model.Row, candidateBatchRows+1)
	for i := range candidates {
		candidates[i] = *accountRow(int64(i+1), "a", 0)
	}
	if err := m.WriteCandidates(context.Background(), tx, candidates); err != nil {
		t.Fatal(err)
	}
	if len(tx.execs) != 2 {
		t.Fatalf("expected two bounded statements, got %d", len(tx.execs))
	}
	if len(tx.execs[0].args[0].([]int64)) != candidateBatchRows || len(tx.execs[1].args[0].([]int64)) != 1 {
		t.Fatal("wrong candidate batch boundaries")
	}
	if err := m.WriteCandidates(context.Background(), &fakeTx{}, []model.Row{*accountRow(1, strings.Repeat("x", candidateBatchBytes), 0)}); err == nil {
		t.Fatal("oversized candidate accepted")
	}
}

func TestMutatorForShadowUsesOnlyShadowTable(t *testing.T) {
	if _, err := NewMutatorFor("accounts; DROP TABLE accounts", testAccountsSchema()); err == nil {
		t.Fatal("unsafe destination identifier accepted")
	}
	if _, err := NewMutatorFor("accounts", nil); err == nil {
		t.Fatal("nil schema descriptor accepted")
	}
	m, err := NewMutatorFor("accounts_shadow", testAccountsSchema())
	if err != nil {
		t.Fatal(err)
	}
	tx := &fakeTx{}
	if err := m.Apply(context.Background(), tx, model.Change{Op: model.OpInsert, Row: accountRow(7, "shadow", 0)}); err != nil {
		t.Fatal(err)
	}
	if len(tx.execs) != 1 || !strings.Contains(tx.execs[0].sql, "INSERT INTO accounts_shadow") {
		t.Fatalf("wrong write target: %+v", tx.execs)
	}
}

func TestMutatorApplyRejectsMissingRowPayload(t *testing.T) {
	m := mustMutator(t, "accounts")
	// Inserts, updates, and deletes all require a row payload (deletes key the
	// primary key from the payload). A payload-less change is a decode bug and
	// must fail closed instead of writing an empty row or skipping the delete.
	for _, c := range []model.Change{
		{Op: model.OpInsert},
		{Op: model.OpUpdate},
		{Op: model.OpDelete},
	} {
		if err := m.Apply(context.Background(), &fakeTx{}, c); err == nil {
			t.Fatalf("change with no row payload for op %q accepted", c.Op)
		}
	}
}

func TestMutatorApplyRejectsRowSchemaMismatch(t *testing.T) {
	m := mustMutator(t, "accounts")
	short := &model.Row{Values: []model.Value{model.Int64Value(1), model.TextValue("a")}}
	if err := m.Apply(context.Background(), &fakeTx{}, model.Change{Op: model.OpInsert, Row: short}); err == nil {
		t.Fatal("row shorter than the schema accepted")
	}
}

func TestMutatorApplyBatchUsesFinalStatePerKey(t *testing.T) {
	m := mustMutator(t, "accounts")
	tx := &fakeTx{}
	changes := []model.Change{
		{Op: model.OpInsert, Row: accountRow(1, "old", 1)},
		{Op: model.OpUpdate, Row: accountRow(1, "new", 2)},
		{Op: model.OpInsert, Row: accountRow(2, "gone", 3)},
		{Op: model.OpDelete, Row: accountRow(2, "gone", 3)},
		{Op: model.OpDelete, Row: accountRow(3, "gone", 4)},
		{Op: model.OpInsert, Row: accountRow(3, "back", 5)},
	}
	if err := m.ApplyBatch(context.Background(), tx, changes); err != nil {
		t.Fatal(err)
	}
	if len(tx.execs) != 2 {
		t.Fatalf("expected one set delete and one set upsert, got %d statements", len(tx.execs))
	}
	deletes := tx.execs[0].args[0].([]int64)
	if len(deletes) != 1 || deletes[0] != 2 {
		t.Fatalf("final deletes=%v, want [2]", deletes)
	}
	if !strings.Contains(tx.execs[0].sql, "DELETE FROM accounts") {
		t.Fatalf("delete targeted a wrong table: %q", tx.execs[0].sql)
	}
	ids := tx.execs[1].args[0].([]int64)
	owners := tx.execs[1].args[1].([]*string)
	deref := make([]string, len(owners))
	for i, p := range owners {
		deref[i] = *p
	}
	if fmt.Sprint(ids) != "[1 3]" || fmt.Sprint(deref) != "[new back]" {
		t.Fatalf("final upserts ids=%v owners=%v", ids, deref)
	}
}

func TestMutator_ApplyDeleteUsesPrimaryKeyValue(t *testing.T) {
	m := mustMutator(t, "accounts")
	tx := &fakeTx{}
	if err := m.Apply(context.Background(), tx, model.Change{Op: model.OpDelete, Row: accountRow(42, "gone", 0)}); err != nil {
		t.Fatal(err)
	}
	if len(tx.execs) != 1 || tx.execs[0].args[0] != int64(42) {
		t.Fatalf("delete did not key the primary key value: %+v", tx.execs)
	}
}

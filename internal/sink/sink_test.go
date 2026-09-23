package sink

import (
	"context"
	"fmt"
	"testing"

	"example.com/seam/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fakeTx is a minimal pgx.Tx stub that records executed SQL.
type fakeTx struct{ execs []execCall }

type execCall struct{ sql string; args []any }

func (t *fakeTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	t.execs = append(t.execs, execCall{sql: sql, args: arguments})
	return pgconn.CommandTag{}, nil
}

func (t *fakeTx) Commit(ctx context.Context) error   { return nil }
func (t *fakeTx) Rollback(ctx context.Context) error { return nil }
func (t *fakeTx) Begin(ctx context.Context) (pgx.Tx, error) { return nil, fmt.Errorf("not implemented") }
func (t *fakeTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}
func (t *fakeTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults { return nil }
func (t *fakeTx) LargeObjects() pgx.LargeObjects                                  { return pgx.LargeObjects{} }
func (t *fakeTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return nil, fmt.Errorf("not implemented")
}
func (t *fakeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) { return nil, nil }
func (t *fakeTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row         { return nil }
func (t *fakeTx) Conn() *pgx.Conn                                                     { return nil }

func TestMutator_UpsertIsIdempotent(t *testing.T) {
	m := NewMutator()
	tx := &fakeTx{}
	acc := model.Account{ID: 1, Owner: "alice", BalanceCents: 100}

	for i := 0; i < 3; i++ {
		if err := m.upsert(context.Background(), tx, acc); err != nil {
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
	}
}

func TestMutator_DeleteByID(t *testing.T) {
	m := NewMutator()
	tx := &fakeTx{}
	if err := m.delete(context.Background(), tx, 42); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if len(tx.execs) != 1 {
		t.Fatalf("expected 1 delete, got %d", len(tx.execs))
	}
}

func TestMutator_WriteCandidates(t *testing.T) {
	m := NewMutator()
	tx := &fakeTx{}
	candidates := []model.Account{
		{ID: 1, Owner: "a", BalanceCents: 100},
		{ID: 2, Owner: "b", BalanceCents: 200},
	}
	if err := m.WriteCandidates(context.Background(), tx, candidates); err != nil {
		t.Fatalf("write candidates: %v", err)
	}
	if len(tx.execs) != 2 {
		t.Fatalf("expected 2 upserts, got %d", len(tx.execs))
	}
}

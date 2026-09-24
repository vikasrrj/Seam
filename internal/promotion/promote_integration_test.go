//go:build integration

package promotion

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"example.com/seam/integration/itest"
	"example.com/seam/internal/checkpoint"
	"example.com/seam/internal/model"
	"example.com/seam/internal/transport"
	"github.com/jackc/pgx/v5"
)

func TestAtomicPromotionAndIdempotentRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	admin, err := itest.DestConn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	dbName := fmt.Sprintf("seam_promotion_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+dbName); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(context.Background(), `DROP DATABASE `+dbName+` WITH (FORCE)`)
	baseURL, err := url.Parse(itest.DestDSN())
	if err != nil || baseURL.Scheme != "postgres" {
		t.Fatalf("integration destination DSN must be a postgres URI: %v", err)
	}
	baseURL.Path = "/" + dbName
	dsn := baseURL.String()
	store := checkpoint.NewStore(dsn)
	if err := store.EnsureTables(ctx); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"accounts", "accounts_shadow"} {
		if err := store.EnsureDestinationTableFor(ctx, table); err != nil {
			t.Fatal(err)
		}
	}
	conn, err := transport.ConnectPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, `DROP TABLE accounts_shadow`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `CREATE TABLE accounts_shadow (id BIGINT NOT NULL, owner TEXT PRIMARY KEY, balance_cents BIGINT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := checkpoint.FingerprintFor(ctx, conn, "accounts_shadow"); err == nil {
		t.Fatal("schema fingerprint accepted a primary key on owner instead of id")
	}
	if _, err := conn.Exec(ctx, `DROP TABLE accounts_shadow`); err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureDestinationTableFor(ctx, "accounts_shadow"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO accounts VALUES (1, 'old', 1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO accounts_shadow VALUES (1, 'new', 2)`); err != nil {
		t.Fatal(err)
	}
	config := Config{DestDSN: dsn, LiveJobID: "live", ShadowJobID: "shadow"}
	liveCP, err := store.CreateJob(ctx, model.JobConfig{JobID: "live", SourceDSN: "source", DestTable: "accounts"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	shadowCP, err := store.CreateJob(ctx, model.JobConfig{JobID: "shadow", SourceDSN: "source", DestTable: "accounts_shadow"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateJob(ctx, model.JobConfig{JobID: "second-live", SourceDSN: "source", DestTable: "accounts"}, 10); err == nil {
		t.Fatal("two active jobs claimed the same public destination table")
	}
	advance := func(cp *model.Checkpoint, offset int64) {
		t.Helper()
		tx, err := store.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		cp.CompletedThrough = 10
		cp.NextKafkaOffset = offset
		if err := store.UpdateCheckpoint(ctx, tx, cp, 0); err != nil {
			tx.Rollback(ctx)
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	advance(liveCP, 5)
	advance(shadowCP, 4)
	if _, err := commitPromotion(ctx, config, 5, nil, nil); err == nil {
		t.Fatal("promotion accepted lagging shadow frontier")
	}
	var owner string
	if err := conn.QueryRow(ctx, `SELECT owner FROM accounts WHERE id = 1`).Scan(&owner); err != nil || owner != "old" {
		t.Fatalf("failed promotion changed public data: %q, %v", owner, err)
	}
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	shadowCP.NextKafkaOffset = 5
	if err := store.UpdateCheckpoint(ctx, tx, shadowCP, 4); err != nil {
		tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := commitPromotion(ctx, config, 5, func(context.Context, pgx.Tx) error {
		return fmt.Errorf("deliberate validation mismatch")
	}, nil); err == nil {
		t.Fatal("promotion ignored a validation failure")
	}
	if err := conn.QueryRow(ctx, `SELECT owner FROM accounts WHERE id = 1`).Scan(&owner); err != nil || owner != "old" {
		t.Fatalf("validation failure changed public data: %q, %v", owner, err)
	}
	result, err := commitPromotion(ctx, config, 5, func(ctx context.Context, _ pgx.Tx) error {
		// The full comparison holds a read-compatible SHARE lock. A second
		// connection must still be able to query the public table.
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		var got string
		if err := conn.QueryRow(readCtx, `SELECT owner FROM accounts WHERE id = 1`).Scan(&got); err != nil {
			return err
		}
		if got != "old" {
			return fmt.Errorf("reader saw %q before atomic cutover", got)
		}
		return nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT owner FROM accounts WHERE id = 1`).Scan(&owner); err != nil || owner != "new" {
		t.Fatalf("public name did not switch atomically: %q, %v", owner, err)
	}
	if err := conn.QueryRow(ctx, `SELECT owner FROM `+result.RetiredTable+` WHERE id = 1`).Scan(&owner); err != nil || owner != "old" {
		t.Fatalf("old table not retained: %q, %v", owner, err)
	}
	if err := store.EnsureDestinationTableFor(ctx, "accounts_shadow"); err != nil {
		t.Fatalf("next shadow table cannot be created after cutover: %v", err)
	}
	if _, err := checkpoint.FingerprintFor(ctx, conn, "accounts_shadow"); err != nil {
		t.Fatalf("next shadow table has wrong schema: %v", err)
	}
	if _, err := store.CreateJob(ctx, model.JobConfig{JobID: "next-shadow", SourceDSN: "source", DestTable: "accounts_shadow"}, 10); err != nil {
		t.Fatalf("inactive shadow job still owns physical route: %v", err)
	}
	shadowAfter, err := store.LoadCheckpoint(ctx, "shadow")
	if err != nil || shadowAfter.Active {
		t.Fatalf("shadow writer not fenced: %+v, %v", shadowAfter, err)
	}
	retry, err := commitPromotion(ctx, config, 5, nil, nil)
	if err != nil || !retry.AlreadyPromoted || retry.RetiredTable != result.RetiredTable {
		t.Fatalf("ambiguous-commit retry was not idempotent: %+v, %v", retry, err)
	}
}

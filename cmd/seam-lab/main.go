// Command seam-lab is an independent verifier and failure lab for Seam.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"example.com/seam/internal/schema"
	"example.com/seam/internal/transport"
	"github.com/jackc/pgx/v5"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sourceDSN := envOrDefault("SOURCE_SQL_DSN", "postgres://postgres:postgres@localhost:5433/source?sslmode=disable")
	destDSN := envOrDefault("DEST_SQL_DSN", "postgres://postgres:postgres@localhost:5434/dest?sslmode=disable")

	switch os.Args[1] {
	case "verify":
		table := envOrDefault("SEAM_SOURCE_TABLE", "accounts")
		if err := verify(ctx, sourceDSN, destDSN, table); err != nil {
			fmt.Fprintf(os.Stderr, "verify failed: %v\n", err)
			os.Exit(1)
		}
	case "verify-fenced":
		cancel()
		verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer verifyCancel()
		jobID := envOrDefault("SEAM_JOB_ID", "seam-default")
		topic := envOrDefault("KAFKA_TOPIC", "seam.accounts")
		brokers := strings.Split(envOrDefault("KAFKA_BROKERS", "localhost:9092"), ",")
		if err := verifyFenced(verifyCtx, sourceDSN, destDSN, brokers, topic, jobID); err != nil {
			fmt.Fprintf(os.Stderr, "verify-fenced failed: %v\n", err)
			os.Exit(1)
		}
	case "stale-update":
		table := envOrDefault("SEAM_SOURCE_TABLE", "accounts")
		if err := staleUpdate(ctx, sourceDSN, destDSN, table); err != nil {
			fmt.Fprintf(os.Stderr, "stale-update failed: %v\n", err)
			os.Exit(1)
		}
	case "checkpoint":
		jobID := envOrDefault("SEAM_JOB_ID", "seam-default")
		if err := showCheckpoint(ctx, destDSN, jobID); err != nil {
			fmt.Fprintf(os.Stderr, "checkpoint failed: %v\n", err)
			os.Exit(1)
		}
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Println(`seam-lab: independent verifier and failure lab

Commands:
  verify         Compare row counts and exact contents between source and
                 destination for SEAM_SOURCE_TABLE.
  verify-fenced  Block source writes, wait for a Kafka barrier and the
                 destination checkpoint, then compare exact ordered rows.
  stale-update   Demonstrate a stale overwrite by copying source rows directly to
                 destination after modifying the source.
  checkpoint     Print durable checkpoint state for SEAM_JOB_ID.

Environment:
  SOURCE_SQL_DSN    Source PostgreSQL DSN (default postgres://postgres:postgres@localhost:5433/source?sslmode=disable)
  DEST_SQL_DSN      Destination PostgreSQL DSN (default postgres://postgres:postgres@localhost:5434/dest?sslmode=disable)
  SEAM_SOURCE_TABLE Source table verified or demonstrated (default accounts)
  SEAM_JOB_ID       Job id used by the checkpoint command (default seam-default)`)
}

func staleUpdate(ctx context.Context, sourceDSN, destDSN, table string) error {
	descriptor, err := schema.Load(ctx, sourceDSN, "public", table)
	if err != nil {
		return fmt.Errorf("load source schema for %q: %w", table, err)
	}
	src, err := transport.ConnectPostgres(ctx, sourceDSN)
	if err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	defer src.Close(context.Background())

	dst, err := transport.ConnectPostgres(ctx, destDSN)
	if err != nil {
		return fmt.Errorf("connect dest: %w", err)
	}
	defer dst.Close(context.Background())
	for _, conn := range []*pgx.Conn{src, dst} {
		if _, err := conn.Exec(ctx, `SET TIME ZONE 'UTC'`); err != nil {
			return fmt.Errorf("set UTC session: %w", err)
		}
	}

	// Read the first row as canonical cells.
	pkOrd := descriptor.PKOrdinal - 1
	pkName := descriptor.PKColumn().Name
	srcRows, err := src.Query(ctx, fmt.Sprintf("SELECT %s FROM %s ORDER BY %s LIMIT 1",
		descriptor.CanonicalScanList(), descriptor.SQLTable(), pkName))
	if err != nil {
		return fmt.Errorf("select source row: %w", err)
	}
	cells := make([]*string, len(descriptor.Columns))
	dests := make([]any, len(cells))
	for i := range dests {
		dests[i] = &cells[i]
	}
	if !srcRows.Next() {
		srcRows.Close()
		return fmt.Errorf("source table %q is empty; nothing to demonstrate", table)
	}
	if err := srcRows.Scan(dests...); err != nil {
		srcRows.Close()
		return fmt.Errorf("scan source row: %w", err)
	}
	srcRows.Close()

	// Mutate the first non-null, non-primary-key integer column so the demo
	// writes a recognizably stale value. Integer columns have a canonical text
	// form that can be derived from the stored one.
	changed := -1
	var stale string
	for i, col := range descriptor.Columns {
		if col.PrimaryKey || cells[i] == nil {
			continue
		}
		if _, ok := schema.SupportedTypes[col.TypeOID]; !ok {
			continue
		}
		switch col.TypeOID {
		case 20, 21, 23, 700, 701, 114, 1700, 2950:
			// Numeric-ish: keep canonical text and derive a +1 value where
			// possible, else leave it untouched by trying int64 arithmetic.
			old, perr := strconv.ParseInt(*cells[i], 10, 64)
			if perr != nil {
				continue
			}
			changed = i
			stale = strconv.FormatInt(old+1, 10)
		}
		if changed >= 0 {
			break
		}
	}
	if changed < 0 {
		return fmt.Errorf("table %q has no non-null integer column to falsify; move the demo to an integer column", table)
	}
	col := descriptor.Columns[changed]

	// Advance the source row to the stale value.
	if _, err := src.Exec(ctx, fmt.Sprintf("UPDATE %s SET %s = ($1::text)::%s WHERE %s = ($2::text)::%s",
		descriptor.SQLTable(), col.Name, schema.SupportedTypes[col.TypeOID], pkName, schema.SupportedTypes[descriptor.Columns[pkOrd].TypeOID]),
		stale, cellString(cells[pkOrd])); err != nil {
		return fmt.Errorf("update source: %w", err)
	}

	// Copy the now-stale source row directly to destination (simulates a naive
	// snapshot write that races with CDC).
	values := make([]any, len(descriptor.Columns))
	for i := range descriptor.Columns {
		if i == changed {
			values[i] = stale
		} else if cells[i] == nil {
			values[i] = nil
		} else {
			values[i] = cells[i] // *string; pgx binds the text value
		}
	}
	var sets []string
	for _, c := range descriptor.Columns {
		if !c.PrimaryKey {
			sets = append(sets, c.Name+" = EXCLUDED."+c.Name)
		}
	}
	insertSQL := fmt.Sprintf("INSERT INTO public.%s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
		table, descriptor.CanonicalColumnList(), descriptor.CanonicalApplyList(descriptor.Columns, 1),
		pkName, strings.Join(sets, ", "))
	if _, err := dst.Exec(ctx, insertSQL, values...); err != nil {
		return fmt.Errorf("stale write dest: %w", err)
	}

	fmt.Printf("stale-update: overwrote destination %s id=%s with %s=%s\n", table, cellString(cells[pkOrd]), col.Name, stale)
	fmt.Println("Run 'seam-lab verify' after the pipeline catches up to confirm CDC wins.")
	return nil
}

func showCheckpoint(ctx context.Context, destDSN, jobID string) error {
	dst, err := transport.ConnectPostgres(ctx, destDSN)
	if err != nil {
		return fmt.Errorf("connect dest: %w", err)
	}
	defer dst.Close(context.Background())

	var generation, attempt string
	var upperBound, completedThrough, nextOffset int64
	var lastLSN *string
	var active bool
	err = dst.QueryRow(ctx, `
		SELECT generation, attempt, scan_upper_bound, completed_through_id, next_kafka_offset, last_applied_lsn, active
		FROM seam_checkpoints WHERE job_id = $1`, jobID).Scan(
		&generation, &attempt, &upperBound, &completedThrough, &nextOffset, &lastLSN, &active)
	if err == pgx.ErrNoRows {
		fmt.Printf("no checkpoint found for job %q\n", jobID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("query checkpoint: %w", err)
	}

	fmt.Printf("job:             %s\n", jobID)
	fmt.Printf("generation:      %s\n", generation)
	fmt.Printf("attempt:         %s\n", attempt)
	fmt.Printf("scan_upper_bound: %d\n", upperBound)
	fmt.Printf("completed_through: %d\n", completedThrough)
	fmt.Printf("next_offset:     %d\n", nextOffset)
	if lastLSN != nil {
		fmt.Printf("last_applied_lsn: %s\n", *lastLSN)
	} else {
		fmt.Println("last_applied_lsn: <none>")
	}
	fmt.Printf("active:          %v\n", active)
	return nil
}

func envOrDefault(name, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return fallback
}

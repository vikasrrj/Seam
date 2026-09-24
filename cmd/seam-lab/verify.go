package main

import (
	"context"
	"fmt"
	"strings"

	"example.com/seam/internal/schema"
	"example.com/seam/internal/transport"
	"github.com/jackc/pgx/v5"
)

// verify compares one source table and the destination table exactly: row
// counts, then every cell as canonical PostgreSQL text (NULL equals NULL) in
// primary-key order. The column layout and casts come from the job's schema
// descriptor, so no column set or type is assumed. Both sessions render under
// UTC so canonical text is identical for equal values.
func verify(ctx context.Context, sourceDSN, destDSN, table string) error {
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

	var srcCount, dstCount int64
	if err := src.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", descriptor.SQLTable())).Scan(&srcCount); err != nil {
		return fmt.Errorf("count source rows: %w", err)
	}
	if err := dst.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM public.%s", table)).Scan(&dstCount); err != nil {
		return fmt.Errorf("count dest rows: %w", err)
	}
	fmt.Printf("source rows: %d\ndest rows:   %d\n", srcCount, dstCount)
	if srcCount != dstCount {
		return fmt.Errorf("row count mismatch: source=%d dest=%d", srcCount, dstCount)
	}

	checked, err := compareRows(ctx, src, dst, descriptor, table)
	if err != nil {
		return err
	}
	fmt.Printf("verify: ok (%d rows checked)\n", checked)
	return nil
}

// compareRows streams both sides in primary-key order and compares every cell
// as canonical PostgreSQL text against the schema descriptor. source and dest
// are any schema.Querier (a pgx.Conn or a pgx.Tx for fenced snapshots).
func compareRows(ctx context.Context, source, dest schema.Querier, descriptor *schema.Schema, destTable string) (int64, error) {
	if err := schema.ValidateIdentifier(destTable); err != nil {
		return 0, fmt.Errorf("unsafe destination table %q: %w", destTable, err)
	}
	sourceSQL := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s",
		descriptor.CanonicalScanList(), descriptor.SQLTable(), descriptor.PKColumn().Name)
	destSQL := fmt.Sprintf("SELECT %s FROM public.%s ORDER BY %s",
		descriptor.CanonicalScanList(), destTable, descriptor.PKColumn().Name)

	srcRows, err := source.Query(ctx, sourceSQL)
	if err != nil {
		return 0, fmt.Errorf("query source rows: %w", err)
	}
	defer srcRows.Close()
	dstRows, err := dest.Query(ctx, destSQL)
	if err != nil {
		return 0, fmt.Errorf("query dest rows: %w", err)
	}
	defer dstRows.Close()

	cols := len(descriptor.Columns)
	scanCells := func(rows pgx.Rows) ([]*string, error) {
		cells := make([]*string, cols)
		dests := make([]any, cols)
		for i := range dests {
			dests[i] = &cells[i]
		}
		if err := rows.Scan(dests...); err != nil {
			return nil, err
		}
		return cells, nil
	}
	formatRow := func(cells []*string) string {
		parts := make([]string, len(cells))
		for i, cell := range cells {
			if cell == nil {
				parts[i] = "NULL"
			} else {
				parts[i] = *cell
			}
		}
		return strings.Join(parts, ", ")
	}

	var count int64
	for {
		hasSource := srcRows.Next()
		hasDest := dstRows.Next()
		if !hasSource || !hasDest {
			if err := srcRows.Err(); err != nil {
				return count, err
			}
			if err := dstRows.Err(); err != nil {
				return count, err
			}
			if hasSource != hasDest {
				return count, fmt.Errorf("row count differs after %d equal rows", count)
			}
			return count, nil
		}
		a, err := scanCells(srcRows)
		if err != nil {
			return count, fmt.Errorf("scan source row %d: %w", count, err)
		}
		b, err := scanCells(dstRows)
		if err != nil {
			return count, fmt.Errorf("scan dest row %d: %w", count, err)
		}
		for i := range a {
			if !cellsEqual(a[i], b[i]) {
				return count, fmt.Errorf("first mismatch at row %d (pk %s):\n  source: %s\n  dest:   %s",
					count, cellString(a[descriptor.PKOrdinal-1]), formatRow(a), formatRow(b))
			}
		}
		count++
	}
}

func cellsEqual(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func cellString(cell *string) string {
	if cell == nil {
		return "NULL"
	}
	return *cell
}

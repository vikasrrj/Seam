#!/usr/bin/env bash
set -euo pipefail

# Run each performance-matrix cell in a fresh Go process. Defaults are sized
# for a developer workstation; override the space-separated lists to exercise
# larger data without changing benchmark code.
#
# Example:
#   ROWS_LIST="10000 1000000" WORKERS_LIST="1 2 4 8" \
#   CHUNK_LIST="1000 10000" OWNER_BYTES_LIST="16 1024" \
#   ./scripts/bench-matrix.sh

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ROWS_LIST="${ROWS_LIST:-10000 100000}"
WORKERS_LIST="${WORKERS_LIST:-1 2 4 8}"
CHUNK_LIST="${CHUNK_LIST:-1000}"
OWNER_BYTES_LIST="${OWNER_BYTES_LIST:-16 1024}"
OUTPUT="${OUTPUT:-benchmark-matrix.tsv}"

printf 'rows\tworkers\tchunk_size\towner_bytes\tns_per_op\tmib_per_s\tdiscovery_ms\treconciliation_ms\trows_per_s\tbenchmark\n' >"$OUTPUT"
for rows in $ROWS_LIST; do
  for owner_bytes in $OWNER_BYTES_LIST; do
    for chunk_size in $CHUNK_LIST; do
      # Alternate worker direction by row width to reduce systematic warm-cache
      # bias while retaining one isolated process per cell.
      if [ "$owner_bytes" -eq 16 ]; then
        worker_order="$WORKERS_LIST"
      else
        worker_order="$(printf '%s\n' $WORKERS_LIST | awk '{ a[NR]=$1 } END { for (i=NR;i>=1;i--) printf "%s%s", a[i], (i==1?"":" ") }')"
      fi
      for workers in $worker_order; do
        printf 'running rows=%s workers=%s chunk=%s owner_bytes=%s\n' "$rows" "$workers" "$chunk_size" "$owner_bytes" >&2
        output="$(
          SEAM_BENCH_ROWS="$rows" \
          SEAM_BENCH_WORKERS="$workers" \
          SEAM_BENCH_CHUNK_SIZE="$chunk_size" \
          SEAM_BENCH_OWNER_BYTES="$owner_bytes" \
          go test -tags=integration -count=1 -run '^$' -bench '^BenchmarkBackfillConfigured$' -benchtime=1x ./integration/ 2>&1
        )"
        line="$(printf '%s\n' "$output" | grep -E '^BenchmarkBackfillConfigured-[0-9]+[[:space:]]')"
        if [ -z "$line" ]; then
          printf 'benchmark cell failed or emitted no result:\n%s\n' "$output" >&2
          exit 1
        fi
        parsed="$(printf '%s\n' "$line" | awk '
          $4 == "ns/op" && $6 == "MiB/s" && $8 == "discover-ms/op" &&
          $10 == "reconcile-ms/op" && $12 == "rows/s" {
            printf "%s\t%s\t%s\t%s\t%s\t%s", $3, $5, $7, $9, $11, $1
            found = 1
          }
          END { if (!found) exit 1 }
        ')" || {
          printf 'unexpected benchmark output format:\n%s\n' "$line" >&2
          exit 1
        }
        printf '%s\t%s\t%s\t%s\t%s\n' "$rows" "$workers" "$chunk_size" "$owner_bytes" "$parsed" | tee -a "$OUTPUT"
      done
    done
  done
done

printf 'wrote %s\n' "$OUTPUT" >&2

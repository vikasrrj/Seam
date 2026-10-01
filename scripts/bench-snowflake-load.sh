#!/usr/bin/env bash
set -euo pipefail

: "${SNOWFLAKE_DSN:?SNOWFLAKE_DSN is required}"
: "${SNOWFLAKE_DATABASE:?SNOWFLAKE_DATABASE is required}"

result_dir=${RESULT_DIR:-benchmark-results/snowflake}
rows=${SEAM_SNOWFLAKE_BENCH_ROWS:-100,1000,5000}
payload_bytes=${SEAM_SNOWFLAKE_BENCH_PAYLOAD_BYTES:-96}
upload_parallel=${SEAM_SNOWFLAKE_UPLOAD_PARALLEL:-4}
loaders=${LOADERS:-sql bulk}
timestamp=$(date -u +%Y%m%dT%H%M%SZ)

mkdir -p "$result_dir"
metadata="$result_dir/$timestamp-metadata.txt"
{
	printf 'timestamp_utc=%s\n' "$timestamp"
	printf 'git_commit=%s\n' "$(git rev-parse HEAD)"
	if git diff --quiet && git diff --cached --quiet; then
		printf 'git_dirty=false\n'
	else
		printf 'git_dirty=true\n'
	fi
	printf 'go_version=%s\n' "$(go version)"
	printf 'host=%s\n' "$(hostname)"
	printf 'kernel=%s\n' "$(uname -srmo)"
	printf 'rows=%s\n' "$rows"
	printf 'payload_bytes=%s\n' "$payload_bytes"
	printf 'upload_parallel=%s\n' "$upload_parallel"
	printf 'loaders=%s\n' "$loaders"
} >"$metadata"

for loader in $loaders; do
	case "$loader" in
		sql|bulk) ;;
		*) printf 'invalid loader: %s\n' "$loader" >&2; exit 2 ;;
	esac
	output="$result_dir/$timestamp-$loader.jsonl"
	SEAM_SNOWFLAKE_BENCH_LOADER="$loader" \
	SEAM_SNOWFLAKE_BENCH_ROWS="$rows" \
	SEAM_SNOWFLAKE_BENCH_PAYLOAD_BYTES="$payload_bytes" \
	SEAM_SNOWFLAKE_UPLOAD_PARALLEL="$upload_parallel" \
	go test -json -tags=snowflake_integration -run '^$' \
		-bench '^BenchmarkLiveSnowflakeStageSnapshot$' -benchtime=1x -count=1 \
		./internal/snowflake | tee "$output"
done

printf 'benchmark metadata: %s\n' "$metadata"

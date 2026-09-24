#!/usr/bin/env bash
set -euo pipefail

# Counterbalanced backfill benchmark runner for SEAM.
#
# Each `go test -bench` invocation below is a fresh benchmark process for one
# worker count, so process-local state is reset. PostgreSQL, Kafka, and OS
# caches remain shared. The worker counts alternate order every pass (1 then 4
# / 4 then 1) to reduce ordering bias. Every raw sample is printed and
# retained; none are culled.
#
# Prerequisites: the dedicated integration stack (source 5435, dest 5436,
# Kafka 9094) must be running and the module must build.
#
# Usage: scripts/bench-counterbalanced.sh [PASSES]   (default: 4)

PASSES="${1:-4}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

declare -a ns1 ns4 speed

median_min_max() {
  # reads numbers on stdin, prints "median min max"
  sort -n | awk '
    { a[NR] = $1 }
    END {
      if (NR == 0) { print "n/a n/a n/a"; exit }
      med = (NR % 2 == 1) ? a[(NR+1)/2] : (a[NR/2] + a[NR/2+1]) / 2
      printf "%.3f %.3f %.3f\n", med, a[1], a[NR]
    }'
}

mm() { printf '%s\n' "$@" | median_min_max; }

run_one() {
  local workers="$1" fn line out
  if [ "$workers" -eq 1 ]; then
    fn="BenchmarkBackfillWorkers1"
  else
    fn="BenchmarkBackfillWorkers4"
  fi
  out="$(go test -tags=integration -count=1 -benchtime=1x -run '^$' -bench "^${fn}$" ./integration/ 2>&1)" || {
    printf 'error: go test failed for %s\n%s\n' "$fn" "$out" >&2
    return 1
  }
  line="$(printf '%s\n' "$out" | grep -E "^${fn}-[0-9]+[[:space:]]" || true)"
  if [ -z "$line" ]; then
    printf 'error: no benchmark result line for %s\n%s\n' "$fn" "$out" >&2
    return 1
  fi
  printf '%s\n' "$line"
}

for ((pass = 1; pass <= PASSES; pass++)); do
  if ((pass % 2 == 1)); then
    order=(1 4)
  else
    order=(4 1)
  fi
  printf '=== pass %d (order: %s then %s) ===\n' "$pass" "${order[0]}" "${order[1]}"
  pass_one_ns=""
  pass_four_ns=""
  for w in "${order[@]}"; do
    line="$(run_one "$w")"
    printf '  RAW: %s\n' "$line"
    ns="$(printf '%s\n' "$line" | awk '{print $3}')"
    if [ "$w" -eq 1 ]; then
      ns1+=("$ns")
      pass_one_ns="$ns"
    else
      ns4+=("$ns")
      pass_four_ns="$ns"
    fi
  done
  # Paired speedup for this pass, computed once both samples exist regardless
  # of the order they were collected in.
  speed+=("$(awk -v one="$pass_one_ns" -v four="$pass_four_ns" 'BEGIN { printf "%.3f", one/four }')")
done

printf '\n=== summary (raw samples printed above; one sample per worker per pass) ===\n'
read -r med lo hi <<<"$(mm "${ns1[@]}")"
printf '1 worker : samples=%d median=%s ns/op range=%s..%s\n' "${#ns1[@]}" "$med" "$lo" "$hi"
read -r med lo hi <<<"$(mm "${ns4[@]}")"
printf '4 workers: samples=%d median=%s ns/op range=%s..%s\n' "${#ns4[@]}" "$med" "$lo" "$hi"
read -r med lo hi <<<"$(mm "${speed[@]}")"
printf 'speedup  : samples=%d median=%sx range=%s..%sx (1-worker ns / 4-worker ns, paired per pass)\n' \
  "${#speed[@]}" "$med" "$lo" "$hi"

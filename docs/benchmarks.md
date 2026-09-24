# Backfill benchmark

## Current implementation smoke samples

After adding durable candidate staging, disk-backed streamed transaction
apply, capture fencing, process-shared resource admission, and exact-prefix
promotion, two fresh-process smoke samples were executed with the same
10,000-row, 1,000-row-chunk workload. The benchmark now wires the production
resource controller and PostgreSQL advisory permit pools. Both samples passed
the benchmark's exact row-for-row verification:

| Workers | ns/op | rows/s | Discovery | Reconciliation |
| --- | ---: | ---: | ---: | ---: |
| 1 | 3,086,367,536 | 3,240 | 229 ms | 2,743 ms |
| 4 | 1,436,515,096 | 6,961 | 222 ms | 1,117 ms |

The latest single pair is a **2.15× wall-clock speedup**, not a distribution.
Earlier post-staging smoke pairs bypassed the production resource controller,
so their ratios are not comparable to this corrected pair. Durable staging
adds destination writes and made both absolute results slower than the earlier
in-memory-window samples below, while four workers hid more of that latency.
Run the corrected counterbalanced script again before drawing a performance
conclusion.

A separate fresh-process 100,000-row pair used the same 16-byte payload and
1,000-row chunks, with production resource admission and exact verification:

| Rows | Workers | Wall time | rows/s | Discovery | Reconciliation | Speedup |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100,000 | 1 | 29.59 s | 3,379 | 2.07 s | 27.42 s | 1.00× |
| 100,000 | 4 | 14.89 s | 6,717 | 2.23 s | 12.54 s | 1.99× |

This is also one pair, not a distribution. Similar throughput at 10,000 and
100,000 rows suggests startup cost is not the dominant limit on this host;
the result does not identify whether source I/O, candidate staging,
destination apply, or shared-host contention is the first saturated resource.
The structured raw output is retained in `docs/benchmark-matrix-100k.tsv`.

`BenchmarkBackfillConfigured` and `scripts/bench-matrix.sh` now run each matrix
cell in a fresh process and vary row count, row width, worker count, and chunk
size. The default matrix is intentionally a local-workstation suite; larger
runs can be selected with `ROWS_LIST`, `OWNER_BYTES_LIST`, `WORKERS_LIST`, and
`CHUNK_LIST`. Output is retained as TSV and every cell still performs exact
content verification outside the timer.

## Earlier in-memory-window counterbalanced run

This documents an executed, corrected run of the counterbalanced backfill
benchmark against the dedicated integration stack
(`integration/docker-compose.yml`, host ports 5435/5436/9094). It reports only
what was run; it does not extrapolate to larger workloads.

### What it measures

`BenchmarkBackfillWorkers1` and `BenchmarkBackfillWorkers4` (in
`integration/bench_test.go`) seed a fresh 10,000-row `accounts` table on the
source, start one capture stream, and run the production durable-chunk
reconciler against a freshly truncated destination. The measured window is one
complete backfill cycle, inside the timer:

1. Kafka end offset read
2. source LOW-marker write and barrier wait (capture → Kafka → consumer)
3. source upper-bound read
4. durable job creation (`CreateJobAt`) and chunk discovery
5. reconciler run, including process-shared scan/destination permit admission,
   until the durable checkpoint passes the scan upper bound

Everything else is *outside* the timer: table reset, seeding, capture startup,
per-iteration destination truncation, and the final exact-content
verification.

Timer correctness: Go's benchmark runner calls `StartTimer` before invoking the
benchmark function (`testing.(*B).runN`), so the benchmark stops the timer
before any setup. On every sample below, `ns/op` equals `10,000 / rows/s`
within rounding, i.e. the two reported metrics derive from the same work-only
window and neither includes setup.

Each worker count runs in a fresh `go test` process, so process-local state is
reset. PostgreSQL, Kafka, filesystem, and OS caches remain shared across runs.
The order alternates every pass (1-then-4 / 4-then-1) to reduce ordering bias.
Every raw sample is printed and retained; none are culled.

After each run the full source and destination tables are merge-compared
row-for-row in primary-key order outside the timer (`verifyExactContents`):
missing rows, extra rows, duplicates, reordered rows, and changed column values
all fail the benchmark. Every time reported below is therefore for a run whose
destination contents matched the source exactly.

### Environment

- Host: Omarchy, Linux kernel `7.2.5-3-omarchy`
- CPU: Intel Core Ultra 5 225H, 14 logical CPUs (`GOMAXPROCS=14`)
- Memory: 15.8 GiB
- Go: `go1.27.1 linux/amd64`
- Stack: `postgres:16` (source and destination, ports 5435/5436),
  `confluentinc/cp-kafka:7.6.0` (port 9094), `confluentinc/cp-zookeeper:7.6.0`
- All containers run on the same host as the benchmark; capture, workers, and
  both PostgreSQL servers share the machine.

### Command

Everything below was produced by a single invocation (4 passes, 8 fresh
processes), serialized so no two benchmark runs ever overlapped on the stack:

```bash
./scripts/bench-counterbalanced.sh 4
```

Each fresh process runs one configuration as:

```bash
go test -tags=integration -count=1 -benchtime=1x -run '^$' -bench '^BenchmarkBackfillWorkers1$' ./integration/
```

(and likewise for `BenchmarkBackfillWorkers4`).

### Raw results

Every sample as printed by `go test` (verbatim; 1 iteration each, exact-content
verification passed on all 8):

| Pass | Order            | Worker | ns/op         | rows/s |
|------|------------------|--------|---------------|--------|
| 1    | 1 then 4         | 1      | 1818938953 ns/op | 5498 rows/s |
| 1    | 1 then 4         | 4      | 1003707937 ns/op | 9963 rows/s |
| 2    | 4 then 1         | 4      | 1040873139 ns/op | 9608 rows/s |
| 2    | 4 then 1         | 1      | 1799198211 ns/op | 5558 rows/s |
| 3    | 1 then 4         | 1      | 1723407698 ns/op | 5803 rows/s |
| 3    | 1 then 4         | 4      | 1001090583 ns/op | 9990 rows/s |
| 4    | 4 then 1         | 4      | 1012109699 ns/op | 9882 rows/s |
| 4    | 4 then 1         | 1      | 1883056850 ns/op | 5311 rows/s |

Median and range over the four samples per worker count:

- 1 worker: median 1809068582 ns/op (~1.81 s), range 1723407698..1883056850
  ns/op (1.72–1.88 s); median ~5.53k rows/s
- 4 workers: median 1007908818 ns/op (~1.01 s), range 1001090583..1040873139
  ns/op (1.00–1.04 s); median ~9.92k rows/s

Self-consistency: `ns/op/1e9` × `rows/s` ≈ 10,000 on every sample (e.g. pass 1
one worker: 1.8189 s × 5498 = 9999), confirming setup is outside both metrics.

### Speedup

Four workers complete the 10,000-row backfill faster than one worker in all
four paired passes; the ratio is computed per pass from the same host state
(4-worker ns/op ÷ 1-worker ns/op):

| Pass | 4-worker ns/op | 1-worker ns/op | Time ratio |
|------|----------------|----------------|------------|
| 1    | 1.0037 s       | 1.8189 s       | 0.552×     |
| 2    | 1.0409 s       | 1.7992 s       | 0.579×     |
| 3    | 1.0011 s       | 1.7234 s       | 0.581×     |
| 4    | 1.0121 s       | 1.8831 s       | 0.537×     |

Median time ratio 0.566×, range 0.537×–0.581× → **≈1.77× wall-clock speedup
with four workers (range ≈1.72×–1.86×)**. Throughput agrees: median ~5.53k vs
~9.92k rows/s (≈1.79×).

This supersedes the five ad-hoc runs of the previous single-process
`BenchmarkBackfill` reported earlier in this workspace; the corrected
counterbalanced run (fresh processes, alternating order, exact-content
verification, one sample per worker per pass) is the current evidence.

### Limitations

- **Fixed workload only.** A single 10,000-row workload per sample with
  `-benchtime=1x`, not a distribution of timings or varied row counts. It is a
  starting point, not evidence of larger-scale speedups.
- **No extrapolation to 10M/100M rows.** At 10,000 rows the constant overheads
  (capture start, barrier latency, job creation, worker coordination) are a
  large fraction of total time; the same multiplier must not be assumed for
  large scans.
- **Shared host.** Source, destination, Kafka, and the workers contend for the
  same CPUs; absolute numbers are lower and wall-clock speedup dampened
  compared with dedicated infrastructure. It cannot be treated as
  network-separated performance.
- **One capture stream for both worker counts.** The 4-worker case consumes
  the same single-partition Kafka topic; capture is not sharded, so 4 workers
  cannot be expected to scale capture-bound phases.

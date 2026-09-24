# SEAM

SEAM is a narrow PostgreSQL `public.accounts` (`id BIGINT PRIMARY KEY`) to
PostgreSQL replication experiment. A separate capture process publishes
complete `pgoutput` transactions to one Kafka partition. The reconciler
backfills disjoint key ranges while applying live changes from that partition.
This is infrastructure work in progress, not a general CDC connector.

For a code-grounded explanation of the architecture, invariants, failure
handling, promotion protocol, tests, benchmarks, limitations, and interview
questions, read the [SEAM engineering deep dive](docs/SEAM-engineering-report.md).
For the underlying replication, buffering, batching, scaling, and destination
concepts behind Artie's public architecture, use the separate
[Artie systems study guide](docs/artie-systems-study-guide.md).

## Correctness contract

Run capture before creating a job. `seam --start-fresh` checks the source slot,
publication, source system identifier, and one-partition Kafka topic ID. It
commits a unique source marker and waits until capture publishes it, then
starts the job at the next Kafka offset. A small source transaction is one
versioned Kafka envelope; a large transaction is split into ordered, bounded
version-2 fragments. Capture spills an open transaction above its memory
threshold to a private temp file and acknowledges PostgreSQL only after Kafka
acknowledges the final fragment. The consumer stages incomplete fragments on
disk and does not apply or checkpoint any rows until `Final` proves the whole
transaction. It then replays one bounded fragment at a time through one atomic
destination transaction, so final apply does not materialize the whole source
transaction. After a crash it rebuilds from Kafka because the durable
destination cursor still points before fragment zero. Legacy per-row records,
missing fragments, and inconsistent fragment metadata stop consumption.

The destination records a discovery cursor and a sealed manifest of contiguous
logical `BIGINT` ranges, including sparse gaps. Discovery advances in the same
transaction that inserts a chunk. Each worker surrounds its scan with LOW and
HIGH source markers. A change to a key inside that window evicts its snapshot
candidate. Snapshot candidates and early touched-key tombstones are staged in
`seam_candidates`; a worker releases its row slice after staging instead of
holding every open window in process memory. The coordinator waits at HIGH
for the worker's candidates and commits them before applying later CDC.
Snapshot survivors, the source
transaction, chunk completion, and checkpoint commit in one destination
transaction. Checkpoint compare-and-swap, attempt IDs, and increasing lease
tokens fence stale workers. A per-job owner lease and monotonically increasing
epoch fence a whole stale reconciler process at every destination transaction,
chunk lease, heartbeat, discovery write, and recovery transition. Producer
retries are deduplicated by source commit LSN, including duplicate markers.

The fixed codec rejects `TRUNCATE`, primary-key changes, unknown `pgoutput`
messages, and unexpected relation names, column
order, or PostgreSQL type OIDs. These are deliberate stop conditions; they are
not silently ignored. `SEAM_SOURCE_TABLE` (default `accounts`) and its
`REPLICA IDENTITY FULL` key must match a stable, supported type set:
`accounts` remains the reference table, while `cmd/seam` also accepts a
descriptor provided via `SEAM_SOURCE_DESCRIPTOR`/`SEAM_SOURCE_TABLE`; any
unsupported type or a schema change under a running job fails the pipeline
closed with a rejected-epoch error.

For online resync, a second job can write `accounts_shadow` while the live job
continues writing `accounts`. `seam-promote` drains both durable frontiers,
then installs durable cutover gates that stop both jobs at an identical source
transaction boundary. A short first source fence captures an exact prefix and
opens an MVCC snapshot. Source writes resume while SEAM compares the full
snapshot to the gated shadow. A final short fence advances both jobs to an
exact barrier and compares only keys changed since the validated snapshot,
then swaps PostgreSQL table names in one destination transaction. The old table
is retained as `accounts_retired_<timestamp>`. SEAM destination writes take a
routing lock so a transaction that started before promotion cannot write the
retired table afterward. This is deliberately limited to the fixed PostgreSQL
schema: it rejects views, foreign keys, user triggers, different grants or
owners, row security, secondary indexes, and other constraints. The first and
final source write pauses each have an independent `--max-write-pause`
deadline; the final pause includes barrier catch-up and bounded changed-key validation, and
`--max-delta-keys` bounds suffix state. The full scan can still be long, but it
runs against an MVCC snapshot after source writes resume. Its lock permits destination reads, though
lock contention may delay them; the final rename briefly blocks them. The
cutover path has passed a live integration run in this workspace
(`TestOnlineShadowResync` and `TestAtomicPromotionAndIdempotentRetry`);
there is still no defensible 10M/100M-row performance claim.

## Processes and state

`cmd/seam-capture` owns the logical slot and broker handoff. Capture holds a
source advisory lock and durable monotonic epoch in `seam_capture_owners`;
concurrent owners are rejected and takeover pins source, generation,
publication, and Kafka topic identity. `cmd/seam` owns
the destination cursor and chunk workers. Destination tables `seam_jobs`,
`seam_checkpoints`, `seam_applied_txs`, `seam_chunks`, and `seam_candidates`
hold durable progress and reconciliation state;
`seam_route_fence`, `seam_cutover_gates`, and `seam_promotions` coordinate and record cutovers.
New job rows store a hash of the source DSN rather than its credentials, plus
source system and Kafka topic identities. Existing job rows without these
identities require a new safe snapshot; they are not silently migrated.

The source and destination schemas are in `db/init-source.sql` and
`db/init-dest.sql`. The destination `accounts` table remains directly
queryable during the initial load, but its contents are incomplete until all
ranges finish and CDC catches up. A source slot and a Kafka topic with
sufficient retention are required for recovery. The source SQL role needs
access to `pg_control_system()` to pin the PostgreSQL system identifier.

## Run

```bash
docker compose up -d
go run ./cmd/seam --start-fresh
```

The bundled Compose stack starts capture. On a separate deployment, start
`go run ./cmd/seam-capture` first and wait for its slot and topic. Restart a
job with `go run ./cmd/seam` (without `--start-fresh`). Recovery validates the
source and broker identities, Kafka retention, schema fingerprint, and chunk
manifest, then fences unfinished work under a new attempt.
See [operations](docs/operations.md) for the live/shadow run sequence,
promotion, and recovery limits.

| Variable | Purpose | Default |
| --- | --- | --- |
| `SOURCE_SQL_DSN`, `SOURCE_REPLICATION_DSN`, `DEST_SQL_DSN` | PostgreSQL connections | Bundled local ports 5433/5434 |
| `KAFKA_BROKERS`, `KAFKA_TOPIC` | Ordered broker stream | `localhost:9092`, `seam.accounts` |
| `SEAM_JOB_ID`, `SEAM_SOURCE_SLOT`, `SEAM_SOURCE_PUBLICATION` | Durable job and capture identity | `seam-default`, `seam_slot`, `seam_pub` |
| `SEAM_DEST_TABLE` | Physical table for this job: `accounts` or `accounts_shadow` | `accounts` |
| `SEAM_CHUNK_SIZE`, `SEAM_WORKERS` | Discovery page rows and parallel scan workers | `1000`, `1` |
| `SEAM_MAX_IN_MEMORY_CANDIDATES` | Maximum rows in one scan before durable staging | `1000000` |
| `SEAM_MAX_CANDIDATE_BYTES` | Total scan-buffer budget divided among workers; staged windows live in PostgreSQL | `268435456` |
| `SEAM_MAX_RECORDS_PER_BATCH` | Kafka envelopes per poll | `100` |
| `SEAM_MAX_TX_EVENTS` | Hard capture transaction event cap; byte growth spills to disk and Kafka fragments | `1000000` |
| `SEAM_CAPTURE_OWNER_ID` | Diagnostic capture owner identity; generated when unset | unset |
| `SEAM_MAX_SOURCE_SCANS`, `SEAM_MAX_DESTINATION_TX` | Process-shared source scan and destination transaction caps; every SEAM process must use the same values | `4`, `8` |
| `SEAM_MAX_CDC_LAG_RECORDS`, `SEAM_RESOURCE_POLL_INTERVAL` | Pause new scans above this broker/checkpoint lag; refresh interval | `10000`, `1s` |
| `SEAM_LEASE_DURATION`, `SEAM_HEARTBEAT_INTERVAL` | Worker lease and renewal | `30s`, one third of lease |
| `SEAM_HTTP_ADDR` | Enable `/healthz`, `/metrics`, `/progress` | unset |

## Verification and tests

`go run ./cmd/seam-lab verify` is a quick comparison of moving databases; it
can report transient mismatches under live writes. For a defensible equality
check, use `go run ./cmd/seam-lab verify-fenced`: it temporarily blocks writes
to source `accounts`, sends a marker through capture, waits for the destination
checkpoint, and merge-compares both tables at that stable prefix using constant
application memory. Budget the lock hold time before using it on a large or
busy production table.

```bash
go test ./internal/...
go build ./...
go vet ./...
docker compose -f integration/docker-compose.yml up -d
go test -tags=integration -v ./integration/...
```

Integration tests use dedicated Compose source, destination, and Kafka ports
and reset their tables and topic. `integration/discovery_fencing_test.go`
exercises interrupted discovery and stale lease rejection against PostgreSQL.
`internal/promotion/promote_integration_test.go` exercises cutover transaction
fencing and idempotent retry against PostgreSQL. `integration/online_resync_test.go`
runs capture, both durable coordinators, source changes during shadow load,
and promotion. `integration/ports_test.go` (`TestIntegrationPortsEnforced`)
recursively scans every file under `integration/` and fails if any host port
other than 5435 (source), 5436 (destination), or 9094 (Kafka) appears, so the
suite can never collide with the production fixture stack (5433/5434/9092).
The following command only compiles these tests; it does not run a database:

```bash
go test -tags=integration -run '^$' ./integration/... ./internal/promotion
```

The 10,000-row benchmark (`BenchmarkBackfillWorkers1` and
`BenchmarkBackfillWorkers4`) compares one and four workers on a fixed source
and reports source-barrier-to-durable-completion time; each worker count runs
in a fresh `go test` process with alternating order, and every run is verified
by merging the full destination against the source row-for-row before the time
is accepted. An executed counterbalanced run (4 passes, 8 samples) measured
~5.3–5.8k rows/s (median ~1.81 s) with one worker and ~9.6–10.0k rows/s (median
~1.01 s) with four workers — a median wall-clock speedup of ~1.77× (1.72×–1.86×
per-pass range) before durable candidate staging was added. The latest
corrected post-change smoke sample measured 3.24k rows/s with one worker and
6.96k rows/s with four workers (2.15×). It is one pair rather than a
distribution; earlier post-change samples bypassed the production resource
controller and are not comparable. A separate exact-verified 100,000-row pair
measured 3.38k rows/s and 6.72k rows/s (1.99×). The configurable
fresh-process matrix (`make benchmark-matrix`) varies row count, row width,
worker count, and chunk size. See `docs/benchmarks.md`; none of these local
results justify extrapolation to larger workloads.
The older phase tests still exercise some legacy single-worker paths; passing
them alone is not a proof of the production coordinator or online cutover.

## Evidence and limits

The dedicated-stack integration suite has been executed live against the
workspace's own PostgreSQL/Kafka stack (`integration/docker-compose.yml`, host
ports 5435/5436/9094): 21 integration tests covering the CDC flow, bounded
snapshot windows, delete resurrection, reconciliation, durable checkpoints,
crash-after-chunk-read recovery, hard cases, source replay deduplication,
resource bounds and process-shared admission, CDC continuity, crash/restart chaos, interrupted-discovery
fencing, reconciler and capture leadership takeover, multi-record transaction atomicity,
online shadow resync, and port isolation, plus 2 promotion tests
(`TestAtomicPromotionAndIdempotentRetry`,
`TestPromoteRejectsMissingJobIDs`). `TestIntegrationPortsEnforced` scans every
file under `integration/` recursively and fails if any host port other than
5435/5436/9094 appears. Exact commands and the executed benchmark samples are
in `docs/benchmarks.md`; raw numbers are reported only for what actually ran.

`TestChaos_CrashRestart` uses a fixed seed and 600-operation workload, forces
three restarts at exact operation boundaries, and waits for a unique source
marker to cross Kafka and the durable destination checkpoint before comparing
every row. It runs through the durable chunk store, so it exercises attempt
fencing and chunk recovery rather than the legacy scan loop.

Promotion now uses an exact-prefix MVCC snapshot and changed-key final
validation, but its first and final fences still depend on capture and both
reconcilers reaching their gates before the configured deadline. Performance
work still needs measured source latency,
destination throughput, CDC lag, peak memory, WAL/Kafka retention, and full
catch-up time at 1M/10M/100M-row scale. The single-pair 10,000-row and
100,000-row benchmarks are starting points, not evidence of Artie-like
speedups at larger scale.

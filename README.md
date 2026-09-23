# Seam

Seam backfills a PostgreSQL table into a second PostgreSQL database and keeps
it converged with live writes. It copies one table (`public.accounts`) without
locking the source or the destination, then verifies the result row by row.

## Why this exists

Copying a table while it is still receiving writes is harder than it looks.

A plain snapshot is a point in time. Any commit that lands after the snapshot
is missing from the copy, and nothing tells you which rows were missed. A naive
"copy everything, then replay new writes" approach has the reverse problem: a
row read by the snapshot early can be updated again before the replay reaches
it, and applying the stale snapshot value on top of the newer write loses
data. Taking a table lock to freeze the source is usually not an option in
production.

Seam exists to make that overlap safe. The snapshot is taken as a sequence of
key ranges, and each range is bracketed by control records that travel through
the same replication stream as the data. Everything the stream delivers inside
a range is replayed before the range is committed, so a change landing between
a range's markers evicts the stale snapshot copy. Live data wins by
construction, and the finished table is compared against the source directly
instead of being assumed correct.

The project started as a way to answer one question: can a single-table
backfill with live replication be made exactly-once and still stay simple
enough to reason about? The answer lives in a few small packages
(`internal/checkpoint`, `internal/marker`, `internal/recovery`,
`internal/scan`, `internal/reconcile`) and a crash test that restarts the
whole pipeline mid-write until the source and destination match exactly.

This repository is now being upgraded from that local prototype into a
production-grade, crash-safe, distributed backfill system. The work is tracked
in numbered phases; Phases 1 and 2 are implemented and tested, with the rest
laid out in the roadmap below.

## Architecture

Two processes share one Kafka topic.

- **capture** (`cmd/seam-capture`) opens a logical replication slot on the
  source, streams `pgoutput` for `accounts` and `seam_marker` into the topic,
  and acknowledges LSNs only after Kafka has the records.
- **reconciler** (`cmd/seam`) consumes the topic, backfills the destination in
  ordered key chunks, and applies live changes as they arrive.

Data path:

    source accounts + seam_marker
      -> pgoutput via publication seam_pub, slot seam_slot
      -> capture reader
      -> JSON records on one Kafka topic, one partition
      -> reconciler
      -> destination accounts, seam_jobs, seam_checkpoints, seam_applied_txs, seam_chunks

The reconciler works one chunk at a time. For a chunk
`completed_through_id < id <= scan_upper_bound`:

1. Writes a LOW marker row on the source, as a normal transaction.
2. Reads the chunk rows into an in-memory candidate map.
3. Writes the HIGH marker.
4. Replays Kafka until the HIGH marker shows up. Any source transaction in
   that window that touches a candidate key evicts the candidate.
5. Commits survivors, the checkpoint cursor, and the Kafka offset in one
   destination transaction.

When configured with a chunk store (`cmd/seam` uses `checkpoint.Store`), the
reconciler leases chunks from durable `seam_chunks` state and transitions each
chunk through `pending -> leased -> scanning -> reconciling -> committing ->
completed`. A worker that crashes releases its lease on expiry, and recovery
reschedules unfinished chunks under a new attempt.

Failpoints (`internal/failpoint`) pause the reconciler at fixed points for the
crash tests. HTTP endpoints (`/healthz`, `/metrics`, `/progress`) turn on when
`SEAM_HTTP_ADDR` is set.

## Guarantees

- Exactly-once convergence for a single `BIGINT PRIMARY KEY` table. After a
  run finishes (backfill plus drained CDC), source and destination are equal,
  verified byte for byte by `seam-lab verify` and the chaos test.
- Concurrent writes are safe. An UPDATE or DELETE inside a chunk's LOW/HIGH
  window evicts the stale snapshot value; CDC wins.
- Crash recovery. Unfinished chunks get a new attempt id on restart
  (`recovery.BumpAttempt`); markers from older attempts are ignored.
- Idempotent replay. Account upserts and deletes apply unconditionally;
  `seam_applied_txs` records applied source LSNs, and replay from Kafka is safe
  even when one PostgreSQL transaction is split across two poll batches.

## Production constraints

These follow from the design, so read them before running this in production.

- One processing line. A single capture worker, Kafka partition, and
  reconciler bound throughput; the destination trails the source until the
  backfill finishes and `seam-lab verify` passes.
- Memory scales with chunk size. Each chunk is read into an in-memory
  candidate map, so `SEAM_CHUNK_SIZE` is a straight tradeoff between working
  set and round trips, and an oversized chunk can exhaust memory.
- Retention is the recovery contract. A long outage grows WAL behind the
  source slot, and restart needs the checkpointed Kafka offset still retained
  by the topic. If the slot is gone or the offset is lost, the backfill has to
  be redone.
- Schema changes are coordinated. DDL on the mirrored table trips the
  fingerprint check and stops the pipeline instead of adapting, so altering
  the table means a planned change on both sides plus a restart.

## Configuration (env)

| Variable | Meaning | Default |
| --- | --- | --- |
| `SOURCE_SQL_DSN` | Source PostgreSQL | `postgres://postgres:postgres@localhost:5433/source?sslmode=disable` |
| `SOURCE_REPLICATION_DSN` | Source replication DSN | same host with `replication=database` |
| `DEST_SQL_DSN` | Destination PostgreSQL | `postgres://postgres:postgres@localhost:5434/dest?sslmode=disable` |
| `KAFKA_BROKERS` | Brokers, comma separated | `localhost:9092` |
| `KAFKA_TOPIC` | Target topic | `seam.accounts` |
| `SEAM_JOB_ID` | Job id | `seam-default` |
| `SEAM_SOURCE_SLOT` | Replication slot | `seam_slot` |
| `SEAM_SOURCE_PUBLICATION` | Publication | `seam_pub` |
| `SEAM_CHUNK_SIZE` | Rows per chunk | `1000` |
| `SEAM_WORKER_ID` | Worker identity for chunk leases | `<hostname>-<nanoseconds>` |
| `SEAM_LEASE_DURATION` | Chunk lease TTL | `30s` |
| `SEAM_MAX_IN_MEMORY_CANDIDATES` | Per-chunk candidate map cap | `1000000` |
| `SEAM_MAX_RECORDS_PER_BATCH` | Cap on records processed per poll batch | `100` |
| `SEAM_WORKERS` | Concurrent chunk workers (>1 enables coordinator + worker pool; requires durable chunk state) | `1` |
| `SEAM_MAX_TX_EVENTS` | Cap on in-flight events of one source transaction (capture) | `1000000` |
| `SEAM_SOURCE_TABLE` | Source table to backfill | `accounts` |
| `SEAM_SOURCE_KEY` | Source primary-key column for keyset pagination | `id` |
| `SEAM_ADAPTIVE_CHUNKING` | Resize chunks from measured latency (scanner mode) | `false` |
| `SEAM_TARGET_CHUNK_DURATION` | Target per-chunk duration for adaptive sizing | `5s` |
| `SEAM_CHUNK_SIZE_MIN` | Adaptive lower bound (rows) | `100` |
| `SEAM_CHUNK_SIZE_MAX` | Adaptive upper bound (rows) | `1000000` |
| `SEAM_GENERATION` | Generation tag written into changes and checkpoints | `gen:0` |
| `SEAM_HTTP_ADDR` | Enables HTTP endpoints | unset |

## Requirements

Go 1.25+. Docker with the Compose plugin for the bundled environment, or your
own PostgreSQL 16 (`wal_level=logical`, a replication user) and a Kafka broker.

## Run

```bash
docker-compose up -d        # source:5433, dest:5434, kafka:9092
go run ./cmd/seam -start-fresh
```

`docker-compose up -d` also starts the capture service, which creates the
replication slot and the topic before the reconciler connects. On your own
infra, run `go run ./cmd/seam-capture` too, or no CDC ever reaches the topic.

## Restart and recovery

Restart `cmd/seam` without `-start-fresh`. On restart it:

1. loads `seam_jobs` and `seam_checkpoints`,
2. validates the source DSN and the destination schema fingerprint,
3. fails hard if the replication slot is gone,
4. releases expired chunk leases,
5. loads incomplete chunks and reschedules them under a new attempt,
6. fails hard if the checkpoint offset is no longer retained by the topic,
7. resumes from `next_kafka_offset`.

## Verify

```bash
go run ./cmd/seam-lab verify      # full source vs destination comparison
SEAM_JOB_ID=seam-default go run ./cmd/seam-lab checkpoint
```

## Tests

```bash
docker-compose -f integration/docker-compose.yml up -d
go test -tags=integration -v ./integration/...
go test ./internal/...
```

The phase tests cover CDC flow, bounded snapshots, durability, crash recovery,
keyset chunks with negative and int64-boundary keys, LSN dedupe, chunk-size
bounds, and CDC continuity. `TestChaos_CrashRestart` runs concurrent random
mutations with repeated crashes and demands an exact source equals destination
match at the end.

## Local API

With `SEAM_HTTP_ADDR` set:

- `GET /healthz` returns `ok`
- `GET /metrics` returns Prometheus-style counters
- `GET /progress` returns the checkpoint and counters as JSON

## Roadmap

The upgrade is being worked through in phases. Completed phases are marked.

- [x] Phase 1 — Backfill correctness: LOW/HIGH markers, eviction of touched
  candidates, insert/update/delete safety, multiple changes to the same key,
  idempotent retries, duplicate CDC tolerance.
- [x] Phase 2 — Durable job state: `seam_chunks` table, explicit chunk states
  (`pending/leased/scanning/reconciling/committing/completed/failed`), chunk
  leasing, heartbeat fields, and recovery that reschedules unfinished chunks.
- [x] Phase 3 — Crash recovery: resume from last durable checkpoint at every
  crash boundary (before/during/after LOW, snapshot, HIGH, reconciliation,
  commit).
- [x] Phase 4 — Atomic chunk completion: destination write, checkpoint, and
  chunk completion are committed in the same destination transaction. The
  chunk transitions to `committing`, then `completed`, and only the commit
  makes any of it durable. Verified with crash tests at the pre-commit and
  post-commit boundaries.
- [x] Phase 5 — Bounded memory: chunked scanning, bounded CDC buffers, batch
  limits. The capture decoder caps in-flight events of one source transaction,
  the consumer caps records per poll, and the reconciler enforces per-chunk
  candidate and per-batch record limits before processing.
- [x] Phase 6 — Scalable PostgreSQL scanning: keyset pagination only (no
  OFFSET), configurable chunk size, configurable table and key column with
  SQL-injection-safe identifier validation, and tests that lock the keyset
  query shape. UUID/text key support awaits the generalized chunk-range model
  (Phase 11).
- [x] Phase 7 — Adaptive chunking: a duration-feedback controller
  (`internal/adaptive`) measures completed-chunk latency on a 32-sample sliding
  window, clamps each step to half/double, and resizes the next chunk toward a
  target duration with hard min/max bounds. Wired into the reconciler's
  scanner loop and configurable via `SEAM_ADAPTIVE_CHUNKING`,
  `SEAM_TARGET_CHUNK_DURATION`, `SEAM_CHUNK_SIZE_MIN/MAX`. Supports crash
  recovery unchanged. The coordinator phases (8-10) will extend this to
  wave-based durable-chunk discovery.
- [x] Phase 8 — Parallel workers: a coordinator owns the single Kafka consumer,
  every `r.cp` mutation, and in-order chunk commits; N workers lease chunks
  from the durable store, register their window (LOW marker) before scanning,
  and deliver snapshot candidates over a channel. Each window keeps a pending
  evicted set so markers/events consumed before the scan finishes are never
  lost, and commits are gated in chunk order so `completed_through` never
  regresses. Enabled with `SEAM_WORKERS` (default 1 = unchanged sequential
  path; requires durable chunk state, i.e. `SEAM_ADAPTIVE_CHUNKING=off`).
- [ ] Phase 9 — Chunk leasing with heartbeats and safe reassignment.
- [ ] Phase 10 — Coordinator: job creation, scheduling, progress, pause/resume/cancel.
- [ ] Phase 11 — Multi-table backfills.
- [ ] Phase 12 — Destination batching.
- [ ] Phase 13 — Backpressure.
- [ ] Phase 14 — Source protection / rate limiting.
- [ ] Phase 15 — Retry system.
- [ ] Phase 16 — Connection recovery.
- [ ] Phase 17 — Schema change detection.
- [ ] Phase 18 — Graceful shutdown.
- [ ] Phase 19 — Configuration.
- [ ] Phase 20 — Remote/cloud PostgreSQL.
- [ ] Phase 21 — Observability / Prometheus metrics.
- [ ] Phase 22 — Structured logging.
- [ ] Phase 23 — Health endpoints.
- [ ] Phase 24 — Docker images.
- [ ] Phase 25 — Kubernetes readiness.
- [ ] Phase 26 — Correctness verification tool.
- [ ] Phase 27 — Workload generator.
- [ ] Phase 28 — Failure injection harness.
- [ ] Phase 29 — 10M / 100M benchmark.
- [ ] Phase 30 — 1B row benchmark.
- [ ] Phase 31 — Multi-billion / 10B validation.
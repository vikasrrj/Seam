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
`internal/scan`) and a crash test that restarts the whole pipeline mid-write
until the source and destination match exactly.

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
      -> destination accounts, seam_jobs, seam_checkpoints, seam_applied_txs

The reconciler works one chunk at a time. For a chunk
`completed_through_id < id <= scan_upper_bound`:

1. Writes a LOW marker row on the source, as a normal transaction.
2. Reads the chunk rows into an in-memory candidate map.
3. Writes the HIGH marker.
4. Replays Kafka until the HIGH marker shows up. Any source transaction in
   that window that touches a candidate key evicts the candidate.
5. Commits survivors, the checkpoint cursor, and the Kafka offset in one
   destination transaction.

Once the cursor passes the upper bound, the reconciler stays in CDC-only mode.

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

## Limitations

- One table, fixed schema. No multi-table support, no schema evolution.
- Primary keys cannot change. An UPDATE that rewrites `id` fails at capture
  time.
- TOASTed values are not supported; they error at decode time.
- One capture worker, one partition, one reconciler.
- Schema drift past a fingerprint check refuses to start.
- Transient errors retry with bounded backoff (`internal/retry`); permanent
  errors stop the process.

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
4. bumps the attempt if a chunk was unfinished,
5. fails hard if the checkpoint offset is no longer retained by the topic,
6. resumes from `next_kafka_offset`.

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
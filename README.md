# Seam

Seam is a focused, single-table PostgreSQL online-backfill / reconciliation
tool. It copies one table (`public.accounts`) from a source finite snapshot into
a destination while a live logical-replication CDC stream (via Kafka) keeps the
destination converged with the source — without locking either table.

## Architecture

```
source PostgreSQL                Kafka (1 topic, 1 partition)      destination PostgreSQL
 accounts  ─┐
 seam_marker ├─ pgoutput ──► capture reader ──► JSON records ──► reconciler ──► accounts
            └ (seam_pub, slot seam_slot)                                     + seam_jobs/checkpoints/applied_txs
```

- **Capture** (`cmd/seam-capture`, package `internal/capture`): creates a
  logical replication slot, streams `pgoutput` for `accounts` and `seam_marker`
  to the topic, acknowledges LSNs after producing.
- **Reconciler** (`cmd/seam`, package `internal/reconcile`): consumes Kafka.
  For each primary-key chunk `completed_through_id < id <= scan_upper_bound`:
  1. writes a `LOW` marker row on the source (a normal transaction),
  2. reads the chunk's historical rows into an in-memory candidate map,
  3. writes the `HIGH` marker,
  4. replays Kafka until the `HIGH` marker is observed. Every source
     transaction in that window that touches a candidate key **evicts** the
     candidate from the map,
  5. commits survivors, checkpoint cursor, and Kafka offset atomically in one
     destination transaction.
  After the cursor passes the upper bound it stays in CDC-only mode.
- **Failpoints** (`internal/failpoint`) pause the reconciler at deterministic
  points for crash tests.
- **HTTP endpoints** (off unless `SEAM_HTTP_ADDR` is set): `/healthz`,
  `/metrics` (Prometheus-style counters), `/progress` (checkpoint JSON).

## Guarantees

- **Exactly-once value convergence** for a single `BIGINT PRIMARY KEY` table:
  after `Run` completes (backfill + CDC drained), source and destination rows
  are exactly equal. Verified byte-for-byte by `seam-lab verify` and the chaos
  integration test.
- **Concurrent writes are safe**: any UPDATE/DELETE landing inside a chunk's
  LOW/HIGH window evicts the stale snapshot value; CDC wins.
- **Crash recovery**: on restart, unfinished chunks get a new attempt id
  (`recovery.BumpAttempt`); markers from older attempts are ignored.
- **Replay is idempotent**: account upserts/deletes are applied unconditionally;
  `seam_applied_txs` records applied source LSNs; replays from Kafka offsets are
  safe even when a PostgreSQL transaction was split across two Kafka poll
  batches.

## Explicit limitations

- **Single table**, hard-coded schema: `accounts(id BIGINT PK, owner TEXT,
  balance_cents BIGINT)`. No multi-table support, no schema evolution, no
  migrations framework.
- **No primary-key changes**: an UPDATE that modifies `id` is rejected with a
  fatal error at capture time (`primary key change ... not supported`).
- **Not TOAST-ready**: toasted column values error out at decode time.
- Single CDC worker, single Kafka partition, single reconciler process.
- No schema-change handling beyond the fingerprint check that forces a restart
  refusal.
- Transient errors get bounded retry/backoff (`internal/retry`); permanent
  errors stop the process (restart via the runner).

## Configuration (env)

| Variable | Meaning | Default |
| --- | --- | --- |
| `SOURCE_SQL_DSN` | Source PostgreSQL | `postgres://postgres:postgres@localhost:5433/source?sslmode=disable` |
| `SOURCE_REPLICATION_DSN` | Source replication DSN (`replication=database`) | same host with `replication=database` |
| `DEST_SQL_DSN` | Destination PostgreSQL | `postgres://postgres:postgres@localhost:5434/dest?sslmode=disable` |
| `KAFKA_BROKERS` | Comma-separated brokers | `localhost:9092` |
| `KAFKA_TOPIC` | Target topic | `seam.accounts` |
| `SEAM_JOB_ID` | Job id | `seam-default` |
| `SEAM_SOURCE_SLOT` | Replication slot | `seam_slot` |
| `SEAM_SOURCE_PUBLICATION` | Publication | `seam_pub` |
| `SEAM_CHUNK_SIZE` | Rows per chunk | `1000` |
| `SEAM_GENERATION` | Capture generation tag (written into every change and checkpoint) | `gen:0` |
| `SEAM_HTTP_ADDR` | Optional HTTP address (enables endpoints) | unset |

## Requirements

- Go 1.25+ (`go.mod`), Docker + Docker Compose for the bundled environment, or
  your own PostgreSQL 16 (with `wal_level=logical`, a replication user) and a
  Kafka broker.

## Run

```bash
docker-compose up -d        # source:5433, dest:5434, kafka:9092 (+ capture service)
go run ./cmd/seam -start-fresh
```

`docker-compose up -d` also boots the **capture** service (`cmd/seam-capture`),
which creates the replication slot and Kafka topic before `seam` connects. If
you bring up your own infra instead, run `go run ./cmd/seam-capture` too —
without capture no CDC ever reaches the topic.

### Restart / recovery

Restart `cmd/seam` without `-start-fresh`. On restart Seam:

1. loads `seam_jobs` + `seam_checkpoints`,
2. validates the source DSN and destination schema fingerprint,
3. requires the replication slot to still exist, else fails hard,
4. bumps the attempt if a chunk was unfinished,
5. requires the checkpoint Kafka offset to still be retained by the topic,
   else fails hard (`history lost, reset required`),
6. resumes consuming from `next_kafka_offset`.

### Verify

```bash
go run ./cmd/seam-lab verify      # full source-vs-destination comparison
SEAM_JOB_ID=seam-default go run ./cmd/seam-lab checkpoint
```

## Tests

```bash
docker-compose -f integration/docker-compose.yml up -d
go test -tags=integration -v ./integration/...
go test ./internal/...
```

Phase tests cover CDC flow, bounded snapshots, naive-vs-reconciled behavior,
durability, crash recovery, keyset chunks with negative/int64-boundary keys,
LSN dedupe, chunk-size bounds, and CDC continuity. `TestChaos_CrashRestart`
hounds the system with concurrent random mutations plus repeated crashes and
requires an exact final source == destination match.

## Local API

With `SEAM_HTTP_ADDR` set:

- `GET /healthz` → `ok`
- `GET /metrics` → Prometheus-style counters
- `GET /progress` → checkpoint + counters as JSON

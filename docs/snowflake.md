# Snowflake path: design and operations

This runbook describes the implemented PostgreSQL → Kafka → Snowflake path.
It assumes `cmd/seam-capture` is already publishing SEAM transaction envelopes
for one PostgreSQL table to one Kafka partition.

## What runs

Four processes have distinct ownership:

1. `seam-capture` reads PostgreSQL `pgoutput`, reconstructs full rows, and
   publishes complete ordered source transactions to Kafka.
2. `seam-snowflake-sink` is the only Snowflake CDC writer. It resumes from the
   Snowflake offset table and writes every active physical route.
3. `seam-snowflake-backfill` creates and fills one shadow generation using
   leased parallel source scans.
4. `seam-snowflake-promote` first validates the shadow, then changes the stable
   public view to that physical generation.

The sink must keep running while backfill, validation, and promotion run.
Kafka retention must cover the oldest durable Snowflake frontier and the whole
backfill/recovery interval.

## Destination layout

With `SNOWFLAKE_LIVE_TABLE=ACCOUNTS`, the public query name `ACCOUNTS` is a
view. The first physical generation is `ACCOUNTS__SEAM_BASE`. A backfill uses a
separate physical table such as `ACCOUNTS_SHADOW`. Promotion executes an
idempotent `CREATE OR REPLACE VIEW ... AS SELECT ... FROM ACCOUNTS_SHADOW`.

The internal schema owns:

| Object | Purpose |
| --- | --- |
| `OFFSETS` | Authoritative next Kafka offset and pinned topic identity |
| `SINK_LEASES` | Active sink process, monotonic owner epoch, and expiry |
| `APPLIED_TRANSACTIONS` | Source transaction identity, event count, and deterministic content fingerprint |
| `ROUTES` | Physical tables receiving live CDC |
| `KEY_CLOCKS` | Latest source LSN/sequence per route and key, including delete tombstones |
| `CDC_STAGE` | Bounded staging rows for one source transaction |
| `MARKERS` | Durable LOW/HIGH/barrier positions observed by the sink |
| `BACKFILL_JOBS` | Backfill, validation, and promotion state machine |
| `BACKFILL_CHUNKS` | Sealed ranges, leases, fencing tokens, and scan markers |
| `BACKFILL_FILES` | Content identity, size, state, chunk lease token, and stage path for each snapshot file |
| `SNAPSHOT_STAGE` | Disposable bounded candidates for chunk finalization |
| `SNAPSHOT_FILES` | Named internal stage containing compressed immutable chunk files |

Snowflake constraints are informational, so SEAM enforces stream identity,
frontier continuity, replay identity, route ownership, and state transitions
with explicit compare-and-set statements.

## Correctness lifecycle

The durable backfill states are:

```text
discovering -> running -> ready_to_verify -> verifying
          -> ready -> promoting -> completed
                         \
                          -> failed
```

Preparation creates an empty typed shadow and activates its CDC route before
the source upper bound is sampled. This ordering covers rows inserted above a
premature upper bound: once the route is active, every later change reaches the
shadow even if no snapshot range includes it.

Manifest sealing writes every gap-free range and changes `discovering` to
`running` in one transaction. A worker leases a chunk with a fencing token,
writes LOW, clears disposable stage rows, and scans PostgreSQL. It encodes the
chunk as deterministic gzip JSON lines, records the file identity, uploads it
to the named internal stage, and uses `COPY INTO` to replace that chunk's
disposable candidates. The delete, copy, exact row count check, file state,
and chunk row count commit together. It then writes HIGH, waits until the sink
has committed HIGH, and finalizes. Finalization merges a candidate only when
that key's CDC clock is at or before LOW. A CDC update or tombstone after LOW
therefore wins over the older snapshot row. Target changes, chunk completion,
and candidate cleanup commit together; a stale worker that lost its lease
rolls back.

Validation briefly takes a PostgreSQL `SHARE` fence, exports a repeatable-read
snapshot, writes a marker, waits for the sink, and creates a Snowflake zero-copy
clone of the shadow at that boundary. Another PostgreSQL transaction imports
the exported snapshot before the fence is released. The full range-by-range
comparison then runs while source writes continue.

Promotion replays the Kafka suffix after the validation marker into a bounded
changed-key set. Under a second short source fence it emits a final marker,
waits for the Snowflake frontier, compares those keys against the current
shadow, records promotion intent, repoints the public view, and converges the
routes. If the process dies after the view DDL, both routes remain active and a
retry safely repeats the same pointer assignment.

## Schema contract

The source table must be in `public`, use `REPLICA IDENTITY FULL`, and have one
non-null `BIGINT` primary key. The Snowflake path currently accepts:

- `int2`, `int4`, `int8` → `NUMBER(38,0)`;
- `bool` → `BOOLEAN`;
- `char`, `text`, `bpchar`, `varchar`, `uuid` → `VARCHAR`;
- `date` → `DATE`;
- `timestamp` → `TIMESTAMP_NTZ(6)`;
- `timestamptz` → `TIMESTAMP_TZ(6)`; and
- `jsonb` → `VARIANT` with semantic JSON validation.

Other PostgreSQL types stop startup. Plain `json` is intentionally rejected
because its textual distinctions cannot be proven round-trip through
Snowflake `VARIANT`. A schema fingerprint is pinned for the generation; schema
changes require a new generation.

## Configuration

All Snowflake commands share these variables:

| Variable | Meaning | Default |
| --- | --- | --- |
| `SNOWFLAKE_DSN` | Go Snowflake driver DSN | required |
| `SNOWFLAKE_DATABASE` | Database containing SEAM schemas | required |
| `SNOWFLAKE_SCHEMA` | Public data schema | `SEAM` |
| `SNOWFLAKE_INTERNAL_SCHEMA` | Metadata/staging schema | `SEAM_INTERNAL` |
| `SNOWFLAKE_LIVE_TABLE` | Stable public view | `ACCOUNTS` |
| `SEAM_STREAM_ID` | Immutable pipeline identity | `seam-accounts` |
| `SOURCE_SQL_DSN` | PostgreSQL SQL connection | local source default |
| `SEAM_SOURCE_TABLE` | Source table in `public` | `accounts` |
| `KAFKA_BROKERS`, `KAFKA_TOPIC` | Captured transaction stream | local defaults |

The sink additionally accepts bounded poll/apply/retry controls:
`SEAM_SNOWFLAKE_INITIAL_OFFSET`, `SEAM_SNOWFLAKE_MAX_POLL_RECORDS`,
`SEAM_SNOWFLAKE_APPLY_TIMEOUT`, `SEAM_SNOWFLAKE_RETRY_ATTEMPTS`,
`SEAM_SNOWFLAKE_RETRY_INITIAL`, and `SEAM_SNOWFLAKE_RETRY_MAX`.
Sink ownership is configured with `SEAM_SNOWFLAKE_SINK_OWNER`,
`SEAM_SNOWFLAKE_SINK_LEASE_DURATION`, and
`SEAM_SNOWFLAKE_SINK_HEARTBEAT_INTERVAL`. The default owner contains the host,
PID, and random process-incarnation bytes.

Backfill uses `SEAM_BACKFILL_JOB_ID`, `SEAM_BACKFILL_ATTEMPT`,
`SNOWFLAKE_SHADOW_TABLE`, `SEAM_CHUNK_SIZE`, `SEAM_WORKERS`, `SEAM_WORKER_ID`,
`SEAM_LEASE_DURATION`, `SEAM_HEARTBEAT_INTERVAL`,
`SEAM_MAX_IN_MEMORY_CANDIDATES`, and `SEAM_MAX_CANDIDATE_BYTES`.
`SEAM_SNOWFLAKE_SNAPSHOT_LOADER` selects `bulk` by default or the diagnostic
`sql` path. `SEAM_SNOWFLAKE_BULK_TEMP_DIR` selects the local temporary
directory, and `SEAM_SNOWFLAKE_UPLOAD_PARALLEL` controls Snowflake file upload
parallelism from 1 through 99.

Promotion uses `SNOWFLAKE_VALIDATION_TABLE`, `SEAM_MAX_WRITE_PAUSE`,
`SEAM_SOURCE_LOCK_TIMEOUT`, and `SEAM_MAX_DELTA_KEYS`.

## Run order

Start capture and leave it running:

```bash
go run ./cmd/seam-capture
```

Start the Snowflake sink. On a brand-new stream, `-1` starts at Kafka's
earliest retained offset. Later restarts ignore this bootstrap value and use
the authoritative Snowflake frontier.

```bash
go run ./cmd/seam-snowflake-sink
```

Build the shadow online:

```bash
SEAM_BACKFILL_JOB_ID=backfill-1 \
SEAM_BACKFILL_ATTEMPT=attempt-1 \
SNOWFLAKE_SHADOW_TABLE=ACCOUNTS_SHADOW \
go run ./cmd/seam-snowflake-backfill
```

The command exits only after every chunk reaches `ready_to_verify`. Validate,
then promote, using the same job, attempt, shadow, stream, schemas, source, and
Kafka configuration:

```bash
go run ./cmd/seam-snowflake-promote validate
go run ./cmd/seam-snowflake-promote promote
```

The public view is queryable throughout. Before the first promotion it points
to the base generation, which may be incomplete unless Kafka contains the
table's full history. The online backfill plus promotion establishes the first
complete generation.

## Recovery behavior

- The sink resumes only from `OFFSETS`; it rejects a replaced topic or a
  frontier older than Kafka retention.
- One sink owns a renewable Snowflake lease. Takeover increments a durable
  epoch. Every apply transaction updates that lease row immediately before its
  ledger/frontier compare-and-set, so an expired process cannot commit after a
  new owner appears.
- A source transaction commits row effects, clocks, replay ledger, markers,
  and frontier atomically. An ambiguous commit is retried and classified from
  the durable ledger.
- Reusing the same backfill job/attempt resumes its durable state. Different
  immutable parameters are rejected.
- Expired chunk leases can be taken over; old workers are fenced by token.
- A bulk-loader retry regenerates the same content-addressed file for the same
  lease. `PUT` may be repeated. The transaction deletes that chunk's
  disposable candidates, executes `COPY INTO ... FORCE=TRUE`, verifies the
  exact row count, and advances the manifest and chunk together. An expired
  worker may leave an unused file in the internal stage, but cannot publish
  candidates or progress after takeover.
- Validation restart creates a new source snapshot and marker because an
  exported PostgreSQL snapshot cannot survive its connection.
- Promotion restart repeats an idempotent view assignment and completes route
  metadata convergence.
- An exact validation mismatch marks the job failed and disables its shadow
  route in one transaction.

## Verification

Unit and race tests:

```bash
go test ./internal/snowflake ./internal/snowreconcile ./internal/snowpromotion
go test -race ./internal/snowflake ./internal/snowreconcile ./internal/snowpromotion
```

Real Snowflake control-plane coverage:

```bash
go test -tags=snowflake_integration -count=1 \
  -run '^TestLiveSnowflakeControlPlane$' -v ./internal/snowflake
```

That test creates isolated temporary schemas and verifies actual Snowflake DDL,
transactions, offset initialization, two-phase backfill creation, manifest
sealing, sink lease takeover/fencing, zero-copy validation clone, stable-view
promotion, and cleanup.

The component benchmark runs the old SQL loader and the bulk loader with the
same row counts and payload widths. It writes raw JSON output plus host, commit,
and workload metadata under the ignored `benchmark-results` directory.

```bash
make benchmark-snowflake-load
```

With the dedicated PostgreSQL/Kafka integration stack running, the combined
test deliberately stops the sink and coordinator, takes over under new epochs
and leases, validates, promotes twice, and compares every source and Snowflake
row:

```bash
go test -tags='integration snowflake_integration' -count=1 -timeout 10m \
  -run '^TestSnowflakeOnlineBackfillCrashRecovery$' -v ./integration
```

This test passed on September 30, 2026 in 210.14 seconds. It finished with an
exact 520-row source/destination match after the intentional sink crash,
30-second lease takeover, coordinator stop, four-worker recovery, validation,
promotion, and idempotent promotion retry. This is a small correctness and
recovery scenario, not a throughput benchmark.

## Current performance limits

Correctness is implemented ahead of warehouse-scale throughput. CDC uses one
Kafka partition and stages bounded rows with parameterized inserts before a
set based `MERGE`. Snapshot workers use deterministic compressed files,
Snowflake `PUT`, and `COPY INTO`; the lease fenced candidate merge remains a
separate step. The internal stage currently retains uploaded files, including
orphans from workers fenced after upload, so operators must account for stage
storage until a proven garbage collector exists. There is no Snowpipe Streaming
path, multi-partition ordering protocol, adaptive warehouse controller, or
large Snowflake benchmark yet.

The September 30, 2026 SQL-loader baseline reached 36.6, 160.3, and 188.1
rows/s for 100, 1,000, and 5,000 rows. It is not an end-to-end result. The bulk
path has not yet been run against a live account in the current revision, so no
improvement is claimed. The next measurement must report bytes per second,
rows per second, file encoding and upload time, copy and merge time, warehouse
size and credits, source impact, Kafka lag, and recovery time under fixed row
width distributions.

# SEAM operations: fixed-schema PostgreSQL path

This runbook describes the implemented `public.accounts` → Kafka →
PostgreSQL path. It is a narrow experimental contract, not a general-purpose
connector. The source schema is `id BIGINT PRIMARY KEY`, `owner TEXT NOT NULL`,
and `balance_cents BIGINT NOT NULL`; the destination must be a logged ordinary
table with exactly that column layout and primary key. The source publication
must include `accounts` and `seam_marker`, and Kafka must have one ordered
partition. Source `accounts` must use `REPLICA IDENTITY FULL` so capture can
reconstruct unchanged TOAST columns. Capture and each reconciler are separate
long-running processes. Reconciler scan and destination-transaction admission
is coordinated across processes with PostgreSQL session advisory locks.
Configure identical `SEAM_MAX_SOURCE_SCANS` and
`SEAM_MAX_DESTINATION_TX` values for live and shadow processes; a session or
process failure releases its permits.

## Initial load

The repository Compose stack starts PostgreSQL, Kafka, and capture. Wait until
capture has created its logical slot and topic before starting a job:

```bash
docker compose up -d
SEAM_JOB_ID=live go run ./cmd/seam --start-fresh
```

`--start-fresh` requires an empty physical destination and reserves it for one
active job; a second active job cannot claim the same table. The process writes
a source barrier and waits for its complete Kafka transaction before persisting
the job's start offset. It records source system and topic identities, a hash
of the source DSN, a schema fingerprint, a source upper key bound, and a
durable chunk manifest. The public table is queryable during initial load but
incomplete until the checkpoint reaches the scan upper bound and CDC catches
up. Keep capture running; a stalled capture retains source WAL in its slot.

Restart an interrupted live job with the same configuration and without
`--start-fresh`:

```bash
SEAM_JOB_ID=live go run ./cmd/seam
```

Recovery rejects a missing or replaced source, topic, destination table,
schema, or Kafka history gap. It completes interrupted discovery and starts a
new attempt for unfinished chunks. It does not infer missing WAL from the
current source table. Old job rows lacking pinned identities or the current
schema fingerprint require a new safe snapshot; the metadata migration alone
does not make them recoverable.

## Online shadow resync and promotion

Keep capture and the live reconciler running. In a second terminal start a
new job against the empty shadow table:

```bash
SEAM_JOB_ID=shadow-1 SEAM_DEST_TABLE=accounts_shadow go run ./cmd/seam --start-fresh
```

The live and shadow jobs maintain independent destination checkpoints and
apply the same Kafka stream. `accounts` remains the public read target while
the shadow scans. Ensure both jobs are active, the shadow manifest is sealed,
and the shadow scan finishes. The promotion command also waits for these
conditions, but checking them beforehand reduces its source write pause.

```bash
go run ./cmd/seam-promote --live-job live --shadow-job shadow-1 --timeout 10m
```

Promotion first checks job/source/topic identity, fixed table schemas,
ownership, grants, and unsupported dependencies. It drains both jobs and uses
`seam_cutover_gates` to converge them at one exact transaction boundary. A
short source `SHARE` fence emits a validation barrier and establishes a
repeatable-read source snapshot at that exact prefix. Source writes resume
while SEAM merge-compares the complete snapshot and shadow. A final source
fence emits the cutover barrier, advances both gates to it, and compares only
keys changed since the validated snapshot. The marker does not stop capture.
Promotion then takes SEAM's exclusive routing fence and destination locks. Ordinary destination reads remain possible during the
full comparison, though lock contention may delay reads. The
command finally takes `ACCESS EXCLUSIVE` briefly, rechecks the schema and
both frontiers, deactivates the shadow writer, renames `accounts` to an
`accounts_retired_<timestamp>` name, renames `accounts_shadow` to `accounts`,
and records the cutover in one destination transaction. PostgreSQL readers
may block during this final rename lock. The live job remains the writer for
the public table name; stop the now-inactive shadow process after success.

The full comparison holds an MVCC source snapshot but no source write fence.
The final fence covers barrier catch-up and bounded changed-key validation. The
CLI default timeout is two minutes; `--timeout` changes the whole operation
deadline and `--max-write-pause` defaults to 30 seconds independently for each
of the two source-write fences, while each source/destination lock acquisition
has a five-second lock timeout.
`--max-delta-keys` defaults to 100,000 and aborts rather than growing an
unbounded final validation set. Budget
source write latency and destination capacity before using this on a large
table. A timeout or mismatch before commit leaves the original public table
unchanged. Retrying with the same live and shadow job IDs detects a committed
promotion and reports its retained table. The retired table is not deleted
automatically.

Before promotion, restart an interrupted shadow job with the same job ID and
`SEAM_DEST_TABLE=accounts_shadow`, omitting `--start-fresh`. After promotion,
do not restart the inactive shadow job: its physical table name is gone and
the live job now writes the promoted `accounts`. A new resync needs a new
empty `accounts_shadow` and a new shadow job ID. Do not manually rename or
truncate tables to work around a failed preflight; inspect the checkpoint and
source history first.

## Checkpoints and verification

Read the destination progress without changing it:

```sql
SELECT job_id, attempt, scan_upper_bound, completed_through_id,
       next_kafka_offset, active, updated_at
FROM seam_checkpoints ORDER BY job_id;

SELECT job_id, discovery_cursor, discovery_complete, destination_table
FROM seam_jobs ORDER BY job_id;

SELECT job_id, attempt, status, count(*)
FROM seam_chunks GROUP BY job_id, attempt, status ORDER BY job_id, attempt;

SELECT shadow_job_id, live_job_id, barrier_offset, retired_table, promoted_at
FROM seam_promotions ORDER BY promoted_at;
```

`go run ./cmd/seam-lab verify` compares moving tables and can report a
transient mismatch. For an exact check of the public destination, use:

```bash
SEAM_JOB_ID=live go run ./cmd/seam-lab verify-fenced
```

The fenced verifier requires the active job that writes public `accounts`,
blocks source account writes, waits for a marker and the destination
checkpoint, and merge-compares all ordered rows. Its full-table
scan also extends the source write pause. A completed scan frontier alone
does not prove that CDC is caught up; compare `next_kafka_offset` with a
marker observed in the same topic.

## Testing and performance evidence

Unit tests, the race tests, build, vet, and integration-test compilation pass.
The dedicated-stack integration suite has been executed live on this machine:
21 integration tests (CDC flow, snapshot windows, delete resurrection,
reconciliation, durable checkpoints, crash recovery, replay deduplication,
resource bounds and process-shared admission, CDC continuity, crash/restart chaos, discovery fencing,
reconciler/capture leadership takeover, fragmented-transaction atomicity, online shadow resync,
and port isolation) plus 2 promotion tests. Run
destructive integration tests only against the dedicated
`integration/docker-compose.yml` stack: they reset source/destination tables,
drop the test replication slot, and recreate the test Kafka topic.

```bash
go test ./...
go test -race ./internal/reconcile ./internal/checkpoint ./internal/capture ./internal/kafka
go build ./...
go vet ./...
docker compose -f integration/docker-compose.yml up -d
go test -tags=integration -v ./integration/... ./internal/promotion
./scripts/bench-counterbalanced.sh
./scripts/bench-matrix.sh
```

The counterbalanced benchmark (`BenchmarkBackfillWorkers1` and
`BenchmarkBackfillWorkers4`) seeds a fixed 10,000-row source, then measures the
full source-barrier-to-durable-scan-completion wall time inside the timer:
barrier write/wait, upper-bound read, durable job creation and chunk discovery,
and reconciler run until the checkpoint passes the scan upper bound. Seeding,
capture startup, and verification are outside the timer. Each worker count runs
in a fresh `go test` process and the two counts alternate order every pass
(1-then-4 / 4-then-1) to cancel warmup bias; every raw sample is printed, and
the summary reports median, range, and per-pass paired speedup. After each run
the full source and destination are merge-compared row-for-row in key order
(not just counted), so the reported times are for runs whose contents matched
exactly. Executed samples are in `docs/benchmarks.md`. The fixed 10,000-row
workload does not measure a large online resync, source-write contention, CDC
lag under load, WAL retention, or peak memory at 10M/100M-row scale; do not
infer an Artie-like multiplier from it.

## Deliberate limits

- `TRUNCATE`, key changes, unsupported `pgoutput` messages, and
  source/destination schema changes stop the pipeline rather than being
  silently applied. Unchanged TOAST values are reconstructed from the old
  tuple under the required `REPLICA IDENTITY FULL` contract. Open transactions spill above the 800 KiB
  memory threshold and publish as records bounded below 900 KiB. The hard
  event cap still stops pathological transactions without acknowledging WAL.
- The production binary accepts only `accounts/id`; adaptive chunking is
  disabled because it bypasses the durable manifest. Kafka is one partition.
- The bundled Kafka broker has replication factor one. This is a development
  fixture, not a high-availability durability claim. Retention must exceed
  the longest restart, shadow backfill, and catch-up interval.
- SEAM does not coordinate arbitrary direct destination writers. Capture uses
  a source advisory lock and monotonic owner epoch; reconciler processes for one job use a leased owner
  epoch; an expired process is fenced from durable writes. Promotion
  blocks direct writes during validation, but
  direct destination writes outside promotion are not part of the replicated
  source history.
- The live and shadow writers share one source and broker. A slow shadow can
  consume significant source scan, Kafka retention, destination disk, and
  connection capacity without delaying the live writer's logical frontier;
  infrastructure contention can still delay both.
- The parallel coordinator loads the durable chunk manifest in memory. Its
  memory use scales with chunk count even when each row-candidate window is
  bounded; very fine chunk sizes have not been proven at 100-million-row scale.

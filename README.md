# SEAM

PostgreSQL change data capture and online backfill. It reads committed
transactions, carries them through Kafka, and maintains a PostgreSQL or
Snowflake destination while historical rows are copied in parallel underneath
live traffic.

## System flow

```text
PostgreSQL source
      |
      v
seam-capture ---> Kafka (exactly one ordered partition)
                          |
          +---------------+---------------+
          v                               v
   seam (reconciler)             seam-snowflake-sink
          |                               |
          v                               v
 PostgreSQL destination            Snowflake destination
          |                               |
   seam-promote               seam-snowflake-backfill
  (online rebuild swap)                       |
                                  seam-snowflake-promote
```

Capture and the destination processes are separate and long-running. Capture has
to keep acknowledging WAL no matter what a backfill is doing, and a backfill
crash must never stall the stream. `seam-lab` verifies a destination against the
source.

For local configuration, copy `.env.example` to an ignored `.env` file and add
the Snowflake credentials when using that destination. Load it in each terminal
with `set -a; source .env; set +a` before starting a process.

## Why this project exists

A table copy is unsafe the moment the source keeps taking writes: a snapshot can
read an old row, CDC applies a newer update, and the delayed snapshot overwrites
it — or reinserts a deleted row. SEAM brackets every snapshot chunk with `LOW`
and `HIGH` marker transactions written into the source, so they travel the same
ordered Kafka partition as the data they protect. Any key changed in between is
evicted from the snapshot, so live CDC always wins over stale copy.

## Correctness rules

1. **PostgreSQL is acknowledged only after Kafka acknowledges every fragment.**
   A crash before that ack means the transaction is simply sent again. Decode
   and publish errors return before the ack, so unsupported WAL is re-sent after
   repair rather than skipped.

2. **Destination data and the Kafka frontier commit together, in one
   transaction.** The cursor is not a consumer-group offset; it is
   `next_kafka_offset` in destination state, compare-and-set in the same
   transaction as the row effects.

3. **Replay identity is the source commit LSN, not the Kafka offset.** A crash
   between the Kafka ack and the WAL feedback republishes the same transaction at
   a *different* offset. Deduplication is keyed on
   `(job_id, generation, source_lsn)`, so a republish produces no new effects.

4. **Snapshot rows cannot overwrite a newer update or resurrect a delete.** See
   Backfill below.

5. **Expired process and chunk owners are fenced after takeover.** Leadership is
   a leased monotonic epoch and chunk ownership is a bumped lease token; both are
   proven inside the transaction that performs the write, so a stale process
   fails instead of overwriting.

Ambiguity stops the pipeline rather than skipping data.

## Backfill

Online backfill is where two requirements collide: copy history quickly, but
never let a slow copy clobber a concurrent write. Five points carry it.

1. **Every chunk is bracketed.** A worker writes `LOW`, scans the chunk, writes
   `HIGH`. Both are ordinary inserts into a source-side `seam_marker` table, so
   each becomes its own committed source transaction and its own ordered message.

2. **Anything touched in between is evicted.** The consumer keeps applying live
   changes while the scan runs; a change for a key inside the chunk marks that
   candidate `evicted`. Staging ORs the evicted flag, so a snapshot row arriving
   after an eviction cannot resurrect it. A delete is an eviction plus a
   tombstone.

3. **Markers are attempt-scoped.** A marker carries its job, attempt, and key
   range, and only matches the chunk currently being reconciled. After an
   interrupted job bumps its attempt, every marker and candidate from the
   previous attempt becomes unreachable.

4. **The manifest is proved, not assumed.** Discovery writes each key range
   together with its cursor, so the manifest is never half-written. It counts as
   complete only when it provably starts at the minimum `BIGINT`, has no gaps,
   and ends exactly at the captured upper bound — never inferred from an empty
   queue, which is what a crashed discovery leaves behind.

5. **Scanning is parallel, committing is not.** One coordinator owns the single
   Kafka partition and the one checkpoint row; workers lease disjoint chunks and
   write their own markers. The backfill frontier advances only across a
   contiguous prefix, so out-of-order completions never skip a gap. A worker that
   lost its lease rolls back rather than publishing progress.

## PostgreSQL destination

All pipeline state lives in the destination: `seam_jobs` (pinned source, topic,
generation, and schema identity), `seam_checkpoints` (attempt, owner epoch, scan
frontier, Kafka frontier), `seam_applied_txs` (the exactly-once ledger),
`seam_chunks` (sealed manifest plus leases), `seam_candidates` (staged snapshot
plus eviction tombstones), `seam_route_fence`, `seam_cutover_gates`, and
`seam_promotions`.

Chunks move `pending → leased → scanning → reconciling → committing → completed`.
Rows are applied as upsert-by-primary-key or delete-by-key, so a replayed
transaction is idempotent at the row level even independently of the ledger.
`committing` and `completed` are written in the same transaction as the survivors
and the frontier, so a crash leaves a chunk retryable rather than half-applied.

Source scans and destination transactions take shared PostgreSQL advisory
permits, so a live reconciler and a shadow rebuild draw from one configured
budget, and a crashed owner's permits are released by the server.

For an online rebuild a second job fills a shadow table while the live job keeps
serving. Promotion stops both at one exact Kafka prefix, merge-compares the
source snapshot against the shadow, re-checks only the keys changed since, then
renames both tables in a single destination transaction. The previous table is
retained.

## Snowflake destination

The public name is a **view**; the writable data lives in a physical table. The
sink is the only CDC writer and owns the stream under a renewable lease with a
monotonic epoch, which every apply transaction re-proves immediately before
committing.

One Snowflake transaction applies a source transaction's row effects to every
active physical route, updates a per-key source clock (the ordering key is *commit
LSN, intra-transaction sequence*), records the replay ledger, and advances the
frontier. Deletes are tombstones in the clock rather than rows in the target, so
a stale backfill candidate cannot resurrect a deleted key.

Backfill uses the same `LOW`/`HIGH` protocol, loading each chunk through a
deterministic compressed file uploaded to an internal stage and reloaded with
`COPY INTO`. Validation exports a repeatable-read source snapshot, clones the
shadow at a known marker, and compares after writes resume. Promotion repoints
the view.

## Correctness boundaries

One source table per pipeline, one Kafka partition, one immutable `BIGINT`
primary key, `REPLICA IDENTITY FULL`, and a closed type set. Every generation pins
the source cluster, Kafka topic, table schema, and type mapping. SEAM stops on a
replaced source cluster, a recreated topic, missing retained history, schema
drift, an unsupported operation, or an ambiguous state it cannot prove safe. It
does not silently skip data.

## Performance evidence

| Path | Workload | Result |
|---|---|---|
| PostgreSQL backfill | 100,000 rows, one worker | ~3,379 rows/s |
| PostgreSQL backfill | 100,000 rows, four workers | ~6,717 rows/s (~1.99x wall clock) |
| Snowflake snapshot (SQL loader) | 5,000 rows | ~188 rows/s |

The PostgreSQL samples are an exact verified local pair, not a distribution, and
are not evidence at millions or billions of rows. The Snowflake figure is a
component measurement of candidate staging only. The bulk `COPY INTO` path is
implemented and tested but has not been run against a live account, so no
speedup is claimed from it. Raw results: [docs/benchmarks.md](docs/benchmarks.md).

## Current limitations

One Kafka partition, one table per pipeline, no online schema evolution.
`TRUNCATE`, primary-key changes, and unsupported `pgoutput` messages stop the
pipeline. Adaptive chunking is disabled because it would bypass the durable
manifest. The bundled Kafka stack is a development fixture, not an availability
claim. Large-scale source impact and Snowflake warehouse cost have not been
measured.

## Documentation

- [PostgreSQL operations](docs/operations.md) and
  [Snowflake operations](docs/snowflake.md) - running each destination path
- [Generic row design](docs/generic-row-design.md) - schema and type contract
- [Benchmarks](docs/benchmarks.md) - results and measurement boundaries

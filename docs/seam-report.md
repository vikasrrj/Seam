# SEAM technical report

Status: October 6, 2026

## What SEAM is

SEAM copies a PostgreSQL table into Snowflake while PostgreSQL continues to
accept writes. It combines logical change data capture with an online
historical backfill, then validates and promotes the completed Snowflake copy
without allowing an old snapshot row to overwrite a newer update or resurrect
a deleted row.

The current operating path is:

```text
PostgreSQL source
        |
        | logical decoding
        v
  seam-capture -----> Kafka, one ordered partition
                            |
                            v
                 seam-snowflake-sink ------> active Snowflake routes
                            ^                           |
                            |                           v
                 LOW/HIGH markers              stable public view
                            |                           ^
                            |                           |
                 seam-snowflake-backfill ------> shadow generation
                                                        |
                                                        v
                                             validate and promote
```

The repository also contains a PostgreSQL destination implementation. It is
kept as a supported experimental path and for its integration coverage, but it
is not part of the current PostgreSQL-to-Snowflake deployment.

## The decision that shapes the system

An ordinary table copy is incorrect once the source accepts concurrent writes.
For example, a backfill may read version 1 of a row, CDC may apply version 2,
and the delayed backfill may then overwrite version 2 with version 1. A delete
has the same problem: a delayed snapshot can recreate the deleted row.

The primary decision was therefore to preserve provable ordering before
optimizing throughput. That led to four invariants:

1. PostgreSQL WAL is acknowledged only after Kafka acknowledges the complete
   source transaction.
2. Snowflake row effects, replay identity, and the next Kafka offset are
   committed together.
3. A source transaction is deduplicated by source commit LSN rather than Kafka
   offset, because a capture retry can republish it at a new Kafka offset.
4. A snapshot candidate is applied only if no newer CDC clock exists for its
   key.

Every later design decision follows from these invariants.

## Decisions along the data path

### 1. PostgreSQL source contract

SEAM currently accepts one table per pipeline in the `public` schema, with one
immutable non-null `BIGINT` primary key and `REPLICA IDENTITY FULL`. The source
publication includes both the source table and `seam_marker`.

This narrow contract was chosen because correctness depends on reconstructing
complete before-and-after rows and comparing one stable key across CDC,
backfill, validation, and promotion. Unsupported types, key changes,
`TRUNCATE`, source replacement, and schema drift stop the pipeline instead of
being guessed or skipped.

The source system identity and schema fingerprint are pinned. Restart is
allowed only when SEAM can prove that it is reading the same source and the
same table contract.

### 2. Logical decoding and capture

`seam-capture` reads PostgreSQL `pgoutput`, reconstructs committed
transactions, and publishes them to Kafka. It does not acknowledge WAL after
decoding alone; it waits until Kafka has acknowledged every fragment of the
transaction. A crash before that point causes PostgreSQL to resend the WAL,
which is safe because downstream replay identity is based on source LSN.

Large transactions are fragmented into bounded Kafka records. Fragment order,
source identity, total event count, and the final-fragment flag are checked on
consume. Incomplete fragments never become visible as a partial transaction.
Open transactions and fragment assembly spill to disk above their memory
thresholds, and hard event limits stop pathological inputs rather than growing
memory without bound.

Capture ownership uses a monotonic epoch tied to the logical slot. A stale
capture process cannot continue publishing after another process takes over.

The first measured capture issue was fragment sizing. The old code repeatedly
encoded the whole accumulated fragment, producing quadratic work. For a
1,000-row microbenchmark it took about 686 ms and allocated about 134.2 MB.
Sizing each row once reduced this to about 7.0 ms and 2.89 MB. The change was
kept after producer, fragmentation, unit, race, and end-to-end recovery tests.
Capture CPU is no longer the limiting stage in the tested Snowflake path.

### 3. Kafka ordering and partition count

Each pipeline uses exactly one Kafka partition. This is deliberate: source
transactions, transaction fragments, LOW/HIGH markers, and the durable
Snowflake frontier all share one total order. The Snowflake checkpoint is not
a consumer-group offset; it is application state committed with destination
effects.

The local Kafka benchmark used a 224,077-byte transaction containing 1,000
rows. Produce acknowledgement, fetch, reassembly, and decode together took
14.90-15.24 ms. Produce acknowledgement was about 2.8-2.9 ms, fetch and
reassembly about 6.8-6.9 ms, and decode about 5.2-5.4 ms. This was under 0.5%
of the measured Snowflake staging time, so Kafka was not optimized further.

An October 6 rerun measured 14.14-17.65 ms with a 14.69 ms median, including
2.77-3.94 ms produce acknowledgement, 6.52-7.73 ms fetch/reassembly, and
4.36-5.90 ms decode. It allocated 2.27-2.41 MB in about 10,190 allocations per
1,000-row transaction. This is about 0.10% of the current 14.901-second
end-to-end Snowflake backfill result.

Adding partitions was rejected as a current performance change. It would not
remove the measured Snowflake latency and would break the existing proof of
order. A safe multi-partition design would require per-partition frontiers,
transaction routing rules, and explicit coordination for markers that span
partitions. Until that protocol exists, startup rejects a topic whose partition
count is not one.

Kafka retention must cover the oldest durable Snowflake frontier and the full
backfill or recovery interval. The bundled single-broker, replication-factor-1
stack is a development fixture, not a production availability design.

### 4. Snowflake CDC ownership and apply

`seam-snowflake-sink` is the only CDC writer. It owns a renewable lease with a
monotonic epoch. Every apply transaction re-proves that epoch immediately
before advancing durable state, so a process whose lease expired cannot commit
after a replacement has taken ownership.

The public Snowflake name is a view. Writable data lives in physical generation
tables, and `ROUTES` records which generations receive live CDC. This allows
the current public generation and an in-progress shadow generation to receive
the same changes without changing the name used by readers.

For each source transaction, one Snowflake transaction:

1. validates the current stream, route, and lease;
2. stages bounded row effects;
3. applies them to every active route;
4. updates per-key clocks using commit LSN and intra-transaction sequence;
5. records the replay ledger entry;
6. records any marker; and
7. advances the authoritative Kafka frontier.

Deletes remain as tombstones in `KEY_CLOCKS`, even though the target row is
removed. That tombstone is what prevents a delayed snapshot candidate from
recreating the row.

This transaction boundary is intentionally conservative. It provides replay
and takeover safety, but its Snowflake statements and commit round trips are
now one of the dominant measured costs.

### 5. Backfill preparation and durable manifest

`seam-snowflake-backfill` creates an empty typed shadow and activates its CDC
route before reading the source upper bound. This order matters: a new source
row above the eventual snapshot range still reaches the shadow through CDC.

The source key range is divided into a gap-free durable chunk manifest. The
manifest is sealed transactionally; completion is proved from its boundaries,
not inferred from an empty worker queue. Workers lease chunks with fencing
tokens. A worker that loses its lease may finish local work, but Snowflake
rejects its attempt to publish candidates or progress.

Parallel workers were chosen for source scans and independent chunk work.
Checkpoint progress remains a contiguous frontier, so a later chunk finishing
first cannot skip an unfinished earlier range. Four workers improved measured
Snowflake staging throughput, but not linearly, because Snowflake transaction
work becomes the shared limiting resource.

Adaptive chunk sizing is currently disabled. The earlier implementation could
bypass the durable manifest, which weakened the recovery proof. Chunk size is
therefore explicit and benchmarked rather than changed automatically.

### 6. LOW/HIGH reconciliation protocol

Every chunk writes a LOW marker into PostgreSQL, scans its source range, then
writes a HIGH marker. Both markers travel through logical decoding, Kafka, and
the same Snowflake sink as normal changes.

While the scan is running, CDC updates the clock for every changed key. Once
HIGH is durable in Snowflake, finalization accepts a snapshot candidate only
when its key clock is at or before LOW. An update or tombstone after LOW wins;
the snapshot cannot overwrite or resurrect it.

Markers are scoped to the backfill job, attempt, and key range. A restarted job
uses a new attempt, making markers and candidates from an abandoned attempt
unreachable. Marker-only CDC transactions use a fast path that skips empty row
merges, but they still fence the lease, write replay/frontier state, and commit.
Those fixed Snowflake transactions are significant when chunk sizes are small.

### 7. Serialization, files, PUT, and COPY INTO

The original Snowflake snapshot path issued parameterized `INSERT ... SELECT`
batches. Its live baseline reached only 160.3 rows/s at 1,000 rows and 188.1
rows/s at 5,000 rows. Fixed query and transaction latency dominated.

The chosen replacement is a deterministic gzip JSON-lines file for each chunk:

```text
source rows -> deterministic encoding -> gzip file -> internal stage PUT
            -> COPY INTO disposable candidates -> fenced chunk progress
```

The file is content-addressed and recorded in a durable manifest. A retry can
regenerate and re-upload the same file. `COPY INTO` and the chunk's fenced
progress update commit together, so an uploaded file alone is never proof that
the chunk was loaded.

Performance changes were made one at a time and retained only after a benchmark
and recovery tests:

1. Lease validation and manifest preparation became one conditional `MERGE`.
2. The redundant manifest update after deterministic `PUT` was removed.
3. Exact row verification now uses `COPY INTO`'s `rows_loaded` result instead
   of a separate `COUNT(*)` query.
4. An unused loaded-file state update was removed while retaining the durable
   manifest and atomic COPY/progress transaction.

The final 1,000-row single-worker staging result improved from 5.355 s to
3.278 s, a 38.8% latency reduction. Four-worker throughput improved from 450
to 794 rows/s, and scaling improved from 2.08x to 2.60x.

The loader deliberately retains files in the internal stage. A garbage
collector was not added because it must first prove that a file is no longer
needed by a retry or a fenced worker. Storage cleanup remains an operational
and design task rather than an unsafe deletion shortcut.

### 8. Finalization

After HIGH is visible, the worker merges eligible candidates into the shadow,
marks the chunk complete, and cleans disposable candidates in one fenced
transaction. Lock order was changed so stage cleanup and chunk progress follow
a consistent order; this removed the observed table-lock deadlock without
weakening the transaction boundary.

Finalization remains expensive. In the final recovery profile, ten snapshot
finalization transactions accumulated 42.6 s of service time, including
11.5 s in target merges. Workers overlap some of this work, so these service
times are not wall-time percentages, but they identify finalization as a real
Snowflake-side cost rather than a Go CPU problem.

### 9. Validation and promotion

Backfill completion alone does not prove equality. Validation briefly takes a
PostgreSQL `SHARE` fence, exports a repeatable-read source snapshot, writes a
marker, waits for the Snowflake frontier, and clones the shadow at that exact
boundary. The full comparison runs after ordinary source writes resume.

Promotion then replays the Kafka suffix after the validation marker into a
bounded changed-key set. Under a second short source fence it writes a final
marker, waits for Snowflake, and compares those changed keys. Only then does it
record promotion intent and repoint the stable public view.

View assignment is idempotent. If the process dies after the Snowflake DDL but
before route convergence, a retry repeats the same pointer assignment and
finishes the metadata transition. The old physical generation is not silently
deleted.

### 10. Recovery and failure decisions

Recovery is based on durable facts rather than process memory:

- capture, sink, and workers use monotonic ownership epochs or lease tokens;
- source LSN provides replay identity;
- Snowflake `OFFSETS` is the authoritative consume frontier;
- active routes determine which generations receive CDC;
- chunk manifests and states determine resumable work;
- ambiguous commits are classified from the ledger rather than assumed to
  have failed; and
- missing Kafka retention, source replacement, topic replacement, schema
  drift, and validation mismatch fail closed.

The final live recovery test deliberately exercised sink takeover, worker
restart, replay, validation, promotion, and idempotent promotion retry. The
latest run finished with an exact 520-row source/Snowflake match in 225.23 s.
This is failure-recovery evidence, not a throughput result. Its chunk lease now
matches the production two-minute default; a 30-second test lease correctly
failed closed when warehouse latency exceeded that artificial bound.

## Current operating decision

For the current deployment, PostgreSQL is the source and Snowflake is the only
destination being run. The normal local services are PostgreSQL source,
ZooKeeper, and Kafka. The PostgreSQL `dest` Compose service is optional and is
not started for this path.

Four SEAM commands have separate responsibilities:

| Command | Lifetime | Responsibility |
| --- | --- | --- |
| `seam-capture` | continuous | PostgreSQL WAL to Kafka |
| `seam-snowflake-sink` | continuous | ordered Kafka CDC to active Snowflake routes |
| `seam-snowflake-backfill` | one run per generation | source scan, staged load, and reconciliation |
| `seam-snowflake-promote` | one validate and one promote run | exact validation and public-view cutover |

The Snowflake configuration formerly kept beside a separate local prototype
was moved into SEAM's ignored `.env`; a safe `.env.example` is committed. SEAM
is now self-contained and does not depend on the removed `p-kafka-s` prototype.
Secrets remain local and are not committed.

## Performance evidence and present bottleneck map

### Formal Snowflake staging benchmark

The accepted live benchmark used an X-Small warehouse, 1,000 rows per worker,
and a 96-byte text payload:

| Workers | Rows | Before | After | After throughput | Allocation result |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1,000 | 5.355 s | 3.278 s | 305.1 rows/s | 4.05 MB, 44,975 allocations |
| 4 | 4,000 | 8.885 s | 5.036 s | 794.3 rows/s | 16.63 MB, 187,848 allocations |

For one current 1,000-row staging operation:

| Phase | Time | Approximate share |
| --- | ---: | ---: |
| Manifest preparation | 0.502 s | 15% |
| PUT/upload | 0.776 s | 24% |
| COPY transaction | 1.983 s | 60% |
| Local encode and gzip | 0.016 s | below 1% |

The COPY statement itself averaged 0.537 s. About 1.45 s of the COPY
transaction was therefore begin, lease validation, stage deletion, progress
update, commit, and their client/server round trips. Snowflake query history
showed no warehouse overload or transaction-blocked wait in the single-worker
sample. The current first saturated section is Snowflake transaction and
control-plane latency, not local encoding or Kafka transport.

### October 6 end-to-end backfill profile

The full PostgreSQL-to-Kafka-to-Snowflake path is now phase-instrumented. With
10,000 rows and four workers, changing only the chunk size from 1,000 to 5,000
reduced exact-verified wall time from 62.316 seconds to 20.055 seconds and
raised throughput from 160.5 to 498.6 rows/s, a 3.11x improvement.

The 5,000-row setting turns ten chunks into two. That removes eight manifests,
uploads, COPY/finalization transactions and sixteen LOW/HIGH source
transactions. A fresh 20.117-second control showed the remaining four serial
marker apply transactions consuming 10.960 seconds of Snowflake service time.
Marker wait accumulated 7.528 seconds and finalization about 6.3 seconds;
worker overlap means these service totals are not additive wall percentages.

The existing 10,000-row default completed the same data in 19.203 seconds.
Four configured workers issued 40 lease queries even though the manifest had
one chunk; one worker completed it in 17.017 seconds. The coordinator now caps
local workers at the sealed manifest size. With four still configured, the
same one-chunk profile made two lease calls and completed in 16.632 seconds at
601.2 rows/s, a further 13.4 percent wall-time reduction.

The next retained change replaced the five dependent client-issued marker
statements with one stored-procedure call inside the same caller-owned
transaction.
Two exact runs completed in 14.789 and 15.012 seconds: a 14.901-second median
and about 671 rows/s. That is 10.4 percent less wall time and 11.6 percent more
throughput than the 16.632-second control. The procedure retains the active
route check, marker insert, sink lease fence, replay ledger insert, frontier
compare-and-set, affected-row checks, and Go-owned commit/rollback boundary.
It subsequently passed the full crash/takeover/replay test.

### Bottlenecks that currently matter

1. **Serial marker apply.** The two marker transactions still use a
   5.854-second median, about 39 percent of the 14.901-second end-to-end wall
   time by service-time comparison. One procedure call now handles the
   dependent writes, but begin, replay/frontier classification, the call, and
   commit remain serial.
2. **Per-chunk fixed work.** This was the first proven end-to-end bottleneck.
   Moving from 1,000 to 5,000 rows per chunk cut wall time by 67.8 percent for
   the tested workload.
3. **Finalization transactions.** Candidate filtering, target merge, fenced
   completion, and cleanup used 2.759-3.241 seconds in the two latest runs.
4. **Snowflake transaction round trips.** Correctness-related begin, lease,
   progress, ledger, frontier, and commit calls still cost more than local CPU
   for this small workload.
5. **Sublinear worker scaling.** Four workers achieved 2.60x rather than 4x;
   concurrent Snowflake operations, not Go CPU, are the shared limit in the
   measured staging case.
6. **PUT latency.** Upload is meaningful at about 24% of current staging time,
   but still smaller than the COPY transaction.

Kafka is not a current bottleneck in the measured local workload. One partition
is a future throughput ceiling, but changing it before the ordering protocol is
redesigned would trade correctness for performance and would not address the
current Snowflake limit.

## Decisions retained, deferred, and rejected

| Decision | Status | Reason |
| --- | --- | --- |
| Deterministic gzip files plus PUT/COPY | Retained | Measured faster than row-oriented SQL staging and safely retryable |
| One conditional manifest `MERGE` | Retained | Removed a round trip while preserving lease fencing |
| Use COPY `rows_loaded` | Retained | Removed a count query while preserving exact verification |
| Remove redundant file-state writes | Retained | Improved latency without removing durable proof |
| Marker-only sink fast path | Retained | Avoids empty row merges while preserving ledger and frontier |
| Consistent finalization lock order | Retained | Removed a real deadlock and kept atomic completion |
| Linear capture fragment sizing | Retained | Removed measured quadratic CPU/allocation cost |
| More Kafka partitions now | Rejected | Kafka is not saturated and the current correctness proof requires one order |
| Adaptive chunking now | Deferred | Must be redesigned around the durable manifest |
| Automatic stage-file deletion | Deferred | Needs a proof that retries and fenced workers no longer reference the file |
| Remove PostgreSQL destination code | Rejected | Not needed in the current run, but remains a valid tested repository path |
| Increase chunks beyond 1,000 rows for the measured narrow-row workload | Retained | 5,000 rows cut wall time from 62.316 s to 20.055 s; the existing 10,000 default was faster still |
| Cap local workers at sealed chunk count | Retained | One-chunk wall time fell from 19.203 s to 16.632 s and lease calls from 40 to 2 |
| Remove isolated control queries | Rejected after multiple trials | Query service fell, but end-to-end medians did not improve |
| Add a marker collection delay for batching | Rejected | It saved transaction service but raised wall time to 23.881 s |
| Driver multi-statement marker request | Rejected | It stalled beyond 188 s versus the 16.632 s control |
| Single-marker stored procedure | Retained | Same atomic proof; median wall fell from 16.632 s to 14.901 s |

## Next work, in order

The next optimization cycle should preserve the same method used for the
accepted changes: instrument one stage, identify the limiting physical work,
change one thing, rerun the same benchmark, run correctness and failure tests,
then keep or revert.

1. **Profile finalization, one change at a time.** It is now the largest
   non-marker Snowflake section at 2.759-3.241 seconds. Split candidate
   filtering, target merge, fenced completion, and cleanup using Snowflake
   query history, then optimize only the dominant physical operation.
2. **Profile one realistic large workload.** Repeat the exact phase profile at
   representative row widths and a row count large enough to amortize setup.
   Capture query history, credits, network bytes, local CPU, allocations, peak
   memory, and source impact.
3. **Measure validation and promotion separately.** Record source fence time,
   exported snapshot time, clone time, full comparison, changed-key replay,
   final comparison, view DDL, and route convergence.
4. **Test scale and failure cost.** Repeat at larger row counts and widths,
   record credits and warehouse size, interrupt PUT/COPY/finalization, expire
   leases, restart the sink, and verify exact source/Snowflake contents.
5. **Revisit partitioning only after Snowflake is no longer first.** A
   multi-partition protocol is a correctness feature and architecture change,
   not a Kafka configuration tweak.

## Current limits

SEAM is not yet a general-purpose connector. It supports one table per
pipeline, one ordered Kafka partition, one immutable `BIGINT` primary key, and
a closed PostgreSQL-to-Snowflake type mapping. It has no online schema
evolution, Snowpipe Streaming path, proven stage-file garbage collector,
adaptive warehouse controller, or production high-availability Kafka fixture.

The existing evidence is strong for replay, fencing, crash recovery, exact
validation, and the measured small Snowflake workloads. It is not evidence for
millions of rows, wide production schemas, remote replicated Kafka, sustained
high write rates, or warehouse cost at scale. Those claims require the next
instrumented experiments rather than extrapolation.

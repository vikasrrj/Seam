# SEAM

SEAM is a PostgreSQL change data capture and online backfill project. It reads
committed PostgreSQL transactions, sends them through Kafka, and maintains a
PostgreSQL or Snowflake destination while historical rows are copied in
parallel.

The project focuses on correctness during concurrency and failure. It is not a
general connector platform. The goal is to make a smaller system whose
transaction boundaries, recovery rules, and limitations can be explained and
tested precisely.

## Why this project exists

A normal table copy becomes unsafe when the source is still receiving writes.
For example, a snapshot can read an old row, CDC can apply a newer update, and
the delayed snapshot can overwrite that update. A deleted row can also be
inserted again by an old snapshot.

SEAM surrounds every snapshot chunk with LOW and HIGH marker transactions in
the source database. These markers travel through the same ordered Kafka
partition as normal row changes. Any key changed between LOW and HIGH is
removed from the old snapshot candidates. Live CDC therefore wins over stale
snapshot data.

## System flow

```text
PostgreSQL WAL
      |
      v
seam capture
      |
      v
Kafka partition 0
      |
      +-----------------------------+
      |                             |
      v                             v
PostgreSQL destination        Snowflake destination
      |                             |
      v                             v
shadow table and rename       shadow table and stable view
```

PostgreSQL logical decoding is the source of live changes. Capture preserves
complete source transactions. Large transactions can spill to disk and use
bounded Kafka fragments, but a consumer does not expose them until every
fragment is present.

Capture acknowledges PostgreSQL WAL only after Kafka acknowledges the whole
transaction. A crash can still cause the same source transaction to be
published again, so destinations use the PostgreSQL commit LSN as a stable
replay identity. Destination data and the next Kafka offset are committed
together.

## PostgreSQL destination

The PostgreSQL reconciler combines ordered CDC with leased parallel chunk
scans. Jobs, checkpoints, chunk ranges, worker leases, snapshot candidates,
applied transactions, and promotion state are stored in the destination.

An interrupted job resumes from durable state. Process ownership uses a
renewable lease and a monotonic epoch. Chunk ownership uses a separate token.
A stale process or worker cannot commit after a replacement has taken over.

For an online rebuild, a live job continues maintaining the public table while
a second job fills a shadow table. Promotion aligns both jobs at one Kafka
boundary, compares the shadow with a stable PostgreSQL snapshot, validates the
small set of keys changed afterward, and swaps table names in one destination
transaction. The previous live table is retained.

## Snowflake destination

The Snowflake sink applies each complete source transaction to every active
physical route. One Snowflake transaction contains the row effects, per key
source clocks, delete tombstones, applied transaction identity, markers, and
the next Kafka offset.

Only one sink owns a stream. Its renewable lease has a monotonic epoch, and
every apply transaction proves that epoch before committing its frontier.

An online Snowflake backfill creates a typed shadow table and activates its CDC
route before sampling the source upper bound. Parallel workers process a
sealed chunk manifest using the same LOW and HIGH protocol. Validation compares
a stable PostgreSQL snapshot with a Snowflake clone at the same stream marker.
Promotion then repoints a stable public view to the validated shadow. The view
assignment is idempotent, so retrying cannot switch the old generation back
into service.

## Correctness boundaries

SEAM currently supports one source table per pipeline, one Kafka partition,
and one immutable BIGINT primary key. The source table must use PostgreSQL
REPLICA IDENTITY FULL. Every generation pins the source cluster, Kafka topic,
table schema, and supported type mapping.

The system stops when it sees a replaced source cluster, a recreated Kafka
topic, missing retained history, schema drift, an unsupported operation, or an
ambiguous state it cannot prove safe. It does not silently skip data.

The main guarantees are:

1. A source transaction is not partially exposed at the destination.
2. Destination changes and the Kafka frontier commit together.
3. Replayed source transactions do not create new destination effects.
4. Snapshot rows cannot overwrite a newer update or resurrect a delete.
5. Expired process and worker owners are fenced after takeover.
6. Promotion requires exact validation at a known stream boundary.

## Verification

Go 1.27.1 is the tested toolchain.

```bash
go test ./...
go test -race ./...
go build ./...
go vet ./...
```

The PostgreSQL and Kafka integration suite uses a dedicated destructive test
stack.

```bash
docker compose -f integration/docker-compose.yml up -d
go test -tags=integration -count=1 -timeout 600s ./integration/... ./internal/promotion
```

The live Snowflake control plane test creates temporary schemas and removes
them after the run.

```bash
SNOWFLAKE_DSN='...' SNOWFLAKE_DATABASE='...' \
go test -tags=snowflake_integration -count=1 \
-run '^TestLiveSnowflakeControlPlane$' ./internal/snowflake
```

The complete PostgreSQL, Kafka, and Snowflake recovery test has passed with an
exact 520 row match after a sink crash, lease takeover, coordinator restart,
parallel recovery, validation, promotion, and promotion retry.

## Performance evidence

The latest exact verified PostgreSQL samples copied 100,000 rows at about 3,379
rows per second with one worker and 6,717 rows per second with four workers.
This is about a 1.99 times wall clock improvement for that single local pair.
It is not evidence of performance at millions or billions of rows.

The original Snowflake snapshot path reached about 188 rows per second at
5,000 rows because it issued parameterized SQL batches. SEAM now writes one
deterministic compressed JSON file per leased chunk, uploads it to an internal
Snowflake stage, and reloads the disposable candidate table with `COPY INTO`.
The SQL and bulk paths can be compared with the same component benchmark. No
post-change live result is recorded yet, so the repository does not claim a
speedup from the new path without measurement.

Raw results and measurement boundaries are recorded in
[docs/benchmarks.md](docs/benchmarks.md).

## Current limitations

The main limitations are one Kafka partition, one table per pipeline, and no
online schema evolution. Snowflake snapshot chunks use a bulk file loader, but
CDC transactions still use bounded SQL staging and set based merges. The
bundled Kafka stack is for development and does not provide a production
availability claim. Large scale source impact, warehouse cost, and bulk loader
throughput have not been measured.

These limits are intentional. SEAM currently prioritizes transaction
correctness, recovery, fencing, exact validation, and honest evidence over
connector count.

## Documentation

Start with [the system ownership report](docs/SEAM-system-ownership-report.md)
for a full explanation from first principles.

Use [the PostgreSQL operations guide](docs/operations.md) and
[the Snowflake operations guide](docs/snowflake.md) to run each destination
path.

Detailed benchmark evidence is in [docs/benchmarks.md](docs/benchmarks.md).
The earlier PostgreSQL engineering report is in
[docs/SEAM-engineering-report.md](docs/SEAM-engineering-report.md).

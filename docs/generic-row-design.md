# Generic row transport and schema epochs

SEAM's production pipeline no longer hard-codes `accounts/id/balance_cents`.
Instead, a descriptor-driven pipeline carries any PostgreSQL table whose shape
is explicitly supported and verified. Each job pins one durable, validated
**source schema descriptor** (namespace, table, replica identity, ordered
columns, type OIDs, primary-key ordinal) before capture starts, and every
subsystem — capture, scanner, sink, reconciler, recovery, promotion — is
pinned to that descriptor for the life of the job.

## What changed and why

### 1. Durable schema descriptor

A job's `source_schema` JSONB column (job payload in `seam_jobs`, chunk tables)
carries the validated source table descriptor: namespace/table, replica
identity (`REPLICA IDENTITY FULL` required), column names and order, type OIDs,
and the primary-key ordinal. `source_schema_fingerprint` is a deterministic
hash of that descriptor, pinned on the job and stamped on every change
envelope (`Change.SchemaID`). Every subsystem compares the pinned fingerprint
at each transition and fails closed if the live source ever diverges.

The descriptor is loaded once via `schema.LoadFromConn` and validated before
capture starts. The fixed codec rejects `TRUNCATE`, primary-key changes,
unknown `pgoutput` messages, and unexpected relation names, column order, or
PostgreSQL type OIDs. These are deliberate stop conditions; they are not
silently ignored.

### 2. Supported types

`internal/schema` owns the supported type set: `bigint`, `integer`,
`smallint`, `text`, `varchar(n)`, `character(n)`, `boolean`, `numeric(p,s)`,
`timestamp`, `timestamptz`, `date`, `jsonb`, `json`, `bytea`, `uuid`, and their
NULL variants, plus the primary key (`BIGINT` PK only). Unknown type OIDs and
`money` are rejected at validation; they are never coerced.

### 3. Rows as ordered values

`model.Row.Values` is positional — a tuple of canonical text values in exact
column order. Decoding validates each relation message against the pinned
descriptor (column names, order, OIDs, replica identity) and decodes values
positionally. No float or lossy conversion is performed; `NUMERIC` and
timestamp values round-trip as canonical PostgreSQL text.

### 4. Generic scanner and sink

- `scan.NewChunkReader(ctx, dsn)` reads the descriptor from the source and
  exposes `UpperBound`/`Schema`.
- `sink.NewMutatorFor(table, descriptor)` pins the destination table to the
  descriptor.
- `reconcile.Config` carries `SourceSchema` so the reconciler, scanner, and
  sink agree on the same descriptor.

## Chunking

The scanner creates deterministic keyset ranges over the primary key with no
`OFFSET`, no `OR`, and no index hint keywords. Adaptive chunking and source
tables/keys other than the pinned descriptor are rejected before capture.

## Fail-closed contract

| Condition | Behavior |
|---|---|
| Source table shape changes | Capture fails closed before decoding; no WAL ack |
| Unknown type OID / `money` | Validation rejects the job |
| `REPLICA IDENTITY` ≠ `FULL` | Capture start fails |
| Decoder meets unexpected message | Stop without acknowledging source WAL |
| `Scanner`/`Sink` descriptor mismatch | Reconciler fails closed before any write |

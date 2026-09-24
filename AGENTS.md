# SEAM — Agent Notes

## Project overview

SEAM is a PostgreSQL → PostgreSQL backfill system that copies tables while live
CDC continues. It uses LOW/HIGH markers bracketing primary-key chunks so that
any row changed between markers is replayed from CDC and never overwritten by a
stale snapshot.

## Build and test

- Go 1.27.1 is installed via mise in this environment.
- Unit tests require no services: `go test ./internal/...`
- Integration tests need the Docker Compose stack in `integration/` and the
  `integration` build tag: `go test -tags=integration -v ./integration/...`
- Always run `go build ./... && go vet ./...` after changes.

## Architecture conventions

- `internal/reconcile` owns the core algorithm. It now accepts interface
  dependencies (`consumer`, `checkpointStore`, `chunkStore`, `markerStore`,
  `scanner`, `sink`) so unit tests can use fakes.
- `internal/checkpoint` owns destination metadata tables (`seam_jobs`,
  `seam_checkpoints`, `seam_applied_txs`, `seam_chunks`, `seam_candidates`,
  `seam_route_fence`, `seam_cutover_gates`, `seam_promotions`). `Store.Begin` takes a shared routing lock before a
  writer resolves a destination table name.
- `internal/model` contains shared data types including `Chunk`, `ChunkState`,
  `Checkpoint`, and `JobConfig`.
- `cmd/seam` is the reconciler/worker process. It now discovers chunks and
  passes a `ChunkStore` to the reconciler when running with durable state.
- `cmd/seam-capture` streams `pgoutput` to Kafka.
- `cmd/seam-lab` is the verifier/debug tool.
- `internal/promotion` and `cmd/seam-promote` own the narrow PostgreSQL
  source-write fence, exact shadow comparison, and atomic name swap.

## Key correctness properties to preserve

1. **CDC wins inside the window.** Any account change (insert/update/delete)
   between LOW and HIGH evicts the candidate from the snapshot.
2. **Mark boundaries are source transactions.** LOW and HIGH markers are
   committed as separate source transactions and appear in Kafka order.
3. **Idempotent replay.** The sink uses upsert/delete by primary key, and
   `seam_applied_txs` records source LSNs.
4. **Attempt isolation.** Markers carry the current attempt id; stale markers
   are ignored after recovery bumps the attempt.
5. **Atomic chunk completion.** Chunk state, checkpoint, and survivors are
   updated in the same destination transaction.
6. **Complete source transactions.** Capture publishes one committed source
   transaction as one envelope or ordered bounded fragments. Open transactions
   spill to disk; consumers expose a replayable transaction only after `Final`
   and apply one bounded fragment at a time inside one destination transaction. Unsupported
   `pgoutput` and the hard event cap stop without acknowledging source WAL.
7. **No partial manifest.** Durable discovery advances the cursor and inserts
   its next logical range together. A sealed manifest covers every `BIGINT`
   key through the captured upper bound.
8. **Promotion fence.** Every SEAM destination writer acquires the shared
   route lock before naming a physical table. Promotion takes it exclusively,
   validates the shadow under table locks, and atomically renames tables.
9. **Process leadership.** Every production reconciler owns a leased monotonic
   epoch. Expired owners cannot mutate destination rows, checkpoints, chunks,
   discovery, or recovery state, even if their goroutines continue running.
10. **Capture leadership.** One source advisory-lock owner holds the slot term;
    every takeover advances a durable epoch and must preserve source,
    generation, publication, and topic identity.
11. **Exact-prefix promotion.** Durable gates stop live and shadow at the same
    transaction boundary. Full validation uses an MVCC source snapshot after
    writes resume; the final fence validates only changed keys before swap.
12. **Shared load admission.** Source scans and destination transactions take
    PostgreSQL session advisory-lock permits, so live and shadow processes
    share configured capacity and crashed owners release their slots.

## When modifying code

- Keep changes focused on a stated invariant; preserve failure behavior and
  test it with deterministic fakes or a disposable integration database.
- If you change a metadata table schema, update `checkpoint.EnsureTables` and
  any load/save methods.
- If you add configuration, expose it via environment variable in
  `cmd/seam/main.go` and document it in `README.md`.
- Do not depend on Kubernetes for correctness; SEAM must be correct on its own.

## Current status

- Unit tests, core race tests, build, vet, and integration-test compilation
  pass. This does not establish end-to-end correctness.
- Production `cmd/seam` rejects adaptive chunking and source table/key values
  other than `accounts/id`. The older adaptive and legacy test paths are not
  evidence for the production coordinator.
- Online shadow promotion, exact cutover gates, and capture leadership have passed live integration runs against this
  workspace's dedicated PostgreSQL/Kafka stack (`TestOnlineShadowResync`,
  `TestAtomicPromotionAndIdempotentRetry`). The latest exact-content-verified
  corrected post-change smoke pair, now wired through production resource
  admission, measured 3.09 s with one worker and 1.44 s with four workers
  (2.15x). It is not a distribution. Earlier post-change smoke pairs bypassed
  the resource controller; the older counterbalanced in-memory-window run is
  historical evidence only. See `docs/benchmarks.md`. Results are reported
  only for executed workloads and are not extrapolated to 10M/100M-row scale.
- See `README.md` and `docs/operations.md` for the supported run sequence and
  explicit limitations.

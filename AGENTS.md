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
  `seam_checkpoints`, `seam_applied_txs`, `seam_chunks`).
- `internal/model` contains shared data types including `Chunk`, `ChunkState`,
  `Checkpoint`, and `JobConfig`.
- `cmd/seam` is the reconciler/worker process. It now discovers chunks and
  passes a `ChunkStore` to the reconciler when running with durable state.
- `cmd/seam-capture` streams `pgoutput` to Kafka.
- `cmd/seam-lab` is the verifier/debug tool.

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

## When modifying code

- Keep changes minimal and focused on one phase at a time.
- Add or update unit tests in the same package; use fakes, not a live database.
- If you change a metadata table schema, update `checkpoint.EnsureTables` and
  any load/save methods.
- If you add configuration, expose it via environment variable in
  `cmd/seam/main.go` and document it in `README.md`.
- Do not depend on Kubernetes for correctness; SEAM must be correct on its own.

## Current status

- Phase 1 (backfill correctness), Phase 2 (durable chunk state), Phase 3 (crash
  recovery), Phase 4 (atomic chunk completion), Phase 5 (bounded memory),
  Phase 6 (scalable scanning), Phase 7 (adaptive chunking), and Phase 8
  (parallel workers: coordinator + worker pool, `SEAM_WORKERS`, chunk-ordered
  commits, pending-eviction sets) are implemented and covered by unit tests.
- Remaining phases are listed in `README.md` under Roadmap.

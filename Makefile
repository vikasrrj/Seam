.PHONY: test test-race build vet integration-up test-integration benchmark benchmark-matrix

# Unit tests: no services required.
test:
	go test ./...

# Unit tests with the race detector.
test-race:
	go test -race ./...

build:
	go build ./...

vet:
	go vet ./...

# Bring up the dedicated integration stack (source:5435, dest:5436, kafka:9094).
integration-up:
	docker compose -f integration/docker-compose.yml up -d

# Full integration suite against the dedicated stack.
test-integration:
	go test -tags=integration -count=1 -timeout 600s ./integration/... ./internal/promotion

# Counterbalanced fixed-workload benchmark (1 vs 4 workers). Each worker count
# runs in a fresh benchmark process; order alternates to cancel warmup bias.
benchmark:
	./scripts/bench-counterbalanced.sh

# Fresh-process matrix across rows, row widths, worker counts, and optional
# chunk sizes. Configure lists with ROWS_LIST/OWNER_BYTES_LIST/WORKERS_LIST/CHUNK_LIST.
benchmark-matrix:
	./scripts/bench-matrix.sh

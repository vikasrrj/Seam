.PHONY: test test-race build vet integration-up test-integration test-snowflake-live test-snowflake-e2e benchmark benchmark-matrix benchmark-snowflake-stage benchmark-snowflake-load snowflake-sink snowflake-backfill snowflake-validate snowflake-promote

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

# Creates isolated temporary Snowflake schemas and removes them after the run.
# Requires SNOWFLAKE_DSN and SNOWFLAKE_DATABASE.
test-snowflake-live:
	go test -tags=snowflake_integration -count=1 -run '^TestLiveSnowflakeControlPlane$$' -v ./internal/snowflake

# Requires the dedicated integration Compose stack plus Snowflake credentials.
test-snowflake-e2e:
	go test -tags='integration snowflake_integration' -count=1 -timeout 10m -run '^TestSnowflakeOnlineBackfillCrashRecovery$$' -v ./integration

snowflake-sink:
	go run ./cmd/seam-snowflake-sink

snowflake-backfill:
	go run ./cmd/seam-snowflake-backfill

snowflake-validate:
	go run ./cmd/seam-snowflake-promote validate

snowflake-promote:
	go run ./cmd/seam-snowflake-promote promote

# Counterbalanced fixed-workload benchmark (1 vs 4 workers). Each worker count
# runs in a fresh benchmark process; order alternates to cancel warmup bias.
benchmark:
	./scripts/bench-counterbalanced.sh

# Fresh-process matrix across rows, row widths, worker counts, and optional
# chunk sizes. Configure lists with ROWS_LIST/OWNER_BYTES_LIST/WORKERS_LIST/CHUNK_LIST.
benchmark-matrix:
	./scripts/bench-matrix.sh

# Runs the SQL and bulk Snowflake snapshot loaders with identical inputs.
benchmark-snowflake-stage:
	./scripts/bench-snowflake-load.sh

benchmark-snowflake-load: benchmark-snowflake-stage

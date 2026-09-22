.PHONY: build test integration integration-verbose verify

build:
	go build ./...

# Unit tests only: no services required.
test:
	go test ./...

# Integration tests against the docker compose stack in integration/.
# Bring the services up first: docker compose -f integration/docker-compose.yml up -d
integration:
	go test -tags=integration -count=1 -timeout 300s ./integration/...

integration-verbose:
	go test -tags=integration -count=1 -timeout 300s -v ./integration/...

verify: build test
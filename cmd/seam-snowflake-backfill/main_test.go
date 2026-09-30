package main

import (
	"strings"
	"testing"
	"time"
)

func TestLoadConfigDefaults(t *testing.T) {
	baseEnvironment(t)
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.chunkSize != 10_000 || cfg.workers != 4 || cfg.lease != 2*time.Minute || cfg.heartbeat != 40*time.Second {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestLoadConfigRejectsInvalidResourceBounds(t *testing.T) {
	tests := []struct {
		name  string
		env   map[string]string
		match string
	}{
		{name: "chunk", env: map[string]string{"SEAM_CHUNK_SIZE": "0"}, match: "positive integer"},
		{name: "workers", env: map[string]string{"SEAM_WORKERS": "many"}, match: "positive integer"},
		{name: "memory row cap", env: map[string]string{"SEAM_CHUNK_SIZE": "101", "SEAM_MAX_IN_MEMORY_CANDIDATES": "100"}, match: "must not exceed"},
		{name: "byte cap", env: map[string]string{"SEAM_MAX_CANDIDATE_BYTES": "-1"}, match: "positive integer"},
		{name: "lease", env: map[string]string{"SEAM_LEASE_DURATION": "never"}, match: "positive duration"},
		{name: "heartbeat", env: map[string]string{"SEAM_LEASE_DURATION": "1s", "SEAM_HEARTBEAT_INTERVAL": "1s"}, match: "shorter"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			baseEnvironment(t)
			for name, value := range test.env {
				t.Setenv(name, value)
			}
			if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("error = %v, want substring %q", err, test.match)
			}
		})
	}
}

func baseEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"SNOWFLAKE_DSN", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA", "SNOWFLAKE_INTERNAL_SCHEMA", "SNOWFLAKE_LIVE_TABLE",
		"SEAM_STREAM_ID", "SOURCE_SQL_DSN", "SEAM_SOURCE_TABLE", "KAFKA_BROKERS", "KAFKA_TOPIC",
		"SEAM_BACKFILL_JOB_ID", "SEAM_BACKFILL_ATTEMPT", "SNOWFLAKE_SHADOW_TABLE", "SEAM_WORKER_ID",
		"SEAM_CHUNK_SIZE", "SEAM_WORKERS", "SEAM_MAX_IN_MEMORY_CANDIDATES", "SEAM_MAX_CANDIDATE_BYTES",
		"SEAM_LEASE_DURATION", "SEAM_HEARTBEAT_INTERVAL", "SEAM_SNOWFLAKE_MARKER_POLL",
	} {
		t.Setenv(name, "")
	}
	t.Setenv("SNOWFLAKE_DSN", "user:password@example/account/database/schema")
	t.Setenv("SNOWFLAKE_DATABASE", "DATABASE")
}

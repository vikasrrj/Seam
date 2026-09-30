package main

import (
	"strings"
	"testing"
	"time"
)

func TestLoadConfigRequiresExplicitMode(t *testing.T) {
	baseEnvironment(t)
	for _, args := range [][]string{nil, {"unknown"}, {"validate", "promote"}} {
		if _, err := loadConfig(args); err == nil || !strings.Contains(err.Error(), "usage") {
			t.Fatalf("args=%v error=%v", args, err)
		}
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	baseEnvironment(t)
	cfg, err := loadConfig([]string{"validate"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.validationTable != "ACCOUNTS_SHADOW_VALIDATION" || cfg.maxDeltaKeys != 100_000 || cfg.maxWritePause != 30*time.Second {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestLoadConfigRejectsInvalidLimits(t *testing.T) {
	baseEnvironment(t)
	tests := []struct{ name, env, value, match string }{
		{name: "pause", env: "SEAM_MAX_WRITE_PAUSE", value: "0s", match: "positive duration"},
		{name: "lock", env: "SEAM_SOURCE_LOCK_TIMEOUT", value: "later", match: "positive duration"},
		{name: "delta", env: "SEAM_MAX_DELTA_KEYS", value: "0", match: "positive integer"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			baseEnvironment(t)
			t.Setenv(test.env, test.value)
			if _, err := loadConfig([]string{"promote"}); err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func baseEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"SNOWFLAKE_DSN", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA", "SNOWFLAKE_INTERNAL_SCHEMA", "SNOWFLAKE_LIVE_TABLE",
		"SNOWFLAKE_SHADOW_TABLE", "SNOWFLAKE_VALIDATION_TABLE", "SEAM_STREAM_ID", "SOURCE_SQL_DSN", "SEAM_SOURCE_TABLE",
		"KAFKA_BROKERS", "KAFKA_TOPIC", "SEAM_BACKFILL_JOB_ID", "SEAM_BACKFILL_ATTEMPT",
		"SEAM_SNOWFLAKE_MARKER_POLL", "SEAM_MAX_WRITE_PAUSE", "SEAM_SOURCE_LOCK_TIMEOUT", "SEAM_MAX_DELTA_KEYS", "SEAM_SNOWFLAKE_MAX_POLL_RECORDS",
	} {
		t.Setenv(name, "")
	}
	t.Setenv("SNOWFLAKE_DSN", "user:password@example/account/database/schema")
	t.Setenv("SNOWFLAKE_DATABASE", "DATABASE")
}

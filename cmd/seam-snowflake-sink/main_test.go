package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"example.com/seam/internal/kafka"
	seamsnowflake "example.com/seam/internal/snowflake"
	gosnowflake "github.com/snowflakedb/gosnowflake/v2"
)

type fakeApplier struct {
	calls int
	errs  []error
}

func (applier *fakeApplier) ApplyTransaction(context.Context, *seamsnowflake.SinkLease, *kafka.Transaction) error {
	index := applier.calls
	applier.calls++
	if index < len(applier.errs) {
		return applier.errs[index]
	}
	return nil
}

func TestApplyWithRetryRecoversAmbiguousTransientFailure(t *testing.T) {
	applier := &fakeApplier{errs: []error{&gosnowflake.SnowflakeError{SQLState: "08006"}}}
	cfg := config{
		applyTimeout: time.Second, retryMaxAttempts: 3,
		retryInitial: time.Millisecond, retryMaximum: time.Millisecond,
	}
	if err := applyWithRetry(context.Background(), applier, &seamsnowflake.SinkLease{OwnerID: "owner", Epoch: 1}, &kafka.Transaction{}, cfg); err != nil {
		t.Fatal(err)
	}
	if applier.calls != 2 {
		t.Fatalf("apply calls = %d, want 2", applier.calls)
	}
}

func TestApplyWithRetryStopsOnPermanentSnowflakeError(t *testing.T) {
	permanent := &gosnowflake.SnowflakeError{SQLState: "42000", Message: "syntax error"}
	applier := &fakeApplier{errs: []error{permanent, nil}}
	cfg := config{
		applyTimeout: time.Second, retryMaxAttempts: 3,
		retryInitial: time.Millisecond, retryMaximum: time.Millisecond,
	}
	err := applyWithRetry(context.Background(), applier, &seamsnowflake.SinkLease{OwnerID: "owner", Epoch: 1}, &kafka.Transaction{}, cfg)
	if !errors.Is(err, permanent) || applier.calls != 1 {
		t.Fatalf("error = %v, calls = %d", err, applier.calls)
	}
}

func TestLoadConfigRejectsMalformedAndUnsafeValues(t *testing.T) {
	baseEnvironment(t)
	tests := []struct {
		name  string
		env   string
		value string
		match string
	}{
		{name: "offset", env: "SEAM_SNOWFLAKE_INITIAL_OFFSET", value: "-2", match: "must be -1"},
		{name: "poll records", env: "SEAM_SNOWFLAKE_MAX_POLL_RECORDS", value: "0", match: "positive integer"},
		{name: "apply timeout", env: "SEAM_SNOWFLAKE_APPLY_TIMEOUT", value: "forever", match: "positive duration"},
		{name: "retry attempts", env: "SEAM_SNOWFLAKE_RETRY_ATTEMPTS", value: "zero", match: "invalid integer"},
		{name: "retry order", env: "SEAM_SNOWFLAKE_RETRY_MAX", value: "1ms", match: "at least"},
		{name: "lease duration", env: "SEAM_SNOWFLAKE_SINK_LEASE_DURATION", value: "0s", match: "positive duration"},
		{name: "lease heartbeat", env: "SEAM_SNOWFLAKE_SINK_HEARTBEAT_INTERVAL", value: "30s", match: "must be shorter"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			baseEnvironment(t)
			t.Setenv(test.env, test.value)
			if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("error = %v, want substring %q", err, test.match)
			}
		})
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	baseEnvironment(t)
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.initialOffset != -1 || cfg.maxPollRecords != 100 || cfg.applyTimeout != 2*time.Minute || cfg.leaseDuration != 30*time.Second || cfg.leaseHeartbeat != 10*time.Second || len(cfg.kafkaBrokers) != 1 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func baseEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"SNOWFLAKE_DSN", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA", "SNOWFLAKE_INTERNAL_SCHEMA", "SNOWFLAKE_LIVE_TABLE",
		"SEAM_STREAM_ID", "SOURCE_SQL_DSN", "SEAM_SOURCE_TABLE", "KAFKA_BROKERS", "KAFKA_TOPIC",
		"SEAM_SNOWFLAKE_INITIAL_OFFSET", "SEAM_SNOWFLAKE_MAX_POLL_RECORDS", "SEAM_SNOWFLAKE_APPLY_TIMEOUT",
		"SEAM_SNOWFLAKE_RETRY_ATTEMPTS", "SEAM_SNOWFLAKE_RETRY_INITIAL", "SEAM_SNOWFLAKE_RETRY_MAX",
		"SEAM_SNOWFLAKE_SINK_OWNER", "SEAM_SNOWFLAKE_SINK_LEASE_DURATION", "SEAM_SNOWFLAKE_SINK_HEARTBEAT_INTERVAL",
	} {
		t.Setenv(name, "")
	}
	t.Setenv("SNOWFLAKE_DSN", "user:password@example/account/database/schema")
	t.Setenv("SNOWFLAKE_DATABASE", "DATABASE")
}

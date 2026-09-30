package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"example.com/seam/internal/kafka"
	"example.com/seam/internal/marker"
	"example.com/seam/internal/schema"
	seamsnowflake "example.com/seam/internal/snowflake"
	"example.com/seam/internal/snowpromotion"
	_ "github.com/snowflakedb/gosnowflake/v2"
)

type config struct {
	mode            string
	snowflakeDSN    string
	database        string
	dataSchema      string
	internalSchema  string
	liveTable       string
	streamID        string
	sourceDSN       string
	sourceTable     string
	kafkaBrokers    []string
	kafkaTopic      string
	jobID           string
	attempt         string
	validationTable string
	poll            time.Duration
	maxWritePause   time.Duration
	lockTimeout     time.Duration
	maxDeltaKeys    int
	maxPollRecords  int
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg, err := loadConfig(os.Args[1:])
	if err != nil {
		log.Fatalf("seam-snowflake-promote: %v", err)
	}
	if err := run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("seam-snowflake-promote: %v", err)
	}
}

func run(ctx context.Context, cfg config) error {
	descriptor, err := schema.Load(ctx, cfg.sourceDSN, "public", cfg.sourceTable)
	if err != nil {
		return err
	}
	topicID, err := kafka.TopicIdentity(ctx, cfg.kafkaBrokers, cfg.kafkaTopic)
	if err != nil {
		return err
	}
	earliest, err := kafka.EarliestOffset(ctx, cfg.kafkaBrokers, cfg.kafkaTopic)
	if err != nil {
		return err
	}
	db, err := sql.Open("snowflake", cfg.snowflakeDSN)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return err
	}
	store, err := seamsnowflake.NewStore(db, seamsnowflake.Config{
		Database: cfg.database, Schema: cfg.dataSchema, InternalSchema: cfg.internalSchema,
		LiveTable: cfg.liveTable, StreamID: cfg.streamID, TopicID: topicID, Partition: 0,
	}, descriptor)
	if err != nil {
		return err
	}
	if err := store.EnsureObjects(ctx, earliest); err != nil {
		return err
	}
	markers := marker.NewStore(cfg.sourceDSN)
	if err := markers.EnsureTable(ctx); err != nil {
		return err
	}
	manager, err := snowpromotion.New(snowpromotion.Config{
		SourceDSN: cfg.sourceDSN, JobID: cfg.jobID, Attempt: cfg.attempt, ValidationTable: cfg.validationTable,
		KafkaBrokers: cfg.kafkaBrokers, KafkaTopic: cfg.kafkaTopic, KafkaTopicID: topicID,
		PollInterval: cfg.poll, MaxWritePause: cfg.maxWritePause, LockTimeout: cfg.lockTimeout,
		MaxDeltaKeys: cfg.maxDeltaKeys, MaxPollRecords: cfg.maxPollRecords,
	}, store, markers, descriptor)
	if err != nil {
		return err
	}
	switch cfg.mode {
	case "validate":
		if err := manager.Validate(ctx); err != nil {
			return err
		}
		log.Printf("Snowflake shadow passed exact stable-snapshot validation job=%s", cfg.jobID)
	case "promote":
		if err := manager.Promote(ctx); err != nil {
			return err
		}
		log.Printf("Snowflake shadow promoted through stable view job=%s live=%s", cfg.jobID, cfg.liveTable)
	default:
		return fmt.Errorf("unsupported mode %q", cfg.mode)
	}
	return nil
}

func loadConfig(args []string) (config, error) {
	if len(args) != 1 || (args[0] != "validate" && args[0] != "promote") {
		return config{}, fmt.Errorf("usage: seam-snowflake-promote validate|promote")
	}
	shadow := envOrDefault("SNOWFLAKE_SHADOW_TABLE", "ACCOUNTS_SHADOW")
	cfg := config{
		mode: args[0], snowflakeDSN: strings.TrimSpace(os.Getenv("SNOWFLAKE_DSN")),
		database:   strings.TrimSpace(os.Getenv("SNOWFLAKE_DATABASE")),
		dataSchema: envOrDefault("SNOWFLAKE_SCHEMA", "SEAM"), internalSchema: envOrDefault("SNOWFLAKE_INTERNAL_SCHEMA", "SEAM_INTERNAL"),
		liveTable: envOrDefault("SNOWFLAKE_LIVE_TABLE", "ACCOUNTS"), streamID: envOrDefault("SEAM_STREAM_ID", "seam-accounts"),
		sourceDSN:    envOrDefault("SOURCE_SQL_DSN", "postgres://postgres:postgres@localhost:5433/source?sslmode=disable"),
		sourceTable:  envOrDefault("SEAM_SOURCE_TABLE", "accounts"),
		kafkaBrokers: splitAndTrim(envOrDefault("KAFKA_BROKERS", "localhost:9092")), kafkaTopic: envOrDefault("KAFKA_TOPIC", "seam.accounts"),
		jobID: envOrDefault("SEAM_BACKFILL_JOB_ID", "snowflake-backfill"), attempt: envOrDefault("SEAM_BACKFILL_ATTEMPT", "attempt-1"),
		validationTable: envOrDefault("SNOWFLAKE_VALIDATION_TABLE", shadow+"_VALIDATION"),
	}
	if cfg.snowflakeDSN == "" || cfg.database == "" {
		return config{}, fmt.Errorf("SNOWFLAKE_DSN and SNOWFLAKE_DATABASE are required")
	}
	if len(cfg.kafkaBrokers) == 0 {
		return config{}, fmt.Errorf("KAFKA_BROKERS must contain at least one broker")
	}
	var err error
	if cfg.poll, err = positiveDurationEnv("SEAM_SNOWFLAKE_MARKER_POLL", 250*time.Millisecond); err != nil {
		return config{}, err
	}
	if cfg.maxWritePause, err = positiveDurationEnv("SEAM_MAX_WRITE_PAUSE", 30*time.Second); err != nil {
		return config{}, err
	}
	if cfg.lockTimeout, err = positiveDurationEnv("SEAM_SOURCE_LOCK_TIMEOUT", 5*time.Second); err != nil {
		return config{}, err
	}
	if cfg.maxDeltaKeys, err = positiveIntEnv("SEAM_MAX_DELTA_KEYS", 100_000); err != nil {
		return config{}, err
	}
	if cfg.maxPollRecords, err = positiveIntEnv("SEAM_SNOWFLAKE_MAX_POLL_RECORDS", 100); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func splitAndTrim(value string) []string {
	var result []string
	for _, part := range strings.Split(value, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func positiveDurationEnv(name string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s: invalid positive duration %q", name, value)
	}
	return parsed, nil
}

func positiveIntEnv(name string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s: invalid positive integer %q", name, value)
	}
	return parsed, nil
}

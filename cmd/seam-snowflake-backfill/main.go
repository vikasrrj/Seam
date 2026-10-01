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
	"example.com/seam/internal/scan"
	seamsnowflake "example.com/seam/internal/snowflake"
	"example.com/seam/internal/snowreconcile"
	_ "github.com/snowflakedb/gosnowflake/v2"
)

type config struct {
	snowflakeDSN   string
	database       string
	dataSchema     string
	internalSchema string
	liveTable      string
	streamID       string
	sourceDSN      string
	sourceTable    string
	kafkaBrokers   []string
	kafkaTopic     string
	jobID          string
	attempt        string
	shadowTable    string
	chunkSize      int
	workers        int
	workerID       string
	lease          time.Duration
	heartbeat      time.Duration
	poll           time.Duration
	maxRows        int
	maxBytes       int64
	snapshotLoader seamsnowflake.SnapshotLoader
	bulkTempDir    string
	uploadParallel int
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("seam-snowflake-backfill: %v", err)
	}
	if err := run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("seam-snowflake-backfill: %v", err)
	}
}

func run(ctx context.Context, cfg config) error {
	scanner, err := scan.NewChunkReaderFor(ctx, cfg.sourceDSN, cfg.sourceTable)
	if err != nil {
		return err
	}
	if err := scanner.SetLimits(cfg.maxRows, cfg.maxBytes); err != nil {
		return err
	}
	topicID, err := kafka.TopicIdentity(ctx, cfg.kafkaBrokers, cfg.kafkaTopic)
	if err != nil {
		return fmt.Errorf("validate Kafka topic: %w", err)
	}
	earliest, err := kafka.EarliestOffset(ctx, cfg.kafkaBrokers, cfg.kafkaTopic)
	if err != nil {
		return err
	}
	db, err := sql.Open("snowflake", cfg.snowflakeDSN)
	if err != nil {
		return fmt.Errorf("open Snowflake: %w", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("connect to Snowflake: %w", err)
	}
	store, err := seamsnowflake.NewStore(db, seamsnowflake.Config{
		Database: cfg.database, Schema: cfg.dataSchema, InternalSchema: cfg.internalSchema,
		LiveTable: cfg.liveTable, StreamID: cfg.streamID, TopicID: topicID, Partition: 0,
		SnapshotLoader: cfg.snapshotLoader, BulkTempDir: cfg.bulkTempDir, UploadParallel: cfg.uploadParallel,
	}, scanner.Schema())
	if err != nil {
		return err
	}
	if err := store.EnsureObjects(ctx, earliest); err != nil {
		return err
	}
	next, err := store.NextOffset(ctx)
	if err != nil {
		return err
	}
	if next < earliest {
		return fmt.Errorf("Snowflake frontier %d is before Kafka earliest retained offset %d; retained CDC cannot protect a new backfill", next, earliest)
	}
	markers := marker.NewStore(cfg.sourceDSN)
	if err := markers.EnsureTable(ctx); err != nil {
		return err
	}
	coordinator, err := snowreconcile.New(snowreconcile.Config{
		JobID: cfg.jobID, Attempt: cfg.attempt, ShadowTable: cfg.shadowTable,
		ChunkSize: cfg.chunkSize, Workers: cfg.workers, WorkerID: cfg.workerID,
		Lease: cfg.lease, Heartbeat: cfg.heartbeat, Poll: cfg.poll,
	}, store, scanner, markers)
	if err != nil {
		return err
	}
	log.Printf("Snowflake online backfill started job=%s attempt=%s shadow=%s workers=%d chunk_size=%d frontier=%d",
		cfg.jobID, cfg.attempt, cfg.shadowTable, cfg.workers, cfg.chunkSize, next)
	if err := coordinator.Run(ctx); err != nil {
		return err
	}
	log.Printf("Snowflake online backfill reached ready_to_verify job=%s shadow=%s", cfg.jobID, cfg.shadowTable)
	return nil
}

func loadConfig() (config, error) {
	host, _ := os.Hostname()
	if host == "" {
		host = "unknown"
	}
	cfg := config{
		snowflakeDSN:   strings.TrimSpace(os.Getenv("SNOWFLAKE_DSN")),
		database:       strings.TrimSpace(os.Getenv("SNOWFLAKE_DATABASE")),
		dataSchema:     envOrDefault("SNOWFLAKE_SCHEMA", "SEAM"),
		internalSchema: envOrDefault("SNOWFLAKE_INTERNAL_SCHEMA", "SEAM_INTERNAL"),
		liveTable:      envOrDefault("SNOWFLAKE_LIVE_TABLE", "ACCOUNTS"),
		streamID:       envOrDefault("SEAM_STREAM_ID", "seam-accounts"),
		sourceDSN:      envOrDefault("SOURCE_SQL_DSN", "postgres://postgres:postgres@localhost:5433/source?sslmode=disable"),
		sourceTable:    envOrDefault("SEAM_SOURCE_TABLE", "accounts"),
		kafkaBrokers:   splitAndTrim(envOrDefault("KAFKA_BROKERS", "localhost:9092")),
		kafkaTopic:     envOrDefault("KAFKA_TOPIC", "seam.accounts"),
		jobID:          envOrDefault("SEAM_BACKFILL_JOB_ID", "snowflake-backfill"),
		attempt:        envOrDefault("SEAM_BACKFILL_ATTEMPT", "attempt-1"),
		shadowTable:    envOrDefault("SNOWFLAKE_SHADOW_TABLE", "ACCOUNTS_SHADOW"),
		workerID:       envOrDefault("SEAM_WORKER_ID", fmt.Sprintf("%s-%d", host, os.Getpid())),
		snapshotLoader: seamsnowflake.SnapshotLoader(envOrDefault("SEAM_SNOWFLAKE_SNAPSHOT_LOADER", string(seamsnowflake.SnapshotLoaderBulk))),
		bulkTempDir:    envOrDefault("SEAM_SNOWFLAKE_BULK_TEMP_DIR", os.TempDir()),
	}
	if cfg.snowflakeDSN == "" || cfg.database == "" {
		return config{}, fmt.Errorf("SNOWFLAKE_DSN and SNOWFLAKE_DATABASE are required")
	}
	if len(cfg.kafkaBrokers) == 0 {
		return config{}, fmt.Errorf("KAFKA_BROKERS must contain at least one broker")
	}
	if cfg.snapshotLoader != seamsnowflake.SnapshotLoaderBulk && cfg.snapshotLoader != seamsnowflake.SnapshotLoaderSQL {
		return config{}, fmt.Errorf("SEAM_SNOWFLAKE_SNAPSHOT_LOADER must be %q or %q", seamsnowflake.SnapshotLoaderBulk, seamsnowflake.SnapshotLoaderSQL)
	}
	var err error
	if cfg.chunkSize, err = positiveIntEnv("SEAM_CHUNK_SIZE", 10_000); err != nil {
		return config{}, err
	}
	if cfg.workers, err = positiveIntEnv("SEAM_WORKERS", 4); err != nil {
		return config{}, err
	}
	if cfg.maxRows, err = positiveIntEnv("SEAM_MAX_IN_MEMORY_CANDIDATES", 1_000_000); err != nil {
		return config{}, err
	}
	if cfg.maxBytes, err = positiveInt64Env("SEAM_MAX_CANDIDATE_BYTES", 256<<20); err != nil {
		return config{}, err
	}
	if cfg.uploadParallel, err = positiveIntEnv("SEAM_SNOWFLAKE_UPLOAD_PARALLEL", 4); err != nil {
		return config{}, err
	}
	if cfg.uploadParallel > 99 {
		return config{}, fmt.Errorf("SEAM_SNOWFLAKE_UPLOAD_PARALLEL must not exceed 99")
	}
	if cfg.chunkSize > cfg.maxRows {
		return config{}, fmt.Errorf("SEAM_CHUNK_SIZE must not exceed SEAM_MAX_IN_MEMORY_CANDIDATES")
	}
	if cfg.lease, err = positiveDurationEnv("SEAM_LEASE_DURATION", 2*time.Minute); err != nil {
		return config{}, err
	}
	if cfg.heartbeat, err = positiveDurationEnv("SEAM_HEARTBEAT_INTERVAL", cfg.lease/3); err != nil {
		return config{}, err
	}
	if cfg.heartbeat >= cfg.lease {
		return config{}, fmt.Errorf("SEAM_HEARTBEAT_INTERVAL must be shorter than SEAM_LEASE_DURATION")
	}
	if cfg.poll, err = positiveDurationEnv("SEAM_SNOWFLAKE_MARKER_POLL", 250*time.Millisecond); err != nil {
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

func positiveInt64Env(name string, fallback int64) (int64, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s: invalid positive integer %q", name, value)
	}
	return parsed, nil
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

package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"example.com/seam/internal/capture"
	"example.com/seam/internal/kafka"
	"example.com/seam/internal/model"
	"example.com/seam/internal/retry"
	"example.com/seam/internal/schema"
	seamsnowflake "example.com/seam/internal/snowflake"
	_ "github.com/snowflakedb/gosnowflake/v2"
)

type config struct {
	snowflakeDSN     string
	database         string
	dataSchema       string
	internalSchema   string
	liveTable        string
	streamID         string
	sourceDSN        string
	sourceTable      string
	kafkaBrokers     []string
	kafkaTopic       string
	initialOffset    int64
	maxPollRecords   int
	applyTimeout     time.Duration
	retryMaxAttempts int
	retryInitial     time.Duration
	retryMaximum     time.Duration
	ownerID          string
	leaseDuration    time.Duration
	leaseHeartbeat   time.Duration
}

type transactionApplier interface {
	ApplyTransaction(context.Context, *seamsnowflake.SinkLease, *kafka.Transaction) error
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("seam-snowflake-sink: %v", err)
	}
	if err := run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("seam-snowflake-sink: %v", err)
	}
}

func run(ctx context.Context, cfg config) (retErr error) {
	descriptor, err := schema.Load(ctx, cfg.sourceDSN, "public", cfg.sourceTable)
	if err != nil {
		return fmt.Errorf("load source schema: %w", err)
	}
	topicID, err := kafka.TopicIdentity(ctx, cfg.kafkaBrokers, cfg.kafkaTopic)
	if err != nil {
		return fmt.Errorf("validate Kafka topic: %w", err)
	}
	earliest, err := kafka.EarliestOffset(ctx, cfg.kafkaBrokers, cfg.kafkaTopic)
	if err != nil {
		return err
	}
	initial := cfg.initialOffset
	if initial < 0 {
		initial = earliest
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
	}, descriptor)
	if err != nil {
		return err
	}
	if err := store.EnsureObjects(ctx, initial); err != nil {
		return err
	}
	lease, err := store.AcquireSinkLease(ctx, cfg.ownerID, cfg.leaseDuration)
	if err != nil {
		return err
	}
	runCtx, cancelRun := context.WithCancelCause(ctx)
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		ticker := time.NewTicker(cfg.leaseHeartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				renewCtx, cancel := context.WithTimeout(runCtx, cfg.leaseHeartbeat)
				err := store.RenewSinkLease(renewCtx, lease, cfg.leaseDuration)
				cancel()
				if err != nil {
					cancelRun(fmt.Errorf("Snowflake sink lease renewal failed: %w", err))
					return
				}
			}
		}
	}()
	defer func() {
		cancelRun(context.Canceled)
		<-renewDone
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := store.ReleaseSinkLease(releaseCtx, lease); err != nil && (retErr == nil || errors.Is(retErr, context.Canceled)) {
			retErr = err
		}
	}()
	next, err := store.NextOffset(ctx)
	if err != nil {
		return err
	}
	if next < earliest {
		return fmt.Errorf("Snowflake frontier %d is before Kafka earliest retained offset %d; history was lost and a new generation is required", next, earliest)
	}
	consumer, err := kafka.NewConsumer(cfg.kafkaBrokers, cfg.kafkaTopic, next, decodeChange,
		kafka.WithExpectedTopicID(topicID), kafka.WithMaxPollRecords(cfg.maxPollRecords))
	if err != nil {
		return err
	}
	defer consumer.Close()
	log.Printf("Snowflake CDC sink started stream=%s owner=%s epoch=%d topic=%s topic_id=%s offset=%d target=%s.%s.%s schema_epoch=%s",
		cfg.streamID, lease.OwnerID, lease.Epoch, cfg.kafkaTopic, topicID, next, cfg.database, cfg.dataSchema, cfg.liveTable, descriptor.Fingerprint)
	for {
		transactions, err := consumer.PollTransactions(runCtx)
		if err != nil {
			if cause := context.Cause(runCtx); cause != nil && !errors.Is(cause, context.Canceled) {
				return cause
			}
			return err
		}
		if transactions == nil && runCtx.Err() != nil {
			if cause := context.Cause(runCtx); cause != nil {
				return cause
			}
			return runCtx.Err()
		}
		for _, transaction := range transactions {
			err := applyWithRetry(runCtx, store, lease, transaction, cfg)
			transaction.Close()
			if err != nil {
				return fmt.Errorf("apply source transaction %s at Kafka offsets %d..%d: %w", transaction.Source, transaction.FirstOffset, transaction.FinalOffset, err)
			}
		}
	}
}

func applyWithRetry(ctx context.Context, applier transactionApplier, lease *seamsnowflake.SinkLease, transaction *kafka.Transaction, cfg config) error {
	retryCfg := retry.Config{MaxAttempts: cfg.retryMaxAttempts, Initial: cfg.retryInitial, Max: cfg.retryMaximum, Multiplier: 2}
	return retry.DoVoid(ctx, retryCfg, func() error {
		attemptCtx, cancel := context.WithTimeout(ctx, cfg.applyTimeout)
		defer cancel()
		return applier.ApplyTransaction(attemptCtx, lease, transaction)
	}, seamsnowflake.IsRetryable)
}

func decodeChange(data []byte) (model.Change, error) {
	var codec capture.JSONCodec
	return codec.Decode(data)
}

func loadConfig() (config, error) {
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
		ownerID:        envOrDefault("SEAM_SNOWFLAKE_SINK_OWNER", processOwnerID()),
	}
	if cfg.snowflakeDSN == "" || cfg.database == "" {
		return config{}, fmt.Errorf("SNOWFLAKE_DSN and SNOWFLAKE_DATABASE are required")
	}
	if len(cfg.kafkaBrokers) == 0 {
		return config{}, fmt.Errorf("KAFKA_BROKERS must contain at least one broker")
	}
	var err error
	if cfg.initialOffset, err = int64Env("SEAM_SNOWFLAKE_INITIAL_OFFSET", -1); err != nil {
		return config{}, err
	}
	if cfg.initialOffset < -1 {
		return config{}, fmt.Errorf("SEAM_SNOWFLAKE_INITIAL_OFFSET must be -1 or non-negative")
	}
	if cfg.maxPollRecords, err = positiveIntEnv("SEAM_SNOWFLAKE_MAX_POLL_RECORDS", 100); err != nil {
		return config{}, err
	}
	if cfg.applyTimeout, err = positiveDurationEnv("SEAM_SNOWFLAKE_APPLY_TIMEOUT", 2*time.Minute); err != nil {
		return config{}, err
	}
	if cfg.retryMaxAttempts, err = positiveIntEnv("SEAM_SNOWFLAKE_RETRY_ATTEMPTS", 5); err != nil {
		return config{}, err
	}
	if cfg.retryInitial, err = positiveDurationEnv("SEAM_SNOWFLAKE_RETRY_INITIAL", 250*time.Millisecond); err != nil {
		return config{}, err
	}
	if cfg.retryMaximum, err = positiveDurationEnv("SEAM_SNOWFLAKE_RETRY_MAX", 5*time.Second); err != nil {
		return config{}, err
	}
	if cfg.retryMaximum < cfg.retryInitial {
		return config{}, fmt.Errorf("SEAM_SNOWFLAKE_RETRY_MAX must be at least SEAM_SNOWFLAKE_RETRY_INITIAL")
	}
	if cfg.leaseDuration, err = positiveDurationEnv("SEAM_SNOWFLAKE_SINK_LEASE_DURATION", 30*time.Second); err != nil {
		return config{}, err
	}
	if cfg.leaseHeartbeat, err = positiveDurationEnv("SEAM_SNOWFLAKE_SINK_HEARTBEAT_INTERVAL", cfg.leaseDuration/3); err != nil {
		return config{}, err
	}
	if cfg.leaseHeartbeat >= cfg.leaseDuration {
		return config{}, fmt.Errorf("SEAM_SNOWFLAKE_SINK_HEARTBEAT_INTERVAL must be shorter than SEAM_SNOWFLAKE_SINK_LEASE_DURATION")
	}
	return cfg, nil
}

func processOwnerID() string {
	host, _ := os.Hostname()
	if host == "" {
		host = "unknown"
	}
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		return fmt.Sprintf("%s-%d-%d", host, os.Getpid(), time.Now().UnixNano())
	}
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(random))
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

func int64Env(name string, fallback int64) (int64, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid integer %q", name, value)
	}
	return parsed, nil
}

func positiveIntEnv(name string, fallback int) (int, error) {
	value, err := int64Env(name, int64(fallback))
	if err != nil {
		return 0, err
	}
	if value <= 0 || int64(int(value)) != value {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return int(value), nil
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

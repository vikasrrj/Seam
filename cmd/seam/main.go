package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"example.com/seam/internal/adaptive"
	"example.com/seam/internal/capture"
	"example.com/seam/internal/checkpoint"
	"example.com/seam/internal/kafka"
	"example.com/seam/internal/marker"
	"example.com/seam/internal/model"
	"example.com/seam/internal/reconcile"
	"example.com/seam/internal/recovery"
	resourcecontrol "example.com/seam/internal/resource"
	"example.com/seam/internal/scan"
	"example.com/seam/internal/schema"
	"example.com/seam/internal/server"
	"example.com/seam/internal/sink"
	"example.com/seam/internal/telemetry"
)

type config struct {
	JobID                 string
	SourceDSN             string
	SourceReplDSN         string
	SourceSlot            string
	SourcePublication     string
	SourceTable           string
	SourceKey             string
	DestDSN               string
	DestTable             string
	KafkaBrokers          []string
	KafkaTopic            string
	ChunkSize             int
	WorkerID              string
	LeaseDuration         time.Duration
	MaxInMemoryCandidates int
	MaxCandidateBytes     int64
	MaxRecordsPerBatch    int
	Workers               int
	HeartbeatInterval     time.Duration
	AdaptiveChunking      bool
	TargetChunkDuration   time.Duration
	ChunkSizeMin          int
	ChunkSizeMax          int
	StartFresh            bool
	MaxSourceScans        int
	MaxDestinationTx      int
	MaxCDCLagRecords      int64
	ResourcePollInterval  time.Duration
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("seam: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("seam: %v", err)
	}
}

func run(ctx context.Context, cfg config) (retErr error) {
	if err := cfg.validate(); err != nil {
		return err
	}
	// The durable source descriptor is the single source of truth for the
	// primary key, column list, and schema epoch. Loading it here (and failing
	// closed on unsupported types, a missing BIGINT primary key, or a missing
	// REPLICA IDENTITY FULL) proves every downstream reader agrees before any
	// chunk or job record is created.
	sourceSchema, err := schema.Load(ctx, cfg.SourceDSN, "public", cfg.SourceTable)
	if err != nil {
		return fmt.Errorf("load source schema for public.%s: %w", cfg.SourceTable, err)
	}
	if cfg.SourceKey != "" && cfg.SourceKey != sourceSchema.PKColumn().Name {
		return fmt.Errorf("SEAM_SOURCE_KEY %q does not match the source schema primary key %q", cfg.SourceKey, sourceSchema.PKColumn().Name)
	}
	targetSink, err := sink.NewMutatorFor(cfg.DestTable, sourceSchema)
	if err != nil {
		return err
	}
	cpStore := checkpoint.NewStore(cfg.DestDSN)
	if err := cpStore.EnsureTables(ctx); err != nil {
		return fmt.Errorf("ensure checkpoint tables: %w", err)
	}
	markerStore := marker.NewStore(cfg.SourceDSN)
	if err := markerStore.EnsureTable(ctx); err != nil {
		return fmt.Errorf("ensure marker table: %w", err)
	}

	var cp *model.Checkpoint
	var leader *leadershipGuard
	acquireLeadership := func(candidate *model.Checkpoint) (*model.Checkpoint, error) {
		owned, err := cpStore.AcquireLeadership(ctx, candidate.JobID, leadershipOwnerID(cfg.WorkerID), cfg.LeaseDuration)
		if err != nil {
			return nil, err
		}
		leader = startLeadershipGuard(ctx, cpStore, owned, cfg.LeaseDuration, cfg.HeartbeatInterval)
		ctx = leader.Context()
		return owned, nil
	}
	defer func() {
		if leader == nil {
			return
		}
		if err := leader.Stop(); err != nil && (retErr == nil || errors.Is(retErr, context.Canceled)) {
			retErr = err
		}
	}()
	var sourceSystemID string
	var kafkaTopicID string
	if cfg.StartFresh {
		if err := cpStore.EnsureDestinationTableFor(ctx, cfg.DestTable); err != nil {
			return fmt.Errorf("ensure destination table: %w", err)
		}
		if err := requireEmptyDestination(ctx, cpStore, cfg.DestTable); err != nil {
			return err
		}
		if err := capture.ValidateSourceStart(ctx, cfg.SourceDSN, cfg.SourceSlot, cfg.SourcePublication, cfg.SourceTable); err != nil {
			return err
		}
		systemID, err := capture.SourceSystemID(ctx, cfg.SourceDSN)
		if err != nil {
			return fmt.Errorf("read source system identity: %w", err)
		}
		sourceSystemID = systemID
		topicID, err := kafka.TopicIdentity(ctx, cfg.KafkaBrokers, cfg.KafkaTopic)
		if err != nil {
			return fmt.Errorf("validate Kafka topic: %w", err)
		}
		kafkaTopicID = topicID
		brokerEnd, err := kafka.EndOffset(ctx, cfg.KafkaBrokers, cfg.KafkaTopic)
		if err != nil {
			return fmt.Errorf("get Kafka offset before source barrier: %w", err)
		}
		barrierID, err := markerStore.WriteBarrier(ctx, cfg.JobID, "gen:0:attempt:0")
		if err != nil {
			return fmt.Errorf("write initial source barrier: %w", err)
		}
		barrierCtx, cancelBarrier := context.WithTimeout(ctx, 2*time.Minute)
		startOffset, err := kafka.WaitForBarrier(barrierCtx, cfg.KafkaBrokers, cfg.KafkaTopic, barrierID, brokerEnd, decodeChange)
		cancelBarrier()
		if err != nil {
			return fmt.Errorf("wait for source barrier in Kafka: %w", err)
		}
		if err := cpStore.EnsureTables(ctx); err != nil {
			return err
		}
		jobCfg := model.JobConfig{
			JobID:                 cfg.JobID,
			SourceDSN:             cfg.SourceDSN,
			SourceReplDSN:         cfg.SourceReplDSN,
			SourceSlot:            cfg.SourceSlot,
			SourcePublication:     cfg.SourcePublication,
			SourceSystemID:        systemID,
			DestDSN:               cfg.DestDSN,
			DestTable:             cfg.DestTable,
			KafkaBrokers:          cfg.KafkaBrokers,
			KafkaTopic:            cfg.KafkaTopic,
			KafkaTopicID:          topicID,
			ChunkSize:             cfg.ChunkSize,
			WorkerID:              cfg.WorkerID,
			LeaseDuration:         cfg.LeaseDuration,
			MaxInMemoryCandidates: cfg.MaxInMemoryCandidates,
			MaxRecordsPerBatch:    cfg.MaxRecordsPerBatch,
		}
		upperBoundReader, err := scan.NewChunkReaderFor(ctx, cfg.SourceDSN, cfg.SourceTable)
		if err != nil {
			return fmt.Errorf("configure source scan: %w", err)
		}
		upperBound, err := upperBoundReader.UpperBound(ctx)
		if err != nil {
			return fmt.Errorf("determine scan upper bound: %w", err)
		}
		cp, err = cpStore.CreateJobAt(ctx, jobCfg, sourceSchema, upperBound, startOffset)
		if err != nil {
			return fmt.Errorf("create job: %w", err)
		}
		cp, err = acquireLeadership(cp)
		if err != nil {
			return fmt.Errorf("acquire job leadership: %w", err)
		}
		// Adaptive chunking resizes chunks from measured latency and currently
		// drives the scanner directly; pre-discovering fixed-size chunks would
		// contradict the adaptive size, so they are skipped in this mode.
		if !cfg.AdaptiveChunking {
			if err := cpStore.DiscoverAndCreateChunksForOwned(ctx, cp, cfg.SourceDSN, sourceSchema, upperBound, cfg.ChunkSize); err != nil {
				return fmt.Errorf("discover chunks: %w", err)
			}
		}
	} else {
		if err := capture.ValidateSourceStart(ctx, cfg.SourceDSN, cfg.SourceSlot, cfg.SourcePublication, cfg.SourceTable); err != nil {
			return err
		}
		record, err := cpStore.LoadJobRecord(ctx, cfg.JobID)
		if err != nil {
			return fmt.Errorf("load job record: %w", err)
		}
		if record.Config.KafkaTopic != cfg.KafkaTopic {
			return fmt.Errorf("Kafka topic changed from %q to %q", record.Config.KafkaTopic, cfg.KafkaTopic)
		}
		if record.Config.SourceSlot != cfg.SourceSlot || record.Config.SourcePublication != cfg.SourcePublication {
			return fmt.Errorf("source slot or publication changed; resnapshot required")
		}
		if record.Config.DestTable != cfg.DestTable {
			return fmt.Errorf("destination table changed from %q to %q", record.Config.DestTable, cfg.DestTable)
		}
		if record.SourceSchema == nil {
			return fmt.Errorf("job %q has no durable source schema descriptor; resnapshot required", cfg.JobID)
		}
		if sourceSchema.Fingerprint != record.SourceSchema.Fingerprint {
			return fmt.Errorf("source schema epoch drifted: job pins %s, source currently reports %s; resnapshot required", record.SourceSchema.Fingerprint, sourceSchema.Fingerprint)
		}
		if record.SourceSchemaFingerprint != "" && record.SourceSchemaFingerprint != sourceSchema.Fingerprint {
			return fmt.Errorf("source schema fingerprint drifted from its pinned epoch %s", record.SourceSchemaFingerprint)
		}
		systemID, err := capture.SourceSystemID(ctx, cfg.SourceDSN)
		if err != nil {
			return fmt.Errorf("read source system identity: %w", err)
		}
		if record.Config.SourceSystemID == "" || record.Config.SourceSystemID != systemID {
			return fmt.Errorf("source PostgreSQL system identifier changed or was never recorded; resnapshot required")
		}
		sourceSystemID = systemID
		topicID, err := kafka.TopicIdentity(ctx, cfg.KafkaBrokers, cfg.KafkaTopic)
		if err != nil {
			return fmt.Errorf("validate Kafka topic on recovery: %w", err)
		}
		if record.Config.KafkaTopicID == "" || record.Config.KafkaTopicID != topicID {
			return fmt.Errorf("Kafka topic identity changed or was never recorded; resnapshot required")
		}
		kafkaTopicID = topicID
		// Finish an interrupted discovery before recovery clones unfinished
		// chunks into a new attempt. The persisted cursor prevents re-scanning
		// or silently dropping the undiscovered suffix.
		prior, err := cpStore.LoadCheckpoint(ctx, cfg.JobID)
		if err != nil {
			return fmt.Errorf("load checkpoint before discovery: %w", err)
		}
		if prior == nil {
			return fmt.Errorf("job %q has no checkpoint", cfg.JobID)
		}
		prior, err = acquireLeadership(prior)
		if err != nil {
			return fmt.Errorf("acquire job leadership: %w", err)
		}
		earliest, err := kafka.EarliestOffset(ctx, cfg.KafkaBrokers, cfg.KafkaTopic)
		if err != nil {
			return fmt.Errorf("check Kafka retention: %w", err)
		}
		if prior.NextKafkaOffset < earliest {
			return fmt.Errorf("checkpoint next_kafka_offset %d is before earliest retained offset %d; history lost, reset required", prior.NextKafkaOffset, earliest)
		}
		if !cfg.AdaptiveChunking {
			if err := cpStore.DiscoverAndCreateChunksForOwned(ctx, prior, cfg.SourceDSN, sourceSchema, prior.ScanUpperBound, cfg.ChunkSize); err != nil {
				return fmt.Errorf("resume discovery: %w", err)
			}
		}
		result, err := recovery.Recover(ctx, cpStore, cfg.JobID, cfg.SourceDSN)
		if err != nil {
			return fmt.Errorf("recover: %w", err)
		}
		cp = result.Checkpoint
	}

	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cp.NextKafkaOffset, decodeChange,
		kafka.WithMaxPollRecords(cfg.MaxRecordsPerBatch), kafka.WithExpectedTopicID(kafkaTopicID))
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}
	defer consumer.Close()

	paginator, err := scan.NewChunkReaderFor(ctx, cfg.SourceDSN, cfg.SourceTable)
	if err != nil {
		return fmt.Errorf("configure source scan: %w", err)
	}
	if err := paginator.SetLimits(cfg.MaxInMemoryCandidates, cfg.MaxCandidateBytes/int64(cfg.Workers)); err != nil {
		return err
	}

	metrics := telemetry.NewMetrics()
	permitPoll := 50 * time.Millisecond
	scanPermits, err := resourcecontrol.NewPostgresAdvisoryPool(cfg.SourceDSN, resourcecontrol.SourceScanClass, cfg.MaxSourceScans, permitPoll)
	if err != nil {
		return err
	}
	defer scanPermits.Close()
	destPermits, err := resourcecontrol.NewPostgresAdvisoryPool(cfg.DestDSN, resourcecontrol.DestinationTxClass, cfg.MaxDestinationTx, permitPoll)
	if err != nil {
		return err
	}
	defer destPermits.Close()
	resources, err := resourcecontrol.New(resourcecontrol.Config{
		MaxSourceScans: cfg.MaxSourceScans,
		MaxDestTx:      cfg.MaxDestinationTx,
		MaxCDCLag:      cfg.MaxCDCLagRecords,
		PollInterval:   cfg.ResourcePollInterval,
		EndOffset: func(callCtx context.Context) (int64, error) {
			return kafka.EndOffset(callCtx, cfg.KafkaBrokers, cfg.KafkaTopic)
		},
		ScanPermits: scanPermits,
		DestPermits: destPermits,
	}, cp.NextKafkaOffset)
	if err != nil {
		return err
	}

	if addr := os.Getenv("SEAM_HTTP_ADDR"); addr != "" {
		server.Start(ctx, addr, metrics, cpStore, cfg.JobID)
	}

	// Adaptive chunking runs the scanner loop (no durable chunk store) so the
	// per-chunk latency feedback can resize chunks on the fly.
	var sizer *adaptive.Sizer
	if cfg.AdaptiveChunking {
		sizer = adaptive.NewSizer(cfg.TargetChunkDuration, cfg.ChunkSize, cfg.ChunkSizeMin, cfg.ChunkSizeMax)
	}

	reconcilerCfg := reconcile.Config{
		JobConfig: model.JobConfig{
			JobID:                 cfg.JobID,
			SourceDSN:             cfg.SourceDSN,
			SourceReplDSN:         cfg.SourceReplDSN,
			SourceSlot:            cfg.SourceSlot,
			SourcePublication:     cfg.SourcePublication,
			SourceSystemID:        sourceSystemID,
			DestDSN:               cfg.DestDSN,
			DestTable:             cfg.DestTable,
			KafkaBrokers:          cfg.KafkaBrokers,
			KafkaTopic:            cfg.KafkaTopic,
			ChunkSize:             cfg.ChunkSize,
			WorkerID:              cfg.WorkerID,
			LeaseDuration:         cfg.LeaseDuration,
			MaxInMemoryCandidates: cfg.MaxInMemoryCandidates,
			MaxRecordsPerBatch:    cfg.MaxRecordsPerBatch,
			Workers:               cfg.Workers,
			HeartbeatInterval:     cfg.HeartbeatInterval,
		},
		Checkpoint:      cp,
		Consumer:        consumer,
		CheckpointStore: cpStore,
		MarkerStore:     markerStore,
		Scanner:         paginator,
		Sink:            targetSink,
		Adaptive:        sizer,
		Metrics:         metrics,
		Resources:       resources,
		SourceSchema:    sourceSchema,
	}
	if !cfg.AdaptiveChunking {
		reconcilerCfg.ChunkStore = cpStore
	}
	reconciler := reconcile.New(reconcilerCfg)

	return reconciler.Run(ctx)
}

// validate rejects configurations that cannot be safe to run, before any
// connection or table is touched. Invalid values fail loudly instead of
// silently falling back to defaults.
func (cfg config) validate() error {
	if cfg.SourceTable == "" {
		return fmt.Errorf("SEAM_SOURCE_TABLE cannot be empty")
	}
	if err := schema.ValidateIdentifier(cfg.SourceTable); err != nil {
		return fmt.Errorf("invalid source table: %w", err)
	}
	if err := schema.ValidateIdentifier(cfg.DestTable); err != nil {
		return fmt.Errorf("invalid destination table: %w", err)
	}
	if cfg.AdaptiveChunking {
		return fmt.Errorf("adaptive chunking bypasses the durable manifest and is disabled until it is made crash-safe")
	}
	if cfg.ChunkSize < 1 || cfg.Workers < 1 || cfg.MaxInMemoryCandidates < 1 || cfg.ChunkSize > cfg.MaxInMemoryCandidates {
		return fmt.Errorf("chunk size, worker count, and candidate limit must be positive, with chunk size <= candidate limit")
	}
	if cfg.LeaseDuration <= 0 {
		return fmt.Errorf("SEAM_LEASE_DURATION must be positive, got %s", cfg.LeaseDuration)
	}
	if cfg.MaxRecordsPerBatch <= 0 {
		return fmt.Errorf("SEAM_MAX_RECORDS_PER_BATCH must be positive, got %d", cfg.MaxRecordsPerBatch)
	}
	if cfg.HeartbeatInterval < 0 {
		return fmt.Errorf("SEAM_HEARTBEAT_INTERVAL cannot be negative, got %s; 0 derives the interval from the lease duration", cfg.HeartbeatInterval)
	}
	if cfg.HeartbeatInterval > 0 && cfg.HeartbeatInterval >= cfg.LeaseDuration {
		return fmt.Errorf("SEAM_HEARTBEAT_INTERVAL %s must be strictly shorter than SEAM_LEASE_DURATION %s", cfg.HeartbeatInterval, cfg.LeaseDuration)
	}
	if cfg.TargetChunkDuration <= 0 {
		return fmt.Errorf("SEAM_TARGET_CHUNK_DURATION must be positive, got %s", cfg.TargetChunkDuration)
	}
	if cfg.ChunkSizeMin <= 0 {
		return fmt.Errorf("SEAM_CHUNK_SIZE_MIN must be positive, got %d", cfg.ChunkSizeMin)
	}
	if cfg.ChunkSizeMax < cfg.ChunkSizeMin {
		return fmt.Errorf("SEAM_CHUNK_SIZE_MAX %d must be >= SEAM_CHUNK_SIZE_MIN %d", cfg.ChunkSizeMax, cfg.ChunkSizeMin)
	}
	if cfg.MaxCandidateBytes < minBytesForWorkers(cfg.Workers) {
		return fmt.Errorf("candidate byte budget must allow at least 1 MiB per worker")
	}
	if cfg.MaxSourceScans < 1 {
		return fmt.Errorf("SEAM_MAX_SOURCE_SCANS must be positive, got %d", cfg.MaxSourceScans)
	}
	if cfg.MaxDestinationTx < 1 {
		return fmt.Errorf("SEAM_MAX_DESTINATION_TX must be positive, got %d", cfg.MaxDestinationTx)
	}
	if cfg.MaxCDCLagRecords < 0 {
		return fmt.Errorf("SEAM_MAX_CDC_LAG_RECORDS cannot be negative, got %d", cfg.MaxCDCLagRecords)
	}
	if cfg.ResourcePollInterval <= 0 {
		return fmt.Errorf("SEAM_RESOURCE_POLL_INTERVAL must be positive, got %s", cfg.ResourcePollInterval)
	}
	return nil
}

// minBytesForWorkers returns the minimum candidate byte budget for a worker
// count. The 1 MiB-per-worker arithmetic is checked so that a worker count too
// large to multiply without int64 overflow fails validation instead of
// wrapping around to a deceptively small budget.
func minBytesForWorkers(workers int) int64 {
	perWorker := int64(1) << 20
	if int64(workers) > math.MaxInt64/perWorker {
		return math.MaxInt64
	}
	return int64(workers) * perWorker
}

func decodeChange(data []byte) (model.Change, error) {
	// Reuse the capture codec to avoid duplicating JSON shape.
	var codec capture.JSONCodec
	return codec.Decode(data)
}

func requireEmptyDestination(ctx context.Context, store *checkpoint.Store, table string) error {
	conn, err := store.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	var exists bool
	if err := conn.QueryRow(ctx, fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM %s LIMIT 1)`, table)).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("destination %s is not empty; a fresh job cannot overwrite existing data", table)
	}
	return nil
}

// loadConfig reads every value from the environment and the command line.
// A value that is set but malformed is a hard error; it is never silently
// replaced by a default.
func loadConfig() (config, error) {
	cfg, err := configFromEnv()
	if err != nil {
		return config{}, err
	}
	flags := flag.NewFlagSet("seam", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	flags.BoolVar(&cfg.StartFresh, "start-fresh", false, "Create a new job instead of recovering")
	if err := flags.Parse(os.Args[1:]); err != nil {
		return config{}, err
	}
	return cfg, nil
}

// configFromEnv builds a config from environment variables. Set-but-malformed
// values return an error naming the variable; missing values keep defaults.
func configFromEnv() (config, error) {
	cfg := config{
		JobID:             envOrDefault("SEAM_JOB_ID", "seam-default"),
		SourceDSN:         envOrDefault("SOURCE_SQL_DSN", "postgres://postgres:postgres@localhost:5433/source?sslmode=disable"),
		SourceReplDSN:     envOrDefault("SOURCE_REPLICATION_DSN", "postgres://postgres:postgres@localhost:5433/source?sslmode=disable&replication=database"),
		SourceSlot:        envOrDefault("SEAM_SOURCE_SLOT", "seam_slot"),
		SourcePublication: envOrDefault("SEAM_SOURCE_PUBLICATION", "seam_pub"),
		SourceTable:       envOrDefault("SEAM_SOURCE_TABLE", scan.DefaultChunkReaderTable),
		SourceKey:         os.Getenv("SEAM_SOURCE_KEY"),
		DestDSN:           envOrDefault("DEST_SQL_DSN", "postgres://postgres:postgres@localhost:5434/dest?sslmode=disable"),
		DestTable:         envOrDefault("SEAM_DEST_TABLE", "accounts"),
		KafkaBrokers:      splitAndTrim(envOrDefault("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:        envOrDefault("KAFKA_TOPIC", "seam.accounts"),
		WorkerID:          envOrDefault("SEAM_WORKER_ID", defaultWorkerID()),
	}
	if len(cfg.KafkaBrokers) == 0 {
		return config{}, fmt.Errorf("KAFKA_BROKERS: broker list is empty (got %q); provide at least one host:port", os.Getenv("KAFKA_BROKERS"))
	}
	var err error
	if cfg.ChunkSize, err = intFromEnv("SEAM_CHUNK_SIZE", 1000); err != nil {
		return config{}, err
	}
	if cfg.LeaseDuration, err = durationFromEnv("SEAM_LEASE_DURATION", 30*time.Second); err != nil {
		return config{}, err
	}
	if cfg.MaxInMemoryCandidates, err = intFromEnv("SEAM_MAX_IN_MEMORY_CANDIDATES", 1_000_000); err != nil {
		return config{}, err
	}
	if cfg.MaxCandidateBytes, err = int64FromEnv("SEAM_MAX_CANDIDATE_BYTES", 256<<20); err != nil {
		return config{}, err
	}
	if cfg.MaxRecordsPerBatch, err = intFromEnv("SEAM_MAX_RECORDS_PER_BATCH", 100); err != nil {
		return config{}, err
	}
	if cfg.Workers, err = intFromEnv("SEAM_WORKERS", 1); err != nil {
		return config{}, err
	}
	if cfg.HeartbeatInterval, err = durationFromEnv("SEAM_HEARTBEAT_INTERVAL", 0); err != nil {
		return config{}, err
	}
	if cfg.AdaptiveChunking, err = boolFromEnv("SEAM_ADAPTIVE_CHUNKING", false); err != nil {
		return config{}, err
	}
	if cfg.TargetChunkDuration, err = durationFromEnv("SEAM_TARGET_CHUNK_DURATION", 5*time.Second); err != nil {
		return config{}, err
	}
	if cfg.ChunkSizeMin, err = intFromEnv("SEAM_CHUNK_SIZE_MIN", 100); err != nil {
		return config{}, err
	}
	if cfg.ChunkSizeMax, err = intFromEnv("SEAM_CHUNK_SIZE_MAX", 1_000_000); err != nil {
		return config{}, err
	}
	if cfg.MaxSourceScans, err = intFromEnv("SEAM_MAX_SOURCE_SCANS", 4); err != nil {
		return config{}, err
	}
	if cfg.MaxDestinationTx, err = intFromEnv("SEAM_MAX_DESTINATION_TX", 8); err != nil {
		return config{}, err
	}
	if cfg.MaxCDCLagRecords, err = int64FromEnv("SEAM_MAX_CDC_LAG_RECORDS", 10_000); err != nil {
		return config{}, err
	}
	if cfg.ResourcePollInterval, err = durationFromEnv("SEAM_RESOURCE_POLL_INTERVAL", time.Second); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func defaultWorkerID() string {
	host, _ := os.Hostname()
	if host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d", host, time.Now().UnixNano())
}

func envOrDefault(name, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return fallback
}

// intFromEnv parses a signed integer environment variable. A value that is set
// but not a valid integer is an error; an unset value keeps the fallback.
func intFromEnv(name string, fallback int) (int, error) {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid integer %q", name, v)
	}
	return n, nil
}

func int64FromEnv(name string, fallback int64) (int64, error) {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid integer %q", name, v)
	}
	return n, nil
}

func durationFromEnv(name string, fallback time.Duration) (time.Duration, error) {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid duration %q", name, v)
	}
	return d, nil
}

func boolFromEnv(name string, fallback bool) (bool, error) {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	if v == "" {
		return fallback, nil
	}
	switch v {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	}
	return false, fmt.Errorf("%s: invalid boolean %q", name, v)
}

func splitAndTrim(value string) []string {
	parts := strings.Split(value, ",")
	var out []string
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

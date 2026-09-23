package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
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
	"example.com/seam/internal/scan"
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
	KafkaBrokers          []string
	KafkaTopic            string
	ChunkSize             int
	WorkerID              string
	LeaseDuration         time.Duration
	MaxInMemoryCandidates int
	MaxRecordsPerBatch    int
	AdaptiveChunking      bool
	TargetChunkDuration   time.Duration
	ChunkSizeMin          int
	ChunkSizeMax          int
	StartFresh            bool
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	cfg := loadConfig()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("seam: %v", err)
	}
}

func run(ctx context.Context, cfg config) error {
	cpStore := checkpoint.NewStore(cfg.DestDSN)
	if err := cpStore.EnsureTables(ctx); err != nil {
		return fmt.Errorf("ensure checkpoint tables: %w", err)
	}
	if err := cpStore.EnsureDestinationTable(ctx); err != nil {
		return fmt.Errorf("ensure destination table: %w", err)
	}

	var cp *model.Checkpoint
	if cfg.StartFresh {
		if err := cpStore.EnsureTables(ctx); err != nil {
			return err
		}
		jobCfg := model.JobConfig{
			JobID:                 cfg.JobID,
			SourceDSN:             cfg.SourceDSN,
			SourceReplDSN:         cfg.SourceReplDSN,
			SourceSlot:            cfg.SourceSlot,
			SourcePublication:     cfg.SourcePublication,
			DestDSN:               cfg.DestDSN,
			KafkaBrokers:          cfg.KafkaBrokers,
			KafkaTopic:            cfg.KafkaTopic,
			ChunkSize:             cfg.ChunkSize,
			WorkerID:              cfg.WorkerID,
			LeaseDuration:         cfg.LeaseDuration,
			MaxInMemoryCandidates: cfg.MaxInMemoryCandidates,
			MaxRecordsPerBatch:    cfg.MaxRecordsPerBatch,
		}
		upperBoundReader, err := scan.NewChunkReaderFor(cfg.SourceDSN, cfg.SourceTable, cfg.SourceKey)
		if err != nil {
			return fmt.Errorf("configure source scan: %w", err)
		}
		upperBound, err := upperBoundReader.UpperBound(ctx)
		if err != nil {
			return fmt.Errorf("determine scan upper bound: %w", err)
		}
		cp, err = cpStore.CreateJob(ctx, jobCfg, upperBound)
		if err != nil {
			return fmt.Errorf("create job: %w", err)
		}
		// Adaptive chunking resizes chunks from measured latency and currently
		// drives the scanner directly; pre-discovering fixed-size chunks would
		// contradict the adaptive size, so they are skipped in this mode.
		if !cfg.AdaptiveChunking {
			if err := cpStore.DiscoverAndCreateChunksFor(ctx, cfg.JobID, cp.Attempt, cfg.SourceDSN, cfg.SourceTable, cfg.SourceKey, upperBound, cfg.ChunkSize); err != nil {
				return fmt.Errorf("discover chunks: %w", err)
			}
		}
	} else {
		result, err := recovery.Recover(ctx, cpStore, cfg.JobID, cfg.SourceDSN)
		if err != nil {
			return fmt.Errorf("recover: %w", err)
		}
		cp = result.Checkpoint
		// Hard-fail when checkpointed Kafka history has fallen out of retention.
		earliest, err := kafka.EarliestOffset(ctx, cfg.KafkaBrokers, cfg.KafkaTopic)
		if err != nil {
			return fmt.Errorf("check kafka retention: %w", err)
		}
		if cp.NextKafkaOffset < earliest {
			return fmt.Errorf("checkpoint next_kafka_offset %d is before earliest retained offset %d; history lost, reset required",
				cp.NextKafkaOffset, earliest)
		}
	}

	markerStore := marker.NewStore(cfg.SourceDSN)
	if err := markerStore.EnsureTable(ctx); err != nil {
		return fmt.Errorf("ensure marker table: %w", err)
	}

	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cp.NextKafkaOffset, decodeChange,
		kafka.WithMaxPollRecords(cfg.MaxRecordsPerBatch))
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}
	defer consumer.Close()

	paginator, err := scan.NewChunkReaderFor(cfg.SourceDSN, cfg.SourceTable, cfg.SourceKey)
	if err != nil {
		return fmt.Errorf("configure source scan: %w", err)
	}

	metrics := telemetry.NewMetrics()

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
			DestDSN:               cfg.DestDSN,
			KafkaBrokers:          cfg.KafkaBrokers,
			KafkaTopic:            cfg.KafkaTopic,
			ChunkSize:             cfg.ChunkSize,
			WorkerID:              cfg.WorkerID,
			LeaseDuration:         cfg.LeaseDuration,
			MaxInMemoryCandidates: cfg.MaxInMemoryCandidates,
			MaxRecordsPerBatch:    cfg.MaxRecordsPerBatch,
		},
		Checkpoint:      cp,
		Consumer:        consumer,
		CheckpointStore: cpStore,
		MarkerStore:     markerStore,
		Scanner:         paginator,
		Sink:            sink.NewMutator(),
		Adaptive:        sizer,
		Metrics:         metrics,
	}
	if !cfg.AdaptiveChunking {
		reconcilerCfg.ChunkStore = cpStore
	}
	reconciler := reconcile.New(reconcilerCfg)

	return reconciler.Run(ctx)
}

func decodeChange(data []byte) (model.Change, error) {
	// Reuse the capture codec to avoid duplicating JSON shape.
	var codec capture.JSONCodec
	return codec.Decode(data)
}

func loadConfig() config {
	cfg := config{
		JobID:                 envOrDefault("SEAM_JOB_ID", "seam-default"),
		SourceDSN:             envOrDefault("SOURCE_SQL_DSN", "postgres://postgres:postgres@localhost:5433/source?sslmode=disable"),
		SourceReplDSN:         envOrDefault("SOURCE_REPLICATION_DSN", "postgres://postgres:postgres@localhost:5433/source?sslmode=disable&replication=database"),
		SourceSlot:            envOrDefault("SEAM_SOURCE_SLOT", "seam_slot"),
		SourcePublication:     envOrDefault("SEAM_SOURCE_PUBLICATION", "seam_pub"),
		SourceTable:           envOrDefault("SEAM_SOURCE_TABLE", scan.DefaultChunkReaderTable),
		SourceKey:             envOrDefault("SEAM_SOURCE_KEY", scan.DefaultChunkReaderKey),
		DestDSN:               envOrDefault("DEST_SQL_DSN", "postgres://postgres:postgres@localhost:5434/dest?sslmode=disable"),
		KafkaBrokers:          splitAndTrim(envOrDefault("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:            envOrDefault("KAFKA_TOPIC", "seam.accounts"),
		ChunkSize:             intEnvOrDefault("SEAM_CHUNK_SIZE", 1000),
		WorkerID:              envOrDefault("SEAM_WORKER_ID", defaultWorkerID()),
		LeaseDuration:         durationEnvOrDefault("SEAM_LEASE_DURATION", 30*time.Second),
		MaxInMemoryCandidates: intEnvOrDefault("SEAM_MAX_IN_MEMORY_CANDIDATES", 1_000_000),
		MaxRecordsPerBatch:    intEnvOrDefault("SEAM_MAX_RECORDS_PER_BATCH", 100),
		AdaptiveChunking:      boolEnvOrDefault("SEAM_ADAPTIVE_CHUNKING", false),
		TargetChunkDuration:   durationEnvOrDefault("SEAM_TARGET_CHUNK_DURATION", 5*time.Second),
		ChunkSizeMin:          intEnvOrDefault("SEAM_CHUNK_SIZE_MIN", 100),
		ChunkSizeMax:          intEnvOrDefault("SEAM_CHUNK_SIZE_MAX", 1_000_000),
	}
	flag.BoolVar(&cfg.StartFresh, "start-fresh", false, "Create a new job instead of recovering")
	flag.Parse()
	return cfg
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

func intEnvOrDefault(name string, fallback int) int {
	v := envOrDefault(name, "")
	if v == "" {
		return fallback
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
		return fallback
	}
	return n
}

func durationEnvOrDefault(name string, fallback time.Duration) time.Duration {
	v := envOrDefault(name, "")
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

func boolEnvOrDefault(name string, fallback bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	if v == "" {
		return fallback
	}
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	}
	return fallback
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

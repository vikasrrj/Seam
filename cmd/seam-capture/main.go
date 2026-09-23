package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"example.com/seam/internal/capture"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := capture.ReaderConfig{
		SQLDSN:         envOrDefault("SOURCE_SQL_DSN", "postgres://postgres:postgres@localhost:5433/source?sslmode=disable"),
		ReplicationDSN: envOrDefault("SOURCE_REPLICATION_DSN", "postgres://postgres:postgres@localhost:5433/source?sslmode=disable&replication=database"),
		Slot:           envOrDefault("SEAM_SOURCE_SLOT", "seam_slot"),
		Publication:    envOrDefault("SEAM_SOURCE_PUBLICATION", "seam_pub"),
		KafkaBrokers:   splitAndTrim(envOrDefault("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:     envOrDefault("KAFKA_TOPIC", "seam.accounts"),
		Generation:     envOrDefault("SEAM_GENERATION", "gen:0"),
	}
	if v := strings.TrimSpace(os.Getenv("SEAM_MAX_TX_EVENTS")); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil && n > 0 {
			cfg.MaxTransactionEvents = n
		}
	}

	reader, err := capture.StartReader(ctx, cfg)
	if err != nil {
		log.Fatalf("capture: start: %v", err)
	}
	defer reader.Close()

	if err := reader.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("capture: run: %v", err)
	}
}

func envOrDefault(name, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
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

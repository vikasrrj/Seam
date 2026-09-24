package main

import (
	"context"
	"errors"
	"fmt"
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

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("capture: %v", err)
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

// loadConfig reads capture configuration from the environment. A value that is
// set but malformed or out of range is a hard error; it is never silently
// replaced by a default.
func loadConfig() (capture.ReaderConfig, error) {
	cfg := capture.ReaderConfig{
		SQLDSN:         envOrDefault("SOURCE_SQL_DSN", "postgres://postgres:postgres@localhost:5433/source?sslmode=disable"),
		ReplicationDSN: envOrDefault("SOURCE_REPLICATION_DSN", "postgres://postgres:postgres@localhost:5433/source?sslmode=disable&replication=database"),
		Slot:           envOrDefault("SEAM_SOURCE_SLOT", "seam_slot"),
		Publication:    envOrDefault("SEAM_SOURCE_PUBLICATION", "seam_pub"),
		KafkaBrokers:   splitAndTrim(envOrDefault("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:     envOrDefault("KAFKA_TOPIC", "seam.accounts"),
		Table:          envOrDefault("SEAM_SOURCE_TABLE", "accounts"),
		Generation:     envOrDefault("SEAM_GENERATION", "gen:0"),
		OwnerID:        strings.TrimSpace(os.Getenv("SEAM_CAPTURE_OWNER_ID")),
	}
	if v := strings.TrimSpace(os.Getenv("SEAM_MAX_TX_EVENTS")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return capture.ReaderConfig{}, fmt.Errorf("SEAM_MAX_TX_EVENTS: invalid integer %q", v)
		}
		if n <= 0 {
			return capture.ReaderConfig{}, fmt.Errorf("SEAM_MAX_TX_EVENTS: must be positive, got %d", n)
		}
		cfg.MaxTransactionEvents = n
	}
	return cfg, nil
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

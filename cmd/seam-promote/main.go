package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"example.com/seam/internal/promotion"
)

func main() {
	liveJob := flag.String("live-job", "", "existing live job id")
	shadowJob := flag.String("shadow-job", "", "complete shadow job id")
	timeout := flag.Duration("timeout", 2*time.Minute, "maximum source write fence duration")
	maxWritePause := flag.Duration("max-write-pause", 30*time.Second, "hard limit for the source write pause after pre-catch-up")
	maxDeltaKeys := flag.Int("max-delta-keys", 100_000, "maximum distinct keys changed between validation snapshot and cutover")
	flag.Parse()
	if *liveJob == "" || *shadowJob == "" {
		fmt.Fprintln(os.Stderr, "seam-promote: --live-job and --shadow-job are required")
		flag.Usage()
		os.Exit(2)
	}
	if *liveJob == *shadowJob {
		fmt.Fprintln(os.Stderr, "seam-promote: --live-job and --shadow-job must be distinct")
		flag.Usage()
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	result, err := promotion.Promote(ctx, promotion.Config{
		SourceDSN:     env("SOURCE_SQL_DSN", "postgres://postgres:postgres@localhost:5433/source?sslmode=disable"),
		DestDSN:       env("DEST_SQL_DSN", "postgres://postgres:postgres@localhost:5434/dest?sslmode=disable"),
		KafkaBrokers:  strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaTopic:    env("KAFKA_TOPIC", "seam.accounts"),
		LiveJobID:     *liveJob,
		ShadowJobID:   *shadowJob,
		MaxWritePause: *maxWritePause,
		MaxDeltaKeys:  *maxDeltaKeys,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("promoted at Kafka cursor %d; retired table %s; validation snapshot pause %s; final source write pause %s; already promoted: %v\n", result.BarrierOffset, result.RetiredTable, result.SnapshotFenceDuration, result.FenceDuration, result.AlreadyPromoted)
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

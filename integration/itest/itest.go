// Package itest provides shared integration-test helpers.
package itest

import (
	"context"
	"fmt"
	"os"

	"example.com/seam/internal/transport"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func SourceDSN() string {
	return envOrDefault("SEAM_TEST_SOURCE_DSN", "postgres://postgres:postgres@localhost:5435/source?sslmode=disable")
}

func SourceReplDSN() string {
	return envOrDefault("SEAM_TEST_SOURCE_REPL_DSN", "postgres://postgres:postgres@localhost:5435/source?sslmode=disable&replication=database")
}

func DestDSN() string {
	return envOrDefault("SEAM_TEST_DEST_DSN", "postgres://postgres:postgres@localhost:5436/dest?sslmode=disable")
}

func KafkaBrokers() []string {
	v := envOrDefault("SEAM_TEST_KAFKA_BROKERS", "localhost:9094")
	if v == "" {
		return nil
	}
	return []string{v}
}

func KafkaTopic() string {
	return envOrDefault("SEAM_TEST_KAFKA_TOPIC", "seam.integration")
}

func SourceConn(ctx context.Context) (*pgx.Conn, error) {
	return transport.ConnectPostgres(ctx, SourceDSN())
}

func DestConn(ctx context.Context) (*pgx.Conn, error) {
	return transport.ConnectPostgres(ctx, DestDSN())
}

func envOrDefault(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// ResetTables truncates test tables, drops the replication slot, and recreates
// the Kafka topic so each test starts from a clean offset 0.
func ResetTables(ctx context.Context) error {
	src, err := SourceConn(ctx)
	if err != nil {
		return fmt.Errorf("source conn: %w", err)
	}
	defer src.Close(context.Background())

	dst, err := DestConn(ctx)
	if err != nil {
		return fmt.Errorf("dest conn: %w", err)
	}
	defer dst.Close(context.Background())

	if _, err := src.Exec(ctx, `TRUNCATE accounts, seam_marker`); err != nil {
		return fmt.Errorf("truncate source: %w", err)
	}
	if _, err := src.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'seam_itest_slot'`); err != nil {
		// ignore: slot may not exist
	}

	if _, err := dst.Exec(ctx, `TRUNCATE accounts, seam_jobs, seam_checkpoints, seam_applied_txs, seam_chunks`); err != nil {
		return fmt.Errorf("truncate dest: %w", err)
	}

	// Recreate Kafka topic to wipe retained records from prior test runs.
	if err := resetKafkaTopic(ctx); err != nil {
		return fmt.Errorf("reset kafka topic: %w", err)
	}
	return nil
}

func resetKafkaTopic(ctx context.Context) error {
	client, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(KafkaBrokers()...),
	}, transport.KafkaOptions()...)...)
	if err != nil {
		return err
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	if _, err := admin.DeleteTopics(ctx, KafkaTopic()); err != nil {
		// ignore: topic may not exist
	}
	configs := map[string]*string{
		"min.insync.replicas": kadm.StringPtr("1"),
		"retention.ms":        kadm.StringPtr("604800000"),
	}
	if _, err := admin.CreateTopics(ctx, 1, 1, configs, KafkaTopic()); err != nil {
		return err
	}
	return nil
}

// Package itest provides shared integration-test helpers.
package itest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"example.com/seam/internal/checkpoint"
	"example.com/seam/internal/schema"
	"example.com/seam/internal/transport"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
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

// SourceDescriptor loads and validates the source descriptor for one table.
// Integration fixtures carry REPLICA IDENTITY FULL, so the source validator
// (which requires it) is the right path.
func SourceDescriptor(ctx context.Context, table string) (*schema.Schema, error) {
	conn, err := SourceConn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	return schema.LoadFromConn(ctx, conn, "public", table)
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

// JobID returns a unique job identifier for one integration-test run. The
// dedicated destination keeps rows between runs, so tests must never reuse a
// job id left behind by an earlier interrupted run.
func JobID(base string) string {
	return fmt.Sprintf("%s-%d", base, time.Now().UnixNano())
}

// CloseOnCleanup closes a PostgreSQL connection during test cleanup and
// reports a close failure instead of silently dropping it.
func CloseOnCleanup(t interface {
	Cleanup(func())
	Errorf(string, ...any)
}, name string, conn *pgx.Conn) {
	t.Cleanup(func() {
		if err := conn.Close(context.Background()); err != nil {
			t.Errorf("close %s: %v", name, err)
		}
	})
}

// ResetTables truncates test tables, drops the replication slot, and recreates
// the Kafka topic so each test starts from a clean offset 0.
func ResetTables(ctx context.Context) error {
	// Integration databases may predate metadata migrations. Truncating a
	// referenced seam_jobs table requires the new seam_promotions table to be
	// present in the same TRUNCATE statement.
	if err := checkpoint.NewStore(DestDSN()).EnsureTables(ctx); err != nil {
		return fmt.Errorf("ensure destination metadata: %w", err)
	}
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

	if _, err := src.Exec(ctx, `
		ALTER TABLE accounts REPLICA IDENTITY FULL;
		CREATE TABLE IF NOT EXISTS widgets (
			id BIGINT PRIMARY KEY,
			name TEXT NOT NULL,
			qty INTEGER,
			price NUMERIC(12,2),
			created TIMESTAMPTZ,
			tags JSONB,
			data BYTEA,
			active BOOLEAN,
			note VARCHAR(100)
		);
		ALTER TABLE widgets REPLICA IDENTITY FULL;
		CREATE TABLE IF NOT EXISTS seam_capture_owners (
			slot_name TEXT PRIMARY KEY,
			owner_id TEXT NOT NULL,
			owner_epoch BIGINT NOT NULL,
			backend_pid INTEGER NOT NULL,
			source_system_id TEXT NOT NULL,
			generation TEXT NOT NULL,
			publication TEXT NOT NULL,
			kafka_topic_id TEXT NOT NULL,
			acquired_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
			heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
		);
		DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'seam_pub_widgets') THEN
				CREATE PUBLICATION seam_pub_widgets FOR TABLE widgets, seam_marker;
			END IF;
		END
		$$;
		TRUNCATE accounts, widgets, seam_marker, seam_capture_owners`); err != nil {
		return fmt.Errorf("truncate source: %w", err)
	}
	if _, err := src.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name IN ('seam_itest_slot', 'seam_itest_widgets_slot')`); err != nil {
		return fmt.Errorf("drop integration replication slots: %w", err)
	}

	if _, err := dst.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS widgets (
			id BIGINT PRIMARY KEY,
			name TEXT NOT NULL,
			qty INTEGER,
			price NUMERIC(12,2),
			created TIMESTAMPTZ,
			tags JSONB,
			data BYTEA,
			active BOOLEAN,
			note VARCHAR(100)
		);
		TRUNCATE accounts, widgets, seam_jobs, seam_checkpoints, seam_applied_txs, seam_chunks, seam_candidates, seam_promotions, seam_cutover_gates`); err != nil {
		return fmt.Errorf("truncate dest: %w", err)
	}

	// Recreate Kafka topic to wipe retained records from prior test runs.
	if err := resetKafkaTopic(ctx); err != nil {
		return fmt.Errorf("reset kafka topic: %w", err)
	}
	return nil
}

// resetKafkaTopic recreates the integration topic from a clean state. Kafka
// topic deletion is asynchronous: recreating the same name immediately can
// leave consumers seeing UNKNOWN_TOPIC_OR_PARTITION while the old deletion
// finishes. Both the delete and the create therefore wait until the broker's
// metadata reflects the intended state, so callers start from a topic that is
// actually readable.
func resetKafkaTopic(ctx context.Context) error {
	client, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(KafkaBrokers()...),
	}, transport.KafkaOptions()...)...)
	if err != nil {
		return err
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	delResp, err := admin.DeleteTopics(ctx, KafkaTopic())
	if err != nil {
		return fmt.Errorf("delete kafka topic request: %w", err)
	}
	if e := delResp[KafkaTopic()].Err; e != nil && !errors.Is(e, kerr.UnknownTopicOrPartition) {
		return fmt.Errorf("delete kafka topic: %w", e)
	}
	if err := waitTopicGone(ctx, admin, KafkaTopic()); err != nil {
		return err
	}
	configs := map[string]*string{
		"min.insync.replicas": kadm.StringPtr("1"),
		"retention.ms":        kadm.StringPtr("604800000"),
	}
	// A topic recreated while the broker is still tearing down the previous
	// generation can report TopicAlreadyExists; retry after the topic is gone.
	for {
		createResp, err := admin.CreateTopics(ctx, 1, 1, configs, KafkaTopic())
		if err != nil {
			return fmt.Errorf("create kafka topic request: %w", err)
		}
		if e := createResp[KafkaTopic()].Err; e != nil {
			if errors.Is(e, kerr.TopicAlreadyExists) {
				if err := waitTopicGone(ctx, admin, KafkaTopic()); err != nil {
					return err
				}
				continue
			}
			return fmt.Errorf("create kafka topic: %w", e)
		}
		break
	}
	return waitTopicReady(ctx, admin, KafkaTopic())
}

// waitTopicGone polls broker metadata until the topic no longer exists.
func waitTopicGone(ctx context.Context, admin *kadm.Client, topic string) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		details, err := admin.ListTopics(ctx, topic)
		if err != nil {
			return fmt.Errorf("list topics during delete: %w", err)
		}
		detail, ok := details[topic]
		if !ok || detail.Err != nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("kafka topic %q was not deleted within 30s", topic)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for kafka topic deletion: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// waitTopicReady polls broker metadata until the topic exists with one healthy
// partition and a stable topic ID, mirroring the SEAM consumer contract.
func waitTopicReady(ctx context.Context, admin *kadm.Client, topic string) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		details, err := admin.ListTopics(ctx, topic)
		if err != nil {
			return fmt.Errorf("list topics during create: %w", err)
		}
		detail, ok := details[topic]
		if ok && detail.Err == nil && len(detail.Partitions) == 1 && detail.ID != (kadm.TopicID{}) {
			if partition, ok := detail.Partitions[0]; ok && partition.Err == nil {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("kafka topic %q was not ready within 30s", topic)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for kafka topic ready: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

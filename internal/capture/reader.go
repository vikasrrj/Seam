package capture

import (
	"context"
	"errors"
	"fmt"
	"log"
	"regexp"
	"time"

	"example.com/seam/internal/model"
	"example.com/seam/internal/retry"
	"example.com/seam/internal/transport"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const standbyStatusInterval = 10 * time.Second

var postgresIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ReaderConfig is the minimal configuration for the Seam CDC transport.
type ReaderConfig struct {
	SQLDSN         string
	ReplicationDSN string
	Slot           string
	Publication    string
	KafkaBrokers   []string
	KafkaTopic     string
	Generation     string
	// MaxTransactionEvents bounds the in-memory buffer of a single source
	// transaction. 0 keeps the decoder default (1,000,000 events).
	MaxTransactionEvents int
}

// Reader streams one PostgreSQL publication to one Kafka topic/partition.
type Reader struct {
	cfg        ReaderConfig
	kafka      *kgo.Client
	connection *pgconn.PgConn
	decoder    *Decoder
	durableLSN pglogrepl.LSN
	nextStatus time.Time
	codec      JSONCodec
	runCtx     context.Context
	runCancel  context.CancelFunc
}

// StartReader creates the replication slot if needed and begins streaming.
func StartReader(ctx context.Context, cfg ReaderConfig) (*Reader, error) {
	if !postgresIdentifier.MatchString(cfg.Slot) {
		return nil, fmt.Errorf("invalid replication slot name %q", cfg.Slot)
	}
	if !postgresIdentifier.MatchString(cfg.Publication) {
		return nil, fmt.Errorf("invalid publication name %q", cfg.Publication)
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.KafkaBrokers...),
		kgo.DefaultProduceTopic(cfg.KafkaTopic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.AllowAutoTopicCreation(),
	}
	opts = append(opts, transport.KafkaOptions()...)
	kafkaClient, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	if err := kafkaClient.Ping(ctx); err != nil {
		kafkaClient.Close()
		return nil, fmt.Errorf("ping kafka: %w", err)
	}
	if err := ensureTopic(ctx, kafkaClient, cfg.KafkaTopic); err != nil {
		kafkaClient.Close()
		return nil, err
	}

	runCtx, runCancel := context.WithCancel(ctx)
	reader := &Reader{
		cfg:        cfg,
		kafka:      kafkaClient,
		decoder:    NewDecoder(cfg.Generation),
		nextStatus: time.Now().Add(standbyStatusInterval),
		runCtx:     runCtx,
		runCancel:  runCancel,
	}
	if cfg.MaxTransactionEvents > 0 {
		reader.decoder.MaxTransactionEvents = cfg.MaxTransactionEvents
	}

	startLSN, slotExists, err := findSlotLSN(ctx, cfg.SQLDSN, cfg.Slot)
	if err != nil {
		reader.Close()
		return nil, err
	}
	if slotExists {
		if err := verifySlotRecoverable(ctx, cfg.SQLDSN, cfg.Slot, startLSN); err != nil {
			reader.Close()
			return nil, err
		}
		reader.durableLSN = startLSN
		log.Printf("capture: resuming slot %s at %s", cfg.Slot, startLSN)
	} else {
		createdLSN, err := createSlot(ctx, cfg)
		if err != nil {
			reader.Close()
			return nil, err
		}
		reader.durableLSN = createdLSN
		log.Printf("capture: created slot %s at %s", cfg.Slot, createdLSN)
	}

	conn, err := dialReplication(ctx, cfg, reader.durableLSN)
	if err != nil {
		reader.Close()
		return nil, err
	}
	reader.connection = conn
	return reader, nil
}

// Close releases resources. It does not drop the replication slot.
func (r *Reader) Close() error {
	if r.runCancel != nil {
		r.runCancel()
	}
	if r.connection != nil {
		_ = r.connection.Close(context.Background())
	}
	if r.kafka != nil {
		r.kafka.Close()
	}
	return nil
}

// Run blocks until ctx is cancelled, streaming changes to Kafka.
func (r *Reader) Run(ctx context.Context) error {
	log.Printf("capture: streaming publication %s to topic %s", r.cfg.Publication, r.cfg.KafkaTopic)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		receiveCtx, cancel := context.WithDeadline(r.runCtx, r.nextStatus)
		raw, err := r.connection.ReceiveMessage(receiveCtx)
		cancel()

		if err != nil {
			if r.runCtx.Err() != nil || ctx.Err() != nil {
				_ = r.sendStandby(context.Background(), "shutdown")
				return ctx.Err()
			}
			if errors.Is(err, context.DeadlineExceeded) || pgconn.Timeout(err) {
				if err := r.sendStandby(ctx, "periodic"); err != nil {
					return err
				}
				r.nextStatus = time.Now().Add(standbyStatusInterval)
				continue
			}
			return fmt.Errorf("receive message: %w", err)
		}

		copyData, ok := raw.(*pgproto3.CopyData)
		if !ok || len(copyData.Data) == 0 {
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("parse keepalive: %w", err)
			}
			if keepalive.ReplyRequested {
				if err := r.sendStandby(ctx, "keepalive"); err != nil {
					return err
				}
				r.nextStatus = time.Now().Add(standbyStatusInterval)
			}

		case pglogrepl.XLogDataByteID:
			xlogData, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("parse xlog data: %w", err)
			}
			message, err := pglogrepl.Parse(xlogData.WALData)
			if err != nil {
				return fmt.Errorf("parse pgoutput message: %w", err)
			}
			if err := r.handleMessage(ctx, message, xlogData.WALStart); err != nil {
				return err
			}
		}
	}
}

func (r *Reader) handleMessage(ctx context.Context, message pglogrepl.Message, walStart pglogrepl.LSN) error {
	tx, err := r.decoder.Handle(message)
	if err != nil {
		return fmt.Errorf("decode message at %s: %w", walStart, err)
	}
	if tx == nil {
		return nil
	}
	for {
		change, ok, err := r.decoder.NextEvent()
		if err != nil {
			return fmt.Errorf("pull event: %w", err)
		}
		if !ok {
			break
		}
		if err := r.publish(ctx, change); err != nil {
			return fmt.Errorf("publish event: %w", err)
		}
	}
	r.durableLSN = tx.EndLSN
	if err := r.sendStandby(ctx, "tx_acked"); err != nil {
		return err
	}
	r.nextStatus = time.Now().Add(standbyStatusInterval)
	return nil
}

func (r *Reader) publish(ctx context.Context, change model.Change) error {
	payload, err := r.codec.Encode(change)
	if err != nil {
		return fmt.Errorf("encode change: %w", err)
	}
	record := &kgo.Record{
		Topic: r.cfg.KafkaTopic,
		Key:   []byte(change.Key()),
		Value: payload,
	}
	err = retry.DoVoid(ctx, retry.DefaultConfig(), func() error {
		_, err := r.kafka.ProduceSync(ctx, record).First()
		return err
	}, retry.IsRetryableKafka)
	if err != nil {
		return fmt.Errorf("produce to kafka: %w", err)
	}
	return nil
}

func (r *Reader) sendStandby(ctx context.Context, reason string) error {
	if r.connection == nil {
		return nil
	}
	err := pglogrepl.SendStandbyStatusUpdate(ctx, r.connection, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: r.durableLSN,
		WALFlushPosition: r.durableLSN,
		WALApplyPosition: r.durableLSN,
		ClientTime:       time.Now(),
	})
	if err != nil {
		return fmt.Errorf("send standby status at %s: %w", r.durableLSN, err)
	}
	log.Printf("capture: standby reason=%s lsn=%s", reason, r.durableLSN)
	return nil
}

func createSlot(ctx context.Context, cfg ReaderConfig) (pglogrepl.LSN, error) {
	conn, err := pgconn.Connect(ctx, cfg.ReplicationDSN)
	if err != nil {
		return 0, fmt.Errorf("connect replication: %w", err)
	}
	defer conn.Close(context.Background())

	result, err := pglogrepl.CreateReplicationSlot(ctx, conn, cfg.Slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode:           pglogrepl.LogicalReplication,
		SnapshotAction: "NOEXPORT_SNAPSHOT",
	})
	if err != nil {
		return 0, fmt.Errorf("create slot %q: %w", cfg.Slot, err)
	}
	lsn, err := pglogrepl.ParseLSN(result.ConsistentPoint)
	if err != nil {
		return 0, fmt.Errorf("parse consistent point %q: %w", result.ConsistentPoint, err)
	}
	return lsn, nil
}

func dialReplication(ctx context.Context, cfg ReaderConfig, lsn pglogrepl.LSN) (*pgconn.PgConn, error) {
	conn, err := pgconn.Connect(ctx, cfg.ReplicationDSN)
	if err != nil {
		return nil, fmt.Errorf("connect replication: %w", err)
	}
	if err := pglogrepl.StartReplication(ctx, conn, cfg.Slot, lsn, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", cfg.Publication),
		},
	}); err != nil {
		conn.Close(context.Background())
		return nil, fmt.Errorf("start replication: %w", err)
	}
	return conn, nil
}

func findSlotLSN(ctx context.Context, dsn, slot string) (pglogrepl.LSN, bool, error) {
	conn, err := transport.ConnectPostgres(ctx, dsn)
	if err != nil {
		return 0, false, fmt.Errorf("connect sql: %w", err)
	}
	defer conn.Close(context.Background())

	var lsnText string
	err = conn.QueryRow(ctx, `
		SELECT COALESCE(confirmed_flush_lsn, restart_lsn)::text
		FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&lsnText)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read slot %q: %w", slot, err)
	}
	lsn, err := pglogrepl.ParseLSN(lsnText)
	if err != nil {
		return 0, false, fmt.Errorf("parse slot lsn %q: %w", lsnText, err)
	}
	return lsn, true, nil
}

func verifySlotRecoverable(ctx context.Context, dsn, slot string, startLSN pglogrepl.LSN) error {
	conn, err := transport.ConnectPostgres(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect sql: %w", err)
	}
	defer conn.Close(context.Background())

	var restartText, flushText string
	if err := conn.QueryRow(ctx, `SELECT restart_lsn::text, confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&restartText, &flushText); err != nil {
		return fmt.Errorf("read slot restart lsn: %w", err)
	}

	for _, lsnText := range []string{restartText, flushText} {
		if lsnText == "" {
			continue
		}
		var retained bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 0 FROM pg_ls_waldir() WHERE name = pg_walfile_name($1))`, lsnText).Scan(&retained); err != nil {
			return fmt.Errorf("check wal retention for %s: %w", lsnText, err)
		}
		if !retained {
			return fmt.Errorf("slot %q lsn %s recycled; reset pipeline", slot, lsnText)
		}
	}

	startText := startLSN.String()
	var startRetained bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 0 FROM pg_ls_waldir() WHERE name = pg_walfile_name($1))`, startText).Scan(&startRetained); err != nil {
		return fmt.Errorf("check wal retention for start lsn: %w", err)
	}
	if !startRetained {
		return fmt.Errorf("slot %q start lsn %s recycled; reset pipeline", slot, startText)
	}
	return nil
}

func ensureTopic(ctx context.Context, client *kgo.Client, topic string) error {
	admin := kadm.NewClient(client)
	configs := map[string]*string{
		"min.insync.replicas": kadm.StringPtr("1"),
		"retention.ms":        kadm.StringPtr("604800000"),
	}
	responses, err := admin.CreateTopics(ctx, 1, 1, configs, topic)
	if err != nil {
		return fmt.Errorf("create topic: %w", err)
	}
	resp, err := responses.On(topic, nil)
	if err == nil && resp.Err == nil {
		log.Printf("capture: created topic %s", topic)
		return nil
	}
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("create topic %q: %w", topic, err)
	}
	return nil
}

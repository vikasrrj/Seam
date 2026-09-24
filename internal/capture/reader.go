package capture

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"time"

	"example.com/seam/internal/kafka"
	"example.com/seam/internal/model"
	"example.com/seam/internal/retry"
	"example.com/seam/internal/schema"
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
const maxKafkaTransactionBytes = 900 * 1024

var postgresIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ReaderConfig is the minimal configuration for the Seam CDC transport.
type ReaderConfig struct {
	SQLDSN         string
	ReplicationDSN string
	Slot           string
	Publication    string
	// Table is the replicated source data table (public.<Table>). It must be
	// REPLICA IDENTITY FULL, publish to the configured publication, and match
	// the durable schema descriptor recorded on the job.
	Table        string
	KafkaBrokers []string
	KafkaTopic   string
	Generation   string
	// OwnerID names this capture process in the durable source ownership row.
	// Empty generates a process-unique identity.
	OwnerID string
	// MaxTransactionEvents bounds the in-memory buffer of a single source
	// transaction. 0 keeps the decoder default (1,000,000 events).
	MaxTransactionEvents int
}

// Reader streams one PostgreSQL publication to one Kafka topic/partition.
type Reader struct {
	cfg        ReaderConfig
	producer   transactionProducer
	connection *pgconn.PgConn
	decoder    *Decoder
	// schemaID is the durable fingerprint of the source table descriptor,
	// stamped on every envelope so consumers can reject a schema epoch that
	// drifted under a running job.
	schemaID   string
	durableLSN pglogrepl.LSN
	nextStatus time.Time
	codec      JSONCodec
	runCtx     context.Context
	runCancel  context.CancelFunc
	leader     *captureLeadership
}

// StartReader creates the replication slot if needed and begins streaming.
func StartReader(ctx context.Context, cfg ReaderConfig) (*Reader, error) {
	if !postgresIdentifier.MatchString(cfg.Slot) {
		return nil, fmt.Errorf("invalid replication slot name %q", cfg.Slot)
	}
	if !postgresIdentifier.MatchString(cfg.Publication) {
		return nil, fmt.Errorf("invalid publication name %q", cfg.Publication)
	}
	if cfg.Table == "" {
		cfg.Table = "accounts"
	}
	if !postgresIdentifier.MatchString(cfg.Table) {
		return nil, fmt.Errorf("invalid source table name %q", cfg.Table)
	}
	if cfg.OwnerID == "" {
		cfg.OwnerID = fmt.Sprintf("capture-%d-%d", os.Getpid(), time.Now().UnixNano())
	}

	// The durable schema descriptor is the single source of truth for decode.
	// Loading it here (and failing closed on drift or unsupported types) proves
	// the capture side agrees with the schema epoch the job recorded.
	descriptor, err := schema.Load(ctx, cfg.SQLDSN, "public", cfg.Table)
	if err != nil {
		return nil, fmt.Errorf("load source schema for %s.%s: %w", "public", cfg.Table, err)
	}
	log.Printf("capture: decoding %s.%s under schema epoch %s", descriptor.Namespace, descriptor.Table, descriptor.Fingerprint)

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.KafkaBrokers...),
		kgo.DefaultProduceTopic(cfg.KafkaTopic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
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
	topicID, err := kafka.TopicIdentity(ctx, cfg.KafkaBrokers, cfg.KafkaTopic)
	if err != nil {
		kafkaClient.Close()
		return nil, fmt.Errorf("validate Kafka topic: %w", err)
	}

	runCtx, runCancel := context.WithCancel(ctx)
	reader := &Reader{
		cfg:        cfg,
		producer:   &kafkaProducer{client: kafkaClient},
		decoder:    NewDecoder(cfg.Generation, descriptor),
		schemaID:   descriptor.Fingerprint,
		nextStatus: time.Now().Add(standbyStatusInterval),
		runCtx:     runCtx,
		runCancel:  runCancel,
	}
	if cfg.MaxTransactionEvents > 0 {
		reader.decoder.MaxTransactionEvents = cfg.MaxTransactionEvents
	}
	systemID, err := SourceSystemID(ctx, cfg.SQLDSN)
	if err != nil {
		reader.Close()
		return nil, fmt.Errorf("read source system identity: %w", err)
	}
	reader.decoder.SetSystemID(systemID)
	leader, err := acquireCaptureLeadership(ctx, cfg, systemID, topicID)
	if err != nil {
		reader.Close()
		return nil, err
	}
	reader.leader = leader

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
	if r.leader != nil {
		r.leader.close()
	}
	if r.producer != nil {
		r.producer.Close()
	}
	return nil
}

// Run blocks until ctx is cancelled, streaming changes to Kafka.
func (r *Reader) Run(ctx context.Context) error {
	defer r.decoder.Reset()
	if err := r.leader.assert(ctx, true); err != nil {
		return err
	}
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
				if err := r.leader.assert(ctx, true); err != nil {
					return err
				}
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
		r.decoder.Reset()
		return fmt.Errorf("decode message at %s: %w", walStart, err)
	}
	if tx == nil {
		return nil
	}
	// Losing the source control session releases the advisory lock. Refuse to
	// publish or acknowledge any further WAL before a successor can take over.
	if r.leader != nil {
		if err := r.leader.assert(ctx, false); err != nil {
			return err
		}
	}
	if tx.Count > 0 {
		if err := r.publishTransaction(ctx, tx); err != nil {
			r.decoder.Reset()
			return fmt.Errorf("publish transaction: %w", err)
		}
	}
	r.durableLSN = tx.EndLSN
	if err := r.sendStandby(ctx, "tx_acked"); err != nil {
		return err
	}
	r.nextStatus = time.Now().Add(standbyStatusInterval)
	return nil
}

// publishTransaction converts one committed source transaction into bounded
// Kafka records. A one-change lookahead marks only the last fragment Final.
// WAL acknowledgement remains after this function, so a crash after any
// prefix causes PostgreSQL to replay the transaction and Kafka to retain both
// the abandoned prefix and the later complete retry.
func (r *Reader) publishTransaction(ctx context.Context, tx *CommittedTransaction) error {
	fragmentIndex := 0
	changes := make([]model.Change, 0, 128)
	for {
		change, ok, err := r.decoder.NextEvent()
		if err != nil {
			return fmt.Errorf("pull event: %w", err)
		}
		if !ok {
			if len(changes) == 0 {
				return nil
			}
			envelope := r.transactionFragment(changes, fragmentIndex, true, tx.Count)
			if err := r.publish(ctx, envelope); err != nil {
				return err
			}
			return nil
		}
		changes = append(changes, change)
		probe := r.transactionFragment(changes, fragmentIndex, false, tx.Count)
		payload, err := r.codec.EncodeTransaction(probe)
		if err != nil {
			return fmt.Errorf("encode transaction fragment: %w", err)
		}
		if len(payload) <= maxKafkaTransactionBytes {
			continue
		}
		if len(changes) == 1 {
			return fmt.Errorf("transaction %s contains one change encoding to %d bytes, exceeding Kafka record limit %d; source LSN will not be acknowledged", change.Source, len(payload), maxKafkaTransactionBytes)
		}
		last := changes[len(changes)-1]
		changes = changes[:len(changes)-1]
		if err := r.publish(ctx, r.transactionFragment(changes, fragmentIndex, false, tx.Count)); err != nil {
			return err
		}
		fragmentIndex++
		changes = []model.Change{last}
	}
}

func (r *Reader) transactionFragment(changes []model.Change, index int, final bool, total int) model.TransactionEnvelope {
	return model.TransactionEnvelope{
		Version:       2,
		SchemaID:      r.schemaID,
		Source:        changes[0].Source,
		FragmentIndex: index,
		Final:         final,
		Count:         len(changes),
		TotalCount:    total,
		Changes:       changes,
	}
}

func (r *Reader) publish(ctx context.Context, envelope model.TransactionEnvelope) error {
	payload, err := r.codec.EncodeTransaction(envelope)
	if err != nil {
		return fmt.Errorf("encode transaction: %w", err)
	}
	if len(payload) > maxKafkaTransactionBytes {
		return fmt.Errorf("transaction %s encodes to %d bytes, exceeding Kafka record limit %d; source LSN will not be acknowledged", envelope.Source, len(payload), maxKafkaTransactionBytes)
	}
	record := &kgo.Record{
		Topic: r.cfg.KafkaTopic,
		Key:   []byte(envelope.Source.String()),
		Value: payload,
	}
	err = retry.DoVoid(ctx, retry.DefaultConfig(), func() error {
		return r.producer.ProduceSync(ctx, record)
	}, retry.IsRetryableKafka)
	if err != nil {
		// An intentional Close() tears down the broker client while a
		// transaction may be in flight. That is an orderly shutdown, not a
		// capture failure; surface the cancellation so callers report it as
		// clean teardown instead of a real produce error.
		if r.runCtx.Err() != nil {
			return r.runCtx.Err()
		}
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
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
	// pgoutput renders values with the decoding session's TimeZone. Pin the
	// session to UTC so timestamptz canonical text is deterministic and
	// byte-identical to the ::text form Seam scans under SET TIME ZONE 'UTC'.
	if err := conn.Exec(ctx, "SET TIME ZONE 'UTC'").Close(); err != nil {
		conn.Close(context.Background())
		return nil, fmt.Errorf("set UTC replication session: %w", err)
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

// ValidateSourceStart checks the narrow source contract before a new job takes
// its first snapshot: the publication must include exactly the configured data
// table (governed by REPLICA IDENTITY FULL) plus seam_marker. A slot created
// after this check would leave a gap.
func ValidateSourceStart(ctx context.Context, dsn, slot, publication, table string) error {
	conn, err := transport.ConnectPostgres(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	var slotOK bool
	if err := conn.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_replication_slots
			WHERE slot_name = $1 AND slot_type = 'logical' AND plugin = 'pgoutput'
			  AND database = current_database() AND COALESCE(wal_status, 'reserved') != 'lost'
		)`, slot).Scan(&slotOK); err != nil {
		return fmt.Errorf("validate source slot: %w", err)
	}
	if !slotOK {
		return fmt.Errorf("source slot %q is missing, invalid, or belongs to another database; start capture before the job", slot)
	}
	var published int
	if err := conn.QueryRow(ctx, `
		SELECT COUNT(*) FROM pg_publication_tables
		WHERE pubname = $1 AND schemaname = 'public' AND tablename IN ($2, 'seam_marker')`, publication, table).Scan(&published); err != nil {
		return fmt.Errorf("validate source publication: %w", err)
	}
	if published != 2 {
		return fmt.Errorf("publication %q must include public.%s and public.seam_marker", publication, table)
	}
	var replicaIdentity string
	if err := conn.QueryRow(ctx, `SELECT relreplident::text FROM pg_class WHERE oid = $1::regclass`, fmt.Sprintf("public.%s", table)).Scan(&replicaIdentity); err != nil {
		return fmt.Errorf("validate source replica identity: %w", err)
	}
	if replicaIdentity != "f" {
		return fmt.Errorf("public.%s must use REPLICA IDENTITY FULL to reconstruct unchanged TOAST values", table)
	}
	return nil
}

// SourceSystemID identifies the physical PostgreSQL cluster. Failover within
// the same cluster preserves it; replacing the cluster must not reuse LSNs.
func SourceSystemID(ctx context.Context, dsn string) (string, error) {
	conn, err := transport.ConnectPostgres(ctx, dsn)
	if err != nil {
		return "", err
	}
	defer conn.Close(context.Background())
	var systemID string
	if err := conn.QueryRow(ctx, `SELECT system_identifier::text FROM pg_control_system()`).Scan(&systemID); err != nil {
		return "", err
	}
	return systemID, nil
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
	if resp.Err != nil && !errors.Is(resp.Err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("create topic %q: %w", topic, resp.Err)
	}
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("create topic %q: %w", topic, err)
	}
	return nil
}

package capture

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/seam/internal/model"
	"github.com/jackc/pglogrepl"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// This file exercises the reader's produce/shutdown path deterministically
// with a stubbed transactionProducer and hand-built pgoutput messages. No
// broker is involved: the tests gate ProduceSync by hand and release it with
// an error, reproducing the exact race an intentional Close() triggers while a
// source transaction is in flight.

// stubProducer is a transactionProducer whose ProduceSync behavior is fully
// controlled by the test:
//   - errs: errors returned per ProduceSync call, in order (the last error is
//     repeated once exhausted).
//   - gate: when non-nil, ProduceSync signals on entered and then blocks until
//     release is closed.
type stubProducer struct {
	mu      sync.Mutex
	records []*kgo.Record
	closed  bool

	entered chan struct{}
	release chan struct{}
	errs    []error
}

// newGateProducer returns a producer that blocks every ProduceSync on a
// release channel, and the channel it signals on when a produce begins.
func newGateProducer(errs ...error) (*stubProducer, chan struct{}) {
	s := &stubProducer{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
		errs:    errs,
	}
	if len(s.errs) == 0 {
		s.errs = []error{nil}
	}
	return s, s.entered
}

func newProducer(errs ...error) *stubProducer {
	s := &stubProducer{errs: errs}
	if len(s.errs) == 0 {
		s.errs = []error{nil}
	}
	return s
}

func (s *stubProducer) ProduceSync(_ context.Context, record *kgo.Record) error {
	s.mu.Lock()
	s.records = append(s.records, record)
	err := s.errs[0]
	if len(s.errs) > 1 {
		s.errs = s.errs[1:]
	}
	s.mu.Unlock()
	if s.entered != nil {
		select {
		case s.entered <- struct{}{}:
		default:
		}
	}
	if s.release != nil {
		<-s.release
	}
	return err
}

func (s *stubProducer) Close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
}

func (s *stubProducer) recordsSnapshot() []*kgo.Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*kgo.Record(nil), s.records...)
}

// accountsRelation is the pgoutput Relation message the decoder must see
// before any row change: public.accounts with the fixed id/owner/balance_cents
// schema, id flagged as the replica identity key.
func accountsRelation() *pglogrepl.RelationMessage {
	return &pglogrepl.RelationMessage{
		RelationID:      1,
		Namespace:       "public",
		RelationName:    "accounts",
		ReplicaIdentity: 'f',
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 20, Flags: 1},
			{Name: "owner", DataType: 25},
			{Name: "balance_cents", DataType: 20},
		},
	}
}

func textTuple(datas ...string) *pglogrepl.TupleData {
	cols := make([]*pglogrepl.TupleDataColumn, 0, len(datas))
	for _, d := range datas {
		cols = append(cols, &pglogrepl.TupleDataColumn{DataType: 't', Data: []byte(d)})
	}
	return &pglogrepl.TupleData{Columns: cols}
}

// seedDecoderTx drives the relation, begin, and insert messages through the
// reader so that sending the commit message publishes a complete transaction.
// The row is rendered as canonical text values matching the descriptor.
func seedDecoderTx(t *testing.T, ctx context.Context, r *Reader, row *model.Row) {
	t.Helper()
	const id = uint32(1)
	if err := r.handleMessage(ctx, accountsRelation(), 100); err != nil {
		t.Fatalf("relation: %v", err)
	}
	if err := r.handleMessage(ctx, &pglogrepl.BeginMessage{FinalLSN: 200, Xid: id}, 200); err != nil {
		t.Fatalf("begin: %v", err)
	}
	values := make([]string, len(row.Values))
	for i, v := range row.Values {
		switch v.Kind {
		case model.ValueInt64:
			values[i] = itoa(v.Int)
		case model.ValueText:
			values[i] = v.Text
		default:
			t.Fatalf("seedDecoderTx cannot encode value kind %d", v.Kind)
		}
	}
	if err := r.handleMessage(ctx, &pglogrepl.InsertMessage{RelationID: 1, Tuple: textTuple(values...)}, 300); err != nil {
		t.Fatalf("insert: %v", err)
	}
}

func commitTx(endLSN pglogrepl.LSN) *pglogrepl.CommitMessage {
	return &pglogrepl.CommitMessage{
		CommitLSN:         500,
		TransactionEndLSN: endLSN,
		CommitTime:        time.Unix(0, 0).UTC(),
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// TestReaderPublishOrderlyCancellationOnClose reproduces the deterministic
// shutdown race: a publish begins and blocks inside ProduceSync, Reader.Close
// runs concurrently, the blocked publish is released with the client-closed
// error, and publish reports context.Canceled instead of a spurious produce
// failure. It also proves the replication LSN does NOT advance while the
// produce is outstanding, and does not advance after the failed produce.
func TestReaderPublishOrderlyCancellationOnClose(t *testing.T) {
	producer, entered := newGateProducer(kgo.ErrClientClosed)
	reader := &Reader{
		cfg:      ReaderConfig{KafkaTopic: "seam.test"},
		producer: producer,
		decoder:  NewDecoder("gen:0", accountsDescriptor()),
		schemaID: accountsDescriptor().Fingerprint,
		codec:    JSONCodec{},
	}
	reader.runCtx, reader.runCancel = context.WithCancel(context.Background())

	const txEndLSN = pglogrepl.LSN(600)
	seedDecoderTx(t, context.Background(), reader, accountRow(1, "vik", 100))

	result := make(chan error, 1)
	go func() {
		result <- reader.handleMessage(context.Background(), commitTx(txEndLSN), 500)
	}()

	// Wait until the publish is in flight and blocked in ProduceSync.
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("publish never reached ProduceSync")
	}

	// While the produce is blocked, acknowledge nothing: the durable LSN stays.
	if reader.durableLSN != 0 {
		t.Fatalf("durableLSN advanced to %s while produce was still in flight", reader.durableLSN)
	}

	reader.Close() // intentional shutdown: cancels runCtx and closes the producer
	close(producer.release)

	err := <-result
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("handleMessage error = %v, want context.Canceled (orderly shutdown, not a produce failure)", err)
	}
	if reader.durableLSN != 0 {
		t.Fatalf("durableLSN advanced to %s despite the produce failing", reader.durableLSN)
	}
	if got := len(producer.recordsSnapshot()); got != 1 {
		t.Fatalf("produced %d records, want exactly 1 (the complete source transaction)", got)
	}
	if !producer.closed {
		t.Fatal("Close() did not close the producer")
	}
}

// TestReaderPublishRealErrorIsFatal verifies that a real Kafka error with no
// cancellation stays fatal: handleMessage returns it and the durable LSN does
// not advance (the source transaction is not acknowledged).
func TestReaderPublishRealErrorIsFatal(t *testing.T) {
	producer := newProducer(kerr.MessageTooLarge)
	reader := &Reader{
		cfg:      ReaderConfig{KafkaTopic: "seam.test"},
		producer: producer,
		decoder:  NewDecoder("gen:0", accountsDescriptor()),
		schemaID: accountsDescriptor().Fingerprint,
		codec:    JSONCodec{},
	}
	reader.runCtx, reader.runCancel = context.WithCancel(context.Background())
	defer reader.Close()

	const txEndLSN = pglogrepl.LSN(600)
	seedDecoderTx(t, context.Background(), reader, accountRow(1, "vik", 100))

	err := reader.handleMessage(context.Background(), commitTx(txEndLSN), 500)
	if err == nil {
		t.Fatal("expected a fatal produce error, got nil")
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("fatal produce error was misreported as cancellation: %v", err)
	}
	if !strings.Contains(err.Error(), "produce to kafka") {
		t.Fatalf("error %q does not name the produce step", err)
	}
	if reader.durableLSN != 0 {
		t.Fatalf("durableLSN advanced to %s despite the produce failing; source WAL must not be acknowledged", reader.durableLSN)
	}
}

// TestReaderPublishRetriesTransientThenAdvances verifies the retry contract:
// a retriable Kafka error is retried without reporting cancellation, and the
// durable LSN advances only after a produce eventually succeeds.
func TestReaderPublishRetriesTransientThenAdvances(t *testing.T) {
	producer := newProducer(kerr.NotLeaderForPartition, nil)
	reader := &Reader{
		cfg:      ReaderConfig{KafkaTopic: "seam.test"},
		producer: producer,
		decoder:  NewDecoder("gen:0", accountsDescriptor()),
		schemaID: accountsDescriptor().Fingerprint,
		codec:    JSONCodec{},
	}
	reader.runCtx, reader.runCancel = context.WithCancel(context.Background())
	defer reader.Close()

	const txEndLSN = pglogrepl.LSN(600)
	seedDecoderTx(t, context.Background(), reader, accountRow(1, "vik", 100))

	err := reader.handleMessage(context.Background(), commitTx(txEndLSN), 500)
	if err != nil {
		t.Fatalf("second attempt should have succeeded: %v", err)
	}
	if reader.durableLSN != txEndLSN {
		t.Fatalf("durableLSN = %s, want %s", reader.durableLSN, txEndLSN)
	}
	if got := len(producer.recordsSnapshot()); got != 2 {
		t.Fatalf("produce attempts = %d, want 2 (one retriable failure, one success)", got)
	}
}

// TestReaderPublishAdvancesLSNOnlyAfterDurableAck verifies the durable-publish
// invariant end to end on the success path: the reader produces exactly one
// record carrying the complete source transaction, and the durable LSN equals
// the transaction end LSN only once that record was acknowledged.
func TestReaderPublishAdvancesLSNOnlyAfterDurableAck(t *testing.T) {
	producer := newProducer()
	reader := &Reader{
		cfg:      ReaderConfig{KafkaTopic: "seam.test"},
		producer: producer,
		decoder:  NewDecoder("gen:0", accountsDescriptor()),
		schemaID: accountsDescriptor().Fingerprint,
		codec:    JSONCodec{},
	}
	reader.runCtx, reader.runCancel = context.WithCancel(context.Background())
	defer reader.Close()

	const txEndLSN = pglogrepl.LSN(600)
	seedDecoderTx(t, context.Background(), reader, accountRow(7, "alice", 450))

	if err := reader.handleMessage(context.Background(), commitTx(txEndLSN), 500); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if reader.durableLSN != txEndLSN {
		t.Fatalf("durableLSN = %s, want %s", reader.durableLSN, txEndLSN)
	}

	records := producer.recordsSnapshot()
	if len(records) != 1 {
		t.Fatalf("produced %d records, want exactly 1 for the complete transaction", len(records))
	}
	var envelope model.TransactionEnvelope
	if err := json.Unmarshal(records[0].Value, &envelope); err != nil {
		t.Fatalf("decode produced record: %v", err)
	}
	if envelope.Count != 1 || len(envelope.Changes) != 1 {
		t.Fatalf("envelope has %d changes (count=%d), want 1", len(envelope.Changes), envelope.Count)
	}
	change := envelope.Changes[0]
	if change.Op != model.OpInsert || change.Row == nil {
		t.Fatalf("change = %+v, want a single insert row change", change)
	}
	values := change.Row.Values
	if len(values) != 3 ||
		values[0].Kind != model.ValueInt64 || values[0].Int != 7 ||
		values[1].Kind != model.ValueText || values[1].Text != "alice" ||
		values[2].Kind != model.ValueInt64 || values[2].Int != 450 {
		t.Fatalf("change row = %+v, want id 7/alice/450", change.Row)
	}
	if envelope.SchemaID != accountsDescriptor().Fingerprint {
		t.Fatalf("envelope schema epoch = %q, want pinned fingerprint %q", envelope.SchemaID, accountsDescriptor().Fingerprint)
	}
	if string(records[0].Key) != envelope.Source.String() {
		t.Fatalf("record key %q does not identify source transaction %q", records[0].Key, envelope.Source)
	}
}

func TestReaderFragmentsLargeTransactionAndRemovesSpill(t *testing.T) {
	producer := newProducer()
	reader := &Reader{
		cfg:      ReaderConfig{KafkaTopic: "seam.test"},
		producer: producer,
		decoder:  NewDecoder("gen:0", accountsDescriptor()),
		schemaID: accountsDescriptor().Fingerprint,
		codec:    JSONCodec{},
	}
	reader.runCtx, reader.runCancel = context.WithCancel(context.Background())
	defer reader.Close()
	reader.decoder.SetSystemID("source-1")
	reader.decoder.MaxTransactionBytes = 1024 // force disk spill well before commit

	ctx := context.Background()
	if err := reader.handleMessage(ctx, accountsRelation(), 100); err != nil {
		t.Fatal(err)
	}
	if err := reader.handleMessage(ctx, &pglogrepl.BeginMessage{FinalLSN: 200, Xid: 42}, 200); err != nil {
		t.Fatal(err)
	}
	const changes = 40
	owner := strings.Repeat("wide-row-", 4096)
	for i := 0; i < changes; i++ {
		if err := reader.handleMessage(ctx, &pglogrepl.InsertMessage{RelationID: 1, Tuple: textTuple(itoa(int64(i+1)), owner, "1")}, 300); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	if reader.decoder.transaction.spill == nil {
		t.Fatal("large open transaction did not spill to disk")
	}
	spillName := reader.decoder.transaction.spill.Name()
	const endLSN = pglogrepl.LSN(601)
	if err := reader.handleMessage(ctx, commitTx(endLSN), 500); err != nil {
		t.Fatal(err)
	}
	if reader.durableLSN != endLSN {
		t.Fatalf("durable LSN=%s, want %s", reader.durableLSN, endLSN)
	}
	if _, err := os.Stat(spillName); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("transaction spill was not removed: %v", err)
	}

	records := producer.recordsSnapshot()
	if len(records) < 2 {
		t.Fatalf("large transaction produced %d records, want multiple fragments", len(records))
	}
	total := 0
	for i, record := range records {
		if len(record.Value) > maxKafkaTransactionBytes {
			t.Fatalf("fragment %d is %d bytes, limit %d", i, len(record.Value), maxKafkaTransactionBytes)
		}
		var envelope model.TransactionEnvelope
		if err := json.Unmarshal(record.Value, &envelope); err != nil {
			t.Fatal(err)
		}
		if envelope.Version != 2 || envelope.FragmentIndex != i {
			t.Fatalf("fragment %d metadata=%+v", i, envelope)
		}
		if envelope.Final != (i == len(records)-1) {
			t.Fatalf("fragment %d final=%v", i, envelope.Final)
		}
		total += envelope.Count
	}
	if total != changes {
		t.Fatalf("fragments contain %d changes, want %d", total, changes)
	}
}

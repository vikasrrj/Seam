// Package kafka provides a minimal single-partition consumer for Seam.
package kafka

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"example.com/seam/internal/model"
	"example.com/seam/internal/retry"
	"example.com/seam/internal/transport"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// DecodeFunc converts Kafka record bytes into a Seam change.
type DecodeFunc func([]byte) (model.Change, error)

// Consumer reads from exactly one topic and one partition.
type Consumer struct {
	client                     *kgo.Client
	topic                      string
	decode                     DecodeFunc
	maxPollRecords             int
	expectedTopicID            string
	lastTopicCheck             time.Time
	assembly                   fragmentAssembly
	allowLeadingFragmentSuffix bool
}

// ConsumerOption tunes the consumer's bounded buffering.
type ConsumerOption func(*consumerOptions)

type consumerOptions struct {
	maxPollRecords             int
	expectedTopicID            string
	allowLeadingFragmentSuffix bool
}

// WithExpectedTopicID rejects a topic that has been deleted and recreated
// while a job is running, even when its name and offsets appear unchanged.
func WithExpectedTopicID(id string) ConsumerOption {
	return func(o *consumerOptions) { o.expectedTopicID = id }
}

// WithMaxPollRecords bounds how many records a single Poll returns. It is the
// durable memory frontier between Kafka and Seam: no matter how busy the topic
// is, at most this many decoded records are ever buffered at once. Zero keeps
// the default of 100.
func WithMaxPollRecords(n int) ConsumerOption {
	return func(o *consumerOptions) { o.maxPollRecords = n }
}

// withLeadingFragmentSuffix is used only by barrier scans whose sampled start
// offset may fall inside a concurrently published fragmented transaction.
// Durable job consumers must start at transaction boundaries and never set it.
func withLeadingFragmentSuffix() ConsumerOption {
	return func(o *consumerOptions) { o.allowLeadingFragmentSuffix = true }
}

const defaultMaxPollRecords = 100
const maxEnvelopeBytes = 1 << 20

// NewConsumer creates a consumer that starts at the given absolute offset.
func NewConsumer(brokers []string, topic string, startOffset int64, decode DecodeFunc, opts ...ConsumerOption) (*Consumer, error) {
	o := consumerOptions{maxPollRecords: defaultMaxPollRecords}
	for _, opt := range opts {
		opt(&o)
	}
	if o.maxPollRecords <= 0 {
		o.maxPollRecords = defaultMaxPollRecords
	}
	batch := o.maxPollRecords
	optsKgo := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().At(startOffset)},
		}),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()),
		kgo.FetchMinBytes(1),
		kgo.FetchMaxBytes(4 << 20),
		kgo.FetchMaxPartitionBytes(maxEnvelopeBytes),
	}
	optsKgo = append(optsKgo, transport.KafkaOptions()...)
	client, err := kgo.NewClient(optsKgo...)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	return &Consumer{
		client: client, topic: topic, decode: decode, maxPollRecords: batch,
		expectedTopicID: o.expectedTopicID, allowLeadingFragmentSuffix: o.allowLeadingFragmentSuffix,
	}, nil
}

// Record is one Kafka record decoded into a Seam change.
type Record struct {
	Offset int64
	Change model.Change
}

// Transaction is one complete source transaction backed by either one
// bounded Kafka envelope or a temporary file of bounded fragments. Walk may
// be called more than once; each call decodes at most one fragment at a time.
// The owner must call Close after the transaction has been committed or
// discarded.
type Transaction struct {
	Source      model.SourceTx
	FirstOffset int64
	FinalOffset int64
	TotalCount  int

	decode  DecodeFunc
	payload []byte
	file    *os.File
}

// Walk visits bounded batches in source order. Records use FinalOffset so the
// durable checkpoint advances past the entire fragmented transaction only
// after its destination transaction commits.
func (t *Transaction) Walk(fn func([]Record) error) error {
	if t == nil {
		return fmt.Errorf("nil transaction")
	}
	if fn == nil {
		return fmt.Errorf("nil transaction visitor")
	}
	seen := 0
	visit := func(payload []byte) error {
		fragment, err := decodeFragment(payload, t.decode)
		if err != nil {
			return err
		}
		if fragment.source != t.Source || fragment.total != t.TotalCount {
			return fmt.Errorf("transaction metadata changed while streaming %s", t.Source)
		}
		records := make([]Record, len(fragment.changes))
		for i, change := range fragment.changes {
			records[i] = Record{Offset: t.FinalOffset, Change: change}
		}
		seen += len(records)
		return fn(records)
	}
	if t.file == nil {
		if err := visit(t.payload); err != nil {
			return err
		}
	} else {
		if _, err := t.file.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("rewind transaction %s: %w", t.Source, err)
		}
		reader := bufio.NewReader(t.file)
		for {
			var size uint32
			if err := binary.Read(reader, binary.BigEndian, &size); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return fmt.Errorf("read transaction %s fragment length: %w", t.Source, err)
			}
			payload := make([]byte, int(size))
			if _, err := io.ReadFull(reader, payload); err != nil {
				return fmt.Errorf("read transaction %s fragment: %w", t.Source, err)
			}
			if err := visit(payload); err != nil {
				return err
			}
		}
	}
	if seen != t.TotalCount {
		return fmt.Errorf("transaction %s streamed %d changes, expected %d", t.Source, seen, t.TotalCount)
	}
	return nil
}

// Records materializes a transaction for compatibility with older callers.
// Production reconciliation uses Walk and does not call this method.
func (t *Transaction) Records() ([]Record, error) {
	records := make([]Record, 0, t.TotalCount)
	err := t.Walk(func(batch []Record) error {
		records = append(records, batch...)
		return nil
	})
	return records, err
}

// Close removes any temporary fragment file. It is idempotent.
func (t *Transaction) Close() {
	if t == nil || t.file == nil {
		return
	}
	name := t.file.Name()
	_ = t.file.Close()
	_ = os.Remove(name)
	t.file = nil
}

// Poll returns the next batch of decoded records, or (nil, nil) on graceful
// shutdown.
func (c *Consumer) Poll(ctx context.Context) ([]Record, error) {
	transactions, err := c.PollTransactions(ctx)
	if err != nil {
		return nil, err
	}
	var records []Record
	for _, transaction := range transactions {
		batch, materializeErr := transaction.Records()
		transaction.Close()
		if materializeErr != nil {
			return nil, materializeErr
		}
		records = append(records, batch...)
	}
	return records, nil
}

// PollTransactions returns complete replayable source transactions without
// materializing fragmented transactions. An incomplete prefix remains owned
// by the consumer and is never exposed or checkpointed.
func (c *Consumer) PollTransactions(ctx context.Context) ([]*Transaction, error) {
	return retry.Do(ctx, retry.DefaultConfig(), func() ([]*Transaction, error) {
		return c.pollTransactionsOnce(ctx)
	}, retry.IsRetryableKafka)
}

func (c *Consumer) pollTransactionsOnce(ctx context.Context) ([]*Transaction, error) {
	if c.expectedTopicID != "" && time.Since(c.lastTopicCheck) >= 10*time.Second {
		actual, err := topicIdentityFromClient(ctx, c.client, c.topic)
		if err != nil {
			return nil, err
		}
		if actual != c.expectedTopicID {
			return nil, fmt.Errorf("Kafka topic %q identity changed from %s to %s; resnapshot required", c.topic, c.expectedTopicID, actual)
		}
		c.lastTopicCheck = time.Now()
	}
	fetches := c.client.PollRecords(ctx, c.maxPollRecords)
	if fetches.IsClientClosed() {
		return nil, nil
	}
	if errs := fetches.Errors(); len(errs) > 0 {
		for _, err := range errs {
			if errors.Is(err.Err, context.Canceled) {
				return nil, nil
			}
		}
		return nil, fmt.Errorf("kafka fetch errors: %v", errs)
	}
	iter := fetches.RecordIter()
	var transactions []*Transaction
	for !iter.Done() {
		r := iter.Next()
		assembled, err := c.assembly.pushTransaction(r.Offset, r.Value, c.decode, c.allowLeadingFragmentSuffix)
		if err != nil {
			for _, transaction := range transactions {
				transaction.Close()
			}
			return nil, fmt.Errorf("decode transaction at offset %d: %w", r.Offset, err)
		}
		if assembled != nil {
			transactions = append(transactions, assembled)
		}
	}
	return transactions, nil
}

type decodedFragment struct {
	version int
	source  model.SourceTx
	index   int
	final   bool
	total   int
	changes []model.Change
}

func decodeFragment(data []byte, decode DecodeFunc) (decodedFragment, error) {
	if len(data) > maxEnvelopeBytes {
		return decodedFragment{}, fmt.Errorf("transaction envelope is %d bytes; limit is %d", len(data), maxEnvelopeBytes)
	}
	var wire struct {
		Version       int
		Source        model.SourceTx
		SchemaID      string
		FragmentIndex int
		Final         bool
		Count         int
		TotalCount    int
		Changes       []json.RawMessage
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return decodedFragment{}, err
	}
	if (wire.Version != 1 && wire.Version != 2) || wire.Source.SystemID == "" || wire.Source.Generation == "" || wire.Source.LSN == "" {
		return decodedFragment{}, fmt.Errorf("unsupported or incomplete transaction envelope version %d", wire.Version)
	}
	if wire.Count <= 0 || wire.Count != len(wire.Changes) {
		return decodedFragment{}, fmt.Errorf("transaction %s count %d does not match %d changes", wire.Source, wire.Count, len(wire.Changes))
	}
	if wire.Version == 1 {
		wire.Final = true
		wire.TotalCount = wire.Count
	}
	if wire.FragmentIndex < 0 || wire.TotalCount < wire.Count || wire.TotalCount <= 0 {
		return decodedFragment{}, fmt.Errorf("transaction %s has invalid fragment metadata index=%d count=%d total=%d", wire.Source, wire.FragmentIndex, wire.Count, wire.TotalCount)
	}
	changes := make([]model.Change, 0, wire.Count)
	for i, raw := range wire.Changes {
		change, err := decode(raw)
		if err != nil {
			return decodedFragment{}, fmt.Errorf("change %d: %w", i, err)
		}
		if change.Source != wire.Source {
			return decodedFragment{}, fmt.Errorf("transaction %s change %d has source %s", wire.Source, i, change.Source)
		}
		if change.Row == nil && change.Marker == nil {
			return decodedFragment{}, fmt.Errorf("transaction %s change %d has no payload", wire.Source, i)
		}
		if change.SchemaID != "" && wire.SchemaID != "" && change.SchemaID != wire.SchemaID {
			return decodedFragment{}, fmt.Errorf("transaction %s change %d has schema epoch %q; envelope epoch %q", wire.Source, i, change.SchemaID, wire.SchemaID)
		}
		changes = append(changes, change)
	}
	return decodedFragment{version: wire.Version, source: wire.Source, index: wire.FragmentIndex, final: wire.Final, total: wire.TotalCount, changes: changes}, nil
}

func decodeEnvelope(data []byte, decode DecodeFunc) ([]model.Change, error) {
	fragment, err := decodeFragment(data, decode)
	if err != nil {
		return nil, err
	}
	if fragment.index != 0 || !fragment.final || len(fragment.changes) != fragment.total {
		return nil, fmt.Errorf("transaction %s envelope is only fragment %d; assembly required", fragment.source, fragment.index)
	}
	return fragment.changes, nil
}

// fragmentAssembly keeps incomplete transaction bytes off heap. Its temp file
// is intentionally disposable: the durable recovery source is Kafka and the
// destination checkpoint does not advance until Final is assembled. A crash
// therefore replays fragment zero and reconstructs the file.
type fragmentAssembly struct {
	file        *os.File
	source      model.SourceTx
	expected    int
	totalCount  int
	firstOffset int64
	lastOffset  int64
}

func (a *fragmentAssembly) push(offset int64, data []byte, decode DecodeFunc, allowLeadingSuffix ...bool) ([]Record, error) {
	transaction, err := a.pushTransaction(offset, data, decode, allowLeadingSuffix...)
	if err != nil || transaction == nil {
		return nil, err
	}
	defer transaction.Close()
	return transaction.Records()
}

func (a *fragmentAssembly) pushTransaction(offset int64, data []byte, decode DecodeFunc, allowLeadingSuffix ...bool) (*Transaction, error) {
	fragment, err := decodeFragment(data, decode)
	if err != nil {
		return nil, err
	}
	if fragment.version == 1 {
		if a.file != nil {
			return nil, fmt.Errorf("complete envelope followed an incomplete fragmented transaction %s", a.source)
		}
		return &Transaction{Source: fragment.source, FirstOffset: offset, FinalOffset: offset,
			TotalCount: fragment.total, decode: decode, payload: append([]byte(nil), data...)}, nil
	}
	if fragment.index == 0 {
		a.reset()
		file, err := os.CreateTemp("", "seam-kafka-tx-*")
		if err != nil {
			return nil, fmt.Errorf("create fragment assembly: %w", err)
		}
		a.file = file
		a.source = fragment.source
		a.totalCount = fragment.total
		a.expected = 0
		a.firstOffset = offset
	} else if a.file == nil {
		if len(allowLeadingSuffix) > 0 && allowLeadingSuffix[0] {
			return nil, nil
		}
		return nil, fmt.Errorf("fragment %d for transaction %s has no fragment zero", fragment.index, fragment.source)
	}
	if fragment.source != a.source || fragment.index != a.expected || fragment.total != a.totalCount {
		return nil, fmt.Errorf("non-contiguous transaction fragment: got %s index=%d total=%d, expected %s index=%d total=%d", fragment.source, fragment.index, fragment.total, a.source, a.expected, a.totalCount)
	}
	if fragment.index > 0 && offset != a.lastOffset+1 {
		return nil, fmt.Errorf("transaction %s fragment offsets are not contiguous: got %d after %d", fragment.source, offset, a.lastOffset)
	}
	if err := writeAssemblyRecord(a.file, data); err != nil {
		a.reset()
		return nil, err
	}
	a.expected++
	a.lastOffset = offset
	if !fragment.final {
		return nil, nil
	}
	return a.finishTransaction(offset, decode)
}

func (a *fragmentAssembly) finishTransaction(finalOffset int64, decode DecodeFunc) (*Transaction, error) {
	transaction := &Transaction{Source: a.source, FirstOffset: a.firstOffset, FinalOffset: finalOffset,
		TotalCount: a.totalCount, decode: decode, file: a.file}
	a.file = nil
	*a = fragmentAssembly{}
	return transaction, nil
}

func writeAssemblyRecord(file *os.File, payload []byte) error {
	if len(payload) > int(^uint32(0)) {
		return fmt.Errorf("fragment too large to stage: %d bytes", len(payload))
	}
	if err := binary.Write(file, binary.BigEndian, uint32(len(payload))); err != nil {
		return fmt.Errorf("write fragment length: %w", err)
	}
	if _, err := file.Write(payload); err != nil {
		return fmt.Errorf("write fragment: %w", err)
	}
	return nil
}

func (a *fragmentAssembly) reset() {
	if a.file != nil {
		name := a.file.Name()
		_ = a.file.Close()
		_ = os.Remove(name)
	}
	*a = fragmentAssembly{}
}

// Close releases the consumer.
func (c *Consumer) Close() {
	c.assembly.reset()
	c.client.Close()
}

// TopicIdentity returns Kafka's stable topic ID and enforces the one-partition
// ordering contract. A reused name with a new ID is a retention gap, even if
// offsets happen to look plausible.
func TopicIdentity(ctx context.Context, brokers []string, topic string) (string, error) {
	client, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, transport.KafkaOptions()...)...)
	if err != nil {
		return "", err
	}
	defer client.Close()
	return topicIdentityFromClient(ctx, client, topic)
}

func topicIdentityFromClient(ctx context.Context, client *kgo.Client, topic string) (string, error) {
	admin := kadm.NewClient(client)
	details, err := admin.ListTopics(ctx, topic)
	if err != nil {
		return "", err
	}
	detail, ok := details[topic]
	if !ok || detail.Err != nil {
		return "", fmt.Errorf("Kafka topic %q missing or unavailable: %v", topic, detail.Err)
	}
	if len(detail.Partitions) != 1 {
		return "", fmt.Errorf("Kafka topic %q has %d partitions; SEAM requires exactly one", topic, len(detail.Partitions))
	}
	partition, ok := detail.Partitions[0]
	if !ok {
		return "", fmt.Errorf("Kafka topic %q has no partition zero", topic)
	}
	if partition.Err != nil {
		return "", fmt.Errorf("Kafka topic %q partition zero unavailable: %w", topic, partition.Err)
	}
	if detail.ID == (kadm.TopicID{}) {
		return "", fmt.Errorf("Kafka topic %q does not expose a stable topic ID", topic)
	}
	return detail.ID.String(), nil
}

// EarliestOffset returns the earliest offset currently retained for the topic's
// first partition. It is used to hard-fail recovery when checkpointed history
// has fallen out of retention.
func EarliestOffset(ctx context.Context, brokers []string, topic string) (int64, error) {
	return topicOffset(ctx, brokers, topic, false)
}

// EndOffset is the next append position of partition zero. Capture callers
// sample it before writing a unique source barrier, avoiding an old topic scan.
func EndOffset(ctx context.Context, brokers []string, topic string) (int64, error) {
	return topicOffset(ctx, brokers, topic, true)
}

func topicOffset(ctx context.Context, brokers []string, topic string, end bool) (int64, error) {
	client, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
	}, transport.KafkaOptions()...)...)
	if err != nil {
		return 0, fmt.Errorf("create kafka client: %w", err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	var offsets kadm.ListedOffsets
	if end {
		offsets, err = admin.ListEndOffsets(ctx, topic)
	} else {
		offsets, err = admin.ListStartOffsets(ctx, topic)
	}
	if err != nil {
		return 0, fmt.Errorf("list start offsets: %w", err)
	}
	o, ok := offsets.Lookup(topic, 0)
	if !ok || o.Err != nil {
		return 0, fmt.Errorf("topic %q not found", topic)
	}
	return o.Offset, nil
}

// WaitForBarrier proves that a marker committed on the source reached the
// ordered Kafka stream. The returned cursor starts immediately after its
// complete transaction envelope.
func WaitForBarrier(ctx context.Context, brokers []string, topic, markerID string, startOffset int64, decode DecodeFunc) (int64, error) {
	consumer, err := NewConsumer(brokers, topic, startOffset, decode, withLeadingFragmentSuffix())
	if err != nil {
		return 0, err
	}
	defer consumer.Close()
	for {
		records, err := consumer.Poll(ctx)
		if err != nil {
			return 0, err
		}
		for _, record := range records {
			if record.Change.Marker != nil && record.Change.Marker.ID == markerID && record.Change.Marker.Kind == model.MarkerBarrier {
				return record.Offset + 1, nil
			}
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if records == nil {
			return 0, fmt.Errorf("Kafka consumer stopped before marker %q was observed", markerID)
		}
	}
}

// Package kafka provides a minimal single-partition consumer for Seam.
package kafka

import (
	"context"
	"errors"
	"fmt"

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
	client         *kgo.Client
	topic          string
	decode         DecodeFunc
	maxPollRecords int
}

// ConsumerOption tunes the consumer's bounded buffering.
type ConsumerOption func(*consumerOptions)

type consumerOptions struct{ maxPollRecords int }

// WithMaxPollRecords bounds how many records a single Poll returns. It is the
// durable memory frontier between Kafka and Seam: no matter how busy the topic
// is, at most this many decoded records are ever buffered at once. Zero keeps
// the default of 100.
func WithMaxPollRecords(n int) ConsumerOption {
	return func(o *consumerOptions) { o.maxPollRecords = n }
}

const defaultMaxPollRecords = 100

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
		kgo.FetchMinBytes(1),
		kgo.FetchMaxBytes(10 << 20),
		kgo.MaxBufferedRecords(batch * 10),
	}
	optsKgo = append(optsKgo, transport.KafkaOptions()...)
	client, err := kgo.NewClient(optsKgo...)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	return &Consumer{client: client, topic: topic, decode: decode, maxPollRecords: batch}, nil
}

// Record is one Kafka record decoded into a Seam change.
type Record struct {
	Offset int64
	Change model.Change
}

// Poll returns the next batch of decoded records, or (nil, nil) on graceful
// shutdown.
func (c *Consumer) Poll(ctx context.Context) ([]Record, error) {
	return retry.Do(ctx, retry.DefaultConfig(), func() ([]Record, error) {
		return c.pollOnce(ctx)
	}, retry.IsRetryableKafka)
}

func (c *Consumer) pollOnce(ctx context.Context) ([]Record, error) {
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
	var records []Record
	for !iter.Done() {
		r := iter.Next()
		change, err := c.decode(r.Value)
		if err != nil {
			return nil, fmt.Errorf("decode record at offset %d: %w", r.Offset, err)
		}
		records = append(records, Record{Offset: r.Offset, Change: change})
	}
	return records, nil
}

// Close releases the consumer.
func (c *Consumer) Close() {
	c.client.Close()
}

// EarliestOffset returns the earliest offset currently retained for the topic's
// first partition. It is used to hard-fail recovery when checkpointed history
// has fallen out of retention.
func EarliestOffset(ctx context.Context, brokers []string, topic string) (int64, error) {
	client, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
	}, transport.KafkaOptions()...)...)
	if err != nil {
		return 0, fmt.Errorf("create kafka client: %w", err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	offsets, err := admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("list start offsets: %w", err)
	}
	o, ok := offsets.Lookup(topic, 0)
	if !ok || o.Err != nil {
		return 0, fmt.Errorf("topic %q not found", topic)
	}
	return o.Offset, nil
}

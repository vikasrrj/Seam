package capture

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// transactionProducer is the narrow Kafka seam the capture reader needs: it
// must durably produce one record (a complete source transaction) and close.
// The production implementation wraps a franz-go client; deterministic tests
// substitute a stub to exercise the in-flight-publish shutdown path without a
// broker. This is intentionally the smallest abstraction — not a mocked Kafka.
type transactionProducer interface {
	// ProduceSync blocks until the record is acknowledged by the broker, the
	// context ends, or a fatal error occurs. It returns the record-level
	// error, if any; nil means the broker durably acknowledged the record.
	ProduceSync(ctx context.Context, record *kgo.Record) error
	// Close releases the underlying transport. An in-flight ProduceSync fails
	// with kgo.ErrClientClosed once the client is closed.
	Close()
}

// kafkaProducer adapts a franz-go kgo.Client to transactionProducer.
type kafkaProducer struct {
	client *kgo.Client
}

func (p *kafkaProducer) ProduceSync(ctx context.Context, record *kgo.Record) error {
	_, err := p.client.ProduceSync(ctx, record).First()
	return err
}

func (p *kafkaProducer) Close() {
	p.client.Close()
}

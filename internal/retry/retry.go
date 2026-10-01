// Package retry provides bounded exponential backoff for transient errors.
package retry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/twmb/franz-go/pkg/kerr"
)

// Config controls retry behavior.
type Config struct {
	MaxAttempts int
	Initial     time.Duration
	Max         time.Duration
	Multiplier  float64
}

// DefaultConfig is suitable for startup and steady-state network calls.
func DefaultConfig() Config {
	return Config{
		MaxAttempts: 5,
		Initial:     100 * time.Millisecond,
		Max:         5 * time.Second,
		Multiplier:  2.0,
	}
}

// Do executes op until it succeeds, ctx is cancelled, or MaxAttempts is reached.
// isRetryable should return true only for transient errors.
func Do[T any](ctx context.Context, cfg Config, op func() (T, error), isRetryable func(error) bool) (T, error) {
	var zero T
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 1
	}
	delay := cfg.Initial
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		val, err := op()
		if err == nil {
			return val, nil
		}
		if attempt == cfg.MaxAttempts || !isRetryable(err) || ctx.Err() != nil {
			return zero, err
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return zero, ctx.Err()
		}
		delay = time.Duration(float64(delay) * cfg.Multiplier)
		if delay > cfg.Max {
			delay = cfg.Max
		}
	}
	return zero, fmt.Errorf("retry exhausted")
}

// DoVoid is Do for operations with no return value.
func DoVoid(ctx context.Context, cfg Config, op func() error, isRetryable func(error) bool) error {
	_, err := Do(ctx, cfg, func() (struct{}, error) {
		return struct{}{}, op()
	}, isRetryable)
	return err
}

// IsRetryablePG returns true for transient PostgreSQL/network errors.
func IsRetryablePG(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if pgconn.Timeout(err) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	return false
}

// IsRetryableKafka returns true for transient Kafka broker/network errors.
func IsRetryableKafka(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var ke *kerr.Error
	if errors.As(err, &ke) && ke.Retriable {
		return true
	}
	return false
}

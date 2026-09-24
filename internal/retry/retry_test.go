package retry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.MaxAttempts != 5 {
		t.Errorf("MaxAttempts = %d, want 5", cfg.MaxAttempts)
	}
	if cfg.Initial != 100*time.Millisecond || cfg.Max != 5*time.Second {
		t.Errorf("backoff window = %v..%v, want 100ms..5s", cfg.Initial, cfg.Max)
	}
	if cfg.Multiplier != 2.0 {
		t.Errorf("Multiplier = %v, want 2.0", cfg.Multiplier)
	}
}

func TestDoSucceedsFirstAttempt(t *testing.T) {
	ctx := context.Background()
	cfg := Config{MaxAttempts: 3, Initial: time.Millisecond, Max: time.Millisecond, Multiplier: 2}
	calls := 0
	got, err := Do(ctx, cfg, func() (int, error) {
		calls++
		return 42, nil
	}, func(error) bool { return true })
	if err != nil {
		t.Fatalf("Do returned err %v", err)
	}
	if got != 42 {
		t.Fatalf("Do returned %d, want 42", got)
	}
	if calls != 1 {
		t.Fatalf("op called %d times, want 1", calls)
	}
}

func TestDoRetriesThenSucceeds(t *testing.T) {
	ctx := context.Background()
	cfg := Config{MaxAttempts: 3, Initial: time.Millisecond, Max: time.Millisecond, Multiplier: 2}
	calls := 0
	got, err := Do(ctx, cfg, func() (int, error) {
		calls++
		if calls < 3 {
			return 0, errors.New("transient")
		}
		return 7, nil
	}, func(error) bool { return true })
	if err != nil {
		t.Fatalf("Do returned err %v", err)
	}
	if got != 7 || calls != 3 {
		t.Fatalf("got %d calls %d, want 7 and 3", got, calls)
	}
}

func TestDoStopsOnPermanentError(t *testing.T) {
	ctx := context.Background()
	cfg := Config{MaxAttempts: 5, Initial: time.Millisecond, Max: time.Millisecond, Multiplier: 2}
	calls := 0
	_, err := Do(ctx, cfg, func() (int, error) {
		calls++
		return 0, errors.New("permanent")
	}, func(error) bool { return false })
	if err == nil || err.Error() != "permanent" {
		t.Fatalf("err = %v, want permanent", err)
	}
	if calls != 1 {
		t.Fatalf("op called %d times, want 1 (no retry for permanent errors)", calls)
	}
}

func TestDoExhaustsAttempts(t *testing.T) {
	ctx := context.Background()
	cfg := Config{MaxAttempts: 3, Initial: time.Millisecond, Max: time.Millisecond, Multiplier: 2}
	calls := 0
	_, err := Do(ctx, cfg, func() (int, error) {
		calls++
		return 0, errors.New("still failing")
	}, func(error) bool { return true })
	if err == nil || err.Error() != "still failing" {
		t.Fatalf("err = %v, want still failing", err)
	}
	if calls != 3 {
		t.Fatalf("op called %d times, want 3", calls)
	}
}

func TestDoRespectsContextCancellation(t *testing.T) {
	cfg := Config{MaxAttempts: 5, Initial: time.Second, Max: time.Second, Multiplier: 2}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { time.Sleep(10 * time.Millisecond); cancel() }()
	_, err := Do(ctx, cfg, func() (int, error) {
		return 0, errors.New("transient")
	}, func(error) bool { return true })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestDoBackoffCapsAtMax(t *testing.T) {
	ctx := context.Background()
	// Initial is tiny but the multiplier would explode the delay; the cap must
	// keep each backoff at Max. Without the cap this test would sleep ~1s.
	cfg := Config{MaxAttempts: 2, Initial: time.Millisecond, Max: time.Millisecond, Multiplier: 1000}
	start := time.Now()
	_, err := Do(ctx, cfg, func() (int, error) { return 0, errors.New("x") }, func(error) bool { return true })
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected an error")
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("backoff not capped at Max: elapsed %v", elapsed)
	}
}

func TestDoZeroMaxAttemptsDefaultsToOne(t *testing.T) {
	ctx := context.Background()
	cfg := Config{MaxAttempts: 0, Initial: time.Millisecond, Max: time.Millisecond, Multiplier: 2}
	calls := 0
	_, err := Do(ctx, cfg, func() (int, error) {
		calls++
		return 0, errors.New("x")
	}, func(error) bool { return true })
	if err == nil || calls != 1 {
		t.Fatalf("err = %v calls = %d, want error with 1 call", err, calls)
	}
}

func TestDoVoid(t *testing.T) {
	ctx := context.Background()
	cfg := Config{MaxAttempts: 2, Initial: time.Millisecond, Max: time.Millisecond, Multiplier: 2}
	calls := 0
	err := DoVoid(ctx, cfg, func() error {
		calls++
		if calls == 1 {
			return errors.New("transient")
		}
		return nil
	}, func(error) bool { return true })
	if err != nil {
		t.Fatalf("DoVoid err = %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestIsRetryablePG(t *testing.T) {
	if IsRetryablePG(nil) {
		t.Fatal("nil should not be retryable")
	}
	for _, err := range []error{
		context.Canceled,
		context.DeadlineExceeded,
		io.EOF,
		io.ErrUnexpectedEOF,
		&net.OpError{Op: "dial", Err: errors.New("connection refused")},
	} {
		if !IsRetryablePG(err) {
			t.Errorf("IsRetryablePG(%v) = false, want true", err)
		}
	}
	if IsRetryablePG(errors.New("syntax error at or near")) {
		t.Fatal("plain SQL error should not be retryable")
	}
}

func TestIsRetryableKafka(t *testing.T) {
	if IsRetryableKafka(nil) {
		t.Fatal("nil should not be retryable")
	}
	for _, err := range []error{
		context.Canceled,
		io.ErrUnexpectedEOF,
		&net.OpError{Op: "dial", Err: errors.New("connection refused")},
		&kerr.Error{Retriable: true, Message: "RequestTimedOut"},
	} {
		if !IsRetryableKafka(err) {
			t.Errorf("IsRetryableKafka(%v) = false, want true", err)
		}
	}
	if IsRetryableKafka(&kerr.Error{Retriable: false, Message: "TopicAuthorizationFailed"}) {
		t.Fatal("non-retriable kerr should not be retryable")
	}
	if IsRetryableKafka(errors.New("plain failure")) {
		t.Fatal("plain error should not be retryable")
	}
	wrapped := fmt.Errorf("produce: %w", &kerr.Error{Retriable: true, Message: "NotLeaderForPartition"})
	if !IsRetryableKafka(wrapped) {
		t.Fatal("wrapped retriable kerr should be retryable")
	}
}

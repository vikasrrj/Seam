package snowflake

import (
	"context"
	"testing"
	"time"
)

func TestPhaseObserverIsContextScoped(t *testing.T) {
	calls := 0
	ctx := WithPhaseObserver(context.Background(), func(name string, elapsed time.Duration) {
		calls++
		if name != "test" || elapsed < 0 {
			t.Fatalf("invalid observation: %q %v", name, elapsed)
		}
	})
	startPhase(context.Background(), "unobserved")()
	startPhase(ctx, "test")()
	if calls != 1 {
		t.Fatalf("observer called %d times, want 1", calls)
	}
}

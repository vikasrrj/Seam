package snowflake

import (
	"context"
	"time"
)

type phaseObserverKey struct{}

// WithPhaseObserver enables client-side phase timing for this operation.
// Observers must be concurrency safe, must return promptly, and must not issue
// warehouse queries. Durations include network and server waits, not just CPU.
// Nested phases overlap and must not be summed as exclusive wall time.
func WithPhaseObserver(ctx context.Context, observe func(string, time.Duration)) context.Context {
	return context.WithValue(ctx, phaseObserverKey{}, observe)
}

func startPhase(ctx context.Context, name string) func() {
	observe, _ := ctx.Value(phaseObserverKey{}).(func(string, time.Duration))
	if observe == nil {
		return func() {}
	}
	started := time.Now()
	return func() { observe(name, time.Since(started)) }
}

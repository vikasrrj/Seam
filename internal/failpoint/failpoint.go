// Package failpoint provides deterministic barriers for race and crash tests.
package failpoint

import (
	"context"
	"fmt"
	"sync"
)

// Name identifies a deterministic hook.
type Name string

const (
	// AfterChunkReadBeforeReconciliation pauses after the historical chunk has
	// been read into memory but before the HIGH marker is written.
	AfterChunkReadBeforeReconciliation Name = "after_chunk_read_before_reconciliation"
	// AfterLowMarkerCommitted pauses after the LOW marker is committed.
	AfterLowMarkerCommitted Name = "after_low_marker_committed"
	// AfterHighMarkerObserved pauses after the HIGH marker is observed in Kafka.
	AfterHighMarkerObserved Name = "after_high_marker_observed"
	// BeforeChunkCommit pauses immediately before the chunk completion commit.
	BeforeChunkCommit Name = "before_chunk_commit"
	// AfterDestinationCommit pauses after destination commit but before success
	// response is processed.
	AfterDestinationCommit Name = "after_destination_commit"
)

// Registry holds failpoints configured by tests.
type Registry struct {
	mu     sync.Mutex
	points map[Name]*Point
}

func NewRegistry() *Registry {
	return &Registry{points: make(map[Name]*Point)}
}

// Install creates a reusable failpoint.
func (r *Registry) Install(name Name) *Point {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok := r.points[name]; ok {
		return p
	}
	p := &Point{
		name:    name,
		enabled: true,
		trigger: make(chan struct{}),
		resume:  make(chan struct{}),
	}
	r.points[name] = p
	return p
}

// Get returns an installed point or nil.
func (r *Registry) Get(name Name) *Point {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.points[name]
}

// Resume unblocks anyone waiting on the named point.
func (r *Registry) Resume(name Name) {
	p := r.Get(name)
	if p != nil {
		p.Resume()
	}
}

// Point is a reusable deterministic barrier. Each Reach/Resume pair forms one
// cycle; Resume prepares channels for the next cycle.
type Point struct {
	name    Name
	enabled bool
	mu      sync.Mutex
	trigger chan struct{}
	resume  chan struct{}
}

// WaitForTrigger blocks until Trigger is called for the current cycle.
func (p *Point) WaitForTrigger(ctx context.Context) error {
	if !p.enabled {
		return nil
	}
	trigger := p.currentTrigger()
	select {
	case <-trigger:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Reach signals that the code path reached this failpoint and then blocks
// until Resume is called. It is the method instrumented code calls.
func (p *Point) Reach(ctx context.Context) error {
	if !p.enabled {
		return nil
	}
	p.Trigger()
	resume := p.currentResume()
	select {
	case <-resume:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitForResume blocks until Resume is called for the current cycle.
func (p *Point) WaitForResume(ctx context.Context) error {
	if !p.enabled {
		return nil
	}
	resume := p.currentResume()
	select {
	case <-resume:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Wait blocks until Trigger is called, then until Resume is called.
func (p *Point) Wait(ctx context.Context) error {
	if err := p.WaitForTrigger(ctx); err != nil {
		return err
	}
	return p.WaitForResume(ctx)
}

// Trigger unblocks WaitForTrigger for the current cycle.
func (p *Point) Trigger() {
	if !p.enabled {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.trigger:
	default:
		close(p.trigger)
	}
}

// Resume unblocks WaitForResume and prepares channels for the next cycle.
func (p *Point) Resume() {
	if !p.enabled {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.resume:
	default:
		close(p.resume)
	}
	p.trigger = make(chan struct{})
	p.resume = make(chan struct{})
}

func (p *Point) currentTrigger() chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.trigger
}

func (p *Point) currentResume() chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.resume
}

// String returns a human-readable name.
func (p *Point) String() string {
	return string(p.name)
}

// CrashError is injected by crash failpoints.
type CrashError struct {
	Point Name
}

func (e CrashError) Error() string {
	return fmt.Sprintf("injected crash at %s", e.Point)
}

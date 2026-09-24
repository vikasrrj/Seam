// Package resource bounds backfill pressure independently of configured
// worker count. It also pauses new snapshot scans when CDC lag exceeds a
// configured threshold, allowing the single ordered CDC coordinator to catch
// up without interrupting already-open reconciliation windows.
package resource

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type EndOffsetFunc func(context.Context) (int64, error)

// PermitPool coordinates a bounded resource across processes. Acquire must
// return a release function whose underlying ownership is also lost when the
// owning process or connection dies.
type PermitPool interface {
	Acquire(context.Context) (func(), error)
}

type Config struct {
	MaxSourceScans int
	MaxDestTx      int
	MaxCDCLag      int64
	PollInterval   time.Duration
	EndOffset      EndOffsetFunc
	ScanPermits    PermitPool
	DestPermits    PermitPool
}

type Controller struct {
	scans chan struct{}
	dest  chan struct{}

	mu           sync.Mutex
	maxLag       int64
	applied      int64
	end          int64
	lastPoll     time.Time
	pollInterval time.Duration
	endOffset    EndOffsetFunc
	scanPermits  PermitPool
	destPermits  PermitPool
	refreshing   bool
	changed      chan struct{}
}

func New(cfg Config, appliedOffset int64) (*Controller, error) {
	if cfg.MaxSourceScans < 1 || cfg.MaxDestTx < 1 || cfg.MaxCDCLag < 0 || cfg.PollInterval <= 0 || cfg.EndOffset == nil {
		return nil, fmt.Errorf("resource controller requires positive scan/destination limits and poll interval, a non-negative lag limit, and an end-offset source")
	}
	return &Controller{
		scans: make(chan struct{}, cfg.MaxSourceScans), dest: make(chan struct{}, cfg.MaxDestTx),
		maxLag: cfg.MaxCDCLag, applied: appliedOffset, end: appliedOffset,
		pollInterval: cfg.PollInterval, endOffset: cfg.EndOffset,
		scanPermits: cfg.ScanPermits, destPermits: cfg.DestPermits, changed: make(chan struct{}),
	}, nil
}

func (c *Controller) AcquireScan(ctx context.Context) (func(), error) {
	for {
		if err := c.refresh(ctx); err != nil {
			return nil, err
		}
		c.mu.Lock()
		blocked := c.end-c.applied > c.maxLag
		changed := c.changed
		c.mu.Unlock()
		if blocked {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-changed:
				continue
			case <-time.After(c.pollInterval):
				continue
			}
		}
		select {
		case c.scans <- struct{}{}:
			// Recheck lag after waiting for a slot; return the slot if CDC fell
			// behind while another scan was running.
			c.mu.Lock()
			blocked = c.end-c.applied > c.maxLag
			c.mu.Unlock()
			if blocked {
				<-c.scans
				continue
			}
			releaseShared, err := acquirePermit(ctx, c.scanPermits)
			if err != nil {
				<-c.scans
				return nil, err
			}
			// A process-shared permit may have taken time to acquire. Refresh and
			// recheck lag before admitting new source work.
			if err := c.refresh(ctx); err != nil {
				releaseShared()
				<-c.scans
				return nil, err
			}
			c.mu.Lock()
			blocked = c.end-c.applied > c.maxLag
			c.mu.Unlock()
			if blocked {
				releaseShared()
				<-c.scans
				continue
			}
			return combineRelease(releaseShared, func() { <-c.scans }), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (c *Controller) AcquireDestination(ctx context.Context) (func(), error) {
	select {
	case c.dest <- struct{}{}:
		releaseShared, err := acquirePermit(ctx, c.destPermits)
		if err != nil {
			<-c.dest
			return nil, err
		}
		return combineRelease(releaseShared, func() { <-c.dest }), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func acquirePermit(ctx context.Context, pool PermitPool) (func(), error) {
	if pool == nil {
		return func() {}, nil
	}
	release, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	if release == nil {
		return nil, fmt.Errorf("resource permit pool returned a nil release function")
	}
	return release, nil
}

func combineRelease(first, second func()) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			first()
			second()
		})
	}
}

func (c *Controller) SetAppliedOffset(offset int64) {
	c.mu.Lock()
	if offset > c.applied {
		c.applied = offset
		c.signalLocked()
	}
	c.mu.Unlock()
}

func (c *Controller) Lag() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	lag := c.end - c.applied
	if lag < 0 {
		return 0
	}
	return lag
}

func (c *Controller) refresh(ctx context.Context) error {
	for {
		c.mu.Lock()
		if time.Since(c.lastPoll) < c.pollInterval {
			c.mu.Unlock()
			return nil
		}
		if c.refreshing {
			changed := c.changed
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-changed:
				continue
			}
		}
		c.refreshing = true
		c.mu.Unlock()

		end, err := c.endOffset(ctx)
		c.mu.Lock()
		c.refreshing = false
		if err == nil {
			c.end = end
			c.lastPoll = time.Now()
		}
		c.signalLocked()
		c.mu.Unlock()
		if err != nil {
			return fmt.Errorf("refresh Kafka end offset for resource control: %w", err)
		}
		return nil
	}
}

func (c *Controller) signalLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}

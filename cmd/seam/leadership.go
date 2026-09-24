package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"example.com/seam/internal/checkpoint"
	"example.com/seam/internal/model"
)

type leadershipGuard struct {
	store  *checkpoint.Store
	cp     *model.Checkpoint
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	errCh  chan error
}

func leadershipOwnerID(workerID string) string {
	if workerID == "" {
		workerID = "seam"
	}
	return fmt.Sprintf("%s:pid:%d:start:%d", workerID, os.Getpid(), time.Now().UnixNano())
}

func startLeadershipGuard(parent context.Context, store *checkpoint.Store, cp *model.Checkpoint, leaseDuration, interval time.Duration) *leadershipGuard {
	if interval <= 0 {
		interval = leaseDuration / 3
	}
	if interval <= 0 || interval >= leaseDuration {
		interval = leaseDuration / 3
	}
	if interval <= 0 {
		interval = time.Millisecond
	}
	ctx, cancel := context.WithCancel(parent)
	g := &leadershipGuard{
		store: store, cp: cp, ctx: ctx, cancel: cancel,
		done: make(chan struct{}), errCh: make(chan error, 1),
	}
	go func() {
		defer close(g.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := store.RenewLeadership(ctx, cp, leaseDuration); err != nil {
					select {
					case g.errCh <- fmt.Errorf("renew leadership: %w", err):
					default:
					}
					cancel()
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return g
}

func (g *leadershipGuard) Context() context.Context { return g.ctx }

func (g *leadershipGuard) Stop() error {
	g.cancel()
	<-g.done
	var renewalErr error
	select {
	case renewalErr = <-g.errCh:
	default:
	}
	releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	releaseErr := g.store.ReleaseLeadership(releaseCtx, g.cp)
	if renewalErr != nil {
		// Losing ownership is the primary failure; a failed release is expected
		// when another process has already advanced the epoch.
		return renewalErr
	}
	if releaseErr != nil && !errors.Is(releaseErr, context.Canceled) {
		return fmt.Errorf("release leadership: %w", releaseErr)
	}
	return nil
}

package resource

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type channelPermitPool struct {
	permits chan struct{}
	active  atomic.Int64
}

func newChannelPermitPool(n int) *channelPermitPool {
	return &channelPermitPool{permits: make(chan struct{}, n)}
}

func (p *channelPermitPool) Acquire(ctx context.Context) (func(), error) {
	select {
	case p.permits <- struct{}{}:
		p.active.Add(1)
		var once sync.Once
		return func() {
			once.Do(func() {
				<-p.permits
				p.active.Add(-1)
			})
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestScanWaitsForCDCCatchupAndHonorsConcurrency(t *testing.T) {
	var end atomic.Int64
	end.Store(100)
	controller, err := New(Config{MaxSourceScans: 1, MaxDestTx: 1, MaxCDCLag: 10, PollInterval: time.Millisecond,
		EndOffset: func(context.Context) (int64, error) { return end.Load(), nil }}, 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	acquired := make(chan func(), 1)
	go func() {
		release, _ := controller.AcquireScan(ctx)
		acquired <- release
	}()
	select {
	case <-acquired:
		t.Fatal("scan started while CDC lag exceeded the limit")
	case <-time.After(20 * time.Millisecond):
	}
	controller.SetAppliedOffset(95)
	var first func()
	select {
	case first = <-acquired:
	case <-ctx.Done():
		t.Fatal("scan did not start after CDC caught up")
	}
	second := make(chan func(), 1)
	go func() {
		release, _ := controller.AcquireScan(ctx)
		second <- release
	}()
	select {
	case <-second:
		t.Fatal("second scan exceeded the concurrency limit")
	case <-time.After(20 * time.Millisecond):
	}
	first()
	select {
	case release := <-second:
		release()
	case <-ctx.Done():
		t.Fatal("second scan did not acquire released capacity")
	}
}

func TestDestinationTransactionsAreBounded(t *testing.T) {
	controller, err := New(Config{MaxSourceScans: 1, MaxDestTx: 1, MaxCDCLag: 1, PollInterval: time.Second,
		EndOffset: func(context.Context) (int64, error) { return 0, nil }}, 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first, err := controller.AcquireDestination(ctx)
	if err != nil {
		t.Fatal(err)
	}
	gotSecond := make(chan func(), 1)
	go func() { release, _ := controller.AcquireDestination(ctx); gotSecond <- release }()
	select {
	case <-gotSecond:
		t.Fatal("second destination transaction exceeded limit")
	case <-time.After(20 * time.Millisecond):
	}
	first()
	select {
	case release := <-gotSecond:
		release()
	case <-ctx.Done():
		t.Fatal("second destination transaction did not acquire capacity")
	}
}

func TestSharedPermitBoundsIndependentControllers(t *testing.T) {
	shared := newChannelPermitPool(1)
	newController := func() *Controller {
		controller, err := New(Config{
			MaxSourceScans: 2, MaxDestTx: 2, MaxCDCLag: 1, PollInterval: time.Millisecond,
			EndOffset: func(context.Context) (int64, error) { return 0, nil }, DestPermits: shared,
		}, 0)
		if err != nil {
			t.Fatal(err)
		}
		return controller
	}
	firstController, secondController := newController(), newController()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first, err := firstController.AcquireDestination(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := shared.active.Load(); got != 1 {
		t.Fatalf("active shared permits = %d, want 1", got)
	}
	acquired := make(chan func(), 1)
	go func() {
		release, _ := secondController.AcquireDestination(ctx)
		acquired <- release
	}()
	select {
	case <-acquired:
		t.Fatal("independent controller bypassed shared permit")
	case <-time.After(20 * time.Millisecond):
	}
	first()
	first() // combined release is idempotent
	select {
	case release := <-acquired:
		release()
	case <-ctx.Done():
		t.Fatal("second controller did not acquire released shared permit")
	}
}

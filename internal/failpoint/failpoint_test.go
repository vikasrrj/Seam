package failpoint

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDisabledPointIsNoop(t *testing.T) {
	var p Point // zero value: disabled
	if err := p.Reach(context.Background()); err != nil {
		t.Fatalf("Reach on disabled point = %v, want nil", err)
	}
	if err := p.WaitForTrigger(context.Background()); err != nil {
		t.Fatalf("WaitForTrigger on disabled point = %v, want nil", err)
	}
	if err := p.Wait(context.Background()); err != nil {
		t.Fatalf("Wait on disabled point = %v, want nil", err)
	}
	p.Trigger() // must not panic
	p.Resume()  // must not panic
}

func TestReachBlocksUntilResume(t *testing.T) {
	r := NewRegistry()
	p := r.Install(AfterChunkReadBeforeReconciliation)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- p.Reach(ctx)
	}()
	<-started

	if err := p.WaitForTrigger(ctx); err != nil {
		t.Fatalf("WaitForTrigger = %v, want nil", err)
	}

	select {
	case err := <-done:
		t.Fatalf("Reach returned (%v) before Resume, want it to block", err)
	case <-time.After(50 * time.Millisecond):
	}

	p.Resume()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Reach = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Reach did not unblock after Resume")
	}
}

func TestPointReusableAcrossCycles(t *testing.T) {
	r := NewRegistry()
	p := r.Install(AfterLowMarkerCommitted)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		done := make(chan error, 1)
		go func() { done <- p.Reach(ctx) }()
		if err := p.WaitForTrigger(ctx); err != nil {
			t.Fatalf("cycle %d WaitForTrigger = %v", i, err)
		}
		p.Resume()
		if err := <-done; err != nil {
			t.Fatalf("cycle %d Reach = %v", i, err)
		}
	}
}

func TestRegistryResumeByName(t *testing.T) {
	r := NewRegistry()
	p := r.Install(BeforeChunkCommit)
	ctx := context.Background()
	done := make(chan error, 1)
	go func() { done <- p.Reach(ctx) }()
	if err := p.WaitForTrigger(ctx); err != nil {
		t.Fatal(err)
	}
	r.Resume(BeforeChunkCommit)
	if err := <-done; err != nil {
		t.Fatalf("Reach = %v, want nil", err)
	}
	if got := r.Get(BeforeChunkCommit); got != p {
		t.Fatal("Get returned a different point than the installed one")
	}
}

func TestWaitForTriggerContextCancellation(t *testing.T) {
	r := NewRegistry()
	p := r.Install(AfterDestinationCommit)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.WaitForTrigger(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitForTrigger = %v, want context.Canceled", err)
	}
}

func TestPointString(t *testing.T) {
	r := NewRegistry()
	p := r.Install(AfterHighMarkerObserved)
	if got := p.String(); got != string(AfterHighMarkerObserved) {
		t.Fatalf("String() = %q, want %q", got, string(AfterHighMarkerObserved))
	}
}

func TestCrashError(t *testing.T) {
	err := CrashError{Point: AfterDestinationCommit}
	if got := err.Error(); got != "injected crash at after_destination_commit" {
		t.Fatalf("Error() = %q, want injected crash message", got)
	}
}

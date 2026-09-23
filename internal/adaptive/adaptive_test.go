package adaptive

import (
	"testing"
	"time"
)

func TestSuggestInitial(t *testing.T) {
	s := NewSizer(5*time.Second, 1000, 10, 100000)
	if got := s.Suggest(); got != 1000 {
		t.Fatalf("initial Suggest = %d, want 1000", got)
	}
}

func TestObserveFastChunkGrowsSize(t *testing.T) {
	s := NewSizer(10*time.Second, 1000, 10, 100000)
	// Three very fast chunks: observed << target, so the size must grow.
	for i := 0; i < 3; i++ {
		s.Observe(10*time.Millisecond, 1000)
	}
	if got := s.Suggest(); got <= 1000 {
		t.Fatalf("expected size to grow after fast chunks, got %d", got)
	}
}

func TestObserveSlowChunkShrinksSize(t *testing.T) {
	s := NewSizer(1*time.Second, 1000, 10, 100000)
	for i := 0; i < 3; i++ {
		s.Observe(60*time.Second, 1000)
	}
	if got := s.Suggest(); got >= 1000 {
		t.Fatalf("expected size to shrink after slow chunks, got %d", got)
	}
}

func TestClamps(t *testing.T) {
	s := NewSizer(10*time.Second, 500, 100, 200)
	// initialSize 500 gets clamped into [100, 200] at construction.
	if got := s.Suggest(); got != 200 {
		t.Fatalf("construction clamp = %d, want 200", got)
	}
	// Very slow chunks push toward the minimum but never below it.
	for i := 0; i < 20; i++ {
		s.Observe(time.Hour, 200)
	}
	if got := s.Suggest(); got != 100 {
		t.Fatalf("lower clamp = %d, want 100", got)
	}
	// Very fast chunks push toward the maximum but never above it.
	for i := 0; i < 20; i++ {
		s.Observe(time.Nanosecond, 200)
	}
	if got := s.Suggest(); got != 200 {
		t.Fatalf("upper clamp = %d, want 200", got)
	}
}

func TestWarmupPreventsEarlyMovement(t *testing.T) {
	s := NewSizer(10*time.Second, 1000, 10, 100000)
	s.Observe(10*time.Millisecond, 1000)
	s.Observe(10*time.Millisecond, 1000)
	if got := s.Suggest(); got != 1000 {
		t.Fatalf("size must not move before warmup, got %d", got)
	}
}

func TestConcurrentObserveSuggest(t *testing.T) {
	s := NewSizer(5*time.Second, 1000, 10, 100000)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			s.Observe(time.Duration(i)*time.Millisecond, 1000)
		}
	}()
	for i := 0; i < 1000; i++ {
		_ = s.Suggest()
	}
	<-done
}

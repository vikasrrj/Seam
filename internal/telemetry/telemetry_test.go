package telemetry

import (
	"sync"
	"testing"
)

func TestCounters(t *testing.T) {
	m := NewMetrics()
	m.RecordChunk(ChunkStats{Candidates: 10, Survivors: 8})
	m.RecordChunk(ChunkStats{Candidates: 5, Survivors: 5})
	m.RecordCDC(3)
	if got := m.ChunksCompleted(); got != 2 {
		t.Errorf("ChunksCompleted = %d, want 2", got)
	}
	if got := m.CandidatesSeen(); got != 15 {
		t.Errorf("CandidatesSeen = %d, want 15", got)
	}
	if got := m.SurvivorsWritten(); got != 13 {
		t.Errorf("SurvivorsWritten = %d, want 13", got)
	}
	if got := m.CDCEventsApplied(); got != 3 {
		t.Errorf("CDCEventsApplied = %d, want 3", got)
	}
}

func TestOnChunkCompleteCallback(t *testing.T) {
	m := NewMetrics()
	var got ChunkStats
	m.OnChunkComplete(func(stats ChunkStats) { got = stats })
	m.RecordChunk(ChunkStats{Candidates: 7, Survivors: 4})
	if got.Candidates != 7 || got.Survivors != 4 {
		t.Fatalf("callback got %+v, want {Candidates:7 Survivors:4}", got)
	}
}

func TestRecordChunkWithoutCallback(t *testing.T) {
	m := NewMetrics()
	m.RecordChunk(ChunkStats{Candidates: 1, Survivors: 1}) // must not panic
}

func TestRecordingWithoutCallbackPanicFree(t *testing.T) {
	m := NewMetrics()
	m.RecordCDC(1) // no callback registered; must not panic
}

func TestConcurrentRecordChunk(t *testing.T) {
	m := NewMetrics()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				m.RecordChunk(ChunkStats{Candidates: 1, Survivors: 1})
			}
		}()
	}
	wg.Wait()
	if got := m.ChunksCompleted(); got != 800 {
		t.Fatalf("ChunksCompleted = %d, want 800", got)
	}
	if got := m.CandidatesSeen(); got != 800 {
		t.Fatalf("CandidatesSeen = %d, want 800", got)
	}
}
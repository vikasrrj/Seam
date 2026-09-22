package scan

import (
	"math"
	"testing"

	"example.com/seam/internal/model"
)

func TestNextChunk(t *testing.T) {
	tests := []struct {
		name             string
		completedThrough int64
		chunkSize        int
		upperBound       int64
		wantChunk        model.ChunkRange
		wantOK           bool
	}{
		{"first chunk", 0, 10, 100, model.ChunkRange{Min: 1, Max: 10}, true},
		{"clamps to upper bound", 95, 10, 100, model.ChunkRange{Min: 96, Max: 100}, true},
		{"ends exactly at bound", 90, 10, 100, model.ChunkRange{Min: 91, Max: 100}, true},
		{"exhausted at bound", 100, 10, 100, model.ChunkRange{}, false},
		{"negative keys", -10, 5, -1, model.ChunkRange{Min: -9, Max: -5}, true},
		{"negative exhaustion", -1, 5, -1, model.ChunkRange{}, false},
		{"chunk size one", 41, 1, 42, model.ChunkRange{Min: 42, Max: 42}, true},
		{"int64 max boundary", math.MaxInt64 - 1, 1, math.MaxInt64, model.ChunkRange{Min: math.MaxInt64, Max: math.MaxInt64}, true},
		{"exhausted at int64 max", math.MaxInt64, 1, math.MaxInt64, model.ChunkRange{}, false},
		{"clamps oversized chunk at int64 max", math.MaxInt64 - 1, 100, math.MaxInt64, model.ChunkRange{Min: math.MaxInt64, Max: math.MaxInt64}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := NextChunk(tt.completedThrough, tt.chunkSize, tt.upperBound)
			if ok != tt.wantOK {
				t.Fatalf("NextChunk(%d, %d, %d) ok = %v, want %v", tt.completedThrough, tt.chunkSize, tt.upperBound, ok, tt.wantOK)
			}
			if ok && got != tt.wantChunk {
				t.Fatalf("NextChunk(%d, %d, %d) = %+v, want %+v", tt.completedThrough, tt.chunkSize, tt.upperBound, got, tt.wantChunk)
			}
		})
	}
}
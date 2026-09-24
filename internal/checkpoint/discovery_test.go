package checkpoint

import (
	"math"
	"testing"

	"example.com/seam/internal/model"
)

func TestDiscoveryRangeCoversSparseAndDeletedKeys(t *testing.T) {
	first, err := discoveryRange(nil, model.ChunkRange{Min: 5, Max: 10}, true, 100)
	if err != nil || first != (model.ChunkRange{Min: math.MinInt64, Max: 10}) {
		t.Fatalf("first range = %v, %v", first, err)
	}
	cursor := first.Max
	second, err := discoveryRange(&cursor, model.ChunkRange{Min: 70, Max: 80}, true, 100)
	if err != nil || second != (model.ChunkRange{Min: 11, Max: 80}) {
		t.Fatalf("sparse range = %v, %v", second, err)
	}
	cursor = second.Max
	tail, err := discoveryRange(&cursor, model.ChunkRange{}, false, 100)
	if err != nil || tail != (model.ChunkRange{Min: 81, Max: 100}) {
		t.Fatalf("terminal range = %v, %v", tail, err)
	}
}

func TestDiscoveryRangeIncludesMinimumInt64Key(t *testing.T) {
	range_, err := discoveryRange(nil, model.ChunkRange{Min: math.MinInt64, Max: math.MinInt64}, true, math.MinInt64)
	if err != nil || range_ != (model.ChunkRange{Min: math.MinInt64, Max: math.MinInt64}) {
		t.Fatalf("minimum-key range = %v, %v", range_, err)
	}
}

func TestDiscoveryRangeRejectsOverlap(t *testing.T) {
	cursor := int64(10)
	if _, err := discoveryRange(&cursor, model.ChunkRange{Min: 10, Max: 20}, true, 100); err == nil {
		t.Fatal("overlapping source page accepted")
	}
}

func TestValidateManifestRanges(t *testing.T) {
	valid := []model.ChunkRange{{Min: math.MinInt64, Max: 10}, {Min: 11, Max: 50}, {Min: 51, Max: 100}}
	if err := validateManifestRanges(valid, 100); err != nil {
		t.Fatal(err)
	}
	for _, broken := range [][]model.ChunkRange{
		{{Min: 1, Max: 100}},
		{{Min: math.MinInt64, Max: 10}, {Min: 12, Max: 100}},
		{{Min: math.MinInt64, Max: 10}, {Min: 10, Max: 100}},
		{{Min: math.MinInt64, Max: 99}},
	} {
		if err := validateManifestRanges(broken, 100); err == nil {
			t.Fatalf("invalid manifest accepted: %+v", broken)
		}
	}
}

package scan

import (
	"math"
	"strings"
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

func TestNewChunkReaderFor_ValidatesIdentifiers(t *testing.T) {
	valid := []struct{ table, key string }{
		{"accounts", "id"},
		{"payments", "account_id"},
		{"_internal", "_key"},
	}
	// Keys must be injected safely; anything but a plain identifier is refused.
	invalid := []struct{ table, key string }{
		{"accounts; DROP TABLE accounts", "id"},
		{"accounts ", "id"},
		{"public.accounts", "id"},
		{"accounts", "id; SELECT 1"},
		{"accounts", "id\"--"},
		{"accounts", ""},
		{"", "id"},
		{"account-list", "id"},
		{"accounts", "id order by 1"},
	}
	for _, v := range valid {
		if _, err := NewChunkReaderFor("dsn", v.table, v.key); err != nil {
			t.Fatalf("NewChunkReaderFor(%q, %q) unexpected error: %v", v.table, v.key, err)
		}
	}
	for _, v := range invalid {
		if _, err := NewChunkReaderFor("dsn", v.table, v.key); err == nil {
			t.Fatalf("NewChunkReaderFor(%q, %q) expected error", v.table, v.key)
		}
	}
}

func TestKeysetQueriesArePaginationAnchored(t *testing.T) {
	// The next-chunk query must be keyset based: a strictly-greater-than
	// predicate anchored on the last key. OFFSET-based pagination degrades as
	// the table grows and must never appear.
	q := nextChunkQuery("accounts", "id", 1000)
	for _, forbidden := range []string{"OFFSET", "offset"} {
		if strings.Contains(q, forbidden) {
			t.Fatalf("next chunk query must not use OFFSET: %s", q)
		}
	}
	for _, want := range []string{"id > $1", "ORDER BY id", "LIMIT 1000", "id <= $2"} {
		if !strings.Contains(q, want) {
			t.Fatalf("next chunk query missing %q: %s", want, q)
		}
	}

	// A custom table/key pair must be reflected verbatim in the SQL.
	q2 := nextChunkQuery("payments", "account_id", 50)
	if !strings.Contains(q2, "payments") || !strings.Contains(q2, "account_id > $1") {
		t.Fatalf("custom table/key not applied: %s", q2)
	}
	up := upperBoundQuery("payments", "account_id")
	if up != "SELECT MAX(account_id) FROM payments" {
		t.Fatalf("unexpected upper bound query: %s", up)
	}
}

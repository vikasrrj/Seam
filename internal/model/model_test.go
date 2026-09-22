package model

import "testing"

func TestSourceTxString(t *testing.T) {
	st := SourceTx{Generation: "gen-1", XID: 42, LSN: "0/16A23C8"}
	if got := st.String(); got != "gen-1:0/16A23C8" {
		t.Fatalf("String() = %q, want %q", got, "gen-1:0/16A23C8")
	}
}

func TestChangeKey(t *testing.T) {
	if got := (Change{Account: &Account{ID: 7}}).Key(); got != "account:7" {
		t.Fatalf("account Key() = %q, want %q", got, "account:7")
	}
	if got := (Change{Marker: &Marker{ID: "j:a:low:1:10"}}).Key(); got != "marker:j:a:low:1:10" {
		t.Fatalf("marker Key() = %q, want %q", got, "marker:j:a:low:1:10")
	}
	if got := (Change{}).Key(); got != "unknown" {
		t.Fatalf("empty Key() = %q, want %q", got, "unknown")
	}
}

func TestChunkRangeString(t *testing.T) {
	if got := (ChunkRange{Min: 1, Max: 500}).String(); got != "[1,500]" {
		t.Fatalf("String() = %q, want %q", got, "[1,500]")
	}
}

func TestMarkerKinds(t *testing.T) {
	if MarkerLow != MarkerKind("low") || MarkerHigh != MarkerKind("high") {
		t.Fatal("marker kind constants drifted from their string values")
	}
}

func TestOperations(t *testing.T) {
	if OpInsert != Operation("insert") || OpUpdate != Operation("update") || OpDelete != Operation("delete") {
		t.Fatal("operation constants drifted from their string values")
	}
}
package model

import "testing"

func TestSourceTxString(t *testing.T) {
	st := SourceTx{Generation: "gen-1", SystemID: "pg-1", XID: 42, LSN: "0/16A23C8"}
	if got := st.String(); got != "pg-1:gen-1:0/16A23C8" {
		t.Fatalf("String() = %q, want %q", got, "pg-1:gen-1:0/16A23C8")
	}
}

func TestChangePayload(t *testing.T) {
	row := &Row{Values: []Value{Int64Value(7)}}
	if got := (Change{Op: OpInsert, Row: row}); got.Row == nil || got.Marker != nil {
		t.Fatal("row change must carry a row payload and no marker")
	}
	if got := (Change{Marker: &Marker{ID: "j:a:low:1:10"}}).Marker; got == nil || got.ID != "j:a:low:1:10" {
		t.Fatal("marker change must carry its marker payload")
	}
	if (Change{}).Row != nil || (Change{}).Marker != nil {
		t.Fatal("empty change must carry no payload")
	}
}

func TestValueEqual(t *testing.T) {
	if !NullValue().IsNull() || Int64Value(1).IsNull() || TextValue("x").IsNull() {
		t.Fatal("IsNull must report only ValueNull")
	}
	if !NullValue().Equal(NullValue()) {
		t.Fatal("NULL must equal NULL")
	}
	if !Int64Value(42).Equal(Int64Value(42)) {
		t.Fatal("equal integers must compare equal")
	}
	if Int64Value(42).Equal(Int64Value(41)) {
		t.Fatal("different integers must not compare equal")
	}
	if !TextValue("a b").Equal(TextValue("a b")) {
		t.Fatal("equal canonical text must compare equal")
	}
	if TextValue("a b").Equal(TextValue("a  b")) {
		t.Fatal("different canonical text must not compare equal")
	}
	if Int64Value(7).Equal(TextValue("7")) {
		t.Fatal("a typed int must not compare equal to canonical text")
	}
	if NullValue().Equal(TextValue("")) {
		t.Fatal("NULL must not compare equal to empty text")
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

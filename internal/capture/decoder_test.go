package capture

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"example.com/seam/internal/model"
	"example.com/seam/internal/schema"
	"github.com/jackc/pglogrepl"
)

// accountsDescriptor is the durable descriptor matching relationAccounts();
// it must be identical to what the decoder pins so row decoding and the
// schema-epoch stamp are deterministic.
func accountsDescriptor() *schema.Schema {
	s := &schema.Schema{
		Namespace:       "public",
		Table:           "accounts",
		ReplicaIdentity: "f",
		PKOrdinal:       1,
		Columns: []schema.Column{
			{Ordinal: 1, Name: "id", TypeName: "int8", TypeOID: 20, PrimaryKey: true},
			{Ordinal: 2, Name: "owner", TypeName: "text", TypeOID: 25},
			{Ordinal: 3, Name: "balance_cents", TypeName: "int8", TypeOID: 20},
		},
	}
	s.Fingerprint = s.FingerprintFor()
	return s
}

// accountRow builds the positional generic row these tests assert against:
// id/owner/balance_cents in schema order.
func accountRow(id int64, owner string, balance int64) *model.Row {
	return &model.Row{Values: []model.Value{
		model.Int64Value(id),
		model.TextValue(owner),
		model.Int64Value(balance),
	}}
}

func TestDecoderAccountsInsert(t *testing.T) {
	d := NewDecoder("gen:0", accountsDescriptor())
	d.Handle(relationAccounts())
	d.Handle(beginMessage(1))
	d.Handle(insertAccount(7, "vik", 1000))
	tx, err := d.Handle(commitMessage(1, "0/123", "0/124"))
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if tx == nil || tx.Count != 1 {
		t.Fatalf("expected one event, got %+v", tx)
	}
	change, ok, err := d.NextEvent()
	if err != nil {
		t.Fatalf("next event: %v", err)
	}
	if !ok {
		t.Fatal("expected event")
	}
	if change.Op != model.OpInsert {
		t.Fatalf("expected insert, got %s", change.Op)
	}
	if change.SchemaID != accountsDescriptor().Fingerprint {
		t.Fatalf("row change missing the pinned schema epoch: %q", change.SchemaID)
	}
	if change.Row == nil || len(change.Row.Values) != 3 ||
		change.Row.Values[0].Int != 7 ||
		change.Row.Values[1].Text != "vik" ||
		change.Row.Values[2].Int != 1000 {
		t.Fatalf("unexpected row: %+v", change.Row)
	}
	if change.Source.Generation != "gen:0" || change.Source.LSN != "0/123" {
		t.Fatalf("unexpected source: %+v", change.Source)
	}
}

func TestDecoderMarkerRoundTrip(t *testing.T) {
	d := NewDecoder("gen:0", accountsDescriptor())
	d.Handle(relationMarker())
	d.Handle(beginMessage(2))
	d.Handle(insertMarker("low:1", "low", "job", "gen:0:attempt:0", 1, 10))
	tx, err := d.Handle(commitMessage(2, "0/200", "0/201"))
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if tx == nil || tx.Count != 1 {
		t.Fatalf("expected one event, got %+v", tx)
	}
	change, ok, err := d.NextEvent()
	if err != nil {
		t.Fatalf("next event: %v", err)
	}
	if !ok {
		t.Fatal("expected event")
	}
	if change.Marker == nil {
		t.Fatalf("expected marker: %+v", change)
	}
	if change.Marker.Kind != model.MarkerLow || change.Marker.ChunkMin != 1 || change.Marker.ChunkMax != 10 {
		t.Fatalf("unexpected marker: %+v", change.Marker)
	}
}

func TestDecoderBoundedTransactionEvents(t *testing.T) {
	d := NewDecoder("gen:0", accountsDescriptor())
	d.MaxTransactionEvents = 5
	d.Handle(relationAccounts())
	d.Handle(beginMessage(10))
	for i := 0; i < 5; i++ {
		if _, err := d.Handle(insertAccount(int64(i+1), "o", 1)); err != nil {
			t.Fatalf("handle insert %d: %v", i, err)
		}
	}
	// The sixth event must be rejected: the in-flight buffer is bounded.
	_, err := d.Handle(insertAccount(99, "o", 1))
	if !errors.Is(err, ErrTransactionTooLarge) {
		t.Fatalf("expected ErrTransactionTooLarge, got %v", err)
	}
	// The decoder is not wedged: it still remembers the buffered transaction.
	tx, err := d.Handle(commitMessage(10, "0/500", "0/501"))
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if tx == nil || tx.Count != 5 {
		t.Fatalf("expected 5 buffered events, got %+v", tx)
	}
}

func TestDecoderDefaultTransactionCap(t *testing.T) {
	d := NewDecoder("gen:0", accountsDescriptor())
	if d.MaxTransactionEvents <= 0 {
		t.Fatalf("expected a positive default transaction cap, got %d", d.MaxTransactionEvents)
	}
}

func TestDecoderRejectsTruncateBeforeCommit(t *testing.T) {
	d := NewDecoder("gen:0", accountsDescriptor())
	if _, err := d.Handle(relationAccounts()); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Handle(beginMessage(11)); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Handle(&pglogrepl.TruncateMessage{RelationIDs: []uint32{16384}}); err == nil || !strings.Contains(err.Error(), "TRUNCATE") {
		t.Fatalf("expected fail-closed TRUNCATE error, got %v", err)
	}
}

func TestDecoderRejectsChangedRelationShape(t *testing.T) {
	d := NewDecoder("gen:0", accountsDescriptor())
	relation := relationAccounts()
	relation.Columns[1].Name = "renamed_owner"
	if _, err := d.Handle(relation); err == nil || !strings.Contains(err.Error(), "expected") {
		t.Fatalf("expected schema mismatch error, got %v", err)
	}
}

func TestDecoderRejectsChangedColumnType(t *testing.T) {
	d := NewDecoder("gen:0", accountsDescriptor())
	relation := relationAccounts()
	relation.Columns[1].DataType = 1043 // varchar, not text
	if _, err := d.Handle(relation); err == nil || !strings.Contains(err.Error(), "type OID") {
		t.Fatalf("expected type mismatch error, got %v", err)
	}
}

func TestDecoderRejectsPrimaryKeyChange(t *testing.T) {
	d := NewDecoder("gen:0", accountsDescriptor())
	d.Handle(relationAccounts())
	d.Handle(beginMessage(3))
	// A re-keyed update with a full old tuple is unrepresentable in Seam and
	// fails closed.
	old := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("7")},
			{DataType: 't', Data: []byte("old")},
			{DataType: 't', Data: []byte("1")},
		},
	}
	_, err := d.Handle(&pglogrepl.UpdateMessage{
		RelationID:   16384,
		NewTuple:     insertAccount(8, "vik", 1000).Tuple,
		OldTuple:     old,
		OldTupleType: 'O',
	})
	if err == nil {
		t.Fatal("expected primary key change rejection error")
	}
	if !strings.Contains(err.Error(), "primary key change") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestDecoderRejectsKeyOnlyOldTuple pins the REPLICA IDENTITY FULL contract:
// a key-only ('K') old tuple cannot reconstruct the full old row, so data
// table changes carrying it fail closed for both updates and deletes.
func TestDecoderRejectsKeyOnlyOldTuple(t *testing.T) {
	old := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{{DataType: 't', Data: []byte("7")}},
	}
	d := NewDecoder("gen:0", accountsDescriptor())
	d.Handle(relationAccounts())
	d.Handle(beginMessage(4))
	_, err := d.Handle(&pglogrepl.UpdateMessage{
		RelationID:   16384,
		NewTuple:     insertAccount(7, "updated", 2000).Tuple,
		OldTuple:     old,
		OldTupleType: 'K',
	})
	if err == nil || !strings.Contains(err.Error(), "key-only old tuple") {
		t.Fatalf("expected key-only update rejection, got %v", err)
	}

	d2 := NewDecoder("gen:0", accountsDescriptor())
	d2.Handle(relationAccounts())
	d2.Handle(beginMessage(5))
	_, err = d2.Handle(&pglogrepl.DeleteMessage{
		RelationID:   16384,
		OldTuple:     old,
		OldTupleType: 'K',
	})
	if err == nil || !strings.Contains(err.Error(), "key-only old tuple") {
		t.Fatalf("expected key-only delete rejection, got %v", err)
	}
}

func TestDecoderAcceptsSameKeyUpdate(t *testing.T) {
	d := NewDecoder("gen:0", accountsDescriptor())
	d.Handle(relationAccounts())
	d.Handle(beginMessage(4))
	old := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("7")},
			{DataType: 't', Data: []byte("old")},
			{DataType: 't', Data: []byte("1")},
		},
	}
	_, err := d.Handle(&pglogrepl.UpdateMessage{
		RelationID:   16384,
		NewTuple:     insertAccount(7, "updated", 2000).Tuple,
		OldTuple:     old,
		OldTupleType: 'O',
	})
	if err != nil {
		t.Fatalf("handle update: %v", err)
	}
	tx, err := d.Handle(commitMessage(4, "0/400", "0/401"))
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if tx == nil || tx.Count != 1 {
		t.Fatalf("expected one event, got %+v", tx)
	}
	change, ok, err := d.NextEvent()
	if err != nil || !ok {
		t.Fatalf("expected event, GOT ok=%v err=%v", ok, err)
	}
	if change.Row == nil || len(change.Row.Values) != 3 || change.Row.Values[1].Text != "updated" {
		t.Fatalf("unexpected row: %+v", change.Row)
	}
}

func relationAccounts() *pglogrepl.RelationMessage {
	return &pglogrepl.RelationMessage{
		RelationID:      16384,
		Namespace:       "public",
		RelationName:    "accounts",
		ReplicaIdentity: 'f',
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", Flags: 1, DataType: 20},
			{Name: "owner", DataType: 25},
			{Name: "balance_cents", DataType: 20},
		},
	}
}

func TestDecoderReconstructsUnchangedToastFromFullOldTuple(t *testing.T) {
	d := NewDecoder("gen:0", accountsDescriptor())
	if _, err := d.Handle(relationAccounts()); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Handle(beginMessage(44)); err != nil {
		t.Fatal(err)
	}
	oldTuple := insertAccount(7, strings.Repeat("wide-owner", 100), 100).Tuple
	newTuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		{DataType: 't', Data: []byte("7")},
		{DataType: 'u'},
		{DataType: 't', Data: []byte("200")},
	}}
	if _, err := d.Handle(&pglogrepl.UpdateMessage{RelationID: 16384, OldTupleType: 'O', OldTuple: oldTuple, NewTuple: newTuple}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Handle(commitMessage(44, "0/440", "0/441")); err != nil {
		t.Fatal(err)
	}
	change, ok, err := d.NextEvent()
	if err != nil || !ok {
		t.Fatalf("next event ok=%v err=%v", ok, err)
	}
	if change.Row == nil || len(change.Row.Values) != 3 ||
		change.Row.Values[1].Text != strings.Repeat("wide-owner", 100) ||
		change.Row.Values[2].Int != 200 {
		t.Fatalf("unchanged TOAST reconstruction failed: %+v", change.Row)
	}
}

func TestDecoderRejectsUnchangedToastWithoutFullOldTuple(t *testing.T) {
	d := NewDecoder("gen:0", accountsDescriptor())
	d.Handle(relationAccounts())
	d.Handle(beginMessage(45))
	newTuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		{DataType: 't', Data: []byte("7")}, {DataType: 'u'}, {DataType: 't', Data: []byte("200")},
	}}
	if _, err := d.Handle(&pglogrepl.UpdateMessage{RelationID: 16384, NewTuple: newTuple}); err == nil || !strings.Contains(err.Error(), "REPLICA IDENTITY FULL") {
		t.Fatalf("expected unchanged TOAST failure, got %v", err)
	}
}

func relationMarker() *pglogrepl.RelationMessage {
	return &pglogrepl.RelationMessage{
		RelationID:   16385,
		Namespace:    "public",
		RelationName: "seam_marker",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", Flags: 1, DataType: 25},
			{Name: "job_id", DataType: 25},
			{Name: "attempt", DataType: 25},
			{Name: "kind", DataType: 25},
			{Name: "chunk_min_id", DataType: 20},
			{Name: "chunk_max_id", DataType: 20},
			{Name: "created_at", DataType: 1184},
		},
	}
}

func beginMessage(xid uint32) *pglogrepl.BeginMessage {
	return &pglogrepl.BeginMessage{Xid: xid}
}

func commitMessage(xid uint32, commitLSN, endLSN string) *pglogrepl.CommitMessage {
	lsn, _ := pglogrepl.ParseLSN(commitLSN)
	end, _ := pglogrepl.ParseLSN(endLSN)
	return &pglogrepl.CommitMessage{
		CommitLSN:         lsn,
		TransactionEndLSN: end,
	}
}

func insertAccount(id int64, owner string, balance int64) *pglogrepl.InsertMessage {
	return &pglogrepl.InsertMessage{
		RelationID: 16384,
		Tuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: 't', Data: []byte(strconv.FormatInt(id, 10))},
				{DataType: 't', Data: []byte(owner)},
				{DataType: 't', Data: []byte(strconv.FormatInt(balance, 10))},
			},
		},
	}
}

func insertMarker(id, kind, job, attempt string, minID, maxID int64) *pglogrepl.InsertMessage {
	return &pglogrepl.InsertMessage{
		RelationID: 16385,
		Tuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: 't', Data: []byte(id)},
				{DataType: 't', Data: []byte(job)},
				{DataType: 't', Data: []byte(attempt)},
				{DataType: 't', Data: []byte(kind)},
				{DataType: 't', Data: []byte(strconv.FormatInt(minID, 10))},
				{DataType: 't', Data: []byte(strconv.FormatInt(maxID, 10))},
				{DataType: 't', Data: []byte("2026-01-01 00:00:00+00")},
			},
		},
	}
}

package capture

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"example.com/seam/internal/model"
	"github.com/jackc/pglogrepl"
)

func TestDecoderAccountsInsert(t *testing.T) {
	d := NewDecoder("gen:0")
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
	if change.Account == nil || change.Account.ID != 7 || change.Account.Owner != "vik" || change.Account.BalanceCents != 1000 {
		t.Fatalf("unexpected account: %+v", change.Account)
	}
	if change.Source.Generation != "gen:0" || change.Source.LSN != "0/123" {
		t.Fatalf("unexpected source: %+v", change.Source)
	}
}

func TestDecoderMarkerRoundTrip(t *testing.T) {
	d := NewDecoder("gen:0")
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
	d := NewDecoder("gen:0")
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
	d := NewDecoder("gen:0")
	if d.MaxTransactionEvents <= 0 {
		t.Fatalf("expected a positive default transaction cap, got %d", d.MaxTransactionEvents)
	}
}

func TestDecoderRejectsPrimaryKeyChange(t *testing.T) {
	d := NewDecoder("gen:0")
	d.Handle(relationAccounts())
	d.Handle(beginMessage(3))
	old := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("7")},
		},
	}
	_, err := d.Handle(&pglogrepl.UpdateMessage{
		RelationID:   16384,
		NewTuple:     insertAccount(8, "vik", 1000).Tuple,
		OldTuple:     old,
		OldTupleType: 'K',
	})
	if err == nil {
		t.Fatal("expected primary key change rejection error")
	}
	if !strings.Contains(err.Error(), "primary key change") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDecoderAcceptsSameKeyUpdate(t *testing.T) {
	d := NewDecoder("gen:0")
	d.Handle(relationAccounts())
	d.Handle(beginMessage(4))
	old := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("7")},
		},
	}
	_, err := d.Handle(&pglogrepl.UpdateMessage{
		RelationID:   16384,
		NewTuple:     insertAccount(7, "updated", 2000).Tuple,
		OldTuple:     old,
		OldTupleType: 'K',
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
	if change.Account == nil || change.Account.Owner != "updated" {
		t.Fatalf("unexpected account: %+v", change.Account)
	}
}

func relationAccounts() *pglogrepl.RelationMessage {
	return &pglogrepl.RelationMessage{
		RelationID:   16384,
		Namespace:    "public",
		RelationName: "accounts",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", Flags: 1},
			{Name: "owner"},
			{Name: "balance_cents"},
		},
	}
}

func relationMarker() *pglogrepl.RelationMessage {
	return &pglogrepl.RelationMessage{
		RelationID:   16385,
		Namespace:    "public",
		RelationName: "seam_marker",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", Flags: 1},
			{Name: "job_id"},
			{Name: "attempt"},
			{Name: "kind"},
			{Name: "chunk_min_id"},
			{Name: "chunk_max_id"},
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
			},
		},
	}
}

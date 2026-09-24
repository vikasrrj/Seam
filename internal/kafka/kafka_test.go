package kafka

import (
	"encoding/json"
	"strings"
	"testing"

	"example.com/seam/internal/model"
)

func decodeChange(data []byte) (model.Change, error) {
	var change model.Change
	err := json.Unmarshal(data, &change)
	return change, err
}

// rowChange builds a generic row change whose first column (the message
// primary key in these fixtures) is the given id.
func rowChange(op model.Operation, id int64, owner string, source model.SourceTx) model.Change {
	return model.Change{
		Op:       op,
		SchemaID: "schema-epoch-1",
		Row: &model.Row{Values: []model.Value{
			model.Int64Value(id),
			model.TextValue(owner),
		}},
		Source: source,
	}
}

func rowID(t *testing.T, change model.Change) int64 {
	t.Helper()
	if change.Row == nil || len(change.Row.Values) == 0 || change.Row.Values[0].Kind != model.ValueInt64 {
		t.Fatalf("row change missing primary key: %+v", change)
	}
	return change.Row.Values[0].Int
}

func TestEnvelopeDeliversWholeSourceTransaction(t *testing.T) {
	source := model.SourceTx{Generation: "gen:0", SystemID: "source-1", LSN: "0/123", XID: 7}
	wire, err := json.Marshal(model.TransactionEnvelope{
		Version: 1, Source: source, SchemaID: "schema-epoch-1", Count: 2,
		Changes: []model.Change{
			rowChange(model.OpInsert, 1, "one", source),
			rowChange(model.OpDelete, 2, "", source),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	changes, err := decodeEnvelope(wire, decodeChange)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 2 || rowID(t, changes[0]) != 1 || rowID(t, changes[1]) != 2 {
		t.Fatalf("incomplete transaction: %+v", changes)
	}
	if changes[0].SchemaID != "schema-epoch-1" {
		t.Fatalf("row change lost its schema epoch: %+v", changes[0])
	}
}

func TestEnvelopeRejectsLegacyAndTruncatedPayload(t *testing.T) {
	legacy, _ := json.Marshal(model.Change{Op: model.OpInsert, Row: &model.Row{Values: []model.Value{model.Int64Value(1)}}})
	if _, err := decodeEnvelope(legacy, decodeChange); err == nil {
		t.Fatal("legacy per-row record must fail closed")
	}
	source := model.SourceTx{Generation: "gen:0", SystemID: "source-1", LSN: "0/123", XID: 7}
	bad, _ := json.Marshal(model.TransactionEnvelope{Version: 1, Source: source, Count: 2, Changes: []model.Change{rowChange(model.OpInsert, 1, "one", source)}})
	if _, err := decodeEnvelope(bad, decodeChange); err == nil || !strings.Contains(err.Error(), "count") {
		t.Fatalf("expected incomplete transaction error, got %v", err)
	}
}

func TestEnvelopeRejectsSchemaEpochMismatch(t *testing.T) {
	source := model.SourceTx{Generation: "gen:0", SystemID: "source-1", LSN: "0/123", XID: 7}
	mixed := rowChange(model.OpInsert, 1, "one", source)
	mixed.SchemaID = "schema-epoch-2"
	wire, _ := json.Marshal(model.TransactionEnvelope{
		Version: 1, Source: source, SchemaID: "schema-epoch-1", Count: 1,
		Changes: []model.Change{mixed},
	})
	if _, err := decodeEnvelope(wire, decodeChange); err == nil || !strings.Contains(err.Error(), "schema epoch") {
		t.Fatalf("transaction mixing schema epochs must fail closed, got %v", err)
	}
}

func TestEnvelopeRejectsOversizedRecordBeforeDecode(t *testing.T) {
	if _, err := decodeEnvelope(make([]byte, maxEnvelopeBytes+1), decodeChange); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("expected byte-limit rejection, got %v", err)
	}
}

func TestFragmentAssemblyWaitsForFinalAndUsesFinalOffset(t *testing.T) {
	source := model.SourceTx{Generation: "gen:0", SystemID: "source-1", LSN: "0/999", XID: 9}
	changes := []model.Change{
		rowChange(model.OpInsert, 1, "", source),
		rowChange(model.OpUpdate, 2, "", source),
		rowChange(model.OpDelete, 3, "", source),
	}
	encode := func(index int, final bool, part []model.Change) []byte {
		t.Helper()
		wire, err := json.Marshal(model.TransactionEnvelope{
			Version: 2, Source: source, FragmentIndex: index, Final: final,
			Count: len(part), TotalCount: len(changes), Changes: part,
		})
		if err != nil {
			t.Fatal(err)
		}
		return wire
	}
	var assembly fragmentAssembly
	t.Cleanup(assembly.reset)
	if records, err := assembly.push(10, encode(0, false, changes[:1]), decodeChange); err != nil || len(records) != 0 {
		t.Fatalf("first fragment exposed records=%+v err=%v", records, err)
	}
	if records, err := assembly.push(11, encode(1, false, changes[1:2]), decodeChange); err != nil || len(records) != 0 {
		t.Fatalf("second fragment exposed records=%+v err=%v", records, err)
	}
	records, err := assembly.push(12, encode(2, true, changes[2:]), decodeChange)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != len(changes) {
		t.Fatalf("assembled %d records, want %d", len(records), len(changes))
	}
	for i, record := range records {
		if record.Offset != 12 || rowID(t, record.Change) != int64(i+1) {
			t.Fatalf("record %d=%+v", i, record)
		}
	}
}

func TestFragmentTransactionWalkIsReplayableAndFragmentBounded(t *testing.T) {
	source := model.SourceTx{Generation: "gen:0", SystemID: "source-1", LSN: "0/AAA", XID: 12}
	changes := []model.Change{
		rowChange(model.OpInsert, 1, "", source),
		rowChange(model.OpUpdate, 2, "", source),
		rowChange(model.OpDelete, 3, "", source),
	}
	encode := func(index int, final bool, part []model.Change) []byte {
		wire, err := json.Marshal(model.TransactionEnvelope{Version: 2, Source: source, FragmentIndex: index, Final: final, Count: len(part), TotalCount: len(changes), Changes: part})
		if err != nil {
			t.Fatal(err)
		}
		return wire
	}
	var assembly fragmentAssembly
	t.Cleanup(assembly.reset)
	if transaction, err := assembly.pushTransaction(70, encode(0, false, changes[:1]), decodeChange); err != nil || transaction != nil {
		t.Fatalf("prefix transaction=%v err=%v", transaction, err)
	}
	transaction, err := assembly.pushTransaction(71, encode(1, true, changes[1:]), decodeChange)
	if err != nil {
		t.Fatal(err)
	}
	defer transaction.Close()
	for pass := 0; pass < 2; pass++ {
		var ids []int64
		maxBatch := 0
		if err := transaction.Walk(func(records []Record) error {
			if len(records) > maxBatch {
				maxBatch = len(records)
			}
			for _, record := range records {
				if record.Offset != 71 {
					t.Fatalf("offset=%d, want final offset 71", record.Offset)
				}
				ids = append(ids, rowID(t, record.Change))
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(ids) != 3 || ids[0] != 1 || ids[2] != 3 || maxBatch != 2 {
			t.Fatalf("pass %d ids=%v max batch=%d", pass, ids, maxBatch)
		}
	}
}

func TestFragmentAssemblyReplacesAbandonedPrefixOnRetry(t *testing.T) {
	source := model.SourceTx{Generation: "gen:0", SystemID: "source-1", LSN: "0/ABC", XID: 10}
	changes := []model.Change{
		rowChange(model.OpInsert, 1, "", source),
		rowChange(model.OpInsert, 2, "", source),
	}
	encode := func(index int, final bool, part []model.Change) []byte {
		wire, _ := json.Marshal(model.TransactionEnvelope{Version: 2, Source: source, FragmentIndex: index, Final: final, Count: len(part), TotalCount: 2, Changes: part})
		return wire
	}
	var assembly fragmentAssembly
	t.Cleanup(assembly.reset)
	if _, err := assembly.push(20, encode(0, false, changes[:1]), decodeChange); err != nil {
		t.Fatal(err)
	}
	// Capture crashed after the prefix. PostgreSQL replay publishes a new
	// fragment zero; this resets the disposable assembly and completes from
	// the durable Kafka retry.
	if records, err := assembly.push(30, encode(0, false, changes[:1]), decodeChange); err != nil || len(records) != 0 {
		t.Fatalf("retry fragment zero: records=%+v err=%v", records, err)
	}
	records, err := assembly.push(31, encode(1, true, changes[1:]), decodeChange)
	if err != nil || len(records) != 2 {
		t.Fatalf("complete retry: records=%+v err=%v", records, err)
	}
}

func TestFragmentAssemblyOnlyBarrierScanMaySkipLeadingSuffix(t *testing.T) {
	source := model.SourceTx{Generation: "gen:0", SystemID: "source-1", LSN: "0/DEF", XID: 11}
	change := rowChange(model.OpInsert, 1, "", source)
	wire, _ := json.Marshal(model.TransactionEnvelope{
		Version: 2, Source: source, FragmentIndex: 3, Final: true,
		Count: 1, TotalCount: 4, Changes: []model.Change{change},
	})
	var strict fragmentAssembly
	if _, err := strict.push(50, wire, decodeChange); err == nil {
		t.Fatal("durable checkpoint consumer accepted a mid-transaction start")
	}
	var barrier fragmentAssembly
	if records, err := barrier.push(50, wire, decodeChange, true); err != nil || len(records) != 0 {
		t.Fatalf("barrier scan did not discard leading suffix: records=%v err=%v", records, err)
	}
}

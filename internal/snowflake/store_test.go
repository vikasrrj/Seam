package snowflake

import (
	"context"
	"crypto/sha256"
	"strings"
	"testing"

	"example.com/seam/internal/kafka"
	"example.com/seam/internal/model"
	"example.com/seam/internal/schema"
)

func TestApplyTransactionRequiresSinkLease(t *testing.T) {
	store := &Store{cfg: testConfig(), schema: testSchema()}
	err := store.ApplyTransaction(context.Background(), nil, &kafka.Transaction{})
	if err == nil || !strings.Contains(err.Error(), "requires a sink lease") {
		t.Fatalf("error = %v", err)
	}
}

func TestClassifyReplayUsesSourceIdentityNotOriginalKafkaOffset(t *testing.T) {
	tx := &kafka.Transaction{
		Source:      model.SourceTx{SystemID: "system", Generation: "generation", LSN: "0/20"},
		FirstOffset: 100,
		FinalOffset: 102,
		TotalCount:  3,
	}
	ledger := &appliedTransaction{
		firstOffset: 8,
		finalOffset: 9,
		eventCount:  3,
		fingerprint: "stable-content",
	}
	action, err := classifyReplay(100, tx, 3, "stable-content", ledger)
	if err != nil {
		t.Fatalf("classify replay: %v", err)
	}
	if action != replayAdvanceOnly {
		t.Fatalf("action = %v, want replayAdvanceOnly", action)
	}
}

func TestClassifyReplayStateMachine(t *testing.T) {
	base := kafka.Transaction{
		Source:      model.SourceTx{SystemID: "system", Generation: "generation", LSN: "0/20"},
		FirstOffset: 10,
		FinalOffset: 12,
		TotalCount:  3,
	}
	validLedger := &appliedTransaction{eventCount: 3, fingerprint: "hash"}
	tests := []struct {
		name        string
		frontier    int64
		ledger      *appliedTransaction
		count       int
		fingerprint string
		want        replayAction
		wantError   string
	}{
		{name: "new contiguous transaction", frontier: 10, count: 3, fingerprint: "hash", want: replayApply},
		{name: "duplicate at frontier", frontier: 10, ledger: validLedger, count: 3, fingerprint: "hash", want: replayAdvanceOnly},
		{name: "ambiguous commit replay already passed", frontier: 13, ledger: validLedger, count: 3, fingerprint: "hash", want: replayAlreadyPast},
		{name: "content mismatch", frontier: 10, ledger: validLedger, count: 3, fingerprint: "other", wantError: "content differs"},
		{name: "event count mismatch", frontier: 10, ledger: validLedger, count: 2, fingerprint: "hash", wantError: "content differs"},
		{name: "legacy ledger", frontier: 10, ledger: &appliedTransaction{}, count: 3, fingerprint: "hash", wantError: "resnapshot required"},
		{name: "gap", frontier: 9, count: 3, fingerprint: "hash", wantError: "offset gap"},
		{name: "missing ledger behind frontier", frontier: 14, count: 3, fingerprint: "hash", wantError: "no source-transaction ledger"},
		{name: "frontier inside new transaction", frontier: 11, count: 3, fingerprint: "hash", wantError: "inside unapplied transaction"},
		{name: "frontier inside replay", frontier: 11, ledger: validLedger, count: 3, fingerprint: "hash", wantError: "inside replayed transaction"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tx := base
			action, err := classifyReplay(test.frontier, &tx, test.count, test.fingerprint, test.ledger)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("error = %v, want substring %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("classify replay: %v", err)
			}
			if action != test.want {
				t.Fatalf("action = %v, want %v", action, test.want)
			}
		})
	}
}

func TestTransactionFingerprintIsOffsetIndependentAndContentSensitive(t *testing.T) {
	source := model.SourceTx{SystemID: "system", Generation: "generation", XID: 42, LSN: "0/20"}
	change := model.Change{
		Op:       model.OpUpdate,
		SchemaID: "schema",
		Source:   source,
		Row: &model.Row{Values: []model.Value{
			model.Int64Value(7),
			model.TextValue("value"),
		}},
	}
	fingerprint := func(source model.SourceTx, changes ...model.Change) string {
		h := sha256.New()
		writeFingerprintString(h, "seam-snowflake-transaction-v1")
		writeSourceFingerprint(h, source)
		for i := range changes {
			writeChangeFingerprint(h, &changes[i])
		}
		return string(h.Sum(nil))
	}
	first := fingerprint(source, change)
	if got := fingerprint(source, change); got != first {
		t.Fatal("identical source transaction produced an unstable fingerprint")
	}
	changed := change
	changed.Row = &model.Row{Values: append([]model.Value(nil), change.Row.Values...)}
	changed.Row.Values[1] = model.TextValue("different")
	if got := fingerprint(source, changed); got == first {
		t.Fatal("payload change did not change transaction fingerprint")
	}
	changedSource := source
	changedSource.XID++
	if got := fingerprint(changedSource, change); got == first {
		t.Fatal("source identity change did not change transaction fingerprint")
	}
}

func TestStageInsertSQLBatchesRowsInOneStatement(t *testing.T) {
	cfg := testConfig()
	query, args := stageInsertSQL(cfg, "batch", "source", 32, []stagedChange{
		{sequence: 0, op: model.OpInsert, pk: 1, payload: `{"id":"1"}`},
		{sequence: 1, op: model.OpUpdate, pk: 2, payload: `{"id":"2"}`},
	})
	if got := strings.Count(query, "SELECT ?, ?, ?, ?, ?, ?, PARSE_JSON(?)"); got != 2 {
		t.Fatalf("stage SELECT count = %d, want 2: %s", got, query)
	}
	if !strings.Contains(query, " UNION ALL ") {
		t.Fatalf("stage query is not set-based: %s", query)
	}
	if len(args) != 14 {
		t.Fatalf("argument count = %d, want 14", len(args))
	}
}

func TestMarkerProcedureUsesCallerTransactionAndChecksEveryWrite(t *testing.T) {
	query := createMarkerProcedureSQL(testConfig())
	for _, required := range []string{
		"CREATE OR REPLACE PROCEDURE", "EXECUTE AS CALLER", "getNumRowsAffected",
		"marker insert", "lease fence", "replay ledger insert", "frontier compare-and-set",
		`INSERT INTO \"DB\".\"SEAM_INTERNAL\".\"MARKERS\"`,
		`UPDATE \"DB\".\"SEAM_INTERNAL\".\"SINK_LEASES\"`,
		`INSERT INTO \"DB\".\"SEAM_INTERNAL\".\"APPLIED_TRANSACTIONS\"`,
		`UPDATE \"DB\".\"SEAM_INTERNAL\".\"OFFSETS\"`,
	} {
		if !strings.Contains(query, required) {
			t.Fatalf("marker procedure lacks %q", required)
		}
	}
	for _, forbidden := range []string{"BEGIN TRANSACTION", "COMMIT", "ROLLBACK"} {
		if strings.Contains(query, forbidden) {
			t.Fatalf("marker procedure must remain inside caller transaction, found %q", forbidden)
		}
	}
}

func TestMergeSQLUsesStreamScopedSequenceClock(t *testing.T) {
	cfg := testConfig()
	source := testSchema()
	merge, err := mergeTargetSQL(cfg, source, cfg.LiveTable)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"K.STREAM_ID = ?", "R.SEQUENCE >= K.SOURCE_SEQUENCE", "R.SEQUENCE AS SOURCE_SEQUENCE"} {
		if !strings.Contains(merge, required) {
			t.Fatalf("target merge lacks %q: %s", required, merge)
		}
	}
	for _, required := range []string{`R.PAYLOAD:"id"`, `R.PAYLOAD:"name"`} {
		if !strings.Contains(merge, required) {
			t.Fatalf("target merge changes staged JSON key spelling for %q: %s", required, merge)
		}
	}
	clock := mergeClocksSQL(cfg)
	for _, required := range []string{"K.STREAM_ID = ?", "SOURCE_SEQUENCE", "INSERT (STREAM_ID, ROUTE_ID"} {
		if !strings.Contains(clock, required) {
			t.Fatalf("clock merge lacks %q: %s", required, clock)
		}
	}
}

func TestConfigAndSchemaFailClosed(t *testing.T) {
	cfg := testConfig()
	cfg.Partition = 1
	if err := cfg.validate(); err == nil || !strings.Contains(err.Error(), "partition 0") {
		t.Fatalf("partition validation error = %v", err)
	}
	unsupported := testSchema()
	unsupported.Columns[1].TypeOID = 1700
	unsupported.Columns[1].TypeName = "numeric"
	if err := ValidateSourceSchema(unsupported); err == nil || !strings.Contains(err.Error(), "no proven lossless") {
		t.Fatalf("unsupported type error = %v", err)
	}
}

func testConfig() Config {
	return Config{
		Database:       "DB",
		Schema:         "PUBLIC",
		InternalSchema: "SEAM_INTERNAL",
		LiveTable:      "ACCOUNTS",
		StreamID:       "stream",
		TopicID:        "topic-id",
		Partition:      0,
	}
}

func testSchema() *schema.Schema {
	return &schema.Schema{
		Namespace: "public",
		Table:     "accounts",
		Columns: []schema.Column{
			{Ordinal: 1, Name: "id", TypeName: "int8", TypeOID: 20, PrimaryKey: true},
			{Ordinal: 2, Name: "name", TypeName: "text", TypeOID: 25, Nullable: true},
		},
		PKOrdinal:       1,
		Fingerprint:     "schema",
		ReplicaIdentity: "f",
	}
}

package checkpoint

import (
	"encoding/json"
	"strings"
	"testing"

	"example.com/seam/internal/model"
	"example.com/seam/internal/schema"
)

func testAccountsSchema() *schema.Schema {
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

// TestCandidatePayloadIsCanonicalJSONObjectArray pins the durable staging
// contract: one dumped window is a JSON array with a single object keyed by
// column name, every value travels as canonical PostgreSQL text, and SQL NULL
// is JSON null. The sink rebuilds rows from jsonb_array_elements and casts
// (elem->>'col')::type, so keying by name makes the payload position-agnostic
// and the canonical-text form keeps the transport lossless.
func TestCandidatePayloadIsCanonicalJSONObjectArray(t *testing.T) {
	row := &model.Row{Values: []model.Value{model.Int64Value(7), model.TextValue("alice"), model.NullValue()}}
	payload, err := candidatePayload(testAccountsSchema(), row)
	if err != nil {
		t.Fatal(err)
	}
	var arr []map[string]any
	if err := json.Unmarshal(payload, &arr); err != nil {
		t.Fatal(err)
	}
	if len(arr) != 1 {
		t.Fatalf("payload must be a one-row JSON array, got %d rows", len(arr))
	}
	if id, ok := arr[0]["id"].(string); !ok || id != "7" {
		t.Fatalf("primary key must travel as canonical text, got %#v", arr[0]["id"])
	}
	if owner, ok := arr[0]["owner"].(string); !ok || owner != "alice" {
		t.Fatalf("owner = %#v", arr[0]["owner"])
	}
	if v := arr[0]["balance_cents"]; v != nil {
		t.Fatalf("NULL column must be JSON null, got %#v", v)
	}
}

func TestCandidatePayloadFailsClosedOnMalformedRows(t *testing.T) {
	s := testAccountsSchema()
	if _, err := candidatePayload(s, &model.Row{Values: []model.Value{model.Int64Value(1)}}); err == nil || !strings.Contains(err.Error(), "schema expects") {
		t.Fatalf("short row accepted: %v", err)
	}
	bad := &model.Row{Values: []model.Value{model.Int64Value(1), model.TextValue("a"), {Kind: model.ValueKind(99)}}}
	if _, err := candidatePayload(s, bad); err == nil {
		t.Fatal("unknown value kind accepted")
	}
}

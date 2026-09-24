package schema

import (
	"testing"

	"example.com/seam/internal/model"
)

func testAccountsSchema() *Schema {
	s := &Schema{
		Namespace:       "public",
		Table:           "accounts",
		ReplicaIdentity: "f",
		PKOrdinal:       1,
		Columns: []Column{
			{Ordinal: 1, Name: "id", TypeName: "int8", TypeOID: 20, PrimaryKey: true},
			{Ordinal: 2, Name: "owner", TypeName: "text", TypeOID: 25},
			{Ordinal: 3, Name: "balance_cents", TypeName: "int8", TypeOID: 20},
		},
	}
	s.Fingerprint = s.FingerprintFor()
	return s
}

func TestSupportedTypesExcludesLossyTypes(t *testing.T) {
	// The transport is canonical PostgreSQL text, which round-trips exactly
	// for the listed set. Anything outside it, notably money (whose output
	// depends on lc_monetary), must fail closed at schema load.
	if _, ok := SupportedTypes[790]; ok {
		t.Fatal("money must not be a supported type: its text output is lc_monetary-dependent")
	}
	for _, oid := range []uint32{20, 21, 23} {
		if _, ok := SupportedTypes[oid]; !ok {
			t.Fatalf("integer OID %d must be supported", oid)
		}
	}
	if SupportedTypes[3802] != "jsonb" || SupportedTypes[2950] != "uuid" {
		t.Fatalf("jsonb/uuid must stay in the supported set")
	}
}

func TestFingerprintIsDeterministicAndDriftSensitive(t *testing.T) {
	base := testAccountsSchema()
	if got := base.FingerprintFor(); got != base.Fingerprint {
		t.Fatalf("computed fingerprint %q differs from recorded %q", got, base.Fingerprint)
	}
	if base.FingerprintFor() != base.FingerprintFor() {
		t.Fatal("fingerprint must be deterministic")
	}

	mutate := func(f func(*Schema)) *Schema {
		clone := *base
		clone.Columns = append([]Column(nil), base.Columns...)
		f(&clone)
		return &clone
	}
	cases := []struct {
		name   string
		mutate func(*Schema)
	}{
		{"column renames", func(s *Schema) { s.Columns[1].Name = "holder" }},
		{"column nullability flips", func(s *Schema) { s.Columns[1].Nullable = true }},
		{"column type changes", func(s *Schema) { s.Columns[2].TypeName = "integer"; s.Columns[2].TypeOID = 23 }},
		{"column order changes", func(s *Schema) { s.Columns[0], s.Columns[1] = s.Columns[1], s.Columns[0] }},
		{"replica identity changes", func(s *Schema) { s.ReplicaIdentity = "d" }},
		{"table renames", func(s *Schema) { s.Table = "accounts_shadow" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := mutate(tc.mutate).FingerprintFor(); got == base.Fingerprint {
				t.Fatalf("fingerprint unchanged after %s", tc.name)
			}
		})
	}
}

func TestCanonicalListsEnforceCanonicalTextTransport(t *testing.T) {
	s := testAccountsSchema()
	if got := s.CanonicalColumnList(); got != "id, owner, balance_cents" {
		t.Fatalf("CanonicalColumnList = %q", got)
	}
	if got := s.CanonicalScanList(); got != "(id)::text, (owner)::text, (balance_cents)::text" {
		t.Fatalf("CanonicalScanList = %q", got)
	}
	if got := s.CanonicalApplyList(s.Columns, 1); got != "($1::text)::int8, ($2::text)::text, ($3::text)::int8" {
		t.Fatalf("CanonicalApplyList = %q", got)
	}
	if got := s.PKColumn().Name; got != "id" {
		t.Fatalf("PKColumn = %q", got)
	}
	if got := s.SQLTable(); got != "public.accounts" {
		t.Fatalf("SQLTable = %q", got)
	}
}

func TestKeyFromRowAndChange(t *testing.T) {
	s := testAccountsSchema()
	row := &model.Row{Values: []model.Value{model.Int64Value(7), model.TextValue("alice"), model.Int64Value(500)}}
	if key, err := s.KeyFromRow(row); err != nil || key != 7 {
		t.Fatalf("KeyFromRow = %d, %v", key, err)
	}
	change := model.Change{Op: model.OpUpdate, Row: row}
	if key, err := s.KeyFromChange(&change); err != nil || key != 7 {
		t.Fatalf("KeyFromChange = %d, %v", key, err)
	}

	// Every malformed shape fails closed instead of producing a wrong key.
	if _, err := s.KeyFromRow(nil); err == nil {
		t.Fatal("nil row accepted")
	}
	if _, err := s.KeyFromRow(&model.Row{}); err == nil {
		t.Fatal("row too short for the primary key ordinal accepted")
	}
	if _, err := s.KeyFromChange(&model.Change{}); err == nil {
		t.Fatal("change without a row payload accepted")
	}
	if _, err := s.KeyFromRow(&model.Row{Values: []model.Value{model.NullValue(), model.TextValue("x"), model.Int64Value(1)}}); err == nil {
		t.Fatal("NULL primary key accepted")
	}
	if _, err := s.KeyFromRow(&model.Row{Values: []model.Value{model.TextValue("7"), model.TextValue("x"), model.Int64Value(1)}}); err == nil {
		t.Fatal("primary key not carried as typed int64 accepted")
	}
}

func TestValidateIdentifier(t *testing.T) {
	for _, id := range []string{"accounts", "id", "_internal", "A1_ok"} {
		if err := ValidateIdentifier(id); err != nil {
			t.Fatalf("ValidateIdentifier(%q) rejected a valid identifier: %v", id, err)
		}
	}
	for _, id := range []string{"", "account; DROP TABLE x", "account id", `ac"count`, "1abc", "public.accounts", "account\n", "accounts-"} {
		if err := ValidateIdentifier(id); err == nil {
			t.Fatalf("ValidateIdentifier(%q) accepted an unsafe identifier", id)
		}
	}
}

func TestBindValue(t *testing.T) {
	s := testAccountsSchema()
	if v := s.BindValue(s.Columns[0], model.Int64Value(7)); v.(int64) != 7 {
		t.Fatalf("integer column bound as %#v", v)
	}
	if v := s.BindValue(s.Columns[1], model.TextValue("alice")); v.(string) != "alice" {
		t.Fatalf("text column bound as %#v", v)
	}
	if v := s.BindValue(s.Columns[1], model.NullValue()); v != nil {
		t.Fatalf("NULL bound as %#v", v)
	}
}

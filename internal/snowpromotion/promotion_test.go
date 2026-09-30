package snowpromotion

import (
	"context"
	"strings"
	"testing"

	"example.com/seam/internal/model"
	"example.com/seam/internal/schema"
	seamsnowflake "example.com/seam/internal/snowflake"
)

type stateWarehouse struct {
	Warehouse
	job *seamsnowflake.BackfillJob
}

func (warehouse stateWarehouse) LoadBackfill(context.Context, string) (*seamsnowflake.BackfillJob, error) {
	return warehouse.job, nil
}

func TestValidateIsIdempotentAfterValidation(t *testing.T) {
	for _, state := range []seamsnowflake.BackfillState{
		seamsnowflake.BackfillReady,
		seamsnowflake.BackfillPromoting,
		seamsnowflake.BackfillCompleted,
	} {
		t.Run(string(state), func(t *testing.T) {
			manager := &Manager{cfg: Config{JobID: "job"}, store: stateWarehouse{job: &seamsnowflake.BackfillJob{State: state}}}
			if err := manager.Validate(context.Background()); err != nil {
				t.Fatalf("Validate() = %v", err)
			}
		})
	}
}

func TestValidateRejectsBackfillBeforeScanCompletion(t *testing.T) {
	manager := &Manager{cfg: Config{JobID: "job"}, store: stateWarehouse{job: &seamsnowflake.BackfillJob{State: seamsnowflake.BackfillRunning}}}
	err := manager.Validate(context.Background())
	if err == nil || !strings.Contains(err.Error(), "ready_to_verify") {
		t.Fatalf("Validate() = %v", err)
	}
}

func TestCompareRowsReportsExactFirstDifference(t *testing.T) {
	descriptor := comparisonSchema()
	source := []model.Row{{Values: []model.Value{model.Int64Value(1), model.TextValue("Ada")}}}
	destination := []model.Row{{Values: []model.Value{model.Int64Value(1), model.TextValue("Ada")}}}
	if err := compareRows(descriptor, source, destination); err != nil {
		t.Fatalf("equal rows: %v", err)
	}

	destination[0].Values[1] = model.TextValue("Grace")
	if err := compareRows(descriptor, source, destination); err == nil || !strings.Contains(err.Error(), `key 1 column "name" differs`) {
		t.Fatalf("value mismatch error = %v", err)
	}
	if err := compareRows(descriptor, source, nil); err == nil || !strings.Contains(err.Error(), "row count differs") {
		t.Fatalf("count mismatch error = %v", err)
	}
}

func TestCompareRowsDetectsMissingKeyDespiteEqualCount(t *testing.T) {
	descriptor := comparisonSchema()
	source := []model.Row{{Values: []model.Value{model.Int64Value(1), model.TextValue("Ada")}}}
	destination := []model.Row{{Values: []model.Value{model.Int64Value(2), model.TextValue("Ada")}}}
	if err := compareRows(descriptor, source, destination); err == nil || !strings.Contains(err.Error(), "ordered primary key differs") {
		t.Fatalf("key mismatch error = %v", err)
	}
}

func TestSortedKeysIsDeterministic(t *testing.T) {
	keys := sortedKeys(map[int64]struct{}{9: {}, -1: {}, 3: {}})
	want := []int64{-1, 3, 9}
	for index := range want {
		if keys[index] != want[index] {
			t.Fatalf("keys = %v, want %v", keys, want)
		}
	}
}

func comparisonSchema() *schema.Schema {
	return &schema.Schema{
		Columns: []schema.Column{
			{Ordinal: 1, Name: "id", TypeOID: 20, TypeName: "int8", PrimaryKey: true},
			{Ordinal: 2, Name: "name", TypeOID: 25, TypeName: "text"},
		},
		PKOrdinal: 1,
	}
}

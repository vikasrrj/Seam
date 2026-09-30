package snowflake

import (
	"context"
	"errors"
	"math"
	"regexp"
	"strings"
	"testing"

	"example.com/seam/internal/model"
	"example.com/seam/internal/schema"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestCanonicalJSONUsesSemanticNumberAndObjectEquality(t *testing.T) {
	left, err := canonicalJSON(`{"b":1.0,"a":[true,null]}`)
	if err != nil {
		t.Fatal(err)
	}
	right, err := canonicalJSON(`{"a":[true,null],"b":1}`)
	if err != nil {
		t.Fatal(err)
	}
	if left != right {
		t.Fatalf("semantic JSON differs: %q != %q", left, right)
	}
}

func TestNormalizeSourceRowsMatchesSnowflakeTimestampForms(t *testing.T) {
	descriptor := &schema.Schema{Columns: []schema.Column{
		{Name: "created", TypeOID: 1114, TypeName: "timestamp"},
		{Name: "updated", TypeOID: 1184, TypeName: "timestamptz"},
	}}
	rows, err := NormalizeSourceRows(descriptor, []model.Row{{Values: []model.Value{
		model.TextValue("2026-09-30 12:13:14.12"),
		model.TextValue("2026-09-30 12:13:14.120000+00"),
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].Values[0].Text != "2026-09-30T12:13:14.12" || rows[0].Values[1].Text != "2026-09-30T12:13:14.12Z" {
		t.Fatalf("unexpected normalized timestamps: %+v", rows[0].Values)
	}
}

func TestLiveViewReplacementIsIdempotentPointerChange(t *testing.T) {
	query := createLiveViewSQL(testConfig(), testSchema(), "ACCOUNTS_SHADOW", true)
	if !strings.HasPrefix(query, `CREATE OR REPLACE VIEW "DB"."PUBLIC"."ACCOUNTS" AS SELECT`) || !strings.Contains(query, `FROM "DB"."PUBLIC"."ACCOUNTS_SHADOW"`) {
		t.Fatalf("unexpected view SQL: %s", query)
	}
}

func TestPrepareValidationClonePersistsIntentBeforeDDL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := &Store{db: db, cfg: testConfig(), schema: testSchema()}
	mock.ExpectQuery("SELECT FINAL_OFFSET, SOURCE_LSN FROM .*MARKERS").
		WithArgs("stream", "validation-marker").
		WillReturnRows(sqlmock.NewRows([]string{"offset", "lsn"}).AddRow(50, "32"))
	expectBackfillLoad(mock, string(BackfillReadyToVerify), nil, nil, nil, false, nil, nil)
	mock.ExpectExec("UPDATE .*BACKFILL_JOBS.*VALIDATION_TABLE").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`CREATE OR REPLACE TRANSIENT TABLE "DB"."PUBLIC"."ACCOUNTS_VALIDATION" CLONE "DB"."PUBLIC"."ACCOUNTS_SHADOW"`)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("UPDATE .*BACKFILL_JOBS.*VALIDATION_READY = TRUE").
		WillReturnResult(sqlmock.NewResult(0, 1))
	job, err := store.PrepareValidationClone(context.Background(), "job", "validation-marker", "ACCOUNTS_VALIDATION")
	if err != nil {
		t.Fatal(err)
	}
	if job.State != BackfillVerifying || !job.ValidationReady || job.ValidationOffset != 50 {
		t.Fatalf("unexpected validation job: %+v", job)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPromoteViewCanResumeFromDurableIntent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := &Store{db: db, cfg: testConfig(), schema: testSchema()}
	promotionMarker := "promotion-marker"
	promotionOffset := int64(75)
	mock.ExpectQuery("SELECT FINAL_OFFSET, SOURCE_LSN FROM .*MARKERS").
		WithArgs("stream", promotionMarker).
		WillReturnRows(sqlmock.NewRows([]string{"offset", "lsn"}).AddRow(promotionOffset, "64"))
	expectBackfillLoad(mock, string(BackfillPromoting), stringPointer("ACCOUNTS_VALIDATION"), stringPointer("validation-marker"), int64Pointer(50), true, &promotionMarker, &promotionOffset)
	mock.ExpectExec("CREATE OR REPLACE VIEW").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE .*ROUTES.*TARGET_TABLE").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE .*ROUTES.*ACTIVE = FALSE").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE .*BACKFILL_JOBS.*STATE").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := store.PromoteView(context.Background(), "job", promotionMarker); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestFailBackfillDeactivatesShadowRouteAtomically(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := &Store{db: db, cfg: testConfig(), schema: testSchema()}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT SHADOW_ROUTE_ID, STATE FROM .*BACKFILL_JOBS").
		WithArgs("stream", "job").
		WillReturnRows(sqlmock.NewRows([]string{"route", "state"}).AddRow("shadow:job:attempt", string(BackfillVerifying)))
	mock.ExpectExec("UPDATE .*ROUTES.*ACTIVE = FALSE").
		WithArgs("stream", "shadow:job:attempt").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE .*BACKFILL_JOBS.*ERROR_MESSAGE").
		WithArgs(string(BackfillFailed), "comparison mismatch", "stream", "job", string(BackfillCompleted)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := store.FailBackfill(context.Background(), "job", errors.New("comparison mismatch")); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestFailBackfillDoesNotUndoCompletedPromotion(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := &Store{db: db, cfg: testConfig(), schema: testSchema()}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT SHADOW_ROUTE_ID, STATE FROM .*BACKFILL_JOBS").
		WithArgs("stream", "job").
		WillReturnRows(sqlmock.NewRows([]string{"route", "state"}).AddRow("shadow:job:attempt", string(BackfillCompleted)))
	mock.ExpectCommit()
	if err := store.FailBackfill(context.Background(), "job", errors.New("late failure")); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func expectBackfillLoad(mock sqlmock.Sqlmock, state string, validationTable, validationMarker *string, validationOffset *int64, validationReady bool, promotionMarker *string, promotionOffset *int64) {
	mock.ExpectQuery("SELECT ATTEMPT, STATE, SHADOW_ROUTE_ID").
		WithArgs("stream", "job").
		WillReturnRows(sqlmock.NewRows([]string{
			"attempt", "state", "route", "shadow", "upper", "start", "total", "completed",
			"validation_table", "validation_marker", "validation_offset", "validation_ready",
			"promotion_marker", "promotion_offset", "error",
		}).AddRow("attempt", state, "shadow:job:attempt", "ACCOUNTS_SHADOW", 10, 0, 1, 1,
			validationTable, validationMarker, validationOffset, validationReady, promotionMarker, promotionOffset, nil))
	mock.ExpectQuery("SELECT CHUNK_MIN, CHUNK_MAX FROM .*BACKFILL_CHUNKS").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(int64(math.MinInt64), int64(10)))
}

func stringPointer(value string) *string { return &value }
func int64Pointer(value int64) *int64    { return &value }

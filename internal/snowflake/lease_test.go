package snowflake

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestAcquireSinkLeaseReturnsMonotonicToken(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := &Store{db: db, cfg: testConfig(), schema: testSchema()}
	expires := time.Now().Add(time.Minute)
	mock.ExpectExec("UPDATE .*SINK_LEASES.*OWNER_EPOCH = OWNER_EPOCH \\+ 1").
		WithArgs("owner-a", int64(30000), "stream").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT OWNER_ID, OWNER_EPOCH, LEASE_EXPIRES FROM .*SINK_LEASES").
		WithArgs("stream").
		WillReturnRows(sqlmock.NewRows([]string{"owner", "epoch", "expires"}).AddRow("owner-a", int64(7), expires))
	lease, err := store.AcquireSinkLease(context.Background(), "owner-a", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if lease.OwnerID != "owner-a" || lease.Epoch != 7 {
		t.Fatalf("lease = %+v", lease)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireSinkLeaseReportsCurrentOwner(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := &Store{db: db, cfg: testConfig(), schema: testSchema()}
	expires := time.Now().Add(time.Minute)
	mock.ExpectExec("UPDATE .*SINK_LEASES").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT OWNER_ID, OWNER_EPOCH, LEASE_EXPIRES FROM .*SINK_LEASES").
		WillReturnRows(sqlmock.NewRows([]string{"owner", "epoch", "expires"}).AddRow("owner-a", int64(7), expires))
	_, err = store.AcquireSinkLease(context.Background(), "owner-b", 30*time.Second)
	var held *LeaseHeldError
	if !errors.As(err, &held) || held.OwnerID != "owner-a" || held.Epoch != 7 {
		t.Fatalf("error = %v", err)
	}
}

func TestFenceSinkLeaseRejectsStaleOwner(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := &Store{db: db, cfg: testConfig(), schema: testSchema()}
	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec("UPDATE .*SINK_LEASES.*LEASE_EXPIRES > CURRENT_TIMESTAMP").
		WithArgs("stream", "stale", int64(3)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	err = store.fenceSinkLease(context.Background(), tx, &SinkLease{OwnerID: "stale", Epoch: 3})
	if err == nil || !strings.Contains(err.Error(), "stale or expired") {
		t.Fatalf("error = %v", err)
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRenewSinkLeaseFailsClosedAfterTakeover(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := &Store{db: db, cfg: testConfig(), schema: testSchema()}
	mock.ExpectExec("UPDATE .*SINK_LEASES.*LEASE_EXPIRES = DATEADD").
		WithArgs(int64(30000), "stream", "owner-a", int64(7)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	err = store.RenewSinkLease(context.Background(), &SinkLease{OwnerID: "owner-a", Epoch: 7}, 30*time.Second)
	if err == nil || !strings.Contains(err.Error(), "lease lost") {
		t.Fatalf("error = %v", err)
	}
}

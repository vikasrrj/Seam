//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"example.com/seam/integration/itest"
	"example.com/seam/internal/capture"
)

func TestCaptureLeadershipFencesConcurrentOwnerAndAdvancesEpoch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := itest.ResetTables(ctx); err != nil {
		t.Fatal(err)
	}
	config := func(owner string) capture.ReaderConfig {
		return capture.ReaderConfig{
			SQLDSN: itest.SourceDSN(), ReplicationDSN: itest.SourceReplDSN(),
			Slot: "seam_itest_slot", Publication: "seam_pub", KafkaBrokers: itest.KafkaBrokers(),
			KafkaTopic: itest.KafkaTopic(), Generation: "gen:0", OwnerID: owner,
		}
	}
	first, err := capture.StartReader(ctx, config("capture-one"))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	if contender, err := capture.StartReader(ctx, config("capture-two")); err == nil {
		contender.Close()
		t.Fatal("concurrent capture owner unexpectedly acquired the slot")
	} else if !strings.Contains(err.Error(), "live owner") {
		t.Fatalf("concurrent owner error = %v", err)
	}
	assertCaptureOwner(t, ctx, "capture-one", 1)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := capture.StartReader(ctx, config("capture-two"))
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	assertCaptureOwner(t, ctx, "capture-two", 2)

	changed := config("capture-three")
	changed.Generation = "gen:wrong"
	if reader, err := capture.StartReader(ctx, changed); err == nil {
		reader.Close()
		t.Fatal("capture takeover accepted a changed generation")
	} else if !strings.Contains(err.Error(), "identity changed") {
		t.Fatalf("identity mismatch error = %v", err)
	}
}

func assertCaptureOwner(t *testing.T, ctx context.Context, owner string, epoch int64) {
	t.Helper()
	conn, err := itest.SourceConn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	var gotOwner string
	var gotEpoch int64
	if err := conn.QueryRow(ctx, `SELECT owner_id, owner_epoch FROM seam_capture_owners WHERE slot_name = 'seam_itest_slot'`).Scan(&gotOwner, &gotEpoch); err != nil {
		t.Fatal(err)
	}
	if gotOwner != owner || gotEpoch != epoch {
		t.Fatalf("capture owner=(%q,%d), want (%q,%d)", gotOwner, gotEpoch, owner, epoch)
	}
}

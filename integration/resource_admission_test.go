//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"example.com/seam/integration/itest"
	resourcecontrol "example.com/seam/internal/resource"
)

func TestProcessSharedResourcePermits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	firstPool, err := resourcecontrol.NewPostgresAdvisoryPool(itest.DestDSN(), resourcecontrol.DestinationTxClass, 1, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer firstPool.Close()
	secondPool, err := resourcecontrol.NewPostgresAdvisoryPool(itest.DestDSN(), resourcecontrol.DestinationTxClass, 1, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer secondPool.Close()
	first, err := firstPool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	acquired := make(chan func(), 1)
	errs := make(chan error, 1)
	go func() {
		release, acquireErr := secondPool.Acquire(ctx)
		if acquireErr != nil {
			errs <- acquireErr
			return
		}
		acquired <- release
	}()
	select {
	case release := <-acquired:
		release()
		t.Fatal("second process acquired the only advisory permit while it was held")
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(100 * time.Millisecond):
	}
	first()
	select {
	case release := <-acquired:
		release()
	case err := <-errs:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal("second process did not acquire permit after owner connection closed")
	}
}

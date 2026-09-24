// Package recovery validates continuity and prepares a checkpoint for restart.
package recovery

import (
	"context"
	"fmt"

	"example.com/seam/internal/checkpoint"
	"example.com/seam/internal/model"
	"example.com/seam/internal/transport"
)

// Result is the validated state used to resume a Seam job.
type Result struct {
	Checkpoint *model.Checkpoint
	Record     *checkpoint.JobRecord
}

// Recover loads the job record and checkpoint, validates continuity, and bumps
// the attempt if a previous chunk was left unfinished.
func Recover(ctx context.Context, cpStore *checkpoint.Store, jobID string, expectedSourceDSN string) (*Result, error) {
	rec, err := cpStore.LoadJobRecord(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("load job record: %w", err)
	}
	if rec == nil {
		return nil, fmt.Errorf("job %q not found", jobID)
	}

	cp, err := cpStore.LoadCheckpoint(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("load checkpoint: %w", err)
	}
	if cp == nil {
		return nil, fmt.Errorf("checkpoint missing for job %q", jobID)
	}
	if !cp.Active {
		return nil, fmt.Errorf("job %q is inactive after promotion", jobID)
	}

	if rec.SourceDSNHash == "" || rec.SourceDSNHash != checkpoint.DSNFingerprint(expectedSourceDSN) {
		return nil, fmt.Errorf("source connection fingerprint changed or was never recorded; resnapshot required")
	}

	// Validate that destination schema epoch has not changed. The fingerprint
	// is derived from the job's durable source descriptor, so drift on either
	// side of the pipeline fails closed.
	conn, err := cpStore.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	if rec.SourceSchema == nil {
		return nil, fmt.Errorf("job %q has no durable source schema descriptor; resnapshot required", jobID)
	}
	fp, err := checkpoint.FingerprintFor(ctx, conn, rec.Config.DestTable, rec.SourceSchema)
	if err != nil {
		return nil, fmt.Errorf("fingerprint destination schema: %w", err)
	}
	if rec.SourceSchemaFingerprint != "" && fp != rec.SourceSchemaFingerprint {
		return nil, fmt.Errorf("destination schema epoch drifted: expected %s, current %s", rec.SourceSchemaFingerprint, fp)
	}
	if fp != rec.SchemaFingerprint {
		return nil, fmt.Errorf("schema fingerprint mismatch: expected %s, current %s", rec.SchemaFingerprint, fp)
	}

	// Hard-fail if the replication slot has disappeared (broken CDC history).
	if err := checkSlotContinuity(ctx, expectedSourceDSN, rec.Config.SourceSlot); err != nil {
		return nil, err
	}

	incomplete, err := cpStore.HasIncompleteChunks(ctx, jobID, cp.Attempt)
	if err != nil {
		return nil, fmt.Errorf("load incomplete chunks: %w", err)
	}

	// If a chunk was unfinished, start a fresh attempt with a new marker window.
	if cp.CompletedThrough < cp.ScanUpperBound && !incomplete {
		return nil, fmt.Errorf("job %q has no remaining chunks but backfill frontier %d is below upper bound %d; discovery or chunk state is incomplete", jobID, cp.CompletedThrough, cp.ScanUpperBound)
	}
	if incomplete {
		newAttempt, err := BumpAttempt(cp.Attempt)
		if err != nil {
			return nil, err
		}
		if err := cpStore.BeginAttemptOwned(ctx, cp, cp.Attempt, newAttempt, cp.NextKafkaOffset); err != nil {
			return nil, fmt.Errorf("begin recovery attempt: %w", err)
		}
		cp.Attempt = newAttempt
		logPrintf("recovery: unfinished chunk detected, new attempt=%s", cp.Attempt)
	}

	return &Result{Checkpoint: cp, Record: rec}, nil
}

// checkSlotContinuity ensures the replication slot still exists for capture
// to resume from. PostgreSQL WAL itself may be recycled safely once changes
// are published to Kafka; the Kafka history is the replay source. Kafka offset
// continuity is enforced via recovery.CheckKafkaRetention.
func checkSlotContinuity(ctx context.Context, dsn, slot string) error {
	conn, err := transport.ConnectPostgres(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect source for continuity check: %w", err)
	}
	defer conn.Close(context.Background())

	var exists bool
	if err := conn.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, slot,
	).Scan(&exists); err != nil {
		return fmt.Errorf("read slot %q: %w", slot, err)
	}
	if !exists {
		return fmt.Errorf("replication slot %q no longer exists; CDC history lost, reset required", slot)
	}
	return nil
}

func logPrintf(format string, args ...interface{}) {
	// Use fmt.Printf for now; telemetry can redirect later.
	fmt.Printf("seam: "+format+"\n", args...)
}

// Package recovery validates continuity and prepares a checkpoint for restart.
package recovery

import (
	"context"
	"fmt"
	"time"

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

	if rec.Config.SourceDSN != expectedSourceDSN {
		return nil, fmt.Errorf("source dsn mismatch: expected %q, stored %q", expectedSourceDSN, rec.Config.SourceDSN)
	}

	// Validate that destination schema fingerprint has not changed.
	conn, err := cpStore.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	fp, err := checkpoint.Fingerprint(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("fingerprint destination schema: %w", err)
	}
	if fp != rec.SchemaFingerprint {
		return nil, fmt.Errorf("schema fingerprint mismatch: expected %s, current %s", rec.SchemaFingerprint, fp)
	}

	// Hard-fail if the replication slot has disappeared (broken CDC history).
	if err := checkSlotContinuity(ctx, rec.Config.SourceDSN, rec.Config.SourceSlot); err != nil {
		return nil, err
	}

	// Recover durable chunk state. Expired leases are released so another
	// worker can pick the chunk up safely.
	if _, err := cpStore.ReleaseExpiredLeases(ctx, jobID, time.Now()); err != nil {
		return nil, fmt.Errorf("release expired leases: %w", err)
	}

	incomplete, err := cpStore.LoadIncompleteChunks(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("load incomplete chunks: %w", err)
	}

	// If a chunk was unfinished, start a fresh attempt with a new marker window.
	if cp.CompletedThrough < cp.ScanUpperBound || len(incomplete) > 0 {
		newAttempt, err := BumpAttempt(cp.Attempt)
		if err != nil {
			return nil, err
		}
		cp.Attempt = newAttempt
		logPrintf("recovery: unfinished chunk detected, new attempt=%s", cp.Attempt)

		// Re-schedule any incomplete chunk under the new attempt. Completed
		// chunks are left alone; workers will not re-scan them.
		for _, ch := range incomplete {
			newChunk := ch
			newChunk.Attempt = newAttempt
			newChunk.Status = model.ChunkPending
			newChunk.WorkerID = ""
			newChunk.LeaseStart = nil
			newChunk.LeaseExpiry = nil
			newChunk.HeartbeatAt = nil
			newChunk.ErrorMessage = ""
			if err := cpStore.CreateChunk(ctx, &newChunk); err != nil {
				return nil, fmt.Errorf("reschedule chunk %s: %w", ch.Range(), err)
			}
		}
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

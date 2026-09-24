package main

import (
	"context"
	"fmt"
	"time"

	"example.com/seam/internal/capture"
	"example.com/seam/internal/checkpoint"
	"example.com/seam/internal/kafka"
	"example.com/seam/internal/marker"
	"example.com/seam/internal/model"
	"example.com/seam/internal/schema"
	"example.com/seam/internal/transport"
)

// verifyFenced is an intentionally intrusive correctness oracle. It blocks
// writes to the job's source table while a source barrier crosses Kafka and
// the destination, then compares the two fixed states with O(1) application
// memory. The source table layout comes from the job's pinned schema
// descriptor, never from a hard-coded column set, and the descriptor must
// still match the source catalog or the check fails closed.
func verifyFenced(ctx context.Context, sourceDSN, destDSN string, brokers []string, topic, jobID string) error {
	store := checkpoint.NewStore(destDSN)
	record, err := store.LoadJobRecord(ctx, jobID)
	if err != nil {
		return err
	}
	if record.SourceSchema == nil {
		return fmt.Errorf("job %q has no pinned source schema; cannot verify without a schema epoch", jobID)
	}
	if err := schema.ValidateIdentifier(record.Config.DestTable); err != nil {
		return fmt.Errorf("job %q has unsafe destination table %q: %w", jobID, record.Config.DestTable, err)
	}
	if record.SourceDSNHash == "" || record.SourceDSNHash != checkpoint.DSNFingerprint(sourceDSN) {
		return fmt.Errorf("job %q has a different source connection identity", jobID)
	}
	if record.Config.KafkaTopic != topic || record.Config.KafkaTopicID == "" {
		return fmt.Errorf("job %q has no matching pinned Kafka topic", jobID)
	}
	actualTopicID, err := kafka.TopicIdentity(ctx, brokers, topic)
	if err != nil {
		return err
	}
	if actualTopicID != record.Config.KafkaTopicID {
		return fmt.Errorf("Kafka topic identity changed; verification is invalid")
	}
	systemID, err := capture.SourceSystemID(ctx, sourceDSN)
	if err != nil {
		return err
	}
	if record.Config.SourceSystemID != systemID {
		return fmt.Errorf("source system identity changed; verification is invalid")
	}

	// Load the current source descriptor and require it to still match the
	// pinned schema epoch. Any drift invalidates the comparison.
	current, err := schema.Load(ctx, sourceDSN, "public", record.SourceSchema.Table)
	if err != nil {
		return fmt.Errorf("load current source schema: %w", err)
	}
	if current.FingerprintFor() != record.SourceSchema.Fingerprint {
		return fmt.Errorf("source schema drifted since job creation: job pins %q, source now %q",
			record.SourceSchema.Fingerprint, current.FingerprintFor())
	}

	source, err := transport.ConnectPostgres(ctx, sourceDSN)
	if err != nil {
		return err
	}
	defer source.Close(context.Background())
	if _, err := source.Exec(ctx, `SET TIME ZONE 'UTC'`); err != nil {
		return fmt.Errorf("set source UTC session: %w", err)
	}
	sourceTx, err := source.Begin(ctx)
	if err != nil {
		return err
	}
	defer sourceTx.Rollback(context.Background())
	if _, err := sourceTx.Exec(ctx, `SET LOCAL lock_timeout = '5s'`); err != nil {
		return err
	}
	if _, err := sourceTx.Exec(ctx, fmt.Sprintf("LOCK TABLE %s IN SHARE MODE", current.SQLTable())); err != nil {
		return fmt.Errorf("acquire source verification fence: %w", err)
	}
	markers := marker.NewStore(sourceDSN)
	if err := markers.EnsureTable(ctx); err != nil {
		return err
	}
	end, err := kafka.EndOffset(ctx, brokers, topic)
	if err != nil {
		return err
	}
	barrierID, err := markers.WriteBarrier(ctx, jobID, fmt.Sprintf("verification:%d", time.Now().UnixNano()))
	if err != nil {
		return err
	}
	barrierOffset, err := kafka.WaitForBarrier(ctx, brokers, topic, barrierID, end, decodeLabChange)
	if err != nil {
		return err
	}
	for {
		cp, err := store.LoadCheckpoint(ctx, jobID)
		if err != nil {
			return err
		}
		if cp == nil {
			return fmt.Errorf("job %q has no checkpoint", jobID)
		}
		if !cp.Active {
			return fmt.Errorf("job %q is inactive; its checkpoint cannot verify the table", jobID)
		}
		if cp.CompletedThrough >= cp.ScanUpperBound && cp.NextKafkaOffset >= barrierOffset {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for destination at source barrier: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	dest, err := transport.ConnectPostgres(ctx, destDSN)
	if err != nil {
		return err
	}
	defer dest.Close(context.Background())
	if _, err := dest.Exec(ctx, `SET TIME ZONE 'UTC'`); err != nil {
		return fmt.Errorf("set dest UTC session: %w", err)
	}
	count, err := compareRows(ctx, sourceTx, dest, current, record.Config.DestTable)
	if err != nil {
		return err
	}
	fmt.Printf("verify-fenced: equal at Kafka cursor %d (%d rows)\n", barrierOffset, count)
	return nil
}

func decodeLabChange(data []byte) (model.Change, error) {
	var codec capture.JSONCodec
	return codec.Decode(data)
}

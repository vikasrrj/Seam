package reconcile

import (
	"context"
	"fmt"

	"example.com/seam/internal/failpoint"
	"example.com/seam/internal/kafka"
	"example.com/seam/internal/model"
	"example.com/seam/internal/telemetry"
)

// transactionConsumer is implemented by the production Kafka consumer. Test
// consumers may keep the older Poll contract; production never materializes a
// fragmented source transaction into one []Record.
type transactionConsumer interface {
	PollTransactions(context.Context) ([]*kafka.Transaction, error)
}

func (r *Reconciler) validateStreamTransaction(transaction *kafka.Transaction) error {
	if transaction == nil || transaction.TotalCount <= 0 || transaction.FinalOffset < transaction.FirstOffset {
		return fmt.Errorf("invalid streamed source transaction")
	}
	source := transaction.Source
	if source.Generation != r.cp.Generation || source.LSN == "" {
		return fmt.Errorf("source transaction %s does not match job generation %s", source, r.cp.Generation)
	}
	if r.cfg.SourceSystemID != "" && source.SystemID != r.cfg.SourceSystemID {
		return fmt.Errorf("source system identifier %q does not match pinned %q", source.SystemID, r.cfg.SourceSystemID)
	}
	if r.schemaID == "" {
		return nil
	}
	// Reject the whole transaction before any destination write if a row
	// change carries a schema epoch different from the job's pinned one.
	err := transaction.Walk(func(records []kafka.Record) error {
		for i := range records {
			change := &records[i].Change
			if change.Row != nil && change.SchemaID != r.schemaID {
				return fmt.Errorf("row change at offset %d carries schema epoch %q; job pins %q", records[i].Offset, change.SchemaID, r.schemaID)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// evaluateStreamMarkers makes a bounded first pass over a replayable
// transaction. Callers use the resulting window state to choose normal apply
// or atomic chunk completion, then make a second bounded pass inside the
// destination transaction.
func (r *Reconciler) evaluateStreamMarkers(transaction *kafka.Transaction, w *window) (WindowState, error) {
	state := w.state
	err := transaction.Walk(func(records []kafka.Record) error {
		var err error
		state, err = r.evaluateMarkers(records, w)
		return err
	})
	return state, err
}

func (r *Reconciler) applyStreamTransaction(ctx context.Context, transaction *kafka.Transaction, windows []*window) error {
	if err := r.validateStreamTransaction(transaction); err != nil {
		return err
	}
	if err := r.awaitCutoverPermission(ctx, transaction.FinalOffset+1); err != nil {
		return err
	}
	if transaction.FinalOffset < r.cp.NextKafkaOffset {
		return nil
	}
	tx, err := r.beginOwned(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	source := transaction.Source
	alreadyApplied, err := r.cpStore.IsApplied(ctx, tx, r.cfg.JobID, source.Generation, source.LSN)
	if err != nil {
		return err
	}
	cdcCount := 0
	if !alreadyApplied {
		err = transaction.Walk(func(records []kafka.Record) error {
			changes := make([]model.Change, 0, len(records))
			keys := make([]int64, 0, len(records))
			for i := range records {
				record := &records[i]
				if record.Change.Source != source {
					return fmt.Errorf("streamed transaction %s contains source %s", source, record.Change.Source)
				}
				if record.Change.Row != nil {
					cdcCount++
					key, err := r.changeKey(&record.Change)
					if err != nil {
						return fmt.Errorf("streamed transaction %s change at offset %d: %w", source, record.Offset, err)
					}
					keys = append(keys, key)
					changes = append(changes, record.Change)
				} else if record.Change.Marker == nil {
					return fmt.Errorf("change has no payload")
				}
			}
			if !r.disableEviction {
				if err := r.evictWindows(ctx, tx, windows, keys); err != nil {
					return err
				}
			}
			return r.applyRowChanges(ctx, tx, changes)
		})
		if err != nil {
			return err
		}
		if err := r.cpStore.MarkApplied(ctx, tx, r.cfg.JobID, source.Generation, source.LSN, source.XID); err != nil {
			return err
		}
	}
	nextCP := *r.cp
	newLSN := source.LSN
	if alreadyApplied {
		newLSN = ""
	}
	if err := r.updateCheckpoint(ctx, tx, &nextCP, transaction.FinalOffset, newLSN); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit streamed source transaction: %w", err)
	}
	r.publishCheckpoint(nextCP)
	if cdcCount > 0 {
		r.metrics.RecordCDC(cdcCount)
	}
	return nil
}

func (r *Reconciler) completeChunkStreamAt(ctx context.Context, transaction *kafka.Transaction, chunk *model.Chunk, w *window, completedThrough int64, evictionWindows ...*window) error {
	if err := r.validateStreamTransaction(transaction); err != nil {
		return err
	}
	if err := r.awaitCutoverPermission(ctx, transaction.FinalOffset+1); err != nil {
		return err
	}
	if err := r.maybeWait(ctx, failpoint.AfterHighMarkerObserved); err != nil {
		return err
	}
	tx, err := r.beginOwned(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if r.chunkStore != nil {
		chunk.Status = model.ChunkCommitting
		lsn := transaction.Source.LSN
		chunk.HighLSN = &lsn
		offset := transaction.FinalOffset
		chunk.HighOffset = &offset
		if err := r.chunkStore.UpdateChunk(ctx, tx, chunk); err != nil {
			return fmt.Errorf("update chunk committing: %w", err)
		}
	}
	if len(evictionWindows) == 0 {
		evictionWindows = []*window{w}
	}
	cdcCount := 0
	err = transaction.Walk(func(records []kafka.Record) error {
		changes := make([]model.Change, 0, len(records))
		keys := make([]int64, 0, len(records))
		for i := range records {
			record := &records[i]
			if record.Change.Source != transaction.Source {
				return fmt.Errorf("streamed transaction %s contains source %s", transaction.Source, record.Change.Source)
			}
			if record.Change.Row != nil {
				cdcCount++
				key, err := r.changeKey(&record.Change)
				if err != nil {
					return fmt.Errorf("streamed transaction %s change at offset %d: %w", transaction.Source, record.Offset, err)
				}
				keys = append(keys, key)
				changes = append(changes, record.Change)
			} else if record.Change.Marker == nil {
				return fmt.Errorf("change has no payload")
			}
		}
		if !r.disableEviction {
			if err := r.evictWindows(ctx, tx, evictionWindows, keys); err != nil {
				return err
			}
		}
		return r.applyRowChanges(ctx, tx, changes)
	})
	if err != nil {
		return err
	}
	if err := r.cpStore.MarkApplied(ctx, tx, r.cfg.JobID, transaction.Source.Generation, transaction.Source.LSN, transaction.Source.XID); err != nil {
		return err
	}
	if err := r.maybeWait(ctx, failpoint.BeforeChunkCommit); err != nil {
		return err
	}

	candidateCount := w.candidateCount
	if !w.staged && candidateCount == 0 {
		candidateCount = len(w.candidates)
	}
	var survivorCount int
	if w.staged {
		written, err := r.sink.(stagedCandidateSink).WriteStagedCandidates(ctx, tx, chunk)
		if err != nil {
			return err
		}
		survivorCount = int(written)
		if err := r.chunkStore.(durableCandidateStore).ClearCandidates(ctx, tx, chunk); err != nil {
			return err
		}
	} else {
		w.mu.Lock()
		survivors := make([]model.Row, 0, len(w.candidates))
		for _, row := range w.candidates {
			survivors = append(survivors, row)
		}
		w.candidates = nil
		w.mu.Unlock()
		survivorCount = len(survivors)
		if err := r.sink.WriteCandidates(ctx, tx, survivors); err != nil {
			return fmt.Errorf("write survivors: %w", err)
		}
	}
	if r.chunkStore != nil {
		chunk.RowsApplied = int64(cdcCount)
	}
	nextCP := *r.cp
	nextCP.CompletedThrough = completedThrough
	if err := r.updateCheckpoint(ctx, tx, &nextCP, transaction.FinalOffset, transaction.Source.LSN); err != nil {
		return err
	}
	if r.chunkStore != nil {
		chunk.Status = model.ChunkCompleted
		if err := r.chunkStore.UpdateChunk(ctx, tx, chunk); err != nil {
			return fmt.Errorf("update chunk completed: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit streamed chunk completion: %w", err)
	}
	r.publishCheckpoint(nextCP)
	if cdcCount > 0 {
		r.metrics.RecordCDC(cdcCount)
	}
	if err := r.maybeWait(ctx, failpoint.AfterDestinationCommit); err != nil {
		return err
	}
	r.metrics.RecordChunk(telemetry.ChunkStats{Candidates: candidateCount, Survivors: survivorCount})
	return nil
}

func closeTransactions(transactions []*kafka.Transaction) {
	for _, transaction := range transactions {
		transaction.Close()
	}
}

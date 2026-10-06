package kafka

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"example.com/seam/internal/model"
)

// Each worker owns its assembly, like an independent ordered stream. Increasing
// workers does not claim that a single ordered Seam stream can apply in parallel.
func BenchmarkReassembleWalk(b *testing.B) {
	source := model.SourceTx{SystemID: "benchmark", Generation: "gen:0", LSN: "0/123", XID: 1}
	changes := make([]model.Change, 1000)
	for i := range changes {
		changes[i] = rowChange(model.OpInsert, int64(i), "benchmark-owner", source)
	}
	payload, err := json.Marshal(model.TransactionEnvelope{Version: 2, Source: source, SchemaID: "schema-epoch-1", Final: true, Count: len(changes), TotalCount: len(changes), Changes: changes})
	if err != nil {
		b.Fatal(err)
	}
	for _, workers := range []int{1, 4} {
		b.Run(fmt.Sprintf("workers_%d", workers), func(b *testing.B) {
			jobs := make(chan struct{})
			errs := make(chan error, workers)
			var wg sync.WaitGroup
			for range workers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					var firstErr error
					for range jobs {
						if firstErr != nil {
							continue
						}
						var assembly fragmentAssembly
						tx, err := assembly.pushTransaction(0, payload, decodeChange)
						if err != nil {
							assembly.reset()
							firstErr = err
							continue
						}
						if tx == nil {
							firstErr = fmt.Errorf("complete envelope not assembled")
							continue
						}
						// Snowflake validates/fingerprints, then walks again to stage.
						for range 2 {
							seen := 0
							err = tx.Walk(func(records []Record) error {
								for _, r := range records {
									if r.Change.Row.Values[0].Int != int64(seen) {
										return fmt.Errorf("row ordering mismatch")
									}
									seen++
								}
								return nil
							})
							if err != nil {
								firstErr = err
								break
							}
							if seen != len(changes) {
								firstErr = fmt.Errorf("row count mismatch")
								break
							}
						}
						tx.Close()
					}
					errs <- firstErr
				}()
			}
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				jobs <- struct{}{}
			}
			close(jobs)
			wg.Wait()
			b.StopTimer()
			for range workers {
				if err := <-errs; err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

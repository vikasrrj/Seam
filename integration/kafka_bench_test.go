//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"example.com/seam/integration/itest"
	"example.com/seam/internal/capture"
	"example.com/seam/internal/kafka"
	"example.com/seam/internal/model"
	"example.com/seam/internal/transport"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Uses a unique topic: no source/destination DBs, and no reset of other tests.
// Production capture has one ordered producer. Pre-encoding deliberately
// isolates broker acknowledgement and consumer reassembly from capture CPU.
func BenchmarkKafkaTransport(b *testing.B) {
	for _, rows := range []int{1, 1000} {
		b.Run(fmt.Sprintf("rows_%d", rows), func(b *testing.B) {
			b.StopTimer()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			topic := fmt.Sprintf("seam.benchmark.%d", time.Now().UnixNano())
			opts, err := transport.KafkaOptions()
			if err != nil {
				b.Fatal(err)
			}
			client, err := kgo.NewClient(append([]kgo.Opt{
				kgo.SeedBrokers(itest.KafkaBrokers()...), kgo.DefaultProduceTopic(topic), kgo.RequiredAcks(kgo.AllISRAcks()),
			}, opts...)...)
			if err != nil {
				b.Fatal(err)
			}
			defer client.Close()
			admin := kadm.NewClient(client)
			created, err := admin.CreateTopics(ctx, 1, 1, nil, topic)
			if err != nil {
				b.Fatal(err)
			}
			if err := created[topic].Err; err != nil {
				b.Fatal(err)
			}
			defer func() {
				cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
				defer stop()
				if _, err := admin.DeleteTopics(cleanup, topic); err != nil {
					b.Error(err)
				}
			}()
			consumer, err := kafka.NewConsumer(itest.KafkaBrokers(), topic, 0, benchDecode)
			if err != nil {
				b.Fatal(err)
			}
			defer consumer.Close()
			source := model.SourceTx{SystemID: "benchmark", Generation: "gen:0", LSN: "0/123", XID: 1}
			changes := make([]model.Change, rows)
			for i := range changes {
				changes[i] = model.Change{Source: source, SchemaID: "benchmark", Op: model.OpInsert,
					Row: &model.Row{Values: []model.Value{model.Int64Value(int64(i)), model.TextValue("benchmark-owner")}}}
			}
			payload, err := (capture.JSONCodec{}).EncodeTransaction(model.TransactionEnvelope{
				Version: 2, Source: source, SchemaID: "benchmark", Final: true, Count: rows, TotalCount: rows, Changes: changes,
			})
			if err != nil {
				b.Fatal(err)
			}
			// Warm metadata/connection setup using one verified production decode.
			one := func() (time.Duration, time.Duration, time.Duration, error) {
				start := time.Now()
				record, err := client.ProduceSync(ctx, &kgo.Record{Value: payload}).First()
				ack := time.Since(start)
				if err != nil {
					return 0, 0, 0, err
				}
				start = time.Now()
				var txs []*kafka.Transaction
				for len(txs) == 0 {
					txs, err = consumer.PollTransactions(ctx)
					if err != nil {
						return 0, 0, 0, err
					}
				}
				fetch := time.Since(start)
				defer func() {
					for _, tx := range txs {
						tx.Close()
					}
				}()
				if len(txs) != 1 || txs[0].FinalOffset != record.Offset {
					return 0, 0, 0, fmt.Errorf("transaction/offset mismatch")
				}
				start = time.Now()
				seen := 0
				err = txs[0].Walk(func(records []kafka.Record) error {
					for _, r := range records {
						if r.Change.Row.Values[0].Int != int64(seen) || r.Change.Row.Values[1].Text != "benchmark-owner" {
							return fmt.Errorf("row mismatch at %d", seen)
						}
						seen++
					}
					return nil
				})
				walk := time.Since(start)
				if err == nil && seen != rows {
					err = fmt.Errorf("rows=%d want=%d", seen, rows)
				}
				return ack, fetch, walk, err
			}
			if _, _, _, err := one(); err != nil {
				b.Fatal(err)
			}
			var ack, fetch, walk time.Duration
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for range b.N {
				a, f, w, err := one()
				if err != nil {
					b.Fatal(err)
				}
				ack += a
				fetch += f
				walk += w
			}
			b.StopTimer()
			b.ReportMetric(float64(ack)/float64(time.Millisecond)/float64(b.N), "produce-ack-ms/op")
			b.ReportMetric(float64(fetch)/float64(time.Millisecond)/float64(b.N), "fetch-reassemble-ms/op")
			b.ReportMetric(float64(walk)/float64(time.Millisecond)/float64(b.N), "walk-decode-ms/op")
			b.ReportMetric(float64(len(payload)), "record-bytes")
		})
	}
}

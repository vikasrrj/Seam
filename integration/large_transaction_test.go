//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"example.com/seam/integration/itest"
	"example.com/seam/internal/capture"
	"example.com/seam/internal/kafka"
	"example.com/seam/internal/model"
)

func TestLargeTransactionFragmentsRemainAtomicAcrossPolls(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := itest.ResetTables(ctx); err != nil {
		t.Fatal(err)
	}
	reader, err := capture.StartReader(ctx, capture.ReaderConfig{
		SQLDSN: itest.SourceDSN(), ReplicationDSN: itest.SourceReplDSN(),
		Slot: "seam_itest_slot", Publication: "seam_pub", KafkaBrokers: itest.KafkaBrokers(),
		KafkaTopic: itest.KafkaTopic(), Generation: "gen:0", MaxTransactionEvents: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	readerCtx, stopReader := context.WithCancel(ctx)
	readerDone := make(chan error, 1)
	go func() { readerDone <- reader.Run(readerCtx) }()
	defer func() {
		stopReader()
		reader.Close()
		if err := <-readerDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("capture: %v", err)
		}
	}()

	source, err := itest.SourceConn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	itest.CloseOnCleanup(t, "source connection", source)
	const changeCount = 40
	if _, err := source.Exec(ctx, `INSERT INTO accounts(id, owner, balance_cents)
		SELECT id, repeat('wide-row-', 4096), id FROM generate_series(1, $1::bigint) AS g(id)`, changeCount); err != nil {
		t.Fatal(err)
	}

	var end int64
	for end < 2 {
		end, err = kafka.EndOffset(ctx, itest.KafkaBrokers(), itest.KafkaTopic())
		if err != nil {
			t.Fatal(err)
		}
		if end < 2 {
			time.Sleep(25 * time.Millisecond)
		}
	}
	topicID, err := kafka.TopicIdentity(ctx, itest.KafkaBrokers(), itest.KafkaTopic())
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := kafka.NewConsumer(itest.KafkaBrokers(), itest.KafkaTopic(), 0, func(data []byte) (model.Change, error) {
		return (capture.JSONCodec{}).Decode(data)
	}, kafka.WithMaxPollRecords(1), kafka.WithExpectedTopicID(topicID))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	first, err := consumer.Poll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 0 {
		t.Fatalf("consumer exposed %d changes before final fragment", len(first))
	}
	var assembled []kafka.Record
	for len(assembled) == 0 {
		assembled, err = consumer.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(assembled) != changeCount {
		t.Fatalf("assembled %d changes, want %d", len(assembled), changeCount)
	}
	finalOffset := assembled[0].Offset
	for i, record := range assembled {
		if record.Offset != finalOffset || record.Change.Row == nil || len(record.Change.Row.Values) < 1 || record.Change.Row.Values[0].Int != int64(i+1) {
			t.Fatalf("assembled record %d is not one atomic ordered transaction: %+v", i, record)
		}
	}
}

// Package capture provides the minimal CDC transport Seam needs: one source
// PostgreSQL table and one marker table, published to one Kafka topic with one
// partition. It is intentionally not a generic CDC product.
package capture

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"example.com/seam/internal/model"
	"github.com/jackc/pglogrepl"
)

// defaultMaxTransactionEvents is the default cap on how many row changes a
// single source transaction may buffer in memory before Seam refuses to
// continue. It bounds peak memory regardless of how large a transaction the
// source commits.
const defaultMaxTransactionEvents = 1_000_000

// ErrTransactionTooLarge is returned when a single source transaction exceeds
// the decoder's in-memory event cap. Seam fails deliberately instead of
// buffering an unbounded transaction; the operator must shrink the write or
// raise the cap via configuration.
var ErrTransactionTooLarge = errors.New("transaction exceeds in-memory event cap")

// CommittedTransaction describes a committed source transaction whose events
// are ready to be pulled one at a time.
type CommittedTransaction struct {
	TransactionID uint32
	CommitLSN     pglogrepl.LSN
	EndLSN        pglogrepl.LSN
	CommitTime    time.Time
	Count         int
}

// Decoder converts pgoutput messages into a stream of model.Change records.
// It is a minimal, single-table decoder: accounts and seam_marker only.
type Decoder struct {
	generation  string
	relations   map[uint32]*pglogrepl.RelationMessage
	transaction *pendingTransaction
	pull        *pullState
	// MaxTransactionEvents bounds the in-memory buffer of one open source
	// transaction. A transaction exceeding the cap fails the pipeline rather
	// than growing memory without bound (see ErrTransactionTooLarge).
	MaxTransactionEvents int
}

type pendingTransaction struct {
	id   uint32
	mem  []model.Change
	lsn  pglogrepl.LSN
	end  pglogrepl.LSN
	time time.Time
}

type pullState struct {
	commit  CommittedTransaction
	changes []model.Change
	seq     int
}

func NewDecoder(generation string) *Decoder {
	return &Decoder{
		generation:           generation,
		relations:            make(map[uint32]*pglogrepl.RelationMessage),
		MaxTransactionEvents: defaultMaxTransactionEvents,
	}
}

func (d *Decoder) Handle(message pglogrepl.Message) (*CommittedTransaction, error) {
	switch message := message.(type) {
	case *pglogrepl.RelationMessage:
		d.relations[message.RelationID] = message
		return nil, nil

	case *pglogrepl.BeginMessage:
		if d.transaction != nil {
			return nil, fmt.Errorf("begin tx %d while tx %d open", message.Xid, d.transaction.id)
		}
		d.transaction = &pendingTransaction{id: message.Xid}
		return nil, nil

	case *pglogrepl.CommitMessage:
		if d.transaction == nil {
			return nil, fmt.Errorf("commit with no open transaction")
		}
		tx := d.transaction
		d.transaction = nil
		d.pull = &pullState{
			commit: CommittedTransaction{
				TransactionID: tx.id,
				CommitLSN:     message.CommitLSN,
				EndLSN:        message.TransactionEndLSN,
				CommitTime:    message.CommitTime,
				Count:         len(tx.mem),
			},
			changes: tx.mem,
		}
		return &d.pull.commit, nil

	case *pglogrepl.InsertMessage:
		change, err := d.decodeInsert(message)
		if err != nil {
			return nil, err
		}
		return nil, d.append(change)

	case *pglogrepl.UpdateMessage:
		change, err := d.decodeUpdate(message)
		if err != nil {
			return nil, err
		}
		return nil, d.append(change)

	case *pglogrepl.DeleteMessage:
		change, err := d.decodeDelete(message)
		if err != nil {
			return nil, err
		}
		return nil, d.append(change)

	default:
		return nil, nil
	}
}

func (d *Decoder) NextEvent() (model.Change, bool, error) {
	if d.pull == nil {
		return model.Change{}, false, fmt.Errorf("no open transaction to pull")
	}
	if d.pull.seq >= d.pull.commit.Count {
		d.pull = nil
		return model.Change{}, false, nil
	}
	change := d.pull.changes[d.pull.seq]
	change.Source = model.SourceTx{
		Generation: d.generation,
		XID:        d.pull.commit.TransactionID,
		LSN:        d.pull.commit.CommitLSN.String(),
	}
	d.pull.seq++
	return change, true, nil
}

func (d *Decoder) Reset() {
	d.pull = nil
	d.transaction = nil
	d.relations = make(map[uint32]*pglogrepl.RelationMessage)
}

func (d *Decoder) relation(id uint32) (*pglogrepl.RelationMessage, error) {
	r, ok := d.relations[id]
	if !ok {
		return nil, fmt.Errorf("relation %d not described", id)
	}
	return r, nil
}

func (d *Decoder) append(change model.Change) error {
	if d.transaction == nil {
		return fmt.Errorf("row change outside transaction")
	}
	if d.MaxTransactionEvents > 0 && len(d.transaction.mem) >= d.MaxTransactionEvents {
		return fmt.Errorf("%w: limit %d", ErrTransactionTooLarge, d.MaxTransactionEvents)
	}
	d.transaction.mem = append(d.transaction.mem, change)
	return nil
}

func (d *Decoder) decodeInsert(message *pglogrepl.InsertMessage) (model.Change, error) {
	relation, err := d.relation(message.RelationID)
	if err != nil {
		return model.Change{}, err
	}
	row, err := decodeTuple(relation, message.Tuple, false)
	if err != nil {
		return model.Change{}, fmt.Errorf("decode insert %s.%s: %w", relation.Namespace, relation.RelationName, err)
	}
	change := model.Change{Op: model.OpInsert}
	if err := fillChange(&change, relation.RelationName, row); err != nil {
		return model.Change{}, err
	}
	return change, nil
}

func (d *Decoder) decodeUpdate(message *pglogrepl.UpdateMessage) (model.Change, error) {
	relation, err := d.relation(message.RelationID)
	if err != nil {
		return model.Change{}, err
	}
	newRow, err := decodeTuple(relation, message.NewTuple, false)
	if err != nil {
		return model.Change{}, fmt.Errorf("decode update %s.%s: %w", relation.Namespace, relation.RelationName, err)
	}
	// pgoutput attaches an old tuple to an update whenever the replica identity
	// key changed ('K' key-only form) or the table uses REPLICA IDENTITY FULL.
	// if the old key differs from the new one, the row was re-keyed, which Seam
	// cannot represent.
	if message.OldTuple != nil {
		oldRow, err := decodeTuple(relation, message.OldTuple, message.OldTupleType == 'K')
		if err != nil {
			return model.Change{}, fmt.Errorf("decode update old tuple %s.%s: %w", relation.Namespace, relation.RelationName, err)
		}
		if id, err := parseInt64(oldRow, "id"); err != nil {
			return model.Change{}, fmt.Errorf("decode update old key: %w", err)
		} else if newID, err := parseInt64(newRow, "id"); err != nil {
			return model.Change{}, err
		} else if id != newID {
			return model.Change{}, fmt.Errorf("primary key change id %d -> %d on %q not supported; reset pipeline", id, newID, relation.RelationName)
		}
	}
	change := model.Change{Op: model.OpUpdate}
	if err := fillChange(&change, relation.RelationName, newRow); err != nil {
		return model.Change{}, err
	}
	return change, nil
}

func (d *Decoder) decodeDelete(message *pglogrepl.DeleteMessage) (model.Change, error) {
	relation, err := d.relation(message.RelationID)
	if err != nil {
		return model.Change{}, err
	}
	row, err := decodeTuple(relation, message.OldTuple, message.OldTupleType == 'K')
	if err != nil {
		return model.Change{}, fmt.Errorf("decode delete %s.%s: %w", relation.Namespace, relation.RelationName, err)
	}
	change := model.Change{Op: model.OpDelete}
	switch relation.RelationName {
	case "accounts":
		id, err := parseInt64(row, "id")
		if err != nil {
			return model.Change{}, err
		}
		change.Account = &model.Account{ID: id}
	case "seam_marker":
		// Markers are never deleted in normal operation; tolerate a key-only
		// delete by extracting the id only.
		id, err := requireString(row, "id")
		if err != nil {
			return model.Change{}, err
		}
		change.Marker = &model.Marker{ID: id}
	default:
		return model.Change{}, fmt.Errorf("unexpected table %q", relation.RelationName)
	}
	return change, nil
}

func fillChange(change *model.Change, table string, row map[string]*string) error {
	switch table {
	case "accounts":
		id, err := parseInt64(row, "id")
		if err != nil {
			return err
		}
		owner, err := requireString(row, "owner")
		if err != nil {
			return err
		}
		balance, err := parseInt64(row, "balance_cents")
		if err != nil {
			return err
		}
		change.Account = &model.Account{ID: id, Owner: owner, BalanceCents: balance}
	case "seam_marker":
		id, err := requireString(row, "id")
		if err != nil {
			return err
		}
		kind, err := requireString(row, "kind")
		if err != nil {
			return err
		}
		jobID, err := requireString(row, "job_id")
		if err != nil {
			return err
		}
		attempt, err := requireString(row, "attempt")
		if err != nil {
			return err
		}
		chunkMin, err := parseInt64(row, "chunk_min_id")
		if err != nil {
			return err
		}
		chunkMax, err := parseInt64(row, "chunk_max_id")
		if err != nil {
			return err
		}
		change.Marker = &model.Marker{
			ID:       id,
			Kind:     model.MarkerKind(kind),
			JobID:    jobID,
			Attempt:  attempt,
			ChunkMin: chunkMin,
			ChunkMax: chunkMax,
		}
	default:
		return fmt.Errorf("unexpected table %q", table)
	}
	return nil
}

func decodeTuple(relation *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData, keyOnly bool) (map[string]*string, error) {
	if tuple == nil {
		return nil, fmt.Errorf("tuple is missing")
	}
	// Full-width tuples pair positionally with the relation columns. PostgreSQL
	// also sends key-only ('K') old tuples: usually exactly the replica identity
	// columns, but some settings emit the full width, so support both.
	var columns []*pglogrepl.RelationMessageColumn
	if len(tuple.Columns) == len(relation.Columns) {
		columns = relation.Columns
	} else if keyOnly {
		for _, column := range relation.Columns {
			if column.Flags&1 != 0 {
				columns = append(columns, column)
			}
		}
		if len(tuple.Columns) != len(columns) {
			return nil, fmt.Errorf("tuple columns %d != %s key columns %d", len(tuple.Columns), relation.RelationName, len(columns))
		}
	} else {
		return nil, fmt.Errorf("tuple columns %d != %s columns %d", len(tuple.Columns), relation.RelationName, len(relation.Columns))
	}
	row := make(map[string]*string, len(columns))
	for i, column := range columns {
		value := tuple.Columns[i]
		switch value.DataType {
		case 'n':
			row[column.Name] = nil
		case 't':
			s := string(value.Data)
			row[column.Name] = &s
		case 'u':
			return nil, fmt.Errorf("TOAST column %q not supported", column.Name)
		case 'b':
			return nil, fmt.Errorf("binary column %q not supported", column.Name)
		default:
			return nil, fmt.Errorf("unknown tuple type %q for column %q", value.DataType, column.Name)
		}
	}
	return row, nil
}

func parseInt64(row map[string]*string, column string) (int64, error) {
	v, ok := row[column]
	if !ok || v == nil {
		return 0, fmt.Errorf("column %q missing or null", column)
	}
	n, err := strconv.ParseInt(*v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %q: %w", column, err)
	}
	return n, nil
}

func requireString(row map[string]*string, column string) (string, error) {
	v, ok := row[column]
	if !ok || v == nil {
		return "", fmt.Errorf("column %q missing or null", column)
	}
	return *v, nil
}

// JSONCodec converts model.Change values to and from Kafka record bytes.
type JSONCodec struct{}

func (JSONCodec) Encode(change model.Change) ([]byte, error) {
	return json.Marshal(change)
}

func (JSONCodec) Decode(data []byte) (model.Change, error) {
	var change model.Change
	if err := json.Unmarshal(data, &change); err != nil {
		return model.Change{}, err
	}
	return change, nil
}

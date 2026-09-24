// Package capture provides the minimal CDC transport Seam needs: one source
// PostgreSQL table and one marker table, published to one Kafka topic with one
// partition. It is intentionally not a generic CDC product.
package capture

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"example.com/seam/internal/model"
	"example.com/seam/internal/schema"
	"github.com/jackc/pglogrepl"
)

// markerRelation is the fixed control relation published alongside every job's
// configured source table. Its schema is pinned by the codec below.
const markerRelation = "seam_marker"

// markerRelationIDColumn is the marker id column used for deletes.
const markerRelationIDColumn = "id"

// defaultMaxTransactionEvents is a hard corruption/operator-safety cap. The
// byte threshold below is a memory threshold: larger open transactions spill
// to a private temp file and remain replayable from WAL until Kafka confirms
// every fragment.
const defaultMaxTransactionEvents = 1_000_000
const defaultMaxTransactionBytes = 800 * 1024

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
// It is a minimal, single-table decoder: the job's configured source table
// plus seam_marker only. Row changes are decoded against the durable schema
// descriptor so no fixed column layout is assumed.
type Decoder struct {
	generation  string
	systemID    string
	schema      *schema.Schema
	schemaID    string
	relations   map[uint32]*pglogrepl.RelationMessage
	transaction *pendingTransaction
	pull        *pullState
	// MaxTransactionEvents bounds the in-memory buffer of one open source
	// transaction. A transaction exceeding the cap fails the pipeline rather
	// than growing memory without bound (see ErrTransactionTooLarge).
	MaxTransactionEvents int
	MaxTransactionBytes  int
}

func (d *Decoder) SetSystemID(systemID string) {
	d.systemID = systemID
}

type pendingTransaction struct {
	id    uint32
	mem   []model.Change
	count int
	lsn   pglogrepl.LSN
	end   pglogrepl.LSN
	time  time.Time
	bytes int
	spill *os.File
}

type pullState struct {
	commit  CommittedTransaction
	changes []model.Change
	spill   *os.File
	reader  *bufio.Reader
	seq     int
}

// NewDecoder builds a decoder for the job's source tables. dataSchema is the
// validated durable descriptor of the replicated data table; seam_marker
// keeps its fixed codec.
func NewDecoder(generation string, dataSchema *schema.Schema) *Decoder {
	return &Decoder{
		generation:           generation,
		schema:               dataSchema,
		schemaID:             dataSchema.Fingerprint,
		relations:            make(map[uint32]*pglogrepl.RelationMessage),
		MaxTransactionEvents: defaultMaxTransactionEvents,
		MaxTransactionBytes:  defaultMaxTransactionBytes,
	}
}

func (d *Decoder) Handle(message pglogrepl.Message) (*CommittedTransaction, error) {
	switch message := message.(type) {
	case *pglogrepl.RelationMessage:
		if err := d.validateRelation(message); err != nil {
			return nil, err
		}
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
		pull := &pullState{
			commit: CommittedTransaction{
				TransactionID: tx.id,
				CommitLSN:     message.CommitLSN,
				EndLSN:        message.TransactionEndLSN,
				CommitTime:    message.CommitTime,
				Count:         tx.count,
			},
			changes: tx.mem,
		}
		if tx.spill != nil {
			if _, err := tx.spill.Seek(0, io.SeekStart); err != nil {
				discardSpill(tx.spill)
				return nil, fmt.Errorf("rewind transaction spill: %w", err)
			}
			pull.spill = tx.spill
			pull.reader = bufio.NewReader(tx.spill)
		}
		d.pull = pull
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

	case *pglogrepl.TruncateMessage:
		return nil, fmt.Errorf("TRUNCATE is unsupported; source transaction must not be acknowledged")

	default:
		return nil, fmt.Errorf("unsupported pgoutput message %T; source transaction must not be acknowledged", message)
	}
}

// validateRelation proves one pgoutput RelationMessage exactly matches the
// durable schema descriptor (or the fixed seam_marker codec). Any drift in
// column order, types, identity key, or replica identity fails closed before
// a single row is decoded.
func (d *Decoder) validateRelation(relation *pglogrepl.RelationMessage) error {
	if relation.Namespace != "public" {
		return fmt.Errorf("unsupported relation namespace %q", relation.Namespace)
	}
	var expected []string
	var expectedTypes []uint32
	switch relation.RelationName {
	case d.schema.Table:
		if relation.ReplicaIdentity != 'f' {
			return fmt.Errorf("relation %q must use REPLICA IDENTITY FULL so unchanged TOAST values can be reconstructed", relation.RelationName)
		}
		expected = make([]string, len(d.schema.Columns))
		expectedTypes = make([]uint32, len(d.schema.Columns))
		for i, col := range d.schema.Columns {
			expected[i] = col.Name
			expectedTypes[i] = col.TypeOID
		}
	case markerRelation:
		// The published marker schema includes the created_at bookkeeping column;
		// it carries no reconciliation meaning but is part of the fixed codec.
		expected = []string{"id", "job_id", "attempt", "kind", "chunk_min_id", "chunk_max_id", "created_at"}
		expectedTypes = []uint32{25, 25, 25, 25, 20, 20, 1184}
	default:
		return fmt.Errorf("unsupported published relation %q", relation.RelationName)
	}
	if len(relation.Columns) != len(expected) {
		return fmt.Errorf("relation %q has %d columns; expected %d", relation.RelationName, len(relation.Columns), len(expected))
	}
	for i, name := range expected {
		if relation.Columns[i].Name != name {
			return fmt.Errorf("relation %q column %d is %q; expected %q", relation.RelationName, i, relation.Columns[i].Name, name)
		}
		if relation.Columns[i].DataType != expectedTypes[i] {
			return fmt.Errorf("relation %q column %q has PostgreSQL type OID %d; expected %d", relation.RelationName, name, relation.Columns[i].DataType, expectedTypes[i])
		}
	}
	keyIndex := 0
	if relation.RelationName == d.schema.Table {
		keyIndex = d.schema.PKOrdinal - 1
	}
	if relation.Columns[keyIndex].Flags&1 == 0 {
		return fmt.Errorf("relation %q has no identity key on %q", relation.RelationName, expected[keyIndex])
	}
	return nil
}

func (d *Decoder) NextEvent() (model.Change, bool, error) {
	if d.pull == nil {
		return model.Change{}, false, fmt.Errorf("no open transaction to pull")
	}
	if d.pull.seq >= d.pull.commit.Count {
		d.pull.close()
		d.pull = nil
		return model.Change{}, false, nil
	}
	var change model.Change
	if d.pull.spill != nil {
		var size uint32
		if err := binary.Read(d.pull.reader, binary.BigEndian, &size); err != nil {
			return model.Change{}, false, fmt.Errorf("read spilled change length: %w", err)
		}
		payload := make([]byte, int(size))
		if _, err := io.ReadFull(d.pull.reader, payload); err != nil {
			return model.Change{}, false, fmt.Errorf("read spilled change: %w", err)
		}
		if err := json.Unmarshal(payload, &change); err != nil {
			return model.Change{}, false, fmt.Errorf("decode spilled change: %w", err)
		}
	} else {
		change = d.pull.changes[d.pull.seq]
	}
	change.Source = model.SourceTx{
		Generation: d.generation,
		SystemID:   d.systemID,
		XID:        d.pull.commit.TransactionID,
		LSN:        d.pull.commit.CommitLSN.String(),
	}
	d.pull.seq++
	return change, true, nil
}

func (d *Decoder) Reset() {
	if d.pull != nil {
		d.pull.close()
	}
	if d.transaction != nil && d.transaction.spill != nil {
		discardSpill(d.transaction.spill)
	}
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
	encoded, err := json.Marshal(change)
	if err != nil {
		return err
	}
	if d.transaction.spill == nil && d.MaxTransactionBytes > 0 && d.transaction.bytes+len(encoded) > d.MaxTransactionBytes {
		spill, err := os.CreateTemp("", "seam-capture-tx-*")
		if err != nil {
			return fmt.Errorf("create transaction spill: %w", err)
		}
		d.transaction.spill = spill
		for _, buffered := range d.transaction.mem {
			payload, err := json.Marshal(buffered)
			if err != nil {
				discardSpill(spill)
				d.transaction.spill = nil
				return err
			}
			if err := writeSpilledChange(spill, payload); err != nil {
				discardSpill(spill)
				d.transaction.spill = nil
				return err
			}
		}
		d.transaction.mem = nil
	}
	d.transaction.bytes += len(encoded)
	d.transaction.count++
	if d.transaction.spill != nil {
		if err := writeSpilledChange(d.transaction.spill, encoded); err != nil {
			return err
		}
	} else {
		d.transaction.mem = append(d.transaction.mem, change)
	}
	return nil
}

func writeSpilledChange(file *os.File, payload []byte) error {
	if len(payload) > int(^uint32(0)) {
		return fmt.Errorf("change is too large to spill: %d bytes", len(payload))
	}
	if err := binary.Write(file, binary.BigEndian, uint32(len(payload))); err != nil {
		return fmt.Errorf("write spilled change length: %w", err)
	}
	if _, err := file.Write(payload); err != nil {
		return fmt.Errorf("write spilled change: %w", err)
	}
	return nil
}

func (p *pullState) close() {
	if p.spill != nil {
		discardSpill(p.spill)
		p.spill = nil
	}
}

func discardSpill(file *os.File) {
	name := file.Name()
	_ = file.Close()
	_ = os.Remove(name)
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
	return d.buildChange(relation, model.OpInsert, row)
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
	if hasUnchangedValue(newRow) {
		if message.OldTuple == nil || message.OldTupleType != 'O' {
			return model.Change{}, fmt.Errorf("decode update %s.%s: unchanged TOAST value without a full old tuple; REPLICA IDENTITY FULL is required", relation.Namespace, relation.RelationName)
		}
		oldRow, err := decodeTuple(relation, message.OldTuple, false)
		if err != nil {
			return model.Change{}, fmt.Errorf("decode update old tuple %s.%s: %w", relation.Namespace, relation.RelationName, err)
		}
		for name, value := range newRow {
			if !value.unchanged {
				continue
			}
			old, ok := oldRow[name]
			if !ok || old.unchanged {
				return model.Change{}, fmt.Errorf("unchanged TOAST column %q is absent from full old tuple", name)
			}
			newRow[name] = old
		}
	}
	// pgoutput attaches an old tuple to an update whenever the replica identity
	// key changed ('K' key-only form) or the table uses REPLICA IDENTITY FULL.
	// if the old key differs from the new one, the row was re-keyed, which Seam
	// cannot represent. With REPLICA IDENTITY FULL required, a key-only old
	// tuple is a contract violation and fails closed.
	if relation.RelationName == d.schema.Table && message.OldTuple != nil {
		if message.OldTupleType == 'K' {
			return model.Change{}, fmt.Errorf("decode update %s.%s: key-only old tuple; REPLICA IDENTITY FULL is required", relation.Namespace, relation.RelationName)
		}
		oldRow, err := decodeTuple(relation, message.OldTuple, false)
		if err != nil {
			return model.Change{}, fmt.Errorf("decode update old tuple %s.%s: %w", relation.Namespace, relation.RelationName, err)
		}
		oldID, err := d.rowKey(oldRow)
		if err != nil {
			return model.Change{}, fmt.Errorf("decode update old key: %w", err)
		}
		newID, err := d.rowKey(newRow)
		if err != nil {
			return model.Change{}, err
		}
		if oldID != newID {
			return model.Change{}, fmt.Errorf("primary key change %d -> %d on %q not supported; reset pipeline", oldID, newID, relation.RelationName)
		}
	}
	return d.buildChange(relation, model.OpUpdate, newRow)
}

func (d *Decoder) decodeDelete(message *pglogrepl.DeleteMessage) (model.Change, error) {
	relation, err := d.relation(message.RelationID)
	if err != nil {
		return model.Change{}, err
	}
	if relation.RelationName == d.schema.Table && message.OldTupleType == 'K' {
		return model.Change{}, fmt.Errorf("decode delete %s.%s: key-only old tuple; REPLICA IDENTITY FULL is required", relation.Namespace, relation.RelationName)
	}
	row, err := decodeTuple(relation, message.OldTuple, message.OldTupleType == 'K')
	if err != nil {
		return model.Change{}, fmt.Errorf("decode delete %s.%s: %w", relation.Namespace, relation.RelationName, err)
	}
	return d.buildChange(relation, model.OpDelete, row)
}

// buildChange turns a decoded tuple into a generic row change (with the
// source schema epoch attached) or a marker change.
func (d *Decoder) buildChange(relation *pglogrepl.RelationMessage, op model.Operation, row decodedRow) (model.Change, error) {
	switch relation.RelationName {
	case d.schema.Table:
		values, err := d.rowValues(row)
		if err != nil {
			return model.Change{}, fmt.Errorf("decode %s row %s.%s: %w", op, relation.Namespace, relation.RelationName, err)
		}
		return model.Change{Op: op, SchemaID: d.schemaID, Row: &model.Row{Values: values}}, nil

	case markerRelation:
		id, err := requireString(row, markerRelationIDColumn)
		if err != nil {
			return model.Change{}, fmt.Errorf("decode marker: %w", err)
		}
		if op == model.OpDelete {
			// Markers are never deleted in normal operation; tolerate a
			// key-only delete by extracting the id only.
			return model.Change{Op: op, Marker: &model.Marker{ID: id}}, nil
		}
		kind, err := requireString(row, "kind")
		if err != nil {
			return model.Change{}, err
		}
		jobID, err := requireString(row, "job_id")
		if err != nil {
			return model.Change{}, err
		}
		attempt, err := requireString(row, "attempt")
		if err != nil {
			return model.Change{}, err
		}
		chunkMin, err := parseInt64(row, "chunk_min_id")
		if err != nil {
			return model.Change{}, err
		}
		chunkMax, err := parseInt64(row, "chunk_max_id")
		if err != nil {
			return model.Change{}, err
		}
		return model.Change{Op: op, Marker: &model.Marker{
			ID:       id,
			Kind:     model.MarkerKind(kind),
			JobID:    jobID,
			Attempt:  attempt,
			ChunkMin: chunkMin,
			ChunkMax: chunkMax,
		}}, nil

	default:
		return model.Change{}, fmt.Errorf("unexpected table %q", relation.RelationName)
	}
}

// rowKey extracts the message primary key from a decoded row map using the
// descriptor's pinned key column.
func (d *Decoder) rowKey(row decodedRow) (int64, error) {
	pk := d.schema.PKColumn()
	if pk == nil {
		return 0, fmt.Errorf("schema has no primary key column")
	}
	return parseInt64(row, pk.Name)
}

// rowValues converts a decoded row map into positional values aligned with
// the durable schema descriptor. Integer columns are typed int64; every other
// column keeps its exact canonical text; NULL columns are ValueNull.
func (d *Decoder) rowValues(row decodedRow) ([]model.Value, error) {
	values := make([]model.Value, len(d.schema.Columns))
	for i, col := range d.schema.Columns {
		value, ok := row[col.Name]
		if !ok {
			return nil, fmt.Errorf("column %q absent from tuple", col.Name)
		}
		if value.unchanged {
			return nil, fmt.Errorf("column %q is unchanged without a merged old value", col.Name)
		}
		if value.text == nil {
			values[i] = model.NullValue()
			continue
		}
		if schema.IsIntegerType(col.TypeOID) {
			n, err := strconv.ParseInt(*value.text, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parse %q: %w", col.Name, err)
			}
			values[i] = model.Int64Value(n)
			continue
		}
		values[i] = model.TextValue(*value.text)
	}
	return values, nil
}

type decodedValue struct {
	text      *string
	unchanged bool
}

type decodedRow map[string]decodedValue

func hasUnchangedValue(row decodedRow) bool {
	for _, value := range row {
		if value.unchanged {
			return true
		}
	}
	return false
}

func decodeTuple(relation *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData, keyOnly bool) (decodedRow, error) {
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
	row := make(decodedRow, len(columns))
	for i, column := range columns {
		value := tuple.Columns[i]
		switch value.DataType {
		case 'n':
			row[column.Name] = decodedValue{}
		case 't':
			s := string(value.Data)
			row[column.Name] = decodedValue{text: &s}
		case 'u':
			row[column.Name] = decodedValue{unchanged: true}
		case 'b':
			return nil, fmt.Errorf("binary column %q not supported", column.Name)
		default:
			return nil, fmt.Errorf("unknown tuple type %q for column %q", value.DataType, column.Name)
		}
	}
	return row, nil
}

func parseInt64(row decodedRow, column string) (int64, error) {
	v, ok := row[column]
	if !ok || v.text == nil || v.unchanged {
		return 0, fmt.Errorf("column %q missing or null", column)
	}
	n, err := strconv.ParseInt(*v.text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %q: %w", column, err)
	}
	return n, nil
}

func requireString(row decodedRow, column string) (string, error) {
	v, ok := row[column]
	if !ok || v.text == nil || v.unchanged {
		return "", fmt.Errorf("column %q missing or null", column)
	}
	return *v.text, nil
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

func (JSONCodec) EncodeTransaction(transaction model.TransactionEnvelope) ([]byte, error) {
	return json.Marshal(transaction)
}

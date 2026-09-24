// Package model contains the small, explicit data types shared across Seam.
package model

import (
	"fmt"
	"time"
)

type Operation string

const (
	OpInsert Operation = "insert"
	OpUpdate Operation = "update"
	OpDelete Operation = "delete"
)

// ValueKind is the exact Go representation Seam uses for one replicated cell.
//
// Seam never converts PostgreSQL values through lossy generic strings. The
// transport form is PostgreSQL's own canonical text representation of the
// declared type (type in/out functions are exact inverses, rendered under a
// UTC session so timestamptz is deterministic), and every value is bound back
// through an explicit ::type cast so the destination parses the identical
// value. Only the message primary key and the small integer family are
// additionally carried as typed int64 values, because chunking and eviction
// index rows by that key.
type ValueKind uint8

const (
	// ValueNull is a SQL NULL column value.
	ValueNull ValueKind = iota
	// ValueInt64 is an int8/int4/int2 value (and the message key).
	ValueInt64
	// ValueText is the canonical PostgreSQL text form of any other
	// supported column type (text family, bytea, numeric, float, boolean,
	// uuid, json/jsonb, date/time/timestamp family, interval).
	ValueText
)

// Value is one typed cell of a replicated row.
type Value struct {
	Kind ValueKind
	Int  int64
	Text string
}

// NullValue returns a SQL NULL value.
func NullValue() Value { return Value{Kind: ValueNull} }

// Int64Value returns a typed integer value.
func Int64Value(v int64) Value { return Value{Kind: ValueInt64, Int: v} }

// TextValue returns a value carried as canonical PostgreSQL text.
func TextValue(s string) Value { return Value{Kind: ValueText, Text: s} }

// IsNull reports whether the value is SQL NULL.
func (v Value) IsNull() bool { return v.Kind == ValueNull }

// Equal reports exact value equality. Comparing canonical text is exact for
// the supported type set: two equal values of any supported type print
// identically (under a UTC session for timestamptz), and NULL equals NULL.
func (v Value) Equal(o Value) bool {
	if v.Kind != o.Kind {
		return false
	}
	switch v.Kind {
	case ValueNull:
		return true
	case ValueInt64:
		return v.Int == o.Int
	default:
		return v.Text == o.Text
	}
}

// Row is one full replicated row: positional values in schema column order,
// with the primary key at the schema's PK ordinal. Rows are schema-relative;
// the durable descriptor that gives them meaning is carried on jobs and
// envelopes, never repeated per row.
type Row struct {
	Values []Value
}

// Change is one record on the ordered Kafka stream.
type Change struct {
	Op Operation
	// SchemaID is the deterministic fingerprint of the source schema this
	// row conforms to; it is empty for marker changes. A mismatch with the
	// job's pinned schema fails the pipeline closed: the source table shape
	// changed under a running job.
	SchemaID string
	// Row is non-nil for row changes. Inserts and updates carry the full new
	// tuple; deletes carry the full old tuple (REPLICA IDENTITY FULL).
	Row *Row
	// Marker is non-nil for LOW/HIGH control changes.
	Marker *Marker
	// Source identifies the committed source transaction.
	Source SourceTx
}

// Marker is a control record published through the same CDC stream as the
// replicated data table.
type Marker struct {
	ID       string
	Kind     MarkerKind // low or high
	JobID    string
	Attempt  string
	ChunkMin int64
	ChunkMax int64
}

type MarkerKind string

const (
	MarkerLow     MarkerKind = "low"
	MarkerHigh    MarkerKind = "high"
	MarkerBarrier MarkerKind = "barrier"
)

// SourceTx identifies a committed source transaction. It is stable across
// Kafka republishes: the same PostgreSQL commit LSN is republished at a new
// Kafka offset after source-reader recovery, so dedupe must use SourceTx, not
// the Kafka offset.
type SourceTx struct {
	Generation string // logical generation of the replication pipeline
	SystemID   string // PostgreSQL system identifier; fences a replaced source cluster
	XID        uint32 // source transaction id (informational)
	LSN        string // source commit LSN
}

func (s SourceTx) String() string {
	return fmt.Sprintf("%s:%s:%s", s.SystemID, s.Generation, s.LSN)
}

// TransactionEnvelope carries either a complete source transaction (version 1)
// or one ordered fragment (version 2). A version-2 transaction is complete only
// after fragments [0..FragmentIndex] have been observed and Final is true.
// Consumers must never expose or checkpoint a prefix as an applied transaction.
// SchemaID is the source schema epoch of every row change in the transaction;
// a transaction that mixes schema epochs is rejected.
type TransactionEnvelope struct {
	Version       int
	Source        SourceTx
	SchemaID      string
	FragmentIndex int
	Final         bool
	Count         int
	TotalCount    int
	Changes       []Change
}

// JobConfig identifies one backfill job.
type JobConfig struct {
	JobID             string
	SourceDSN         string
	SourceReplDSN     string
	SourceSlot        string
	SourcePublication string
	SourceSystemID    string
	DestDSN           string
	DestTable         string
	KafkaBrokers      []string
	KafkaTopic        string
	KafkaTopicID      string
	ChunkSize         int
	WorkerID          string
	LeaseDuration     time.Duration
	// MaxInMemoryCandidates bounds the candidate map held for one chunk. 0
	// means unlimited (unit-test default). A chunk read that exceeds the cap
	// fails the job instead of growing memory without bound.
	MaxInMemoryCandidates int
	// MaxRecordsPerBatch bounds how many Kafka records are processed at once.
	// 0 means unlimited (unit-test default).
	MaxRecordsPerBatch int
	// Workers is the number of concurrent chunk workers. Values greater than
	// one switch the reconciler into coordinator + worker pool mode (requires a
	// durable chunk store; the legacy scanner loop stays sequential).
	Workers int
	// HeartbeatInterval controls how often a worker renews its chunk lease
	// while executing it. 0 (the default) uses LeaseDuration/3, clamped to at
	// a positive interval below LeaseDuration. Keep it well below LeaseDuration so a healthy worker
	// never loses its lease to a scanner that runs long.
	HeartbeatInterval time.Duration
	// DisableHeartbeat turns lease renewal off (testing only). With it set, a
	// scan longer than LeaseDuration loses the chunk to reassignment.
	DisableHeartbeat bool
}

// Checkpoint is the durable progress state for a job.
type Checkpoint struct {
	JobID      string
	Generation string
	Attempt    string
	// OwnerID and OwnerEpoch fence process ownership. OwnerEpoch increases on
	// every successful takeover; a process must present both values in every
	// destination transaction so an expired owner cannot resume writing.
	OwnerID          string
	OwnerEpoch       int64
	OwnerLeaseExpiry *time.Time
	ScanUpperBound   int64
	CompletedThrough int64 // all IDs <= this are fully backfilled
	NextKafkaOffset  int64
	LastAppliedLSN   string
	Active           bool
}

// ChunkState is the lifecycle state of a backfill chunk.
type ChunkState string

const (
	ChunkPending     ChunkState = "pending"
	ChunkLeased      ChunkState = "leased"
	ChunkScanning    ChunkState = "scanning"
	ChunkReconciling ChunkState = "reconciling"
	ChunkCommitting  ChunkState = "committing"
	ChunkCompleted   ChunkState = "completed"
	ChunkFailed      ChunkState = "failed"
)

// Chunk is the durable state of one primary-key range within a job.
type Chunk struct {
	JobID        string
	ChunkMinID   int64
	ChunkMaxID   int64
	Attempt      string
	Status       ChunkState
	WorkerID     string
	LeaseToken   int64
	LeaseStart   *time.Time
	LeaseExpiry  *time.Time
	HeartbeatAt  *time.Time
	LowOffset    *int64
	HighOffset   *int64
	LowLSN       *string
	HighLSN      *string
	RowsScanned  int64
	RowsApplied  int64
	ErrorMessage string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// Range returns the inclusive primary-key interval for the chunk.
func (c *Chunk) Range() ChunkRange {
	return ChunkRange{Min: c.ChunkMinID, Max: c.ChunkMaxID}
}

// ChunkRange is an inclusive primary-key interval.
type ChunkRange struct {
	Min int64
	Max int64
}

func (c ChunkRange) String() string {
	return fmt.Sprintf("[%d,%d]", c.Min, c.Max)
}

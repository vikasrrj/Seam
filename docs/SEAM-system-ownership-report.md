# Owning SEAM: system architecture, guarantees, decisions, and interview guide

> **Current as of September 30, 2026.** This report describes the current
> uncommitted working tree. It explains the system and its engineering
> decisions without walking through source code. A separate code-level report
> should be written only after this system model is understood.

## How to use this report

The goal is not to memorize vocabulary. The goal is to be able to reconstruct
the design from the problem.

For every major part of SEAM, practice this answer shape:

```text
real problem -> invariant -> mechanism -> failure case -> tradeoff -> evidence
```

The report uses first-person decision language such as **“I chose…”** so it can
be used directly in an interview. Only say that a property is proved when the
tests described here actually exercise it. State the limitations yourself
before an interviewer has to discover them.

---

## 1. SEAM in the simplest possible language

SEAM keeps a destination database synchronized with a PostgreSQL source while
also being able to copy all the rows that existed before synchronization
started.

It has two kinds of work:

1. **CDC, or change data capture**, carries new inserts, updates, and deletes
   after PostgreSQL commits them.
2. **Backfill**, also called snapshot loading, copies the existing table.

SEAM sends PostgreSQL changes through Kafka and currently supports two
destination paths:

- PostgreSQL → Kafka → PostgreSQL
- PostgreSQL → Kafka → Snowflake

The difficult part is combining the old snapshot with live changes. If a
snapshot reads an old row and writes it after CDC has already applied a newer
update or delete, the destination becomes wrong. SEAM uses ordered LOW and HIGH
markers around every snapshot chunk so that live CDC always wins that race.

For online rebuilding, SEAM fills a separate shadow destination while the
current destination remains queryable. After exact validation, it promotes the
shadow atomically:

- PostgreSQL promotes with a transactional table-name swap.
- Snowflake promotes by atomically repointing a stable view.

SEAM is therefore both a CDC system and an online-backfill system. It is not
just a backfill script, and it is not a complete general-purpose connector
platform.

### My 30-second explanation

> I built SEAM to study the correctness boundary between an ordered CDC stream
> and a parallel snapshot. PostgreSQL logical decoding publishes complete
> source transactions to one Kafka partition. A destination applies those
> transactions atomically and tracks its own durable frontier. In parallel,
> workers scan sealed primary-key ranges bracketed by LOW and HIGH markers.
> Any key changed inside that window is excluded from the old snapshot, so an
> update cannot be overwritten and a delete cannot be resurrected. For an
> online resync, SEAM dual-writes to a shadow generation, validates it at an
> exact stream boundary, and promotes it while the old generation remains
> queryable. I deliberately kept the connector surface narrow so I could make
> recovery, fencing, transaction boundaries, and performance evidence real.

---

## 2. The fundamentals underneath SEAM

### 2.1 A database transaction

A transaction groups operations into one commit decision. If an application
changes five rows and commits, downstream replication should not expose two of
those changes as committed while the other three are missing. Source
transaction boundaries are part of the data's meaning.

**My decision:** I preserve the source transaction as the unit of delivery and
destination application. I do not treat each changed row as an independent
message merely because row-at-a-time processing is easier.

### 2.2 PostgreSQL WAL

PostgreSQL first records changes in its write-ahead log, or WAL. WAL is the
durable ordered history PostgreSQL uses for crash recovery and replication.
The physical WAL contains low-level database records; logical decoding turns
that history into table-level inserts, updates, deletes, relation metadata,
and transaction boundaries.

SEAM reads the `pgoutput` logical replication protocol through a replication
slot. The slot tells PostgreSQL that a consumer still needs WAL. PostgreSQL
must retain that WAL until the consumer reports progress.

This creates a real operational obligation: if capture stops for too long, WAL
can accumulate on the source disk. A slot is not a free queue.

### 2.3 CDC

CDC means reading committed database changes and applying them somewhere else.
It is useful because it avoids repeatedly scanning the entire table.

A serious CDC path must answer:

- Which source transaction produced this change?
- Is the transaction complete?
- In what order must transactions be applied?
- What happens when publishing succeeds but acknowledgement fails?
- How is a replay distinguished from new work?
- What happens when the log history needed for recovery is gone?

SEAM does not claim a distributed transaction between PostgreSQL and Kafka.
Instead, capture is at least once and destinations deduplicate stable source
transaction identities.

### 2.4 Backfill

CDC only contains changes available from a chosen log position onward. It does
not automatically give a new destination every row that already existed.
Backfill reads those historical rows from the source table.

A basic backfill can copy a static table. An online backfill is harder because
the table keeps changing while it is scanned. The output of an online backfill
is correct only if snapshot work and CDC work are reconciled under a precise
ordering rule.

### 2.5 Kafka's role

Kafka is the durable handoff log between capture and destination processes. It
decouples source reading from destination speed:

- PostgreSQL capture can continue while a destination is temporarily down.
- A new shadow destination can replay the same history independently.
- Recovery can restart from a durable destination offset.

Kafka does not make an overloaded destination fast. If capture produces
100,000 rows/s and the destination applies 20,000 rows/s, backlog grows by
80,000 rows/s. Kafka buys recovery time and decoupling; it does not remove the
capacity mismatch.

### 2.6 Source of truth, transport, and materialized state

The three layers have different roles:

| Layer | Role in SEAM |
| --- | --- |
| PostgreSQL source | Authoritative application state and ordered WAL history |
| Kafka | Retained ordered transport history |
| Destination PostgreSQL or Snowflake | Queryable materialization plus durable replication frontier |

The destination is not considered correct merely because it has many rows. It
must know exactly which ordered source prefix those rows represent.

---

## 3. The complete architecture

```text
Application writes
       |
       v
Source PostgreSQL
  table + marker table + WAL + logical slot
       |
       v
seam-capture
  schema validation
  transaction reconstruction
  bounded fragmentation
  source ownership fencing
       |
       v
Kafka topic, partition 0
  ordered transaction envelopes
       |
       +---------------------------------------+
       |                                       |
       v                                       v
PostgreSQL destination path              Snowflake destination path
seam reconciler                          seam-snowflake-sink
  CDC apply                                CDC staging + set-based merge
  durable candidates                       ledger + offset + key clocks
  parallel chunk workers                   renewable sink epoch
  checkpoints                                     |
       |                                           |
       v                                           v
PostgreSQL live/shadow tables             Snowflake base/shadow tables
       |                                           |
       v                                           v
seam-promote                             seam-snowflake-backfill/promote
  exact-prefix validation                  LOW/HIGH parallel backfill
  transactional rename                    zero-copy validation clone
                                            stable-view promotion
```

### The running roles

| Role | Responsibility |
| --- | --- |
| `seam-capture` | Own the PostgreSQL logical slot, decode committed transactions, publish them to Kafka, and acknowledge WAL only after Kafka acknowledges every fragment |
| `seam` | Own one PostgreSQL-destination job, consume CDC, discover and process chunks, persist candidates, update checkpoints, and recover interrupted work |
| `seam-promote` | Validate a PostgreSQL shadow at an exact prefix and atomically swap table names |
| `seam-snowflake-sink` | Own the Snowflake CDC frontier and apply every complete source transaction to all active routes |
| `seam-snowflake-backfill` | Build and execute a durable parallel Snowflake shadow backfill |
| `seam-snowflake-promote` | Validate a Snowflake shadow and atomically repoint the stable public view |
| `seam-lab` | Seed demonstrations and perform moving or fenced verification |

**My decision:** I separated these roles because they fail and scale
independently. Capture should not lose its slot simply because a warehouse is
slow. A backfill worker should not own the public query name. Promotion should
remain a narrow, auditable state transition rather than an accidental side
effect of finishing a scan.

---

## 4. The exact supported contract

SEAM is deliberately narrow:

- one source table per pipeline;
- the source table is in PostgreSQL's `public` schema;
- exactly one immutable, non-null `BIGINT` primary key;
- `REPLICA IDENTITY FULL` on the source table;
- insert, update, and delete;
- one Kafka topic with one partition;
- one pinned schema fingerprint per generation;
- one source PostgreSQL cluster identity;
- PostgreSQL and Snowflake destination implementations;
- initial backfill and online shadow rebuild;
- explicit validation and promotion.

PostgreSQL destinations accept the closed exact type set defined by SEAM's
schema contract. Values use PostgreSQL's canonical text representation and are
parsed through the declared type at the destination. Integer keys remain typed
integers for chunking and comparisons.

Snowflake has a narrower proven mapping:

- PostgreSQL integer types → Snowflake `NUMBER(38,0)`;
- boolean → `BOOLEAN`;
- character/text/UUID families → `VARCHAR`;
- date → `DATE`;
- timestamp without time zone → `TIMESTAMP_NTZ`;
- timestamp with time zone → `TIMESTAMP_TZ`;
- JSONB → `VARIANT`.

Unsupported types stop before consumption. Schema changes during a generation
also stop the pipeline.

**My decision:** I chose a closed supported set instead of pretending all data
types are equivalent across databases. Cross-database type mapping is a
correctness contract, not a formatting task. I only accept mappings whose
round trip and comparison behavior I can defend.

### Explicit non-goals

SEAM currently does not provide:

- multiple Kafka partitions;
- composite or non-BIGINT primary keys;
- DDL replication or online schema evolution;
- automatic support for arbitrary source schemas;
- `TRUNCATE` replication;
- primary-key updates;
- coordination with arbitrary direct destination writers;
- production broker high availability in the bundled development stack;
- automatic retention sizing, deployment, alerting, backup, or multi-region
  operation;
- warehouse-scale file loading;
- a large-scale Artie-equivalent benchmark.

These are boundaries, not hidden TODOs. Crossing one requires a new invariant
and new failure tests.

---

## 5. The identities that make recovery safe

Several identifiers exist because each protects a different failure domain.

| Identity | Meaning | What it prevents |
| --- | --- | --- |
| Source system ID | Physical PostgreSQL cluster identity | Resuming against a replaced database that happens to use the same hostname |
| Generation | Logical history lineage | Mixing records from incompatible replication generations |
| Schema fingerprint | Exact ordered column/type/nullability/key contract | Applying a row after schema drift |
| Source commit LSN | Stable identity of a committed PostgreSQL transaction | Reapplying a transaction republished at a new Kafka offset |
| Source XID | PostgreSQL transaction number, mainly diagnostic | It is not used alone because XIDs wrap |
| Kafka topic ID | Identity of the actual topic object | Treating a deleted and recreated same-named topic as continuous history |
| Kafka offset | Ordered transport position | Resuming from the correct record in one partition |
| Job ID | One logical destination/backfill job | Separating independent live and shadow consumers |
| Attempt | Recovery incarnation of unfinished chunk work | Letting stale markers or candidates from a previous run affect the new run |
| Process owner epoch | Monotonic destination-process ownership term | A paused old process writing after takeover |
| Chunk lease token | Monotonic ownership term for one range | Two workers finalizing the same chunk |
| Snowflake sink epoch | Monotonic ownership term for the warehouse sink | Two sink processes advancing one Snowflake frontier |
| Route-fence epoch | Destination routing generation | A normal writer resolving a table name during promotion |

**My decision:** I do not use one generic lock token for everything. Source
identity, transport identity, process ownership, work ownership, and routing
ownership are different questions. Keeping them separate makes failure
analysis precise.

---

## 6. End-to-end live CDC flow

Consider an application transaction that updates account 42 and inserts
account 100.

### Step 1: PostgreSQL commits

PostgreSQL records the transaction in WAL. Logical decoding does not expose it
as committed until the source commits.

### Step 2: capture reconstructs the transaction

Capture validates relation identity, column order, PostgreSQL type identities,
replica identity, and the pinned schema. Inserts and updates carry complete new
rows. Deletes carry the old row under `REPLICA IDENTITY FULL`.

Unsupported relations, messages, binary tuple forms, `TRUNCATE`, primary-key
changes, or schema drift stop capture. Capture does not skip something it does
not understand and then acknowledge past it.

### Step 3: large transactions remain bounded

An open transaction can contain too many events to keep safely in RAM. SEAM
spills open transaction data to disk above its memory threshold. At commit it
publishes either one envelope or ordered fragments sized below the Kafka record
limit.

A fragment contains transaction identity, fragment index, final/non-final
status, event counts, and schema identity. Consumers do not expose a
transaction until every fragment through the final fragment is present.

**My decision:** I bounded memory without weakening atomicity. Fragmentation is
a transport detail; the destination still sees one source transaction.

### Step 4: Kafka acknowledges publication

Capture waits synchronously for Kafka to acknowledge each record. Only after
all fragments of the committed source transaction have been acknowledged does
capture advance its PostgreSQL standby status.

If capture dies after Kafka accepted the records but before PostgreSQL received
the acknowledgement, PostgreSQL sends that transaction again. This is why the
handoff is at least once.

### Step 5: a consumer reconstructs the complete transaction

The Kafka consumer starts from the destination's durable next offset. It
validates topic identity and envelope structure. Fragmented transactions are
assembled in order and can be backed by a temporary file. An incomplete
fragment prefix is never returned as an applyable transaction.

### Step 6: the destination applies and advances together

The destination applies every row effect from the source transaction and
advances its durable Kafka frontier in one destination transaction. It also
records the stable source transaction identity.

If a destination commit succeeds but the client loses the response, a retry
uses the durable ledger to classify the ambiguous result. Kafka offsets alone
are insufficient because the same source LSN can be republished at different
offsets.

### The CDC guarantee

The precise statement is:

> Capture is at least once. A destination atomically applies complete source
> transactions, records their stable source identity, and advances its
> transport frontier. For supported operations, replay produces effectively
> once destination effects.

I do not describe this as a magical end-to-end distributed “exactly once”
transaction. PostgreSQL, Kafka, and the destination do not share a two-phase
commit.

---

## 7. Why naive online backfill fails

Assume the source contains:

```text
id=42, owner=Ada, balance=100
```

### Stale update overwrite

1. A snapshot reads balance 100.
2. The application commits balance 200.
3. CDC writes 200 to the destination.
4. The delayed snapshot writes 100.

The destination is stale even though neither operation failed.

### Delete resurrection

1. A snapshot reads row 42.
2. The application deletes row 42.
3. CDC deletes it at the destination.
4. The delayed snapshot inserts its saved copy.

The deleted row has been resurrected.

### A timestamp does not automatically solve it

Comparing application timestamps is unsafe unless every writer follows a
strict version contract and clocks, precision, transaction ordering, and tie
behavior are all defined. Snapshot reads also need a clear relationship to the
CDC log.

### Pausing CDC does not solve it

If CDC is paused during a large scan, changes accumulate and the destination
becomes stale. Replaying afterward still requires a correct boundary and enough
retained history.

### Locking the source for the whole copy is correct but unavailable

A long source write lock can make a copy consistent, but it turns a backfill
into an outage. SEAM reserves short fences for boundary establishment and
final cutover, not for the full scan.

---

## 8. LOW/HIGH reconciliation: SEAM's core algorithm

Every chunk scan is bracketed by two marker transactions written into the same
source PostgreSQL database and publication as the real table changes.

```text
LOW(chunk)
    source scan of the chunk
HIGH(chunk)
```

Because the markers and row changes travel through the same ordered Kafka
partition, the destination can classify races.

### Chunk lifecycle

1. Lease a durable chunk range.
2. Write LOW at the source.
3. Wait until LOW appears in the destination's ordered stream.
4. Read the source rows in that range.
5. Store them as candidates rather than immediately treating them as truth.
6. Write HIGH at the source.
7. Continue applying ordered CDC until HIGH appears.
8. Evict any candidate key changed between LOW and HIGH.
9. Write only surviving candidates.
10. Complete the chunk and advance durable progress atomically.

### Why it prevents stale overwrites

If account 42 changes after LOW and before HIGH, CDC identifies key 42 during
the window. The old candidate is excluded. The newer CDC value remains.

### Why it prevents delete resurrection

A delete is also a change to key 42. It evicts the snapshot candidate. The
snapshot therefore cannot reinsert the row after CDC deletes it.

### Changes before LOW

The scan happens after LOW is committed. A change fully ordered before LOW is
already part of the state the scan can observe, and CDC remains idempotent.

### Changes after HIGH

They are ordinary CDC events applied after the chunk window. They do not need
to invalidate that earlier snapshot.

### Keys outside the chunk

CDC remains global and ordered. A change outside the current chunk must still
be applied to the destination, but it does not evict a candidate from this
range.

### Inserts above the sampled upper bound

CDC is active before or while a shadow is built. Rows inserted above the
snapshot upper bound arrive through CDC even though no historical chunk covers
them. In the Snowflake lifecycle, the shadow route is activated before the
upper bound is sampled specifically to close this race.

### My decision

> I used markers in the source database instead of local timestamps because I
> needed the boundary to share PostgreSQL commit order with actual row changes.
> I used one Kafka partition because that gives the first version of the system
> one total order. The tradeoff is a throughput ceiling and a future need for a
> partition-aware ordering protocol.

---

## 9. Durable chunk planning and parallel execution

### Why chunks exist

A single full-table scan has poor recovery granularity and cannot use parallel
source/destination capacity. SEAM divides the primary-key domain into bounded
ranges.

### Gap-free manifest

Discovery is durable. It records the cursor and the next logical range
together, then seals a manifest that covers every key position through the
sampled upper bound. Sparse keys do not create uncovered holes.

A partial manifest is unsafe because a restart might confuse “not discovered
yet” with “no data exists there.” SEAM distinguishes discovery from execution
and will not treat an unsealed manifest as complete.

### Coordinator and workers

Workers can scan different chunks concurrently. The ordered CDC stream still
has a single logical coordinator/frontier. Parallelism is used where the work
is independent: source range scans, candidate staging, and chunk-level
reconciliation.

### Worker leases

Each worker owns a chunk for a bounded time and renews it. A monotonic token
changes on reassignment. Finalization requires the same owner and token, so a
worker paused by a long GC stop or network partition cannot wake up and commit
after another worker has taken over.

### Process leadership

One reconciler process owns a job through a renewable lease and monotonic
epoch. Destination mutations prove that process ownership inside the durable
transaction. Chunk tokens protect individual ranges; the process epoch
protects the entire reconciler.

### Recovery attempt

After an interrupted run, unfinished chunks move into a new attempt. Markers
and candidates contain the attempt identity, so old work cannot contaminate
the new run. Completed durable progress is retained; unsafe partial work is
re-read.

### Chunk-size tradeoff

- Too small: marker, lease, transaction, and metadata overhead dominate.
- Too large: memory, retry cost, scan latency, and load imbalance grow.
- Too few workers: hardware stays idle.
- Too many workers: source I/O, caches, connection pools, Kafka, and the
  destination contend.

**My decision:** I use a fixed, durable manifest in the production path.
Adaptive chunking exists as an experiment but is rejected by production
configuration because changing ranges without preserving the durable coverage
proof would weaken recovery correctness.

---

## 10. PostgreSQL destination path

The PostgreSQL destination acts as both data plane and durable control plane.

### Persistent state

It stores:

- job identity and immutable source/topic/schema configuration;
- current attempt and scan upper bound;
- next Kafka offset and last applied source identity;
- process owner and epoch;
- durable chunk ranges, states, owners, tokens, and leases;
- snapshot candidates and eviction state;
- applied source transactions;
- route-fence state;
- cutover gates and promotion records.

### Fresh job boundary

A new job writes a unique barrier into the source and waits for that barrier's
complete Kafka transaction. The barrier gives the job a real stream position.
The job then persists source identity, topic identity, schema, upper bound,
manifest, and initial destination frontier.

### Applying CDC

Within one PostgreSQL transaction, SEAM:

- proves current process leadership and routing generation;
- applies inserts/updates/deletes by primary key;
- evicts relevant snapshot candidates when a chunk window is open;
- records source-transaction application;
- advances the Kafka checkpoint;
- completes chunk transitions when appropriate.

If that database transaction rolls back, its checkpoint also rolls back. The
system never records progress past destination effects that did not commit.

### Candidate staging

Snapshot rows are stored durably as candidates. This costs destination writes,
but it prevents a process crash from turning an in-memory decision into
ambiguous state. Candidate payloads are generic, schema-relative canonical
values rather than fixed `accounts` fields.

### Live and shadow jobs

An online PostgreSQL rebuild runs two independent jobs against the same Kafka
history:

- the live job continues maintaining the public table;
- the shadow job maintains `accounts_shadow` and performs its snapshot.

Both have independent checkpoints, attempts, candidates, and chunk states.
They share source, broker, and infrastructure capacity, so isolation is
logical rather than free physical capacity.

---

## 11. PostgreSQL validation and atomic promotion

Finishing all chunks is necessary but not sufficient. The shadow must be
proved equal to a specific source prefix before it becomes public.

### Phase 1: align exact frontiers

Promotion installs durable cutover gates for both live and shadow jobs. It
drives them to a common transaction boundary rather than comparing two moving
destinations at unrelated offsets.

### Phase 2: establish a stable full comparison

A short source write fence emits a validation barrier and establishes a
repeatable-read PostgreSQL snapshot at that exact prefix. The snapshot remains
valid after writes resume. SEAM then compares all ordered rows between that
source snapshot and the shadow while applications can write again.

This avoids holding the source write fence for an O(N) table comparison.

### Phase 3: capture the suffix

Changes after the validation marker are collected into a bounded changed-key
set. This set represents the small suffix that could make the earlier full
comparison stale.

### Phase 4: final fence and delta validation

Under a second short source fence, SEAM emits a cutover barrier, advances both
jobs to it, and compares only the changed keys against current source state.
If the changed-key set exceeds its configured bound, promotion aborts instead
of creating an unbounded write pause.

### Phase 5: atomic rename

Promotion takes the exclusive destination routing fence and required table
locks. In one PostgreSQL transaction it:

- rechecks schema and frontiers;
- deactivates the shadow writer;
- renames the old live table to a timestamped retained name;
- renames the shadow to the public live name;
- records the promotion.

Readers may wait briefly for the final table lock. Before this final transition
the original public table remains unchanged. Retrying the same promotion
recognizes the committed record instead of swapping generations back.

### My decision

> I rejected “compare once, then rename later” because the source can change
> between comparison and cutover. I also rejected holding writes for the full
> comparison because the pause grows with table size. I used an MVCC snapshot
> for the full proof and a bounded changed-key proof under the final fence.

---

## 12. Snowflake destination model

Snowflake has different transaction, constraint, and DDL behavior from
PostgreSQL, so I did not force the PostgreSQL implementation behind a fake
universal sink abstraction.

### Stable public name and physical generations

The public Snowflake object is a stable view such as `ACCOUNTS`. Initially it
points to a physical base table such as `ACCOUNTS__SEAM_BASE`. A rebuild fills
a separate table such as `ACCOUNTS_SHADOW`.

Queries keep using the stable view while the shadow is incomplete. Promotion
repoints the view to the validated shadow.

### Snowflake internal state

The internal schema stores:

| State | Purpose |
| --- | --- |
| Offset frontier | Next Kafka offset expected by the sink |
| Sink lease | Active owner, monotonic epoch, and expiry |
| Applied transaction ledger | Stable source identity, event count, and content fingerprint |
| Routes | Physical tables currently receiving live CDC |
| Key clocks | Latest source LSN and sequence for each route/key, including tombstones |
| CDC staging | Bounded rows for one source transaction |
| Markers | LOW, HIGH, validation, and promotion boundaries observed by the sink |
| Backfill jobs | Durable lifecycle and immutable parameters |
| Backfill chunks | Ranges, states, owners, tokens, and markers |
| Snapshot staging | Disposable candidate rows for a chunk |

Snowflake constraints are not used as if they were PostgreSQL enforcement.
SEAM explicitly checks uniqueness assumptions, transition preconditions,
frontier continuity, and affected-row counts.

### One Snowflake apply transaction

For one complete PostgreSQL transaction, the sink:

1. validates source, schema, topic, offsets, counts, and content identity;
2. writes bounded rows into a temporary CDC staging table;
3. loads all active routes;
4. performs set-based merge/delete operations into each route;
5. advances per-key clocks and preserves delete tombstones;
6. records any markers;
7. proves its current sink epoch;
8. records the applied transaction fingerprint;
9. advances the authoritative Kafka offset;
10. clears staging and commits.

Row effects, replay identity, key clocks, marker visibility, and transport
frontier therefore share one commit decision.

### Why key clocks exist

The visible destination table cannot remember the version of a deleted row
because the row is gone. A durable tombstone clock records that deletion. When
an old snapshot candidate later appears, its LOW boundary can be compared with
the key clock and the candidate is rejected.

The clock also orders multiple events for one key within the same source
transaction using a sequence number in addition to source LSN.

### Snowflake sink fencing

Only one process may own a stream's Snowflake frontier. The owner renews a
lease with a monotonic epoch. Every actual apply transaction touches and proves
that lease after its potentially slow stage/merge work and before ledger and
frontier commit.

This placement is deliberate. A process can begin while it is leader, pause
for a long time, and lose leadership during a merge. It must prove ownership
again at the commit boundary. A takeover changes the same lease row, so the
stale transaction cannot silently commit afterward.

### My decision

> I used a warehouse-specific ledger/frontier transaction rather than relying
> on Kafka consumer-group commits. The destination's own durable state is the
> authority because it must advance atomically with the data mutation.

---

## 13. Snowflake online backfill lifecycle

The Snowflake backfill is a durable state machine:

```text
discovering
    -> running
    -> ready_to_verify
    -> verifying
    -> ready
    -> promoting
    -> completed

validation mismatch -> failed
```

### Preparation order

SEAM creates an empty typed shadow and activates its live CDC route before
sampling the source upper bound. This matters because an insert can occur while
preparation is running. Once the route is active, any later insert is owned by
CDC even if its key lies above the sampled snapshot bound.

### Manifest sealing

Discovery produces gap-free chunks through the captured upper bound. The
manifest and transition to `running` commit together. A restart either sees
discovery still in progress or a complete manifest; it never treats a partial
manifest as complete.

### Chunk execution

For each leased range, a worker:

1. records LOW in PostgreSQL;
2. establishes the scanning state;
3. reads the source range;
4. replaces the disposable snapshot-stage rows for that chunk;
5. records HIGH in PostgreSQL;
6. waits until the ordered Snowflake sink has committed HIGH;
7. merges candidates into the shadow only where no key clock is newer than
   LOW;
8. completes the chunk and clears staging in the same transaction.

Expired chunk leases can be taken over. The old token cannot finalize after a
new token exists.

### Why CDC writes to both base and shadow

While a shadow is being built, live changes must reach both the current and
future generations. The routes table makes dual writing explicit. It also
makes recovery possible because route membership is durable rather than a
temporary in-process flag.

### Failure behavior

- Crash before staging commits: the replacement worker scans again.
- Crash after staging but before finalization: staging is disposable and the
  fenced replacement safely replaces it.
- Crash after finalization commit: the chunk is already complete and is not
  repeated as unfinished work.
- Lease expiry during a slow operation: finalization fails closed; another
  worker can take over.
- Sink downtime: markers and changes wait in Kafka; workers waiting for HIGH
  do not guess that catch-up happened.

---

## 14. Snowflake validation and promotion

### Validation boundary

Validation briefly fences source writes, writes a validation marker, and
exports a repeatable-read PostgreSQL snapshot. It waits for the Snowflake sink
to cross the marker and creates a zero-copy clone of the shadow at that same
warehouse boundary.

A second PostgreSQL transaction imports the exported snapshot before the
source fence is released. The full ordered comparison can then continue while
applications resume writing.

The comparison uses normalized values so that equivalent PostgreSQL and
Snowflake timestamp/JSON representations are compared under an explicit
contract rather than raw display formatting.

If validation fails, the job becomes failed and its shadow route is disabled.
An invalid shadow must not continue consuming capacity or accidentally become
promotable.

### Promotion suffix

SEAM replays the Kafka suffix after the validation marker into a bounded set of
changed keys. Under a second short source fence, it emits the final marker,
waits for the sink, and compares those keys between current PostgreSQL and the
shadow.

### Durable intent before DDL

Snowflake DDL is not treated like a PostgreSQL transactional rename. Before
repointing the view, SEAM records durable promotion intent. It then executes an
idempotent pointer assignment:

```text
public view -> validated shadow table
```

If the process dies after the view changes but before route metadata converges,
both routes remain safe to write and a retry assigns the same view target
again. It never toggles back to the old generation.

### Why a view instead of renaming physical tables

A stable view gives consumers one query name and makes promotion an atomic
metadata pointer change appropriate to Snowflake. Physical generations retain
clear identities for validation and recovery.

**My decision:** I used destination-specific promotion mechanisms. PostgreSQL
gives me transactional renames; Snowflake gives me stable-view replacement and
zero-copy clones. A serious connector should share invariants, not pretend all
databases have identical primitives.

---

## 15. Failure analysis from source to destination

### Capture crashes before Kafka acknowledges

PostgreSQL has not received an advanced standby acknowledgement. The slot
replays the source transaction. No committed destination work is assumed.

### Kafka acknowledges but capture crashes before PostgreSQL acknowledgement

The same PostgreSQL transaction may appear again at new Kafka offsets. The
stable source commit LSN and transaction content identity let the destination
recognize the replay.

### Capture crashes halfway through a fragmented transaction

Kafka may contain an abandoned fragment prefix. Consumers do not expose it as
a transaction because the final fragment is absent. After source replay, a
complete new fragment sequence can be applied.

### Kafka is unavailable

Capture does not acknowledge source WAL past an unconfirmed publish. The slot
retains WAL. This protects correctness but can consume source disk; operations
must monitor slot lag.

### Kafka deletes and recreates the topic

The topic name can be identical while its history is unrelated. SEAM pins the
topic ID and rejects the replacement. Offset numbers alone do not establish
continuity.

### Kafka retention deletes required history

SEAM fails recovery instead of reading from a later offset and pretending the
missing changes never existed. The safe remedy is a new snapshot/generation.

### The source hostname points to a different PostgreSQL cluster

SEAM compares the source system ID. It rejects the replacement even if the DSN,
database, tables, and usernames look the same.

### A second capture starts

Capture leadership uses a source advisory lock and durable epoch. A live owner
cannot be displaced. A replacement advances the epoch; the stale process
cannot continue publishing and acknowledging WAL as the current owner.

### A second PostgreSQL reconciler starts for the same job

The existing unexpired process lease blocks it. After expiry, takeover advances
the owner epoch. Destination mutations from the old epoch are rejected.

### A chunk worker pauses beyond its lease

Another worker can acquire a higher lease token. The stale worker cannot
complete or advance the chunk even if it wakes up with old in-memory data.

### Destination commit succeeds but the client sees a timeout

The applied-transaction ledger and durable frontier classify the retry. The
system does not blindly replay an ambiguous transaction as new work.

### Schema changes during a run

The schema fingerprint no longer matches. SEAM stops and requires a new safe
generation. It does not reinterpret old positional rows under a new schema.

### A process crashes during PostgreSQL promotion

Before the final destination transaction commits, the old public table remains
the public table. If commit succeeded but the response was lost, the durable
promotion record makes retry idempotent.

### A process crashes after Snowflake view replacement

Durable intent exists and the view assignment is idempotent. A retry points to
the same shadow again and finishes route convergence.

### Validation finds a mismatch

Promotion stops. PostgreSQL leaves the live table unchanged. Snowflake marks
the backfill failed and disables its shadow route. Correctness wins over
availability of the new generation.

---

## 16. Backpressure, bounded memory, and resource protection

### Backpressure is conservation of work

If upstream produces faster than downstream consumes, work must accumulate in
one of four places:

- PostgreSQL WAL retained by the slot;
- Kafka retained records;
- process memory/disk buffers;
- destination staging/queues.

SEAM does not pretend buffering removes the imbalance. It bounds local memory
and exposes the slower stage through lag and throughput.

### Transaction memory

Large open source transactions spill to disk rather than growing RAM without
limit. Kafka fragment assembly can also be disk-backed. Hard event and record
size limits stop pathological transactions without acknowledging past data the
system cannot represent.

### Snapshot memory

Each scan has configurable row and byte limits. PostgreSQL candidates are
durably staged. Snowflake uses bounded staging batches. A chunk that exceeds a
configured safety budget fails rather than risking process exhaustion.

The full chunk manifest still exists in memory in parts of the coordinator, so
extremely fine ranges at hundred-million-row scale remain unproven.

### CDC-lag admission

The PostgreSQL backfill controller checks the Kafka end offset against the
applied frontier. If CDC lag crosses its configured threshold, it pauses
admission of new source scans. Existing windows finish, allowing the ordered
CDC path to catch up.

### Cross-process limits

Local goroutine limits are insufficient when live and shadow jobs are separate
processes. SEAM uses PostgreSQL session advisory locks as shared permits for:

- concurrent source scans;
- concurrent destination transactions.

If a process dies, PostgreSQL closes the session and releases its permits.

### Retries without failure storms

Transient PostgreSQL, Kafka, network, and selected Snowflake availability or
serialization errors use bounded exponential backoff. SQL/data/schema errors
are not treated as transient. Snowflake apply retries are safe because the
ledger and frontier classify whether an ambiguous transaction committed.

**My decision:** I bounded retries and classified errors instead of immediately
retrying everything. Retrying a deterministic schema error wastes capacity;
retrying an overloaded service with no delay makes the outage worse.

### My decision

> I separated correctness from load control. LOW/HIGH markers make a scan
> correct; permits and lag admission decide whether starting that scan is safe
> for the shared infrastructure. Throttling cannot be allowed to redefine the
> correctness boundary.

---

## 17. What limits throughput today

Throughput is determined by the slowest saturated stage, measured in bytes as
well as rows:

```text
PostgreSQL storage/CPU
    -> logical decoding
    -> serialization
    -> network
    -> Kafka partition/broker
    -> consumer/decode
    -> destination staging
    -> destination merge/index/WAL/warehouse
```

### Source PostgreSQL

Potential limits include storage bandwidth, random heap reads, cache churn,
CPU spent decoding tuples, connection count, and interference with application
queries. More workers help only until the source is saturated. Past that point
they increase latency and evict useful cache pages.

### Chunking and parallelism

Parallel workers hide per-query latency and use more I/O concurrency. Speedup
is sublinear because discovery, markers, ordered CDC, coordination, and shared
hardware remain serial or contended.

### Serialization and memory copies

Canonical values are encoded into envelopes, decoded, staged, and parsed by
the destination. Wide rows can make CPU, allocations, memory bandwidth, and
network dominant even when rows/s looks low.

### Kafka

One partition gives SEAM a simple total order but caps parallel production and
consumption. The bundled broker uses replication factor one, so its benchmark
behavior is not a production durability claim. Broker disk, acknowledgements,
retention, and network can all become bottlenecks under larger loads.

### PostgreSQL destination

The destination pays for heap writes, indexes, WAL, transactions, candidate
staging, and checkpoint metadata. Durable candidate staging improves recovery
but adds write amplification. Frequent small chunks increase commit and marker
overhead.

### Snowflake destination

The original snapshot path staged rows with parameterized multi-row SQL before
the set based `MERGE`. Its executed component baseline reached:

| Staged rows | Wall time | Rows/s |
| ---: | ---: | ---: |
| 100 | 2.73 s | 36.6 |
| 1,000 | 6.24 s | 160.3 |
| 5,000 | 26.58 s | 188.1 |

This is not full-pipeline throughput. It excludes PostgreSQL scanning, Kafka,
LOW/HIGH catch-up, final merge, validation, and promotion. SEAM now replaces
that mechanism by default with one deterministic compressed file per chunk,
`PUT`, and a lease-fenced transactional `COPY INTO`. The old SQL path remains
as the control. No post-change live number is recorded yet.

### The justified Snowflake performance step

Run the SQL and bulk loaders against the same isolated schemas and workload,
then measure:

- rows/s and uncompressed/compressed bytes/s;
- file size and count;
- upload, copy, and merge time separately;
- warehouse size and credit consumption;
- Kafka lag;
- source load;
- recovery time after failures.

That work is deliberately deferred in the current stopping point.

### Why rows/s alone is weak evidence

Ten thousand rows of 100 bytes and ten thousand rows of 10 KB are different
systems workloads. Every serious benchmark should record row width, total
bytes, schema, index shape, source/destination resources, concurrency, and
correctness verification.

---

## 18. Current performance evidence

### PostgreSQL backfill

The latest exact-verified samples with production resource admission reported:

| Rows | Workers | Wall time | Rows/s | Speedup |
| ---: | ---: | ---: | ---: | ---: |
| 10,000 | 1 | 3.09 s | 3,240 | 1.00× |
| 10,000 | 4 | 1.44 s | 6,961 | 2.15× |
| 100,000 | 1 | 29.59 s | 3,379 | 1.00× |
| 100,000 | 4 | 14.89 s | 6,717 | 1.99× |

Each reported run performed exact row-for-row verification. The 100,000-row
figures are one pair, and the latest 10,000-row figures are one pair. They are
useful smoke evidence, not a performance distribution.

An earlier counterbalanced 10,000-row run under the older in-memory-window
implementation measured a median of roughly 5.53k rows/s with one worker and
9.92k rows/s with four workers, about 1.77× speedup. It is historical evidence
and is not directly comparable after durable staging changed the write path.

### What the results prove

- Multiple workers reduce wall-clock time on the tested local workload.
- Scaling is not linear.
- Durable correctness machinery has measurable cost.
- Exact verification can be coupled to performance experiments.

### What the results do not prove

- 10–20× Artie-like speedup;
- sustained production CDC under concurrent application traffic;
- performance at 1M, 10M, 100M, or 20B rows;
- behavior on separate source, broker, and destination machines;
- cloud warehouse cost efficiency;
- source impact under a production query workload.

**My interview wording:**

> I measured about 2× backfill speedup from four workers in two current smoke
> workloads, with exact content verification. I do not extrapolate that to
> Artie's public numbers. Their result depends on workload, source and
> destination capacity, starting bottleneck, and bulk-loading architecture.

---

## 19. Testing strategy and actual evidence

### Unit tests

Unit tests attack state transitions, SQL-generation contracts, schema/value
handling, transaction framing, retry classification, fencing, chunk discovery,
candidate eviction, and promotion comparison behavior.

### Race tests

The full repository passes Go's race detector. This tests Go memory races; it
does not replace distributed failure testing.

### PostgreSQL/Kafka integration tests

The executed dedicated-stack suite covers:

- basic CDC flow;
- bounded static snapshot;
- demonstrations of naive stale-update and delete-resurrection failures;
- reconciled concurrent updates and deletes;
- unrelated CDC events;
- durable checkpoints;
- crashes after chunk reads;
- edge ranges and sparse keys;
- replay deduplication;
- resource bounds and telemetry;
- CDC continuity after backfill;
- random concurrent changes with crash/restart;
- durable discovery recovery;
- process leadership fencing;
- capture leadership takeover;
- large fragmented source transactions;
- process-shared resource admission;
- online live/shadow resync;
- atomic PostgreSQL promotion and idempotent retry;
- isolation of destructive integration ports.

### Live Snowflake control-plane test

This creates isolated schemas in real Snowflake and exercises object creation,
transactions, offset initialization, job preparation, manifest sealing, sink
lease takeover, stale-owner rejection, validation cloning, promotion, and
cleanup.

### Full PostgreSQL/Kafka/Snowflake recovery test

The full-path test passed in **210.14 seconds** and ended with an exact
**520-row** source/Snowflake match after:

- a real logical decoding stream;
- live CDC into Snowflake;
- an intentional sink crash without lease release;
- lease expiry and higher-epoch takeover;
- updates before the crash;
- deletes and inserts while the sink was down;
- an intentional coordinator stop;
- restart with four workers;
- exact stable-snapshot validation;
- changes after validation;
- promotion;
- idempotent promotion retry;
- final normalized cell-by-cell comparison.

This is strong correctness evidence for one bounded scenario. It is not a
large performance benchmark or a formal proof.

### What the full test found before it passed

The first real full-path executions exposed bugs that isolated unit tests had
missed:

- PostgreSQL's `not null` catalog flag had been interpreted as `nullable`,
  reversing the schema contract.
- Snowflake SQL identifier normalization uppercased JSON object keys, so a
  lower-case PostgreSQL key such as `id` was read as a missing value.
- PostgreSQL exact validation projected the BIGINT key as text and used an
  ambiguous `ORDER BY id`, producing lexical order (`1, 10, 11, ...`) instead
  of numeric order.
- A deliberately tiny four-second test lease was shorter than normal
  Snowflake control-plane latency; the test now uses the same realistic
  30-second scale as the sink default.

The first three were correctness defects and were fixed. The fourth was an
invalid test assumption. This is an important engineering result: a test that
only checks isolated helpers would not have proved the complete system. The
full test forced source catalogs, JSON representation, database ordering,
leases, Kafka recovery, and promotion to interact.

### Final gates

The current working tree passes:

- all unit tests;
- all race tests;
- build;
- static vetting;
- tagged integration compilation;
- formatting/diff checks;
- the full Snowflake recovery scenario described above.

---

## 20. The exact guarantees I claim

For the supported contract and while required PostgreSQL WAL and Kafka history
remain available:

1. **Complete transaction exposure:** a fragmented source transaction is not
   exposed or checkpointed before its final fragment exists.
2. **Atomic destination progress:** destination effects and the next transport
   frontier commit together.
3. **Replay-safe application:** a stable source transaction identity and
   content identity distinguish replay from new work.
4. **CDC wins snapshot races:** a key changed between LOW and HIGH cannot be
   overwritten or resurrected by that chunk's snapshot.
5. **Attempt isolation:** stale candidates and markers from an old attempt do
   not become current work.
6. **Worker fencing:** an expired chunk owner cannot finalize after takeover.
7. **Process fencing:** an expired reconciler, capture owner, or Snowflake sink
   cannot legitimately commit as the new owner.
8. **Manifest completeness:** execution uses a sealed gap-free logical range
   manifest through the sampled upper bound.
9. **Identity continuity:** replaced source clusters or Kafka topics are
   rejected.
10. **Exact promotion boundary:** promotion requires full stable validation
    plus bounded suffix validation at final cutover.
11. **Queryable online rebuild:** the old public generation remains queryable
    while the shadow is built.
12. **Idempotent retry:** ambiguous or interrupted promotion converges on one
    intended generation rather than toggling.

These guarantees depend on explicit assumptions:

- no unsupported DDL or operations;
- no arbitrary direct destination writers;
- one ordered Kafka partition;
- sufficient log retention;
- configured resource limits and credentials are valid;
- destination transaction semantics behave as required;
- the source table satisfies the schema contract.

---

## 21. Features that are implemented but easy to overlook

### Fail-closed configuration

Malformed integers, booleans, durations, missing IDs, identical promotion job
IDs, invalid table names, incomplete TLS credentials, and partial SASL
configuration produce startup errors. Silent defaults are dangerous when they
change durability or ownership behavior.

### TLS and authentication configuration

PostgreSQL TLS modes and Kafka TLS/SCRAM settings are configurable. A requested
but invalid secure configuration fails instead of silently downgrading.

### Canonical value transport

SEAM avoids generic floating JSON conversion for PostgreSQL values. It carries
canonical PostgreSQL text plus explicit type knowledge, with typed integers for
key operations. UTC session behavior removes timezone-dependent rendering.

### Schema epochs

The ordered schema descriptor includes column names, order, types,
nullability, key position, and replica identity. Its fingerprint travels with
jobs and envelopes. Schema drift is detected across capture, scanning, apply,
and recovery.

### Topic and source identity pinning

Names and offsets are not treated as sufficient identity. Source system ID,
topic ID, publication/table contract, and source configuration identity are
pinned with the job.

### Health and progress endpoints

The PostgreSQL reconciler can expose health, counters, and job progress over a
small optional HTTP endpoint. Counters include chunks completed, candidates
seen, survivors written, and CDC events applied.

This is basic visibility, not a full observability platform. There are no
finished freshness SLOs, distributed traces, warehouse-cost dashboards, or
automatic slot/disk alerts.

### Crash-releasing resource permits

Cross-process scan and destination-transaction permits are tied to PostgreSQL
sessions. Process death releases capacity without requiring a separate lock
service cleanup job.

### Retained old generation

PostgreSQL promotion renames rather than immediately deletes the old live
table. This helps inspection and manual recovery. Automatic rollback is not
claimed because rolling back while CDC continues requires its own consistency
protocol.

---

## 22. Major decisions and how I defend them

| Decision | Why I chose it | Cost or limitation |
| --- | --- | --- |
| One Kafka partition | Gives one unambiguous order for transactions and markers | Caps transport and consumer parallelism |
| At-least-once capture plus destination dedupe | Avoids pretending PostgreSQL and Kafka share a transaction | Duplicate transport records and ledger complexity |
| Source commit LSN as replay identity | Stable when Kafka offsets change after republish | Requires source-generation/system identity as context |
| Preserve source transaction boundaries | Prevents partial business transactions at the destination | Large transactions need spill and fragmentation machinery |
| LOW/HIGH source markers | Put snapshot boundaries in the same order as real changes | Marker latency and a dependency on the ordered stream |
| Durable candidates | Makes crash recovery explicit and inspectable | Destination write amplification |
| Sealed fixed manifest | Proves gap-free coverage and deterministic recovery | Less adaptive load balancing; memory scales with chunk count |
| Leases plus monotonic fencing tokens | Handles paused stale processes, not only clean crashes | Heartbeats, durable metadata, and expiry tuning |
| Destination-owned frontier | Couples progress to actual applied data | Separate state and logic for each destination |
| Closed schema/type contract | Makes lossless behavior defensible | Narrow connector breadth |
| Fail closed on history/identity mismatch | Prevents silent divergence | Some incidents require a new snapshot |
| Two-stage validation/cutover | Keeps full comparison outside the long write pause | More markers, state, and promotion complexity |
| PostgreSQL rename promotion | Uses transactional DDL native to PostgreSQL | Brief exclusive lock and narrow supported dependencies |
| Snowflake view promotion | Uses an atomic stable pointer appropriate to the warehouse | Requires durable intent and route convergence around DDL |
| Key clocks and tombstones in Snowflake | Prevent stale candidates resurrecting deleted rows | Per-key metadata growth and merge work |
| Resource admission separate from worker count | Protects shared infrastructure across live/shadow processes | Requires consistent configuration and does not guarantee fairness |
| Honest component benchmarks | Identifies actual bottlenecks without inflated end-to-end claims | Results look smaller but are defensible |

---

## 23. What I would say when challenged

### “Is SEAM exactly once?”

> I avoid using exactly once without qualification. PostgreSQL-to-Kafka
> capture is at least once because there is no distributed transaction. SEAM
> preserves a stable source commit identity, applies a complete source
> transaction atomically with the destination frontier, and deduplicates
> replay. That produces effectively once effects for the supported operations.

### “Why not commit the Kafka consumer offset normally?”

> A broker-side consumer offset can advance independently of the destination
> transaction. I need the authoritative frontier in the destination so data
> effects and progress share one commit decision. Kafka remains the transport;
> destination state defines what is actually materialized.

### “Why do you need both offset and LSN?”

> Offset orders one Kafka topic incarnation. LSN identifies the source
> transaction across republish. The same source transaction can appear at a
> new offset after capture recovery, while a recreated topic can reuse old
> offset numbers. They answer different questions.

### “What stops a stale worker after a network pause?”

> Time-based expiry alone is insufficient because the old process can wake up.
> Every takeover advances a durable token or epoch, and the final mutation
> proves that token inside its transaction. The stale owner cannot finalize
> after a successor exists.

### “Why not copy the table and replay CDC afterward?”

> That can work only with an exact starting boundary, sufficient retained
> history, and a correct way to make CDC win every collision. SEAM expresses
> that relationship per chunk with LOW/HIGH markers and durable key conflict
> handling while CDC continues.

### “Why can't the snapshot just upsert?”

> Upsert is not version-aware. A stale snapshot upsert can overwrite a newer
> value, and it can recreate a row that CDC deleted. Idempotence does not imply
> correct ordering.

### “Why one partition?”

> It makes transaction and marker ordering defensible for the first version.
> Multiple partitions require a partitioning rule, per-key ordering, multi-key
> transaction handling, barrier coordination, and a promotion frontier vector.
> Adding partitions before defining those invariants would trade correctness
> for a benchmark number.

### “Why isn't four workers four times faster?”

> Workers share source storage, database CPU, connections, Kafka, destination
> writes, and serial coordination. Once one resource saturates, more workers
> create contention. The current exact-verified smoke results are about 2×,
> which is evidence of useful but sublinear parallelism.

### “Can this reach Artie's public backfill numbers?”

> I would not promise a multiplier without comparable workload and
> infrastructure. SEAM now has a lease-fenced bulk snapshot path using
> deterministic compressed files, `PUT`, and transactional `COPY INTO`, but it
> still needs controlled experiments that record row width, bytes, source
> load, warehouse size, merge cost, credits, lag, and recovery. Implementing a
> faster mechanism is not evidence of an Artie-scale result by itself.

### “Why not build more connectors?”

> A connector is more than changing output format. It needs type mappings,
> transaction and retry semantics, atomic progress, bulk-load behavior,
> tombstones, validation, and a destination-specific promotion primitive. I
> chose PostgreSQL and Snowflake to show two materially different destination
> designs before adding breadth.

### “What is the strongest part of the project?”

> The strongest part is not feature count. It is the chain from stated
> invariant to failure mechanism to durable protocol to executed failure test:
> transaction framing, LOW/HIGH reconciliation, process and worker fencing,
> exact-prefix validation, and destination-specific idempotent promotion.

### “What is the weakest part?”

> Scale evidence and operational maturity. Kafka is single-partition, the
> bundled broker is not HA, Snowflake staging is slow, metadata migrations are
> additive rather than a mature versioned framework, and there is no sustained
> large workload with production traffic. I intentionally stop the claims at
> the evidence boundary.

---

## 24. Limitations I should state without being asked

1. **One partition.** Ordering is simple; Kafka throughput and consumer
   parallelism are capped.
2. **One table per pipeline.** There is no multi-table foreign-key or
   cross-table transaction product contract.
3. **One BIGINT key.** Composite keys and other key types need new chunk and
   ordering designs.
4. **No schema evolution.** Drift is detected and rejected.
5. **No `TRUNCATE` or primary-key updates.** They fail closed.
6. **PostgreSQL promotion is narrow.** The current public rename workflow is
   specifically the `accounts`/`accounts_shadow` example and rejects complex
   dependencies.
7. **Snowflake staging is not warehouse-scale.** It uses parameterized staged
   rows rather than file-based bulk load.
8. **Broker fixture is not HA.** Replication factor one cannot support a
   production durability claim.
9. **Retention is operationally configured.** Insufficient WAL or Kafka
   history forces a resnapshot.
10. **No arbitrary destination writers.** External writes are outside the
    replication history and can invalidate equality.
11. **Basic observability only.** There is no complete alerting/SLO platform.
12. **Manifest scale is unproven.** Very large numbers of tiny chunks can grow
    coordinator metadata memory.
13. **Benchmarks are small and environment-specific.** There is no evidence at
    Artie's largest public workloads.
14. **Metadata migrations are immature.** There is no full numbered migration
    and compatibility framework.
15. **Deployment is unfinished.** Secrets, multi-AZ operation, backup/restore,
    rolling upgrades, and cost controls are not a productized system.

These limits make the project more credible when stated clearly. Hiding them
would make the stronger correctness work harder to trust.

---

## 25. The story of the project as my engineering decisions

Use this narrative in a technical conversation:

> I started with the observation that a CDC demo and an online replication
> system are different. Reading WAL and inserting rows proves connectivity, but
> it does not solve snapshot races, transaction atomicity, replay, or cutover.
>
> I first made source transaction identity explicit. Capture preserves complete
> PostgreSQL commits, bounds large transactions with disk spill and Kafka
> fragments, and only acknowledges WAL after broker acknowledgement. Because
> this is at least once, destinations own a durable apply ledger and frontier.
>
> I then attacked the hardest snapshot race directly. Every chunk scan is
> bracketed by source LOW/HIGH markers that share the CDC order. Rows are
> candidates until HIGH, and any key changed in the window is excluded. This
> prevents stale updates and delete resurrection.
>
> To survive concurrency and crashes, I made discovery, candidates, chunks,
> checkpoints, attempts, process epochs, and worker tokens durable. Recovery
> never infers missing history from the current table. It either proves
> continuity or requires a new snapshot.
>
> For online rebuilding, I separated the public generation from the shadow and
> kept CDC flowing to both. I made validation an exact-prefix problem rather
> than a row-count check. A stable MVCC comparison handles the full table while
> writes continue, then a bounded changed-key comparison closes the final
> suffix under a short fence.
>
> I implemented destination-specific cutover. PostgreSQL uses a transactional
> rename. Snowflake uses a stable view, zero-copy validation clone, durable
> promotion intent, dual routes, key clocks, and tombstones. The shared idea is
> an exact validated generation and idempotent promotion; the database
> primitive is allowed to differ.
>
> Finally, I measured instead of copying vendor claims. PostgreSQL gained about
> 2× from four workers in the latest exact-verified smoke runs. Snowflake row
> staging plateaued around 188 rows/s in the original component measurement.
> I replaced that path with deterministic bulk files plus `PUT` and
> transactional `COPY INTO`, while retaining the SQL path as a benchmark
> control. The post-change live comparison is still unmeasured, so I do not
> turn the mechanism into a throughput claim.

---

## 26. A mental model for every persistent object

You should be able to explain persistent state without remembering table
definitions.

### Source PostgreSQL

- **Application table:** authoritative rows.
- **Marker table:** control events ordered with application changes.
- **Logical slot:** source's retained CDC position.
- **Capture ownership:** who may publish and acknowledge that slot.

### Kafka

- **Topic identity:** which history this is.
- **Partition 0 offsets:** transport order.
- **Transaction envelopes/fragments:** complete source transaction framing.

### PostgreSQL destination

- **Job:** immutable replication/backfill identity.
- **Checkpoint:** current process term, scan prefix, and Kafka frontier.
- **Applied transaction ledger:** replay identity.
- **Chunks:** durable work manifest and per-range leases.
- **Candidates:** historical rows awaiting window reconciliation.
- **Route fence:** coordination with promotion.
- **Cutover gates:** exact common boundary for live and shadow.
- **Promotion record:** idempotent final result.

### Snowflake destination

- **Offset:** authoritative next Kafka record.
- **Sink lease:** who owns that frontier.
- **Applied ledger:** source identity and fingerprint.
- **Routes:** which physical generations receive CDC.
- **Key clocks:** latest source version/tombstone for each key and route.
- **CDC stage:** bounded set for one transaction.
- **Markers:** observed control boundaries.
- **Backfill job/chunks:** durable state machine and workers.
- **Backfill files:** content hash, file size, stage path, lease token, and load
  state for each bulk snapshot file.
- **Snapshot stage:** disposable candidates.
- **Snapshot file stage:** uploaded compressed chunk files retained for retry.
- **Public view:** stable consumer-facing pointer.

---

## 27. What the finished hiring project demonstrates

At its current stopping point, SEAM demonstrates that I can reason about:

- PostgreSQL WAL and logical decoding;
- source transaction framing;
- at-least-once delivery and idempotent effects;
- transport offsets versus source identities;
- online snapshot/CDC races;
- delete tombstones;
- durable state machines;
- process leadership and fencing epochs;
- chunk leases and recovery attempts;
- bounded memory and backpressure;
- cross-process resource control;
- exact-prefix validation;
- MVCC snapshots;
- destination-specific atomic cutover;
- ambiguous commits and idempotent retry;
- honest benchmarks and bottleneck isolation;
- destructive integration and chaos testing;
- the difference between a proved invariant and an unmeasured aspiration.

It does not need to become a complete Artie competitor to be valuable. Its
hiring value comes from the depth and evidence behind a smaller set of hard
problems.

---

## 28. Study order for actually owning the system

### Pass 1: explain the pipeline

Be able to draw:

```text
PostgreSQL WAL -> capture -> Kafka -> destination sink
PostgreSQL table -> chunk scan -> candidates -> reconciliation
live generation + shadow generation -> validation -> promotion
```

### Pass 2: explain three races

Without notes, explain:

1. stale snapshot update;
2. delete resurrection;
3. stale worker waking after lease takeover.

For each, state the invariant and mechanism.

### Pass 3: trace one transaction

Trace one PostgreSQL transaction through commit, logical decoding, Kafka
fragmentation, consumer assembly, destination mutation, ledger, and frontier.
Explain every crash boundary.

### Pass 4: trace one backfill chunk

Trace lease, LOW, scan, candidates, HIGH, CDC collision, survivor merge, and
completion. Then repeat with a crash after each step.

### Pass 5: explain both promotions

Explain what PostgreSQL and Snowflake share:

- live destination stays queryable;
- CDC reaches shadow;
- stable full validation;
- bounded suffix validation;
- durable intent/result;
- idempotent retry.

Then explain why rename and view replacement differ.

### Pass 6: defend performance honestly

Know the executed numbers, what their timers include, and what they exclude.
Be ready to explain why the Snowflake bottleneck points to bulk loading and why
more workers alone will not fix it.

### Pass 7: state the limits first

Practice saying one partition, one table, BIGINT key, no schema evolution,
small benchmarks, and row-staged Snowflake without sounding apologetic. Each
limit protected time for a deeper invariant.

---

## 29. Final ownership checklist

You own the system when you can answer all of these without reading the code:

- Why is SEAM both CDC and backfill?
- Why can a correct CDC stream still produce a wrong destination during a
  naive snapshot?
- Why do LOW and HIGH need to be source transactions?
- Why is a delete tombstone needed in Snowflake?
- Why are Kafka offset and PostgreSQL LSN different identities?
- Why is capture at least once?
- Why are fragmented transactions still atomic at the destination?
- Why does the destination own its frontier?
- Why are a process epoch and a chunk token both necessary?
- What proves the chunk manifest has no logical holes?
- What happens if Kafka history is gone?
- What happens if the topic is recreated with the same name?
- What happens if the source cluster is replaced behind the same DSN?
- Why does full validation use an MVCC snapshot?
- Why is a second changed-key validation required?
- Why is PostgreSQL promotion a rename while Snowflake promotion is a view
  replacement?
- What makes promotion retry idempotent?
- What is currently bounded in memory, and what scale risk remains?
- What causes backpressure, and where does backlog accumulate?
- Why did four workers produce about 2× rather than 4× speedup?
- Why is current Snowflake staging the immediate bottleneck?
- Which current claims are backed by live tests?
- Which production concerns are deliberately unfinished?

When those answers are natural, the next report can explain how the code
implements each mechanism, package by package and function by function.

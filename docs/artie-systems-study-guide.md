# Engineering a high-throughput CDC and online-backfill pipeline

This is a performance-engineering guide for understanding the machinery that could sit underneath a system such as Artie. It assumes the reader already knows that a pipeline can be bottlenecked. The subject here is how engineers remove each bottleneck, how the bytes move differently after each change, how pressure propagates through the whole pipeline, and how to prove that an optimization worked without weakening correctness.

The guide deliberately follows one continuous data path:

```text
source storage
  -> PostgreSQL pages and caches
  -> PostgreSQL executor, index, and heap access
  -> balanced backfill scheduler and scanner connections
  -> application receive buffers
  -> decoding, conversion, and serialization
  -> bounded batch queues
  -> Kafka producer and network
  -> Kafka broker, partitions, page cache, and disk
  -> consumer and bounded in-memory batches
  -> destination bulk ingestion and staging
  -> set-based finalization
  -> destination WAL, indexes, and durable storage
```

Every faster stage pushes pressure into the next one. If storage delivers rows twice as fast, PostgreSQL CPU, the client protocol, or the application may become limiting. If the application becomes efficient, Kafka or the network may become limiting. If the stream moves quickly, the destination often becomes the wall. The sustainable pipeline rate is approximately the capacity of its slowest required stage, not the sum or average of stage capacities.

## Evidence labels

Any statement specifically about Artie uses one of these labels:

- **PUBLICLY KNOWN** — stated in Artie's public documentation, product material, API, or engineering writing.
- **PLAUSIBLE IMPLEMENTATION** — a defensible design that could implement the public behavior, but is not claimed to be Artie's private implementation.
- **GENERAL HIGH-PERFORMANCE TECHNIQUE** — an established technique that applies to this class of system.
- **ALTERNATIVE DESIGN** — a different valid architecture with different tradeoffs.

Artie's current private implementation is unavailable. This guide never fills that gap with invented details.

## 1. The capacity model that connects the pipeline

Use bytes as the common currency even when product metrics display rows. A million 80-byte rows and a million 200-KiB rows are different workloads. At each stage measure both logical work and physical work:

```text
rows/s, events/s, distinct keys/s, transactions/s
bytes/s before and after compression
requests/s and rows/request
service time and queue-wait time
CPU-seconds, I/O operations, and allocated bytes per million rows
```

For a steady pipeline with input rate `lambda` and service rate `mu`:

```text
queue growth per second = lambda - mu       when lambda > mu
spare catch-up capacity = mu - lambda       when mu > lambda
catch-up time ~= backlog / (mu - lambda)
```

If the source produces 500,000 rows/s and the destination durably applies 100,000 rows/s, Kafka stores about 400,000 rows/s. It changes where the backlog lives; it does not change the final service rate. If the destination later sustains 600,000 rows/s, the pipeline can drain the backlog at roughly 100,000 rows/s while accepting the continuing 500,000 rows/s.

Latency and throughput must be separated. A 10-ms request has a serial ceiling of 100 requests/s, but a request holding 10,000 rows yields a theoretical one-million-row/s request path. Several asynchronous requests can overlap the 10-ms wait. That is why batching, pipelining, and bounded concurrency convert latency into throughput. They stop helping when some shared resource becomes saturated or when batches no longer grow.

Near saturation, small changes cause large queueing delays. A destination that can complete 100,000 rows/s may look healthy at 70,000, unstable at 99,000 during normal variance, and mathematically unable to keep up at 101,000. Production capacity therefore needs headroom. The correct target is the highest rate that keeps queues, p95/p99 latency, memory, error rate, and recovery time bounded under expected bursts—not the largest number observed during a short run.

## 2. Source storage: turn random waiting into parallel sequential delivery

The first physical limit is how quickly PostgreSQL can obtain heap and index pages. The useful diagnosis is more precise than “disk is slow.”

### Bandwidth, IOPS, and latency are different ceilings

| Ceiling | Physical signature | Typical cause | Useful response |
| --- | --- | --- | --- |
| bandwidth | MB/s approaches device or volume limit; requests are already large; queueing grows | large sequential reads exhaust the storage channel | faster volume/NVMe, more striped devices, replica, reduce bytes |
| IOPS | operations/s approaches limit while MB/s is modest; requests are small/random | index-to-heap random reads or provisioned-IOPS cap | restore locality, larger/sequential scans, provision more IOPS |
| latency/insufficient queue depth | device is not at bandwidth or IOPS limit, but each worker waits on `await`; few I/Os are outstanding | serial query pattern, remote block storage latency, low prefetch/concurrency | prefetch, controlled parallel scans, larger reads, more in-flight I/O |

Use operating-system device metrics such as read bytes/s, reads/s, average request size, `await`, queue depth, and utilization together. PostgreSQL adds `pg_stat_io`, `pg_stat_database`, and `EXPLAIN (ANALYZE, BUFFERS, SERIALIZE)` evidence. `shared read` blocks with high read time indicate physical work; `shared hit` means PostgreSQL found the page in its buffer cache, though the OS page cache can still make a PostgreSQL “read” avoid the device.

### Make the access pattern sequential

**GENERAL HIGH-PERFORMANCE TECHNIQUE.** A sequential scan asks for neighboring heap blocks. The kernel and storage device can merge and read ahead, so a request transfers many useful bytes. An index range scan first walks B-tree pages and then visits heap locations. If heap tuples are physically scattered, the access stream becomes many small reads. Sequential scanning can therefore beat an apparently selective index plan when the job needs a large fraction of the table.

The data flow changes from:

```text
B-tree leaf -> heap block 91 -> B-tree leaf -> heap block 8002 -> ...
```

to:

```text
heap blocks 0, 1, 2, 3 ... -> large contiguous client batches
```

This reduces seeks, B-tree work, and storage commands per byte. It stops helping when the device's sequential bandwidth is full, PostgreSQL CPU cannot inspect tuples as fast as they arrive, or the client/network cannot drain query output. It can also read dead tuples and unneeded columns, so it is not automatically best for a small selective range.

PostgreSQL can issue asynchronous prefetch based on `effective_io_concurrency`; the useful value depends on the storage. Higher concurrency can hide latency on capable SSD or network storage, but excessive concurrency increases latency for production queries. Hardware remedies—NVMe, higher provisioned IOPS/throughput, striped storage or RAID where operationally appropriate—raise the physical ceiling. They do not repair a random access pattern that wastes most operations.

Multiple scan workers help only while the device or replica has unused queue depth and bandwidth. With one latency-bound scan, four workers may keep the device busy. Once bandwidth is full, sixteen workers merely divide the same bytes/s while adding PostgreSQL executor work and contention.

### Move bulk work away from the primary

**PUBLICLY KNOWN.** Artie's public online-backfill material says historical reads can use a read replica so the primary continues handling production work.

**GENERAL HIGH-PERFORMANCE TECHNIQUE.** A replica isolates backfill I/O, cache pollution, query CPU, and connections. It does not create free capacity: the replica still has finite storage and CPU, and replay lag defines how recent its snapshot is. Several replicas can be assigned disjoint tables or chunks if the system has a consistency model that tolerates their different replay positions. Spreading one logical snapshot over replicas without coordinating snapshot time can produce a dataset whose chunks represent different source moments; a live-change reconciliation layer must close that gap.

Read isolation is often more valuable than raw speed. A backfill that can drive the primary at 2 GB/s but destroys customer p99 latency is a failed optimization. Source-side admission should enforce maximum active scans, maximum I/O or CPU pressure, and optionally a token-bucket byte rate. When production latency rises, the scanner yields even if its own throughput falls.

### What proves the storage fix worked

Record before and after:

- rows/s and uncompressed bytes/s returned by the query;
- device MB/s, IOPS, average request size, `await`, and queue depth;
- `pg_stat_io` reads, read time, evictions, and bulk-read context;
- `EXPLAIN (ANALYZE, BUFFERS, SERIALIZE)` plan, heap/index blocks, and output serialization time;
- primary/replica CPU and foreground-query p95/p99 latency;
- rows and bytes per physical block read.

If rows/s rises, device bandwidth rises, and customer latency stays within budget, parallelism used idle storage capacity. If workers rise while aggregate bytes/s stays flat and latency worsens, storage is saturated. Once storage is no longer limiting, expect PostgreSQL CPU, tuple-to-client encoding, or network throughput to become visible.

## 3. Caches: protect the production working set while streaming cold data

Rows may be served from PostgreSQL `shared_buffers`, the OS page cache, or storage. More RAM helps only if useful pages are reused before eviction. A one-pass scan of a table much larger than memory has almost no reuse; after the scan has filled memory with old blocks, early blocks have already been evicted. RAM cannot turn a streaming 10-TB read into a cached workload on a 128-GB host.

PostgreSQL also uses the OS cache, so increasing `shared_buffers` until it consumes nearly all RAM can reduce the cache available to the kernel and other processes. PostgreSQL's documentation describes a moderate fraction of system memory as a starting point and notes diminishing value at high fractions. Large operations can use limited buffer-access rings to reduce shared-buffer churn, but the scan still consumes I/O bandwidth and can affect the OS cache.

### Engineering choices

1. **Use a read replica.** This gives the backfill a separate PostgreSQL and OS cache, the cleanest isolation.
2. **Throttle or schedule scans.** A bounded byte rate and lower concurrency preserve I/O and CPU for foreground queries. Run aggressive work during low traffic only if the completion-time requirement permits it.
3. **Prefer forward, non-overlapping scans.** Re-reading overlapping ranges defeats cache and storage locality. Keep each worker on a contiguous run where possible.
4. **Avoid prewarming a one-pass dataset blindly.** `pg_prewarm` can move pages into the OS or PostgreSQL cache, but loading more than fits evicts earlier pages and may evict production data. Prewarm only data that will be reused enough to pay back the extra read.
5. **Measure cache impact.** Track foreground buffer hit ratio cautiously, relation residency with `pg_buffercache` when warranted, major faults, OS cache, and production query latency. A high hit ratio can hide the fact that the few misses are expensive.

Warm-cache and cold-cache benchmarks answer different questions. A warm run measures executor, protocol, and application capacity when data is resident. A controlled cold run measures storage. Report both, randomize or state run order, and do not call the second through fifth execution independent cold measurements. On shared infrastructure, dropping OS caches is disruptive and often prohibited; use a fresh replica/volume or a dataset larger than cache instead.

When cache misses disappear but throughput stays flat, RAM was not the current limit. Inspect executor CPU, client encoding, network, and downstream queue blocking.

## 4. Index and heap access: choose a plan for the fraction of the table being read

For a range query such as:

```sql
SELECT id, c1, c2
FROM source_table
WHERE id >= $1 AND id < $2
ORDER BY id;
```

an ordinary B-tree range scan descends from root to leaf, walks matching index entries, follows each tuple identifier to a heap page, and checks MVCC visibility. The index determines candidates; the heap normally determines visibility and supplies non-indexed columns. If keys and heap rows are physically aligned, neighboring index entries reuse heap pages. If updates and insert patterns have scattered rows, the scan can bounce across the heap.

### When an index helps

An index is useful when a chunk selects a small fraction of the table, the query needs ordered keyset pagination, or physical correlation between key and heap is high. It avoids reading unrelated heap pages and gives a restartable logical boundary. Chunk queries should use keyset predicates, not `LIMIT/OFFSET`: offset pagination repeatedly walks and discards earlier rows, so later pages become increasingly expensive and concurrent changes make page membership awkward.

Larger logical chunks reduce repeated B-tree descents and query/RPC overhead. The improvement stops when each query is already dominated by transferring rows or when a large chunk causes long transactions, timeouts, memory spikes, or slow recovery. Prepared statements can reduce parse/plan overhead, although planning is rarely the dominant cost of a multi-gigabyte scan.

### When an index makes the scan worse

If the backfill needs most rows, a B-tree plus random heap fetch may perform more I/O and CPU than one sequential pass. `ORDER BY id` can force the system to preserve logical order even when physical order would be cheaper. Compare real plans and buffers; do not force index usage because the predicate names a primary key.

An index-only scan can avoid most heap visits only when every required output column is available in the index and the corresponding heap pages are marked all-visible in the visibility map. Frequently updated tables clear those bits, so the executor must visit the heap anyway. A wide covering index also increases storage and foreground write amplification. Creating one solely to accelerate a one-time backfill can cost more than it saves.

`CLUSTER` physically rewrites a table in index order, making later key ranges more local. It is a heavy, one-time operation, requires operational planning and extra space, and future changes erode the ordering. Rewriting a busy source merely to speed a backfill is usually a poor default; it can make sense on a maintained replica or a table already clustered for production reasons.

Table partitions are an excellent natural unit when each partition can be read independently. Partition pruning removes irrelevant data, scheduling whole or subdivided partitions preserves locality, and old partitions may be nearly immutable and all-visible. The scheduler must still split an oversized partition and must not assume equal partition sizes.

The proof is a plan comparison over representative cold and warm ranges: heap blocks read, index blocks read, heap fetches for index-only scans, physical bytes, executor CPU, serialized bytes, query p95, and rows/s. The best plan may differ for a small tail chunk and a full historical partition.

## 5. Balanced chunks: schedule estimated bytes, then correct estimates at runtime

Equal primary-key width does not mean equal work. Keys may be sparse, tenants may own radically different row counts, and row width may vary by range. Static ranges can leave three workers idle while one holds the final 60-GB chunk. Parallelism then collapses to the slowest assignment.

### Boundary strategies

**Sampled key boundaries.** Sample ordered keys and select quantiles so each chunk has a similar estimated row count. This is inexpensive and robust enough for many tables, but a small sample can miss dense pockets and says nothing about width variation.

**Planner statistics and histograms.** PostgreSQL column statistics can estimate key distribution. They are cheap to read, but histograms have bounded resolution, may be stale, and normally model values rather than total row bytes. Treat them as seed estimates, not proof.

**Row-count-balanced ranges.** Compute or sample boundaries such that each range has approximately `N` rows. This balances executor tuple work when row widths are similar.

**Byte-balanced ranges.** Estimate `sum(row_bytes)` rather than rows. A practical sampler records primary key plus `pg_column_size(row)` or selected-column sizes, groups sample observations by key interval, and extrapolates bytes using estimated density. Historical destination or prior-chunk telemetry can refine `bytes/key interval`. Exact `COUNT`/size scans would duplicate much of the backfill, so production schedulers accept estimates and adapt.

**Partition-aware ranges.** Start from table partitions or date/tenant boundaries, then split large units by sampled bytes. This preserves locality and lets policy prioritize cold partitions.

### Dynamic scheduling and work stealing

Put many more chunks than workers in a durable queue. Workers lease the next eligible chunk rather than owning one large static quarter of the keyspace. A fast worker therefore consumes more chunks. The tail cost is at most roughly the largest remaining chunk rather than the largest original quarter.

Smaller chunks improve balance, retry cost, and observability, but increase query setup, B-tree traversal, checkpoint writes, and destination batch fragmentation. Choose a target duration or byte size—often enough work to amortize fixed cost but short enough to retry—and adapt it from completed chunks.

A slow pending chunk can be split before it starts. Splitting an active chunk is harder: the coordinator must atomically replace the unscanned suffix with child chunks while preserving non-overlap and coverage. The worker needs a progress boundary it can publish safely. For a key scan, a last-emitted key can define the completed prefix under the chosen snapshot. For a physical scan, a block boundary can do so. Killing and restarting the full chunk is simpler but wastes work.

A sophisticated scheduler maintains:

```text
chunk identity and immutable bounds
estimated rows and bytes
lease owner, fencing token, and expiry
snapshot/epoch identity
last safe split boundary
actual rows, bytes, source time, and downstream time
retry count and terminal state
```

It uses work stealing for utilization, but a global admission controller still limits simultaneous source queries, bytes in flight, and destination loaders. Work stealing should not bypass safety limits.

### What proves balancing worked

Measure coefficient of variation or p95/p50 of chunk duration and bytes, worker idle percentage, queue depth, straggler tail after most bytes finish, estimate error, retries, and total wall time. If median worker utilization rises and the final tail shrinks without more source pain, scheduling helped. If all workers remain busy but aggregate source bytes/s is flat, the next ceiling is shared storage, CPU, network, or destination capacity.

## 6. CTID and physical-page scanning: use location for traversal, never identity

A PostgreSQL CTID is an item pointer: `(heap block number, item offset within that block)`. Dividing by block ranges can make workers traverse physical regions:

```text
worker A -> blocks     0.. 9,999
worker B -> blocks 10,000..19,999
worker C -> blocks 20,000..29,999
```

This can produce sequential access, predictable physical bytes, and balanced initial assignments when blocks have similar live-row density. It avoids the key-distribution problem and works when there is no integer key suitable for range division.

**PUBLICLY KNOWN.** Artie publicly documents a PostgreSQL CTID backfill option, configurable shard size, maximum parallelism, and process CPU limit. Its public material also warns that CTIDs can change and recommends the strategy for very large tables with a read replica. That public behavior is the boundary of what can be claimed.

### Why CTID is unstable

PostgreSQL updates create a new tuple version rather than overwriting the old logical row. A HOT update may place the new version on the same page when indexed columns are unchanged and space permits, but it is still another tuple version. Other updates can land on a different block. Vacuum removes dead versions; operations that rewrite a relation can replace physical layout entirely. A CTID observed today is therefore not a durable row identifier.

If workers execute independent statements under ordinary `READ COMMITTED`, this race is possible:

1. the block-0 worker finishes block 9;
2. a row originally in future block 15 is updated and its visible version lands in already-scanned block 8;
3. the block-15 worker sees the old version as invisible;
4. nobody reads the new version from block 8.

The inverse can duplicate a logical row when versions move into a future range. Vacuum and relation rewrites can invalidate boundaries or make restart behavior differ from the first attempt.

### Safe design strategies

**GENERAL HIGH-PERFORMANCE TECHNIQUE — one exported MVCC snapshot.** A coordinator opens a repeatable-read transaction, exports its snapshot, and every scanner imports it before querying. All workers see the same logical database moment even if physical versions later change. They must start their transactions while the exporting transaction remains valid. Long-lived snapshots retain dead tuples and can increase vacuum pressure and storage; they also require careful connection and crash handling. A relation rewrite or operational event may still require aborting the run.

**GENERAL HIGH-PERFORMANCE TECHNIQUE — snapshot plus ordered live-change reconciliation.** Record a change-stream boundary associated with the snapshot, scan physical ranges for that snapshot, then apply changes after the boundary by stable logical key. CTID locates scan work; the primary/unique key identifies destination rows. Updates and deletes that occur after the snapshot are corrected by the live path. Correct boundary construction and retention are mandatory.

**GENERAL HIGH-PERFORMANCE TECHNIQUE — durable page coverage.** Persist relation identity, snapshot identity, block interval, attempt/fence token, and completion. On restart, reuse the same valid snapshot only if the database/session mechanism permits it; otherwise start a new generation and reconcile or discard old partial output. Never silently resume CTID ranges under a different snapshot as if they represented one point in time.

**ALTERNATIVE DESIGN — conservative overlap/rescan.** Overlap neighboring page ranges or rescan changed logical keys and deduplicate by stable key. This reduces boundary risk but does not by itself establish snapshot correctness. A final stable-key reconciliation is still needed if concurrent changes can move visible versions.

### Strategy comparison

| Strategy | Best use | Parallel balance | Source access | Correctness anchor | Main cost |
| --- | --- | --- | --- | --- | --- |
| PK ranges | stable integer/orderable key, moderate-large table | poor if distribution/width skew is ignored | index plus heap, good if correlated | logical key bounds and snapshot/change boundary | random heap access and skew |
| byte-balanced PK ranges | same, with sampling/telemetry | good after estimates converge | index plus heap | logical key bounds | estimator and scheduler complexity |
| CTID/page ranges | extremely large PostgreSQL heap, physical scan desirable | usually good by blocks, imperfect with dead/wide tuples | sequential physical locality | shared snapshot plus logical-key reconciliation | CTID instability and operational constraints |
| native parallel sequential scan | one query can use PostgreSQL parallel workers | PostgreSQL assigns blocks dynamically | sequential heap | one query snapshot | less application control; source workers/leader overhead |
| table partitions | naturally partitioned tables | good if partitions are subdivided when needed | partition-local | snapshot plus partition manifest | skewed partitions and schema/DDL lifecycle |

Choose based on measured access cost and the correctness boundary. PK ranges are easier to resume and explain. Physical ranges can win when index-to-heap access is expensive. Native parallel scan is attractive when PostgreSQL can schedule blocks well and the client can receive one stream, though a single connection/protocol stream may become the next limit. Partitions are best when they already match physical and operational boundaries.

## 7. PostgreSQL CPU, connections, and worker parallelism

Once pages arrive quickly, PostgreSQL must walk plan nodes, check tuple visibility, deform tuples, evaluate predicates, copy selected columns, and encode protocol output. Backfill CPU should be spent returning required bytes, not computing values that can be handled downstream.

### Reduce source CPU per row

- Select only required columns. This reduces tuple deformation, protocol bytes, network, application decode work, and all downstream storage.
- Use simple range/block predicates and avoid expensive functions, casts, joins, sorts, or source-side JSON construction.
- Use a sequential plan for a large fraction when index traversal and heap probes cost more CPU.
- Increase chunk size until parse/plan/startup and B-tree descent are amortized, then stop before timeout, memory, and retry cost become unacceptable.
- Stream results with PostgreSQL `COPY TO STDOUT` or another efficient protocol path where semantics permit. Binary format avoids text formatting/parsing for many types, but is less portable and still requires correct type handling.
- Let PostgreSQL parallel query split block work when its plan is actually parallel-aware; do not assume setting a worker parameter forces useful parallelism.
- Move optional transformation off the source. A saturated production CPU is more valuable than spare application CPU.

These changes transform the source flow from many short planned queries and text values into fewer long-lived streams of selected binary values. The next limits are client drain rate, application CPU, and network.

### Connections are admission slots, not throughput multipliers

One active scanner commonly needs one connection because a query/transaction and imported snapshot are connection-scoped. A bounded pool reuses authenticated sessions and caps source pressure. Sharing one connection between concurrent scans only serializes protocol work or complicates cancellation, while opening hundreds of connections creates backend processes, memory use, scheduling, locks, and I/O competition.

Use:

- one leased connection per active scan;
- a small separate control pool for manifests, snapshots, and health;
- semaphores for active queries even if the pool contains more idle connections;
- long-lived connections and prepared statements where they remove measured setup cost;
- per-replica concurrency limits;
- connection acquisition timeouts and queue metrics;
- adaptive admission based on source p95 latency and utilization.

Connections stop helping when PostgreSQL CPU, storage, locks, the client stream, or downstream admission is full. `max_connections` is a safety ceiling, not a performance target.

### Why the worker curve rises, flattens, then falls

This result is normal:

```text
 1 worker  =  30k rows/s
 4 workers = 110k rows/s
 8 workers = 150k rows/s
16 workers = 153k rows/s
32 workers = 130k rows/s
```

One worker leaves storage queue depth, CPU cores, network, or destination slots idle. Four overlap waits and use more cores. By eight, a shared resource approaches saturation. Sixteen add scheduling and contention for almost no completed work. Thirty-two add cache churn, connection/backend overhead, queueing, lock pressure, larger memory working sets, and possibly more random I/O, so useful throughput falls.

Goroutines are cheap scheduling units, not free database capacity. Separate the number of queued chunks from the number of active source scans, CPU-transform workers, Kafka in-flight requests, and destination loaders. Each stage gets its own bounded semaphore because its optimal concurrency differs.

### Adaptive worker admission

A practical controller changes concurrency slowly, using a stable interval longer than normal query jitter:

1. increase one step while source p95, production p99, queue memory, and downstream queues are below limits and completed bytes/s improves materially;
2. hold when the gain is within noise;
3. decrease quickly on production-latency, error, memory, or queue pressure;
4. use hysteresis and cooldown so the system does not oscillate;
5. keep hard minimum/maximum limits and an operator override.

Destination latency and Kafka lag matter even though the controller governs source scans: faster scans are harmful if they only fill a downstream queue. Metrics proving a good worker count include completed source and end-to-end bytes/s, CPU by core, runnable processes, I/O queue depth, connection wait, query p95/p99, foreground p99, worker idle time, queue bytes, and retry/error rate.

## 8. Bounded RAM queues and backpressure: make overload travel upstream

A queue decouples short scheduling differences; it does not repair a permanent capacity deficit. At 100,000 produced rows/s and 80,000 consumed rows/s, an unbounded queue grows by 20,000 rows/s. If an encoded row occupies 1 KiB plus object overhead, the lower-bound growth is about 20 MiB/s—more than 70 GiB/hour before duplication and GC overhead.

### Queue forms and their physical behavior

**Bounded channel.** Simple ownership transfer. When full, producer send blocks, propagating pressure. Per-element channels can add synchronization overhead at extreme rates, so pass batches rather than individual rows.

**Ring buffer.** Preallocated slots avoid repeated allocation and can offer predictable memory/cache behavior. Correct multi-producer/multi-consumer implementations are harder; a ring still needs a full policy and cannot overwrite uncommitted data.

**Batch queue.** Each item owns a contiguous batch plus byte count. It reduces locks and scheduler operations per row and supports a byte-based global budget.

**Disk spill.** Useful for bounded bursts or expensive-to-reconstruct work, but turns memory pressure into local disk capacity, cleanup, encryption, replay, and crash-consistency work. If Kafka already durably owns the input, it is often safer to stop consuming than to build a second ad hoc durable queue.

**Kafka.** A durable, replayable shock absorber. It lets source capture continue through temporary destination outages within retention and disk capacity. It cannot absorb an infinite long-term rate mismatch.

### High and low water marks

Do not wait until allocation fails. Define a high-water byte threshold that pauses admission and a lower threshold that resumes it. The gap prevents rapid pause/resume oscillation. Count actual or conservatively estimated retained bytes, including decoded objects, encoded buffers, compression workspaces, client buffers, and in-flight requests—not just channel length.

The destination slowdown should propagate as:

```text
destination service time rises
  -> destination queue reaches high-water bytes
  -> consumer pauses assigned Kafka partitions or stops fetching work
  -> Kafka lag grows durably
  -> historical scanner's downstream credits run out
  -> scanner stops leasing chunks or blocks before the next batch
  -> source query concurrency/rate falls
```

For CDC, preserve durable capture as long as Kafka retention and source safety allow; pause the consumer before accumulating volatile RAM. For a historical backfill, scanning is discretionary and should throttle early. A useful policy reserves destination and memory capacity for live changes so bulk work cannot make current data arbitrarily stale.

### Backpressure mechanisms

- blocking bounded queues for exact local capacity;
- byte-counting semaphores or credits for variable row sizes;
- token buckets for source bytes/s or requests/s;
- queue high/low water marks for pause/resume;
- Kafka consumer `pause`/`resume` without giving up partition assignment;
- adaptive batch size to amortize a slower destination without violating memory/latency limits;
- adaptive scanner and loader concurrency;
- destination-specific rate limits and circuit breakers.

Credit flow is especially clear: the downstream grants `N` bytes of capacity; an upstream batch must acquire its encoded or estimated bytes before it is produced; capacity returns only after the batch is durably handed to the next owner. This bounds in-flight work across several queues.

Backpressure costs peak throughput during bursts and can extend backfills. It preserves the more important properties: bounded memory, predictable recovery, and production safety. Prove it with a controlled destination slowdown. Resident memory should plateau, queue bytes should stay within bounds, Kafka lag should grow at the expected slope, source scan rate should fall, and the system should recover without loss or a huge latency oscillation.

## 9. Global memory accounting

Every component retains memory simultaneously:

```text
scanner receive buffers
decoded rows
transform batches
dedup maps
Kafka producer accumulator
Kafka consumer fetch buffers
compression workspaces
destination batches
database-driver buffers
runtime heap and metadata
```

`64 workers * 100 MiB/worker = 6.4 GiB` is only the worker portion. Row limits fail on wide or compressed data: 100,000 tiny rows may be safe while 500 large JSON rows exhaust memory.

Use a hierarchy:

```text
process hard budget
  - runtime/headroom reserve
  - Kafka client reserve
  - connection/client reserve
  = explicit pipeline byte budget

pipeline budget -> per-stage maximum in-flight bytes
stage budget    -> per-worker/batch lease
```

Row-count and time limits remain useful for latency and cardinality, but byte limits enforce safety. Charge a conservative decoded-size estimate before expansion, then adjust to measured retained size. If an individual row exceeds normal batch size, use a special single-row path with a hard maximum instead of deadlocking forever on a credit it can never acquire.

Metrics are RSS, Go heap in-use, heap goal, GC CPU fraction and pause distribution, explicit owned bytes per stage, queue bytes, batch p95/max bytes, Kafka producer available buffer, consumer fetch bytes, spill bytes, and OOM/rejection count. RSS must not be inferred solely from Go heap because native client, mmap, stacks, and kernel buffers matter.

## 10. Application CPU: turn per-row work into parallel batch work

Once PostgreSQL and the network supply data quickly, the application can become the next limiter. Each row may be decoded from the database protocol, converted into a neutral type system, filtered, transformed, keyed, hashed, checksummed, serialized, compressed, encrypted, and copied into a Kafka record. At 500,000 rows/s, even two microseconds of single-core work per row consumes one full CPU-second per second; ten microseconds needs five cores before runtime and I/O overhead.

### Remove work before parallelizing it

The fastest transformation is the one that is unnecessary. Preserve a typed binary value when the next stage accepts it instead of formatting it as text and parsing it again. Filter columns at the earliest safe point. Avoid building general maps or JSON trees for a fixed schema. Cache immutable schema/type conversion plans per table rather than resolving them per row. Skip no-op transformations only when semantic equality is defined correctly for nulls, timestamps, decimals, and nested values.

Serialization choices change both CPU and bytes:

- JSON is debuggable and portable but repeats field names, escapes values, and often allocates strings.
- Avro/Protobuf-style binary encodings can be compact and faster, but require schema/version management and may still allocate if used through generic reflection-heavy APIs.
- A custom/internal binary batch can minimize work but increases compatibility and maintenance burden.
- Compression reduces network and Kafka disk bytes when data compresses well, at the cost of producer and consumer CPU. Compressing whole batches normally finds more redundancy than compressing rows individually.

Measure before changing formats. A format that halves bytes but triples CPU can help a network-bound pipeline and hurt a CPU-bound one.

### Pipeline independent CPU stages

A useful structure is:

```text
scanner -> bounded raw batches -> transform workers -> ordered/partitioned encoded batches -> producer
```

Transform workers operate on different batches in parallel. If output order matters, assign a monotonically increasing batch sequence and use a small reorder buffer before the ordered sink, or partition input so each key/partition has a single ordered lane. Unbounded reordering is dangerous: one slow batch can make every later completed batch occupy memory.

Vectorized/batch processing changes the inner loop from function dispatch and queue transfer for every row to tight loops over arrays/slices. It amortizes schema lookup, encoder initialization, checksums, syscalls, locks, and queue operations. Contiguous representations improve CPU-cache locality. Batch workers stop helping when CPU cores are saturated, memory bandwidth is saturated, a serial ordering stage dominates, or downstream credits block them.

Use CPU profiles and hardware/runtime metrics together: CPU-seconds per million input rows, time by decode/convert/hash/encode/compress/TLS function, rows and bytes per batch, run-queue length, per-core utilization, context switches, and end-to-end completed bytes/s. A faster transform whose output queue merely grows did not improve the system's sustainable rate.

## 11. Allocations, garbage collection, and memory copies

At one million rows/s, ten heap allocations per row create ten million allocations/s. The costs include allocator synchronization, metadata, cache misses, GC marking, pointer scanning, and the live heap needed to hold objects until a collection. Tail latency can rise even if average CPU appears tolerable.

### Build ownership around batches

Preallocate a batch slice to an expected row or byte capacity. Append values into batch-owned arenas or buffers. When the downstream stage completes, release the whole batch. This gives an explicit lifetime and reduces millions of tiny objects to a small number of batch allocations.

Useful Go techniques include:

- `make([]T, 0, expectedRows)` when estimates are credible;
- reusing `[]byte` capacity with clear ownership;
- streaming encoders that write into an existing buffer;
- avoiding `[]byte -> string -> []byte` conversions;
- storing offsets into a batch byte arena rather than one object per value;
- checking compiler escape analysis for hot helpers;
- using value slices instead of pointer-rich trees when practical;
- retaining only the latest value per key in a dedup map when current-state semantics allow it.

`sync.Pool` can reuse temporary buffers across garbage collections and reduce allocation rate. It is appropriate for interchangeable scratch objects whose contents are completely reset. It is not a durable cache; the runtime may clear it. Pooling makes performance worse when pooled objects retain huge backing arrays, increase memory residency, cross ownership unsafely, require expensive reset logic, or make a low-allocation path more complex without measured gain. Use size classes and discard unusually large buffers.

Proof comes from Go benchmarks and production profiles: `allocs/op`, allocated bytes/row, allocation rate, live heap, GC CPU fraction, collection frequency, pause p95/p99, RSS, and completed rows/s. A lower pause time with a much larger retained heap may only have traded GC for memory risk.

### Reduce copies through the whole path

A naive row can travel:

```text
kernel socket buffer
 -> database-driver buffer
 -> decoded Go objects
 -> JSON/Avro output buffer
 -> Kafka client accumulator
 -> kernel socket buffer
```

Each arrow may copy the payload and touch memory bandwidth. At 5 GB/s of logical data, four full copies mean roughly 20 GB/s of memory traffic before metadata, reads, and writes. On a many-core machine the memory channels or last-level cache can become the shared ceiling even when individual cores are not fully busy.

**GENERAL HIGH-PERFORMANCE TECHNIQUE.** Fewer intermediate representations, streaming decode/encode, buffer ownership transfer, scatter/gather I/O where APIs permit it, and batch/columnar layouts reduce copying. True zero-copy is constrained by protocol framing, transformation, compression, encryption, garbage-collected ownership, and lifetime requirements; “zero-copy” usually means eliminating one or more copies, not literally none.

Columnar/vectorized batches help when the same operation touches one field across many rows, because values are contiguous and SIMD/cache behavior improves. Row-oriented batches can be better for whole-row serialization and keyed updates. Select by downstream work, not fashion.

Detect memory-bandwidth pressure with memory-controller counters where available, LLC miss rate, stalled cycles, flat throughput despite adding CPU workers, high aggregate memory bandwidth, and copy-heavy CPU profiles. Reducing copies should lower CPU-seconds and memory bytes per logical byte. The next exposed limit is commonly NIC bandwidth, Kafka acknowledgements, or destination ingestion.

## 12. Network, latency, and batching: send fewer, fuller requests

Network limits have three distinct forms:

| Limit | Symptom | Mechanism |
| --- | --- | --- |
| bandwidth | bytes/s near NIC, link, VPN, NAT, or cloud endpoint limit | payload volume fills the channel |
| latency | low link utilization but workers wait on request/ack round trips | too few requests are in flight or each contains little work |
| packet/request overhead | high CPU or packets/s, small average request, modest payload bandwidth | headers, syscalls, TLS, broker/database parsing dominate |

Batching helps all three differently. It does not reduce uncompressed logical bytes, but it reduces headers, syscalls, RPCs, and acknowledgements per row. Batch compression can reduce payload bandwidth. Larger requests make each round trip accomplish more work. Pipelining and multiple bounded in-flight requests overlap round-trip latency.

For a request latency `L`, rows/request `B`, and in-flight request count `C`, the latency-only ceiling is approximately:

```text
throughput <= C * B / L
```

At `L = 10 ms`, `B = 10,000`, and `C = 4`, this ceiling is four million rows/s before bandwidth or service time. At one row/request it is only 400 rows/s.

### Batch-size selection

Compare the physical paths:

```text
row-at-a-time:
  encode -> syscall/RPC -> parse -> transaction work -> acknowledgement   x 100,000

10,000-row batches:
  encode 10,000 -> a few large writes -> one batch parse/load/ack          x 10
```

Going from one to 100 rows often wins dramatically because fixed cost dominates. From 10,000 to 100,000 may help only slightly once payload work dominates, while increasing latency, memory, retry scope, lock duration, and recovery work.

Use three simultaneous flush conditions:

- maximum bytes for memory, request, and destination safety;
- maximum rows/distinct keys for cardinality and database plan behavior;
- maximum age for freshness under low traffic.

Flush when any limit is reached. An adaptive batcher may increase bytes while destination fixed overhead dominates and latency/memory are healthy, then shrink on high p95 service time, errors, memory pressure, or latency-SLA risk. Use hysteresis and bounded steps.

Compression should be evaluated as `(bytes saved, CPU added, latency added)`. A fast codec may win end-to-end over a maximum-ratio codec. Co-location or high-bandwidth networking removes topology cost; multiple connections help when one connection's congestion window, TLS stream, or server lane is limiting, but stop when the shared NIC/service is full. Separating source, Kafka replication, and destination traffic prevents one flow from starving the others.

Metrics are wire bytes/s, logical bytes/s, compression ratio, packets/s, average request bytes, requests/s, RTT, in-flight requests, retransmits, socket send/receive wait, TLS CPU, and per-request queue/service time. Batch changes worked when requests/row and wire bytes/logical byte fall while end-to-end committed throughput rises within latency and memory budgets.

## 13. Kafka producer: fill batches without giving up durability

The producer can saturate before the Kafka cluster. It owns serialization, partition choice, compression, a bounded accumulator, network connections, retries, and acknowledgement tracking.

### Producer batching and in-flight requests

Kafka accumulates records per partition. `batch.size` controls the target buffer for a partition batch; `linger.ms` permits a small delay so lightly loaded partitions can collect more records. Under high load, batches fill before linger expires. A larger batch reduces requests and improves compression, but reserves more memory across active partitions and can add latency under low load.

Multiple in-flight requests overlap broker/network latency. Retrying non-idempotent sends with several requests in flight can reorder records when an earlier request fails and a later one succeeds. Kafka's idempotent producer is designed to suppress duplicate writes from producer retries within its guarantees and constrains compatible acknowledgement/in-flight settings. That protects Kafka append semantics; it does not make the entire database-to-destination pipeline exactly once.

Acknowledgements trade durability for response time. A serious CDC stream normally needs durable broker acknowledgement, appropriate replication, and retry behavior. Weakening acknowledgement solely for a benchmark measures a different correctness contract. Compression is best applied to record batches; it saves network and log bytes but adds producer and consumer CPU.

### When tuning stops helping

- `batch.size` no longer helps once batches are payload-bound and mostly full already.
- `linger` hurts latency without improving batch occupancy when traffic is too sparse or partitions too numerous.
- more in-flight requests stop helping when broker/network capacity or producer CPU is full.
- stronger compression hurts when CPU, not network/disk, is limiting.
- more producer threads contend on the same accumulator/partitions or only create smaller batches.

Producer proof includes records/s and bytes/s, `batch-size-avg/max`, records/request, request rate/latency, compression ratio, buffer available bytes, buffer-pool wait time, waiting threads, error/retry rate, throttle time, record queue time, and broker-side produce queue/local/replication time. If application threads block on the producer buffer while broker capacity is idle, the client or its configuration is limiting. If broker throttling and request time rise, the pressure is downstream.

## 14. Kafka partitions: buy parallel lanes, then pay for coordination

One partition is one ordered append/read lane and can be processed by at most one ordinary consumer in a consumer group at a time. More partitions distribute leaders across brokers and allow parallel consumers, but Kafka only gives total order within each partition.

### Partitioning choices

**By table.** Keeps table order simple and isolates hot tables. One huge table can still saturate one partition, and transactions touching tables cross partitions.

**By primary-key hash.** Spreads one large table and preserves per-key order because every event for a key hashes to the same partition. Hot keys remain hot, and transaction-wide order is lost.

**By source shard/database.** Mirrors natural ownership and may preserve each source's order. Load can be skewed and cross-source coordination remains separate.

**Random/round-robin batches.** Balances raw bytes well but can reorder changes for the same key and is unsuitable unless a later repartition/order stage repairs it.

Adding partitions changes the data path from one serial consumer to multiple partition-local consumers and destination lanes. It helps when producer, broker, consumer CPU, or destination work can operate independently. It stops when a single destination merge/WAL/device is shared, when a hot partition dominates, or when coordination becomes the serial fraction.

### Transactions that span keys or tables

Per-key order is sufficient only if destination semantics are independent per key and cross-key atomic visibility is not required. If one source transaction changes A and B, placing them on different partitions permits A to appear before B and makes crash progress separate.

Possible designs are:

1. **Single transaction partition.** Hash by source transaction so every event in it stays together. This preserves transaction atomicity but events for the same key in different transactions must still map consistently; those two requirements can conflict.
2. **Transaction metadata plus barrier.** Emit `BEGIN`, numbered fragments, expected fragment/partition membership, and `COMMIT`. Consumers stage fragments, and a coordinator releases/apply them only when complete. This preserves atomicity at the cost of staging, coordination, timeout/recovery state, and head-of-line blocking.
3. **Global source sequence and watermarks.** Partitions process locally but publish their highest complete sequence. A merge stage advances a global watermark only when every relevant partition has covered the prefix. This supports consistent prefixes but the slowest partition controls progress.
4. **Repartition by destination key.** First ingest transaction records, then redistribute by key to parallel apply lanes. A transaction coordinator must still decide whether atomic multi-key application is required.
5. **Relax semantics deliberately.** Require per-key ordering and eventual current-state convergence, not transaction-atomic destination visibility. This scales well but must be stated and tested; it is unacceptable for workloads that rely on cross-row invariants.
6. **Single ordered stream.** Retain one partition when simplicity and strict total order are worth the ceiling. Scale within batch processing and destination operations instead.

**PLAUSIBLE IMPLEMENTATION.** An Artie-like current-state replicator could use partition-local ordering plus stable-key deduplication where connector semantics permit, while routing operations needing transaction-wide atomicity through a coordinating path. This is a design possibility, not a statement about Artie.

Prove partition scaling with throughput per partition, bytes and records skew, leader distribution, consumer utilization, lag per partition, hot-key frequency, rebalance time, coordinator wait, watermark gap, and destination correctness under cross-partition retries. Aggregate lag alone hides one stalled lane.

## 15. Kafka broker: append sequentially, cache aggressively, replicate deliberately

Kafka converts producer batches into partition-log appends. Batching produces larger network packets and sequential disk operations. The broker relies heavily on the filesystem page cache and can send cached log data efficiently to consumers. Replication multiplies broker network and storage work: the leader accepts each batch, followers fetch it, and durable acknowledgement may wait for in-sync replicas.

Potential ceilings are:

- broker network processors or NIC bandwidth;
- request-handler CPU, validation, compression/message conversion, TLS;
- page-cache misses and storage read bandwidth for lagging consumers;
- log append/storage bandwidth and fsync behavior;
- replication bandwidth and slow followers;
- too many partition leaders, open files, and metadata operations;
- skewed leader placement;
- quotas or cloud-service limits.

Solutions follow the limiting resource. Add brokers and redistribute partition leaders for horizontal network/storage/CPU capacity. Use faster storage for cold/backlogged reads and sustained writes. Preserve producer-side batches and compression so the broker handles fewer requests and bytes. Size replication and in-sync requirements to the durability contract. Avoid a huge partition count “just in case,” because every partition has metadata, files, replication, recovery, and controller cost.

Metrics that locate the broker limit include produce/fetch bytes and requests, request queue and response queue time, leader local time, follower/remote wait, network processor idle percentage, request-handler idle percentage, disk throughput/latency/utilization, page-cache hit behavior, under-replicated/offline partitions, ISR changes, follower lag, throttle time, CPU, and GC. Low idle percentage plus rising queue time identifies a busy broker path; disk reads rising only when consumers lag far behind points to page-cache eviction rather than append capacity.

Kafka remains a finite durable shock absorber. If it receives 500,000 rows/s and the destination completes 100,000, retained bytes grow with the 400,000-row/s difference. Retention expiry eventually destroys replayability; disk exhaustion can destabilize the cluster; freshness latency increases; and even after repair, catch-up requires spare destination capacity.

## 16. Kafka backlog and consumer flow control

Backlog has four dimensions:

```text
record lag       = high watermark - consumed offset
byte lag         = bytes represented by those records
time lag         = age of oldest unapplied event
recovery horizon = time until retention or disk safety margin is exhausted
```

Rows alone mislead when sizes vary, and offset lag does not tell users how stale the destination is. A consumer should fetch large enough chunks to amortize broker/network overhead, but place them into a bounded byte queue. When downstream reaches its high-water mark, pause assigned partitions while continuing the poll/heartbeat behavior required by the client, then resume below the low-water mark. Do not keep fetching into RAM merely to make Kafka lag look small.

For a backfill plus live CDC, capacity allocation is a product decision:

- reserve a fraction or priority lane for CDC so current changes do not wait behind terabytes of historical data;
- throttle backfill first when destination latency/queueing rises;
- scale destination consumers only while partitions and destination lanes permit it;
- increase destination bulk efficiency before adding consumers that all contend on one merge;
- retain enough Kafka bytes/time for the worst credible outage plus catch-up;
- alert on time-to-retention-exhaustion, not only current lag.

**PUBLICLY KNOWN.** Artie's public architecture places Kafka between its reader and writer as a durable buffer. Its public flush documentation says the writer temporarily buffers/deduplicates messages, writes an optimized destination batch, and commits the Kafka offset after successful completion. Public backfill material describes continuing to queue live changes while historical data is copied, then draining them. These statements do not reveal private partitioning, worker counts, or coordination internals.

**ALTERNATIVE DESIGN.** A system can bypass Kafka for historical rows and send them through a separately checkpointed bulk-loader path while Kafka carries only live changes. This avoids writing every historical byte through Kafka, but creates two paths that must converge safely and share destination admission. Another design routes both paths through one durable log for simpler replay at the cost of broker/storage/network volume.

Consumer proof includes fetch bytes/request, fetch latency, records and bytes consumed/s, processing and destination service time, paused duration, bounded queue bytes, lag per partition in records/bytes/time, commit latency/errors, rebalance count, and duplicate/replay rate. Sustainable recovery is demonstrated only when lag decreases under continuing source traffic and memory remains bounded.

## 17. Destination ingestion: replace per-row transactions with bulk data movement

The destination commonly becomes the main wall because it must turn incoming values into durable table and index state. A row-by-row path can pay, for every row:

```text
client/server round trip
parse/bind/execute
transaction or statement bookkeeping
constraint and uniqueness checks
heap write
primary and secondary index writes
WAL generation
commit acknowledgement, sometimes fsync
```

Even when one row takes only one millisecond, a serial writer tops out near 1,000 rows/s. More writers overlap waits, but quickly contend on WAL, indexes, locks, buffers, CPU, connections, and storage.

### Increasing levels of ingestion efficiency

1. **Prepared row operations.** Remove repeated parse/plan work and reuse connections. This helps modestly; it retains per-row protocol and executor overhead.
2. **Multi-row `INSERT`.** One statement carries many tuples, reducing round trips and parse/commit work. Statement size, parameter count, memory, and error isolation set limits.
3. **Larger transactions.** Many statements share one commit/fsync. This improves commit amortization but increases lock duration, rollback/retry work, WAL spikes, and visibility delay.
4. **`COPY`/binary `COPY`.** Stream rows through PostgreSQL's bulk path with compact framing and far fewer command cycles. Binary avoids text conversion for supported, correctly typed values. It is less portable and malformed/type-incompatible input can fail a larger batch.
5. **Staging plus set-based SQL.** Bulk-load raw operations into a staging table, then execute a small number of `INSERT`, `UPDATE`, `DELETE`, or `MERGE` statements against the target.
6. **Destination-native loaders.** Warehouses often ingest files from object storage or expose load jobs that are much more efficient than row SQL. Generate appropriately sized files, upload in parallel, then invoke the native loader.
7. **Partitioned/sharded writes.** Route independent keys/partitions to independent physical targets or destination partitions, then finalize. This helps only when underlying resources and correctness allow independence.

The physical difference between row writes and `COPY` is not magic. `COPY` streams a long sequence through one command/connection, amortizes protocol and executor setup, uses bulk-write behavior, and lets the database process tuples in a tight path. Staging separates fast append from expensive conflict resolution. Set-based finalization lets the database scan/join groups of rows instead of performing a network round trip and lookup plan for each row.

Metrics are destination rows and bytes/s, requests/s, rows/request, connection utilization, statement and commit p95, WAL bytes/s, WAL fsync time, CPU, buffer hits/reads/writes, disk throughput/latency, locks/conflicts, index write rate, and source-to-visible latency. The test must verify final contents and retry behavior, not merely loader completion.

## 18. Staging and set-based finalization

The core flow is:

```text
parallel workers
  -> large destination-format batches
  -> COPY/native load into batch-scoped staging
  -> validate expected batch metadata
  -> one/few set-based operations
  -> record durable progress
  -> clean or recycle staging
```

A staging row normally carries the stable key, operation (`insert/update/delete` or before/after state), source ordering metadata, batch/transaction ID, and payload. The exact schema depends on whether the destination stores current state or history.

### Set-based operation strategies

**Insert-only/history.** Append staging rows to the target after deduplicating exact retry identities if required. Never collapse legitimate repeated events merely because their logical key matches.

**Current-state upsert.** Within staging, choose the latest event for each key according to a stable source order. Update matching target rows and insert missing keys, using native `MERGE` or separate statements. The ordering column must disambiguate events; wall-clock arrival time is unsafe.

**Deletes.** Keep tombstones in staging. Delete target rows whose keys have a latest delete operation. A delete arriving after an upsert for the same key must win; deduplication must compare source order, not operation type.

**Separate update/insert/delete.** Some destinations execute three simpler set-based statements more predictably than one complex `MERGE`. This can scan staging or target more than once, so measure.

**Multi-step merge.** Accumulate several microbatches in an intermediate table, then merge a larger group into the final table. This lowers expensive target-merge frequency and reduces small files in warehouses/lakehouses, but increases visibility delay, intermediate storage, and progress complexity.

**PUBLICLY KNOWN.** Artie publicly describes destination-specific optimized batches, primary/unique-key deduplication in its memory buffer, and a multi-step merge feature for reducing target merge frequency. Its online-backfill article says historical backfill rows can be appended and followed by a final dedupe pass, avoiding a merge per backfill batch. These are public behaviors; private query plans and staging schemas are unknown.

### Crash recovery and idempotency

The dangerous window is “destination committed, progress did not.” A retry must not produce an incorrect second effect. Designs include:

- deterministic batch IDs with a durable applied-batch table;
- batch-scoped staging names and states (`loading`, `loaded`, `finalized`);
- idempotent current-state upsert keyed by stable key and source version;
- uniqueness on event identity for append/history mode;
- one destination transaction containing finalization plus progress when supported;
- reconciliation of ambiguous outcomes by querying batch/progress state before retry.

Do not truncate shared staging before the corresponding progress is known durable. Concurrent loaders need isolated batch IDs or physical staging partitions. Cleanup is a retriable state-machine step, not proof that data was committed.

Staging stops helping when the target merge dominates, staging I/O doubles the storage path, load jobs queue, or batches are too small to amortize fixed cost. Measure staging load time separately from queue time, target finalize time, commit/progress time, cleanup time, bytes scanned/written, target partitions touched, duplicate suppression, and retry result.

## 19. Destination indexes: every useful read structure is write work

In PostgreSQL-like destinations, each inserted tuple writes the heap and every applicable index. A primary/unique index is often required for identity, conflict handling, or constraints. Secondary indexes may be required for query availability, but each adds CPU, WAL, page writes, cache pressure, and possible random I/O. Hot monotonically increasing or skewed keys can concentrate contention on a small set of index pages.

### Backfill/rebuild choices

**Load first, build secondary indexes later.** Bulk heap load followed by one index build can use sorting and sequential I/O more efficiently than maintaining the index per row. Queries during the build lack that index; building uses CPU, memory, temporary storage, locks, and time.

**Shadow table with required final indexes.** Load a separate table, build/validate indexes, then swap. This keeps the live table queryable but temporarily doubles table/index storage and complicates dependencies, grants, constraints, views, and cutover locks.

**Keep essential indexes only.** Preserve primary/unique constraints needed for correct key semantics. Remove/rebuild optional indexes only in a controlled resync where their absence does not break serving requirements.

**Partition-wise load and index.** Load/build individual partitions, then attach/swap them when destination semantics permit. This reduces working set and enables parallelism, but partition metadata and global uniqueness constraints can limit it.

For continuous CDC into a live table, indexes cannot normally be dropped for every batch. Instead, deduplicate changes, use larger set-based operations, touch only needed partitions, and tune batch/concurrency to the index and WAL capacity.

Proof includes WAL bytes/row, total bytes written/row, index size growth, destination CPU, buffer churn, page splits, lock waits, load time, index-build time, final query plans/latency, and cutover time. Report full lifecycle time; a fast heap load plus a longer unreported index build is not a faster backfill.

## 20. Destination WAL, fsync, and checkpoints

Durable PostgreSQL writes first create WAL records. Commit normally requires the relevant WAL to be made durable, while heap/index dirty pages can be written later and recovered by replay. Many tiny commits cause many acknowledgement and sync opportunities. Larger transactions amortize commit records and group more work behind each durable flush.

WAL can limit through:

- bytes generated per logical row, including index records;
- WAL insertion/copy CPU and lock contention;
- WAL device write bandwidth;
- fsync latency and commit rate;
- replication/archiving bandwidth;
- frequent checkpoints and full-page images after checkpoints;
- dirty-buffer/checkpoint write pressure on the data volume.

Legitimate optimizations that preserve the chosen durability contract include larger but bounded transactions, `COPY`, fewer maintained indexes, sufficient WAL buffers for high generation, checkpoint sizing/timing that avoids constant forced checkpoints, fast reliable WAL storage, spreading checkpoint writes, and letting concurrent commits benefit from group commit. Measure on the exact storage and replication configuration.

Turning off `fsync`, weakening acknowledgements, using unlogged final tables, or disabling safety features may produce a benchmark number but changes the failure contract. Such a run must be labeled non-durable and is not evidence for a production replication system. An unlogged or transient staging table may be acceptable only if its contents can be reconstructed and the state machine handles its loss explicitly.

Transaction size has a U-shaped tradeoff. Too small: excessive commits/fsync and protocol overhead. Too large: big memory/WAL bursts, long locks, delayed visibility, larger rollback/retry, replication lag, and timeout risk. Tune bytes/transaction using WAL and latency evidence, not a universal row count.

Metrics include `pg_stat_wal` bytes/records/full-page images, `pg_stat_io` WAL writes/fsyncs/time, commits/s, commit p95, checkpoints requested/timed, checkpoint write/sync time, `pg_wal` growth, archive/replica lag, dirty buffers, data/WAL device throughput and `await`, and WAL bytes per applied logical byte.

## 21. Destination parallelism: align lanes with independent resources

One writer may underuse CPU, storage, and network. A handful of writers can overlap load/finalize operations. One hundred writers can reduce throughput through:

- lock conflicts between overlapping key/table ranges;
- uniqueness/index-page contention;
- WAL insert and fsync contention;
- buffer-cache churn;
- connection/backend overhead;
- storage and NIC saturation;
- destination warehouse queueing or concurrency limits;
- deadlocks and transaction retries;
- hot keys appearing in multiple batches.

Partition or shard writes by stable key so a key has one active owner. Route disjoint destination table partitions to separate writers. If finalization needs one global merge, parallel staging can still help only until that serial merge dominates. Multiple destination tables are naturally parallel if they do not share an exhausted warehouse/cluster.

Choose concurrency with a sweep such as `1, 2, 4, 8, 16...` under the same dataset and batch policy. Track completed—not merely submitted—bytes/s, queue and execution p95, lock time, conflicts/deadlocks, WAL and disk utilization, CPU, connection wait, and retry work. Select the knee with safety headroom. An adaptive controller should lower concurrency quickly on lock/error/queue pressure and raise it slowly only when completed throughput improves.

**GENERAL HIGH-PERFORMANCE TECHNIQUE.** Use separate semaphores for bulk load and finalization. A destination may accept several concurrent file/COPY loads but only one or two heavy target merges. Coupling them under one worker count either underuses loaders or overloads merges.

## 22. Online rebuild and cutover

A high-performance resync must keep the existing destination usable while a new copy is built. The public behavior can be implemented with this conceptual flow:

```text
existing live table continues serving queries
live changes -> live table and shadow/staging table
historical snapshot -> shadow/staging table through bulk path
ordered reconciliation -> shadow catches up
validate correctness and required objects
establish an exact cutover boundary
atomic destination metadata/name transition
retain old table for rollback/forensics
```

**PUBLICLY KNOWN.** Artie describes online backfills as dual-writing changes to live and staging tables, backfilling staging, catching up, and atomically swapping staging into the live name while archiving the old table. Its article also states the temporary destination-storage cost is roughly two copies. It does not publicly disclose every destination's transaction, lock, dependency, or ambiguous-failure protocol.

Performance comes from keeping the historical path append/bulk-oriented and paying the expensive final reconciliation/cutover once. Correctness requires much more than a rename:

- define the source snapshot/change boundary;
- prevent a stale historical row from overwriting a newer live change;
- make dual-write retry-safe;
- know when the shadow has reached the exact required prefix;
- validate schema, row/key state, and destination objects;
- stop or fence writes during the minimal final boundary if required;
- ensure writers cannot continue addressing the retired physical table;
- recover an ambiguous swap outcome by durable state inspection;
- preserve grants, indexes, constraints, views, ownership, policies, and downstream dependencies according to destination capabilities.

**PLAUSIBLE IMPLEMENTATION.** A coordinator could attach generation IDs to live/shadow writes, use durable per-destination progress, reserve CDC capacity over historical work, validate a consistent prefix, acquire a short destination routing fence, execute a transactional rename/swap when supported, and record promotion state in the same transaction. Other destinations require view-pointer changes, partition exchange, table clone/replace APIs, or a non-atomic migration plan.

The swap is short only if indexes, schema, and validation are prepared earlier. Full row comparison at cutover makes downtime proportional to table size; a scalable design validates bulk state before the fence and checks only bounded changes across the final interval, while retaining a post-cutover verifier.

Measure live-table availability/latency throughout, historical bytes/s, CDC freshness, shadow progress, dual-write cost, destination storage, validation time, final fence duration, swap lock wait/duration, retry/recovery behavior, and post-swap equality.

## 23. Shared-machine benchmarks and hidden contention

Running source PostgreSQL, destination PostgreSQL, Kafka, and the application on one machine makes them compete for the same CPU scheduler, RAM, OS page cache, memory bandwidth, filesystem, storage device, and sometimes virtual/container network. Increasing scanner workers can evict Kafka or destination pages; a destination checkpoint can slow source reads; application GC can steal CPU from both databases. The observed “source bottleneck” may be a destination write saturating the shared disk.

Containers enforce limits only when configured and do not create physical resources. Loopback removes real network latency and NIC topology, while shared virtual bridges add their own overhead. Page-cache accounting can make one process look memory-efficient while consuming host memory needed by another.

Local benchmarks remain valuable for correctness, crash/retry tests, regression ratios, profiling, and finding gross allocation or query problems. They cannot establish production-scale throughput or isolation. A defensible scale benchmark uses separate machines/volumes or clearly isolated resources, records instance/storage/network configuration, monitors every host, controls cache state, and distinguishes client, source, Kafka, and destination clocks.

## 24. Adaptive control: coordinate the knobs instead of tuning them independently

Static settings are useful safety defaults, but source load, row width, cache warmth, network, and destination behavior change. A sophisticated system can control:

- source scan worker count;
- target chunk bytes/duration;
- source byte/request rate;
- transform worker count;
- batch bytes/rows/age;
- Kafka consumption pause/resume;
- destination load concurrency;
- destination finalize concurrency;
- memory credits split between CDC and backfill.

### A feedback-control design

Define objectives and constraints first:

```text
maximize durable destination bytes/s
subject to:
  source foreground p99 <= budget
  CDC event age <= SLA
  RSS and queue bytes <= budgets
  error/retry rate <= budget
  destination p95/queue <= budget
  recovery horizon >= minimum
```

Use measured completed output, not input accepted into queues. Apply additive increase/multiplicative decrease or another conservative controller: increase one control step when all constraints are comfortably satisfied and marginal throughput improves; decrease quickly when a hard signal violates its limit. Use exponential moving averages, hysteresis, minimum dwell time, and cooldown. Separate fast safety loops (pause on memory high-water) from slow optimization loops (change workers every tens of seconds/minutes).

Knobs interact. Larger batches consume more memory, can improve destination throughput, and increase latency. More scan workers can worsen source p99 and downstream queueing. More destination writers can raise lock/WAL contention. Tune coordinated lanes or change one dimension at a time while learning a response curve.

Keep hard operator-configured bounds. A controller can be wrong during sensor failure, delayed metrics, or a workload phase change. Persist enough state to explain decisions: observed signals, chosen action, previous value, and reason. Control stability is itself tested with step changes: halve destination capacity, inject latency, widen rows, recover capacity, and verify bounded queues without rapid oscillation.

## 25. Finding the current bottleneck: a practical profiling procedure

Suppose end-to-end completion is 50,000 rows/s. Diagnose from the final durable boundary backward while observing every stage concurrently.

### Step 1: define and verify the output metric

Specify whether 50,000 means source rows read, Kafka records acknowledged, distinct keys staged, or destination rows durably visible. Record rows and bytes, batch size, correctness mode, durability, dataset shape, and cache state. Confirm final counts/checksums or keyed equality. Without this, a “speedup” may be dropped work or stronger deduplication.

### Step 2: draw queues and measure their slopes

For each boundary record arrival rate, departure rate, bytes queued, oldest age, and blocked/idle time. The queue immediately before a slow stage grows; the stage after it may be starved. No growing queue can mean the source is limiting, strong backpressure is hiding the queue, or the measurement interval is too short.

### Step 3: inspect the source plan and storage

Run representative `EXPLAIN (ANALYZE, BUFFERS, SERIALIZE)` safely. Inspect sequential/index/index-only plan, actual rows, heap fetches, shared hits/reads, I/O timing, and serialization. Correlate with `pg_stat_io`, database CPU, and device IOPS/MB/s/latency/queue.

- high small-read IOPS, modest MB/s, index/heap reads -> random access/locality limit;
- sequential MB/s at device/volume maximum -> storage bandwidth;
- high `await`, low queue depth and unused device capacity -> insufficient I/O overlap or remote latency;
- warm hits but full PostgreSQL CPU -> executor/MVCC/encoding CPU;
- source query blocked on client socket while app queue is full -> downstream backpressure, not source capacity.

### Step 4: isolate network

Compare source serialized bytes/s with application received bytes/s and link counters. Inspect RTT, retransmits, packets/s, request size, socket stalls, TLS CPU, and cloud endpoint/NAT limits. Large RTT with few in-flight streams and idle bandwidth points to latency; link saturation points to bandwidth; high packets/CPU with small payloads points to request overhead.

### Step 5: profile application CPU, allocation, and blocking

Use CPU profiles, block/mutex profiles, runtime scheduler metrics, allocation profiles, GC metrics, and per-stage timers.

- cores full in decode/encode/hash/compress -> application CPU;
- high allocation rate/GC CPU -> object churn;
- flat scaling plus memory-controller/LLC pressure and copy profiles -> memory copies/bandwidth;
- low CPU with goroutines blocked on bounded downstream queue -> next stage;
- one mutex/reorder coordinator hot -> serial software bottleneck.

Benchmark hot functions with real row shapes and include allocations. Then confirm end-to-end because a microbenchmark improvement can move cost or change batching.

### Step 6: inspect Kafka producer and broker separately

Producer: batch occupancy, compression ratio, buffer wait, record queue time, request latency, retries/errors, throttle, outgoing bytes. Broker: request queue/local/remote/response time, network/request-handler idle, disk, replication lag/ISR, leader skew, produce/fetch bytes.

- producer buffer exhausted, producer CPU full, broker idle -> producer/serialization/config;
- one partition maxed while others idle -> partition lane;
- broker request queue rises and idle percentages fall -> broker CPU/network thread;
- broker storage busy or lagging consumers cause reads -> broker disk/page-cache;
- remote/replication wait dominates -> follower/ack path.

### Step 7: inspect consumer and destination boundary

Measure fetch rate/size, processing time, queue bytes, paused time, partition lag, destination load queue, load execution, finalization queue/execution, and progress-commit time. A consumer with spare CPU that is paused behind a full destination queue correctly indicates the destination.

### Step 8: decompose destination time

Separate connection wait, network, staging/COPY, warehouse queue, merge execution, commit/fsync, lock wait, cleanup, and progress write. Observe CPU, disk, WAL, indexes, locks, conflicts, and warehouse slots.

- row operations and many requests -> protocol/executor overhead;
- staging fast, merge slow with high bytes scanned -> target merge/partition pruning;
- high WAL bytes/fsync/device -> WAL or commit path;
- high lock/conflict/deadlock -> writer overlap/hot keys;
- CPU full, storage not full -> constraint/index/executor CPU;
- warehouse queue high but execution stable -> concurrency/service capacity;
- one index dominates writes/page contention -> index design/hot key.

### Step 9: perturb one resource and observe causality

Double only batch bytes, scanner workers, transform workers, partitions, or destination concurrency. A bottleneck should respond predictably. If doubling destination capacity drains its queue and raises total throughput until CPU saturates, the diagnosis is strong. If nothing changes, the altered resource was not limiting or the experiment did not change its effective capacity.

### Step 10: run long enough to see steady state

Short tests hide cache cooling, checkpoints, GC cycles, Kafka retention reads, warehouse auto-scaling, compaction, and retry storms. Include warmup, sustained interval, repetitions, confidence intervals, and a post-run equality check. Report p50/p95/p99 and queue growth, not only average rows/s.

## 26. Moving bottlenecks: optimization is repeated constraint discovery

Stage capacity examples make the rule concrete:

```text
source scan        500k rows/s
application CPU   200k rows/s
network           300k rows/s
Kafka             800k rows/s
destination       100k rows/s   <- system ceiling
```

The system completes about 100k rows/s because the destination is required and slowest. Optimizing application CPU from 200k to 600k changes nothing yet. Raising destination capacity to 500k exposes application CPU at 200k. Improving CPU to 600k then exposes network at 300k. Raising network capacity may expose source scan at 500k or some previously hidden Kafka/destination behavior at the new load.

Queues make this visible: the queue before the current limiter grows or reaches its high-water mark; upstream stages block; downstream stages starve. Once the limiter improves, that queue drains and a different queue begins to grow. A single dashboard should align per-stage input/output bytes, queue bytes/age, utilization, service time, and blocking time so the migration is visible.

There is no permanent performance fix because the workload also changes: wider rows shift pressure to bytes/memory/network, hot updates increase dedup benefit and index contention, cold data shifts to disk, more tables change scheduling, and a destination scaling event changes the service curve. Performance engineering is a loop:

```text
define semantics and workload
 -> find slowest required stage
 -> improve its work per byte/request or add real capacity
 -> verify correctness and steady-state effect
 -> identify the newly exposed stage
 -> repeat until the objective/headroom is met
```

Stop when the business objective and safety margins are met. Chasing the next theoretical ceiling can add complexity, cost, and failure modes without useful benefit.

## 27. Software inefficiency versus physical capacity

Suppose the storage advertises and independently demonstrates about 1 GB/s, but the backfill reads 100 MB/s. If device queue depth is low, requests are tiny/random, one worker waits serially, CPU is not full, and a sequential benchmark reaches near 1 GB/s, the gap is likely software/access-pattern inefficiency. Better locality, batching, prefetch, and concurrency can plausibly yield a large gain.

If the application issues large sequential reads, queue depth is healthy, the device sustains 950 MB/s, latency grows under more workers, and an independent read test reaches about the same number, the physical channel is full. Further loop optimization cannot produce 2 GB/s of source bytes. Options become fewer bytes, faster/more storage, replicas, sharding, distributed scanning, or accepting the wall-clock time.

Apply the same test to every stage:

- CPU: profiles show useful work on all cores versus a single serial lock/order lane.
- memory: measured bandwidth near platform limit versus excessive copies/allocations.
- network: wire rate near link/service quota versus small requests and idle link.
- Kafka: brokers/NICs/disks busy and balanced versus one hot partition/client bottleneck.
- destination: warehouse/DB resources and WAL/storage full versus row-at-a-time software and tiny batches.

Crossing from software to physical capacity is an evidence claim. Use an independent microbenchmark or native bulk tool to estimate the resource's ceiling under the same durability/topology, then compare utilization and useful-work efficiency. Cloud “up to” specifications are not measurements. Hardware scaling also has correctness and operational costs: replica consistency, shard coordination, failover, resharding, cross-zone traffic, and higher bills.

## 28. How a real 10–20× improvement can emerge

The following numbers are illustrative, not an Artie benchmark. They show multiplicative-looking gains produced by removing several different ceilings. The exact gains cannot be added or promised because each step changes which stage is limiting.

### Baseline: 8,000 rows/s

The naive system uses one primary-key scanner, 1,000-row queries, per-row map allocation, JSON encoding, one Kafka partition with mostly small requests, row-by-row destination upserts, every secondary index active, and a commit every 100 rows. Source, Kafka, and destination share one machine.

The destination performs thousands of statements and frequent fsyncs; application allocation and network requests are high; the scanner sometimes stalls on sparse/skewed ranges. The final rate is 8,000 rows/s.

### Step 1: larger keyset chunks and streaming — 15,000 rows/s, 1.9×

Change: use keyset ranges large enough to amortize query setup/B-tree descent, stream only selected columns, reuse the connection, and remove `LIMIT/OFFSET` behavior.

Why: fewer queries, plans, round trips, and repeated index traversal per row. The source delivers fuller client reads.

New bottleneck: one worker cannot keep storage and CPU busy; skew leaves it on a slow range.

Cost: longer queries and larger retry units unless output batches remain bounded.

Proof: queries/million rows fall, average returned bytes/query rises, rows/s rises, source still has spare CPU/I/O.

### Step 2: sampled byte-balanced chunks and four workers — 30,000 rows/s, 3.75×

Change: sample key density and row size, create many byte-targeted chunks, lease dynamically to four bounded scanners.

Why: scans overlap I/O/latency, workers remain busy, and stragglers shrink. Aggregate source delivery doubles.

New bottleneck: application CPU and GC reach saturation.

Cost: more source load/connections and a durable scheduler/lease state.

Proof: source bytes/s rises, chunk p95/p50 narrows, worker idle falls, production p99 remains inside budget.

### Step 3: batch representation and allocation reduction — 48,000 rows/s, 6×

Change: decode into batch-owned buffers, preallocate, reuse encoder scratch buffers, cache schema conversions, and avoid bytes/string/bytes conversions.

Why: fewer allocations, copies, GC scans, and function/queue operations per row. CPU now converts more bytes per second.

New bottleneck: serialized JSON and network/Kafka producer requests.

Cost: ownership rules become stricter; pooling can retain memory if misused.

Proof: allocated bytes/row and GC CPU fall; completed, not merely decoded, rows/s rises.

### Step 4: compact binary batches, compression, and Kafka producer tuning — 70,000 rows/s, 8.75×

Change: encode compact typed records, batch by bytes/rows/age, enable efficient batch compression, allow a bounded number of in-flight durable producer requests, and size producer memory explicitly.

Why: fewer Kafka requests, better compression, fewer wire/log bytes, and overlapped acknowledgements.

New bottleneck: one Kafka partition/consumer lane and destination row writes.

Cost: schema compatibility, compression CPU, slightly higher low-rate latency, and memory for accumulators.

Proof: batch occupancy and compression improve; request/record and wire bytes/logical byte fall; producer buffer wait remains bounded.

### Step 5: safe partition parallelism — 90,000 rows/s, 11.25×

Change: partition by stable key, preserve per-key order, add consumer lanes, and explicitly coordinate or relax cross-key transaction semantics according to the contract.

Why: broker, consumer CPU, and independent destination staging can work in parallel.

New bottleneck: target upserts, WAL, and indexes.

Cost: no simple global order; transaction/barrier state and skew handling become real distributed-systems work.

Proof: per-partition lag is balanced, consumers are busy, correctness tests cover cross-partition retries and ordering.

### Step 6: `COPY`/native load into staging plus set-based merge — 135,000 rows/s, 16.9×

Change: consumers create large destination batches, bulk-load staging, deduplicate by stable key/order, and finalize with set-based operations. Live CDC receives reserved loader capacity.

Why: protocol, parse, lookup, and commit overhead are amortized; append-oriented staging is fast; one merge handles many keys.

New bottleneck: destination WAL/storage or target merge scanning.

Cost: staging storage, batch state machine, ambiguous-commit recovery, and larger retry units.

Proof: destination statements/row and commits/row collapse; rows/load rise; staging time and merge time are measured separately; equality/retry tests pass.

### Step 7: rebuild-time index and infrastructure strategy — 165,000 rows/s, 20.6×

Change: build a shadow table with only required load-time indexes, bulk-build optional indexes later, use a read replica, and place source, Kafka, application, and destination on separately provisioned resources.

Why: destination avoids per-row secondary-index maintenance during the historical load; source and destination no longer fight for one disk/page cache; each service can use its own physical capacity.

New bottleneck: final index build/validation, destination merge/WAL, network, or source replica bandwidth depending on the run.

Cost: roughly another table copy, index-build time/space, cutover coordination, and infrastructure cost. Full lifecycle wall time must include index build and validation.

Proof: wall time includes scan, load, catch-up, index build, validation, and cutover; service latency remains healthy; post-cutover equality holds.

### Step 8: adaptive backpressure stabilizes 150,000 rows/s sustainable

The peak was 165,000, but the controller operates around 150,000 with memory, CDC age, source p99, destination queue, and error rate inside budgets. When the destination slows, it cuts backfill admission before queues explode. This may lower headline peak throughput while making the system safe for a multi-hour or multi-day run.

The 20× result did not come from “parallelism.” It came from fixing query granularity, work balance, allocation/copy cost, serialization, network/Kafka batching, parallel lanes with explicit ordering, destination bulk loading, index lifecycle, and resource isolation. On a system whose destination is already physically saturated, most earlier steps would yield little until destination capacity changes.

## 29. Applying the design to an Artie-like system

### What is publicly known

**PUBLICLY KNOWN.** Artie's public architecture describes a separated control plane and data plane with a reader, Kafka buffer, and writer/Transfer component. Public flush documentation says changes are temporarily buffered in memory, deduplicated by primary or unique key, flushed on time/distinct-message/byte conditions, written as an optimized batch, and followed by offset commit. Public backfill material describes historical scanning alongside live capture, optional replica reads, normal/interval/CTID strategies, parallel table backfills, online dual writes, catch-up, and atomic swap. Public monitoring material exposes row lag, ingestion lag, message-processing latency/count, and flush counts/latencies. These facts do not reveal the private algorithms behind scheduling, snapshots, partitioning, or cutover recovery.

### A hypothetical high-performance architecture

Everything below is **PLAUSIBLE IMPLEMENTATION**, not a claim about Artie:

```text
                         CONTROL PLANE
 pipeline config -> capability planner -> durable job/chunk manifest
                         | policies, epochs, budgets
                         v

                         DATA PLANE
 source primary ---- live-change reader ---- durable Kafka partitions ----+
       |                                                            CDC |
       +-> optional read replica                                        |
                    |                                                   |
             snapshot coordinator                                      |
                    | exported snapshot/change boundary                |
             byte-aware chunk scheduler                                |
                    | leased/fenced chunks                             |
             bounded parallel scanners                                |
                    | batch-owned binary rows                          |
             transform/encode workers                                 |
                    | bounded byte credits                             |
             backfill bulk lane ----------------------------------+     |
                                                                |     |
                                    priority/admission controller <-----+
                                                                |
                                      connector-specific batcher
                                                                |
                    +---------------------------+-------------------+
                    |                           |                   |
              OLTP COPY staging        warehouse files/load   lake files
                    |                           |                   |
              set-based upsert            set-based merge      manifest commit
                    +---------------------------+-------------------+
                                                |
                                      shadow/live generation state
                                                |
                                validate prefix -> final barrier
                                                |
                                  atomic/native cutover mechanism
```

### Where parallelism exists

- tables and byte-balanced chunks scan concurrently within source budgets;
- independent transform batches use CPU workers;
- Kafka partitions provide ordered parallel lanes where semantics allow;
- bulk files/COPY loads run concurrently up to destination capacity;
- independent tables or destination partitions finalize in parallel;
- index/file compaction can run separately when it does not violate cutover dependencies.

### Where ordering remains serial or coordinated

- each stable key needs a deterministic source order;
- multi-key transaction atomicity, if promised, needs a transaction coordinator/barrier;
- durable progress cannot pass an incomplete required prefix;
- a target table may permit only limited concurrent merges;
- promotion of one logical table is one state transition even if preparation is parallel;
- schema-generation changes must be ordered with data interpreted under that schema.

### Where buffers exist

- PostgreSQL/OS caches buffer source pages;
- scanner/client buffers hold a bounded read batch;
- transform queues hold byte-budgeted batches;
- Kafka durably holds accepted live changes and temporary downstream backlog;
- writer RAM holds only a bounded working set for deduplication/batching;
- destination staging or object-store files form the bulk-ingest buffer;
- the destination WAL buffers committed mutations for durable recovery.

Each buffer has an owner, byte limit, durability level, and release event. RAM is released after durable handoff or retry-safe completion; Kafka offsets advance after destination success; staging is cleaned after finalization and progress become durable.

### How backpressure protects production

The destination controller reserves CDC capacity and grants remaining byte credits to historical work. Slow merge/load drains fewer credits. Backfill batch queues fill, scanners stop leasing chunks, and source query concurrency falls. Independently, source p99/CPU/I/O policies can cut scanner permits even when downstream is empty. The live reader continues publishing within Kafka capacity so source-side change retention does not grow merely because historical work is slow.

### How memory remains bounded

The process has one global byte budget. Scanner batches, decoded/encoded forms, producer accumulation, consumer queues, dedup state, and destination batches lease from named pools. Batch ownership avoids accidental duplicate retention. Row, key, and age limits complement bytes. Oversized rows use a controlled path. Kafka holds durable backlog; the consumer pauses instead of mirroring it into RAM.

### How CDC stays current during a huge backfill

Live changes enter the durable path immediately. Destination scheduling gives them priority or reserved service. The shadow receives change versions with stable ordering metadata so historical rows cannot overwrite newer state. Historical data uses append/bulk operations where possible. After all historical chunks are complete, the coordinator drains to a precise boundary, validates the shadow, handles the final delta, and performs the destination-specific cutover.

### Likely scaling ceilings

At modest scale, destination per-batch overhead or source query granularity may dominate. After batching and parallelism, source replica storage, application CPU/memory bandwidth, Kafka hot partitions, cross-partition coordination, destination loader concurrency, target merge scan bytes, WAL/index maintenance, object-store file counts, and final validation become candidates. At very large table counts, control-plane manifests, connector API quotas, and fair scheduling also matter. The profiling loop, not the architecture diagram, identifies which is current.

## 30. One connected stage table

“Current bottleneck” below means the signal that would identify that stage as the present limiter.

| Stage | Current bottleneck | Possible solutions | Why solution works | What it costs | What metric proves it | Next likely bottleneck |
| --- | --- | --- | --- | --- | --- | --- |
| source storage | MB/s, IOPS, or latency/queue-depth ceiling | sequential/page scans, prefetch, bounded parallelism, NVMe/provisioned capacity, replica | turns small waits into larger concurrent reads or adds real capacity | source load, hardware, snapshot/replica complexity | device MB/s/IOPS/await, `pg_stat_io`, source p99, useful bytes/block | PostgreSQL CPU or network |
| PostgreSQL/OS cache | cold misses or production working-set eviction | replica, scan ring/locality, throttle, careful prewarm, controlled cold/warm runs | isolates or reduces destructive cache competition | replica cost, slower backfill | hits/reads, relation residency, major faults, foreground p99 | storage or executor CPU |
| index/heap access | many B-tree/heap probes and random blocks | choose seq scan, larger keyset ranges, physical locality, index-only only when valid, partitions | reduces probes and heap random I/O per returned byte | reads unrelated tuples, chunk/rewrite complexity | plan, heap/index blocks, heap fetches, rows/block | storage bandwidth or tuple CPU |
| chunk planning | skew and long straggler tail | sampled row/byte boundaries, many chunks, work stealing, adaptive split | keeps workers busy and bounds tail by smaller work unit | manifest/scheduler/checkpoint overhead | chunk byte/duration distribution, worker idle, tail time | shared source capacity |
| CTID/page scan | logical-range skew/random heap access | shared MVCC snapshot, page ranges, durable coverage, stable-key reconciliation | physical locality and balanced blocks | long snapshot, CTID instability, restart rules | block bytes/s, coverage, duplicates/misses tests, source impact | storage bandwidth or reconciliation |
| PostgreSQL CPU | cores full in visibility/deform/filter/encode | fewer columns, simpler predicates, efficient protocol/COPY, parallel plan, replica | lowers CPU/row or adds isolated cores | portability/type complexity, source load | CPU-seconds/million rows, serialization time, query p95 | client/network |
| DB connections | acquisition wait or excessive backend contention | bounded pool, one connection/active scanner, reuse, per-replica semaphores | reuses sessions while capping active work | admission delay | pool wait, active/idle, DB backends, completed throughput | source CPU/storage |
| scanner parallelism | one worker idle resources; many workers flatten/fall | concurrency sweep, dynamic queue, adaptive permits | overlaps I/O and uses cores until shared saturation | contention, memory, source impact | throughput curve, source p99, queue/CPU/I/O | source shared resource or application |
| application queue | volatile bytes grow or producers block | bounded batch queues, byte credits, high/low water marks, disk spill only if needed | absorbs short jitter and propagates sustained pressure | lower bursts, spill complexity | queue bytes/age, blocked time, RSS plateau | downstream service stage |
| application CPU | cores full in decode/convert/hash/encode/compress | remove transforms, binary format, batch/vector processing, parallel stages | lowers instructions/row and uses independent cores | schema/ordering complexity | CPU profile, CPU-sec/million rows, output throughput | allocations, memory, network |
| allocations/GC | high allocations and GC CPU/p99 | batch arenas, preallocation, reuse, size-class pooling, avoid conversions | fewer objects/copies to allocate and scan | ownership complexity, retained buffers | allocs/row, bytes/row, GC CPU/p99, RSS | memory bandwidth or serialization |
| memory copies | flat scaling with copy/cache stalls | streaming encode, ownership transfer, fewer forms, contiguous/vector batches | moves each byte fewer times | tighter API/lifetime constraints | copy profile, memory bandwidth, bytes copied/logical byte | NIC/network |
| network | link full, RTT seriality, or tiny packets | batch, compression, pipelining, bounded async, multiple streams, co-location/faster NIC | fewer requests, smaller wire bytes, overlap RTT | CPU, memory, latency, cost | wire/logical bytes, RTT, request size/rate, retransmits | producer/broker/service CPU |
| Kafka producer | buffer wait, low batch occupancy, high request latency | batch/linger, compression, durable idempotent sends, bounded in-flight requests | fuller compressed requests and overlapped acks | memory, CPU, low-rate latency | batch-size avg, compression, buffer wait, request latency/retries | partition or broker |
| Kafka partition | one lane hot and consumer maxed | more partitions, key/table partitioning, consumer groups, coordinator/watermarks | independent ordered lanes run in parallel | loss of global order, skew, coordination | per-partition bytes/lag, consumer utilization, watermark gap | broker or destination |
| Kafka broker | request queues, low handler/network idle, disk/replica lag | more brokers, leader balance, faster storage/NIC, batches/compression, right replication | distributes or reduces broker work | cost and operations; durability tradeoffs if changed | queue/local/remote time, idle %, disk/NIC, ISR/follower lag | consumers/destination |
| Kafka backlog | record/byte/time lag grows | speed destination, scale valid lanes, reserve CDC, throttle backfill/upstream, retention sizing | restores `service > arrival` or buys finite recovery time | freshness, storage, slower backfill | lag slope, oldest age, time-to-retention, catch-up rate | destination stage |
| destination protocol | many statements/commits and RTT | prepared/multi-row writes, larger bounded transactions, COPY/native load | amortizes parse, RPC, transaction, and fsync costs | batch failure scope, memory, latency | rows/request, statements/row, commits/row, load p95 | staging/merge/WAL |
| staging/finalization | load fast but merge slow | set-based upsert/delete, partition pruning, multi-step merge, larger batches | one relational operation handles many rows and fewer target scans | storage, delayed visibility, state machine | load vs queue vs merge time, bytes scanned/written | target storage/compute |
| destination indexes | high WAL/CPU/write amplification or hot pages | defer optional indexes during shadow build, bulk build, partition-wise load, dedupe | avoids per-row index maintenance | query availability, build/cutover time, storage | WAL/row, index writes/splits, full lifecycle wall time | WAL/storage or index build |
| destination WAL/fsync | WAL device/commit/checkpoint ceiling | bounded large transactions, COPY, fewer indexes, adequate WAL/checkpoint sizing, fast reliable WAL | amortizes commits and smooths durable writes | larger retries/locks/replication lag | WAL B/s, fsync count/time, commit p95, checkpoints | data storage/CPU/locks |
| destination writers | throughput falls with more writers, locks/conflicts rise | disjoint key/partition lanes, separate load/merge semaphores, adaptive concurrency | parallelizes independent work without overlapping hot state | routing and coordination | completed throughput sweep, lock/conflict/queue/WAL | serial merge or physical capacity |
| online cutover | long validation/fence or ambiguous swap | prebuild/validate, exact progress boundary, bounded final delta, destination-native atomic swap, durable state | moves expensive work outside the fence and makes transition recoverable | double storage, dependencies, coordinator complexity | availability, fence/swap time, equality, retry tests | post-cutover verification/cleanup |
| whole system | queue/latency explodes near capacity | headroom, priorities, adaptive feedback, hard budgets, isolation | operates below unstable saturation and moves pressure safely | lower headline peak, controller complexity | sustainable output, p95/p99, bounded queues/memory/errors | whichever stage becomes slowest next |

## 31. Final continuous narrative: a row starts in PostgreSQL

A row starts as a visible tuple version on a PostgreSQL heap page. A snapshot coordinator has already chosen the logical moment that historical scanning represents and the durable change-stream boundary that will repair everything after that moment. The chunk scheduler does not assume equal integer ranges contain equal work. It estimates rows and bytes, creates many bounded chunks, records coverage durably, and leases them with fencing so a crashed worker cannot later complete stale work.

A scanner acquires a source permit, memory credits, and one pooled connection. If the job uses a key range, PostgreSQL walks the B-tree and fetches the tuple's heap page; if the table and workload justify a physical strategy, the worker scans a page interval under the shared snapshot and still treats the primary key—not CTID—as row identity. Neighboring work remains physically local where possible. Prefetch and a few workers keep capable storage busy, but the admission controller reduces permits if production query latency, CPU, or I/O crosses its budget. A replica can isolate this work from the primary.

PostgreSQL checks MVCC visibility, deforms only requested columns, and streams them through an efficient protocol in a query large enough to amortize planning and B-tree startup. It does not compute optional JSON or transformations on valuable source CPU. The row arrives in a batch-owned application buffer. The batch has a byte lease, so wide rows cannot multiply across workers until the process runs out of memory.

A transform worker applies a cached schema conversion plan. It writes typed values into a reusable contiguous batch buffer rather than constructing a tree of temporary objects. It does not convert a byte field to a string and back. Several batches transform in parallel; sequence metadata or stable-key partitioning preserves the required order. The system profiles CPU, allocations, GC, and copies, so it knows whether more transform workers add useful completed output or merely contend for memory bandwidth.

The serializer builds a compact batch. It flushes when byte, row/key, or age limit is reached. Bytes cap memory and request size, rows cap cardinality, and age caps freshness. Compression sees repetition across the batch, reducing network and Kafka log bytes. A small bounded number of asynchronous requests overlaps network latency. The producer waits for the durability required by the stream and uses retry semantics that do not knowingly trade correctness for a benchmark.

Kafka chooses a partition according to the ordering contract. If events for one key may be applied independently, stable-key partitioning keeps that key ordered while allowing other partitions to progress in parallel. If a source transaction must appear atomically across keys, transaction metadata and a barrier/coordinator hold fragments until the transaction is complete. Kafka appends the compressed batch to a replicated partition log, using sequential I/O and page cache. Kafka now owns a durable copy, so volatile upstream buffers can be released after the proper acknowledgement.

The consumer fetches batches rather than individual events. It never pulls the entire backlog into RAM. A byte-bounded queue feeds destination batching; at the high-water mark the consumer pauses partitions, and it resumes only below the low-water mark. Historical work has lower priority than live CDC, so a destination slowdown first removes backfill credits. Kafka lag may grow temporarily, but memory remains bounded and the oldest-event-age alarm shows the real freshness effect.

For current-state replication, the destination batcher can retain only the latest ordered event per stable key; history mode keeps every legitimate event. The batch crosses a destination-specific connector boundary. For PostgreSQL, that may mean binary `COPY` into batch-scoped staging. For a warehouse, it may mean writing well-sized compressed files and invoking its native bulk loader. The connector does not turn everything into row-by-row SQL.

Staging accepts append-oriented data quickly. A set-based operation then resolves inserts, updates, and tombstone deletes against the target. Batch identity, source ordering metadata, and durable progress make a retry after ambiguous success safe. The destination updates heap/columnar files, required indexes or metadata, and WAL/log structures. Transactions are large enough to amortize commits and fsync, but bounded so locks, rollback, replication lag, and recovery remain manageable. Load concurrency and merge concurrency have separate semaphores because their resource curves differ.

During an online rebuild, the existing live table still serves queries while a shadow generation receives historical batches and ordered live changes. Historical data uses the high-throughput bulk path; CDC has reserved capacity. When all chunks are covered, the coordinator drains to an exact prefix, validates the shadow and its schema/index/dependency requirements, processes a bounded final delta, and invokes the destination's atomic or best available cutover mechanism. Durable promotion state lets recovery determine what happened if the client loses the cutover response. The old table remains available according to rollback policy.

At every arrow, the system records input/output rows and bytes, service time, queue time, queue bytes/age, utilization, retries, and correctness progress. If source storage becomes faster, PostgreSQL or application CPU may become slowest. If allocation and serialization improve, the network or Kafka partition may become slowest. If Kafka scales, destination merge/WAL/indexes may become slowest. The controller operates below those ceilings with headroom, and engineers repeat the same evidence loop until wall-clock time, CDC freshness, production impact, cost, and recovery behavior meet the objective.

That is how high throughput emerges: not from one magic worker count, but from making every required stage do less work per logical byte, using parallelism only where work is independent, amortizing fixed costs with bounded batches, preserving durable ordering and retry semantics, and forcing overload to travel upstream instead of accumulating invisibly in RAM.

## 32. Artie facts, plausible machinery, and claims you can defend

### PUBLICLY KNOWN

- Artie describes a Reader -> Kafka -> Writer/Transfer data path and a separate control/data plane.
- Its flush documentation describes in-memory deduplication by primary/unique key, time/count/byte triggers, optimized destination batches, and offset commit after successful destination work.
- Its public backfill material describes simultaneous historical scanning and live buffering, optional read replicas, normal/interval/CTID strategies, dual writes for online backfills, catch-up, and atomic swap.
- Artie publishes benchmark and customer figures, including large wall-clock improvements for particular tested backfills. These are workload- and environment-specific product claims, not universal laws.
- Its public metrics include Kafka row/ingestion lag, message processing, and flush counts/latencies.

### PLAUSIBLE IMPLEMENTATION

- byte-aware chunk estimation plus runtime work stealing;
- shared snapshots and stable-key reconciliation for physical scans;
- global memory credits and separate CDC/backfill priorities;
- batch-owned binary representations and pooled scratch buffers;
- partition-local ordering with barriers only where transaction semantics require them;
- connector-specific loader and merge concurrency;
- durable cutover generations, fencing, and ambiguous-outcome recovery;
- feedback controllers driven by source impact, queue pressure, lag, and destination latency.

These mechanisms are reasonable ways to implement the public behaviors. Do not say Artie uses them unless Artie has publicly documented that exact mechanism.

### ALTERNATIVE DESIGNS

- one global ordered Kafka partition for simpler correctness;
- no Kafka for historical bytes, using a separately checkpointed direct bulk path;
- native PostgreSQL parallel sequential scan instead of application page workers;
- byte-balanced primary-key ranges instead of CTID;
- immutable snapshot export/object-store files instead of live source scanning;
- periodic reconciliation rather than transaction-atomic cross-partition apply;
- view/pointer promotion when table rename/swap is unavailable.

## 33. Sources and further reading

Artie public material:

- [Architecture](https://artie.com/docs/concepts/architecture)
- [Flush rules](https://artie.com/docs/concepts/flush-rules)
- [Flush-rule tuning](https://artie.com/docs/concepts/flush-rules/tuning)
- [Backfills](https://artie.com/docs/pipelines/backfill)
- [Online database backfills](https://www.artie.com/blogs/online-database-backfill)
- [PostgreSQL CTID scanning](https://www.artie.com/blogs/postgres-ctid-scanning)
- [Multi-step merge](https://www.artie.com/blogs/multi-step-merge)
- [Available metrics](https://artie.com/docs/monitoring/available-metrics)
- [Artie versus AWS DMS benchmark](https://www.artie.com/blogs/artie-vs-aws-dms)

PostgreSQL primary documentation:

- [EXPLAIN](https://www.postgresql.org/docs/18/sql-explain.html)
- [Parallel plans and scans](https://www.postgresql.org/docs/18/parallel-plans.html)
- [Visibility map](https://www.postgresql.org/docs/18/storage-vm.html)
- [Resource consumption and I/O concurrency](https://www.postgresql.org/docs/18/runtime-config-resource.html)
- [CLUSTER](https://www.postgresql.org/docs/18/sql-cluster.html)
- [COPY](https://www.postgresql.org/docs/18/sql-copy.html)
- [Cumulative statistics and `pg_stat_io`](https://www.postgresql.org/docs/18/monitoring-stats.html)
- [WAL introduction](https://www.postgresql.org/docs/18/wal-intro.html)
- [WAL configuration](https://www.postgresql.org/docs/18/wal-configuration.html)
- [`pg_prewarm`](https://www.postgresql.org/docs/18/pgprewarm.html)
- [`pg_buffercache`](https://www.postgresql.org/docs/18/pgbuffercache.html)

Apache Kafka primary documentation:

- [Kafka design](https://kafka.apache.org/41/design/design/)
- [Kafka protocol and batching](https://kafka.apache.org/41/design/protocol/)
- [Producer configuration](https://kafka.apache.org/40/configuration/producer-configs/)
- [Kafka monitoring](https://kafka.apache.org/40/operations/monitoring/)


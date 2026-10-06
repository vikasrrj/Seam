# Benchmarks

All reported backfill runs compare source and destination rows after the timer.
The Snowflake numbers use a live account. Validation and promotion are not part
of the timed backfill window.

## Results

| Path | Rows | Workers | Wall time | Throughput |
| --- | ---: | ---: | ---: | ---: |
| PostgreSQL backfill | 100,000 | 1 | 29.59 s | 3,379 rows/s |
| PostgreSQL backfill | 100,000 | 4 | 14.89 s | 6,717 rows/s |
| Snowflake bulk staging | 1,000 | 1 | 3.278 s | 305.1 rows/s |
| Snowflake bulk staging | 4,000 | 4 | 5.036 s | 794.3 rows/s |

The PostgreSQL pair used 1,000-row chunks and a 16-byte payload. Raw matrix
output is in `docs/benchmark-matrix-100k.tsv`.

The Snowflake staging rows used a 96-byte payload on an X-Small warehouse. The
older SQL loader took 6.237 seconds for 1,000 rows. The bulk loader writes gzip
JSON, uploads it with `PUT`, and loads it with `COPY INTO`.

## Snowflake end to end

These runs used 10,000 rows, four configured workers, a 250 ms marker poll, and
exact source-to-shadow verification.

| Change | Chunks | Markers | Wall time | Throughput |
| --- | ---: | ---: | ---: | ---: |
| 1,000-row chunks | 10 | 20 | 62.316 s | 160.5 rows/s |
| 5,000-row chunks | 2 | 4 | 20.055 s | 498.6 rows/s |
| One chunk | 1 | 2 | 19.203 s | 520.8 rows/s |
| Cap workers to chunk count | 1 | 2 | 16.632 s | 601.2 rows/s |
| Apply markers with one procedure call | 1 | 2 | 14.901 s median | about 671 rows/s |

The last row is the median of two runs: 14.789 and 15.012 seconds. It is 10.4%
faster than the worker-cap result. The procedure keeps the existing transaction,
lease fence, replay ledger, and frontier compare-and-set.

Latest Snowflake phase times:

| Phase | Time |
| --- | ---: |
| Two marker transactions | 5.854 s median |
| Finalization | 2.759-3.241 s |
| COPY transaction | 1.679-1.700 s |
| PUT | 0.980-1.739 s |

Marker apply is still the largest serial section. Finalization is next.

## Kafka

The local Kafka benchmark used one partition, replication factor one, and a
224,077-byte transaction containing 1,000 rows.

| Work | Median or range |
| --- | ---: |
| Full produce, fetch, reassembly, and decode | 14.69 ms median |
| Produce acknowledgement | 2.77-3.94 ms |
| Fetch and reassembly | 6.52-7.73 ms |
| Decode | 4.36-5.90 ms |
| Go allocation | 2.27-2.41 MB, about 10,190 allocations |

Kafka is not the current limit in this local workload. Seam keeps one partition
because transactions, fragments, markers, and the destination frontier require
one order. Adding partitions needs a new ordering protocol.

## Changes tried

| Change | Result | Decision |
| --- | --- | --- |
| Use 5,000-row chunks | 62.316 s to 20.055 s | Keep |
| Cap workers to sealed chunks | 19.203 s to 16.632 s; lease calls 40 to 2 | Keep |
| Single-marker stored procedure | 16.632 s to 14.901 s median | Keep |
| Remove pre-scan cleanup | 20.830 s and 21.594 s | Revert |
| Stop idle workers dynamically | 19.548 s and 20.943 s | Revert |
| Poll markers every second | 21.896 s | Revert |
| Join ledger and frontier reads | 20.503 s median | Revert |
| Wait 1.5 s to batch markers | 23.881 s | Revert |
| Join finalization reads | 20.144 s median versus 20.117 s control | Revert |
| Skip marker route lookup | 20.745 s | Revert |
| Driver multi-statement marker request | Stalled beyond 188 s | Revert |

## Correctness checks

The retained code passed unit tests, race tests, `go vet`, exact 10,000-row
comparisons, and the live crash/replay test. The recovery test stopped the sink
and a worker, took over their leases, replayed, validated, promoted twice, and
finished with an exact 520-row match in 225.23 seconds.

## Run again

PostgreSQL matrix:

```bash
./scripts/bench-matrix.sh
```

Snowflake loader comparison:

```bash
make benchmark-snowflake-load
```

Snowflake end-to-end profile:

```bash
SEAM_RUN_SNOWFLAKE_BACKFILL_PROFILE=1 \
SEAM_SNOWFLAKE_PROFILE_ROWS=10000 \
SEAM_SNOWFLAKE_PROFILE_CHUNK_SIZE=10000 \
SEAM_SNOWFLAKE_PROFILE_WORKERS=4 \
SEAM_SNOWFLAKE_PROFILE_POLL_MS=250 \
go test -tags='integration snowflake_integration' -count=1 \
  -run '^TestSnowflakeBackfillProfile$' -v ./integration
```

Kafka transport:

```bash
go test -tags=integration -run '^$' \
  -bench '^BenchmarkKafkaTransport/rows_1000$' \
  -benchtime=20x -count=3 ./integration
```

These are small shared-host tests, not production capacity claims. Snowflake
service variance, warehouse size, row width, cache state, and credit limits can
change the absolute numbers.

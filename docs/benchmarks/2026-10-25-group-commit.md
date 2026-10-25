# 2026-10-25 · WAL group commit

Same machine, disks, services and traffic shape as the
[baseline](2026-10-25-baseline.md). Raw JSON in
[`2026-10-25-group-commit/`](2026-10-25-group-commit/).

## Change

One writer thread owns WAL appends. It drains everything queued, appends the
batch, fsyncs once and acknowledges every request in it. No timer: idle
requests wait only for their own fsync, and under load the batch grows while
the previous fsync runs. The flush gate and intents are unchanged: a request
still holds the gate until its record is durable.

Two things had to change with it, found by the first measurement after the
writer landed, which showed 1.45 records per fsync in the server although the
in-process stage showed 30:

- `check_capacity` read the WAL size by locking the WAL, which the writer holds
  through every fsync. Every request therefore waited for the fsync in flight
  before it could even queue, so arrivals were synchronised into batches of
  one. The writer now publishes the size in an atomic.
- `fs-bench load` multiplexed every sender over one HTTP/2 connection, which
  capped the generator near 3 000 requests per second. Each sender now has its
  own connection.

## Ingest stage (in process, 5 000 requests, 60 000 spans)

| WAL stage | disk B (plain NVMe) | disk A (NVMe + LUKS) |
|---|---:|---:|
| append + fsync per request, 1 sender (baseline) | 2.08 s | 3.66 s |
| group commit, 64 senders | 90 ms | 160 ms |
| records per fsync | 29.4 | 30.1 |

23× on both disks; the per-span cost of the WAL drops from 33 µs to 1.5 µs,
below the conversion cost.

## Load (release server, all roles, WAL on disk B, bucket RustFS, 30 s)

| senders | req/s | spans/s | p50 | p95 | p99 | errors |
|---:|---:|---:|---:|---:|---:|---:|
| 8, baseline | 2 196 | 26 354 | 3.20 ms | 5.23 ms | 6.95 ms | 0 |
| 32, baseline | 2 216 | 26 589 | 13.40 ms | 24.73 ms | 49.62 ms | 0 |
| 8 | 5 490 | 65 875 | 0.85 ms | 1.24 ms | 40.43 ms | 0 |
| 32 | 9 918 | 119 015 | 1.56 ms | 2.50 ms | 40.79 ms | 10 699 (3.5%) |
| 128 | 8 608 | 103 291 | 6.80 ms | 10.29 ms | 42.00 ms | 65 485 (20%) |

The generator starts every sender behind a barrier after all connections are
open and starts the clock when the barrier opens; an earlier run without the
barrier counted requests sent before the clock started and read 9 524 at 32
senders, within 5% of the honest number.

Across the three runs: 720 970 records in 114 054 batches, 6.3 records per
fsync, 53 s of fsync time over 90 s of load. 8.65 million spans ingested, 65
files flushed, no flush failure.

**Reading.** Throughput now scales with concurrency: 4.3× at 32 senders, with
p50 under 2 ms. The errors at 32 and 128 senders are all
`RESOURCE_EXHAUSTED: tenant buffer full, flush pending`: at 114 k spans/s the
tenant produces about 180 MB/s of Arrow, the flusher writes one file at a
time through sort, Parquet, upload and registration, and the 512 MiB cap on
live plus flushing rows is reached. That is backpressure working as designed,
and it names the next bottleneck: the flush path, which is sequential per
tenant and encodes a whole file in memory. The p99 of about 40 ms in every
run is the same thing seen from the other side, a flush competing for CPU.

Against the DESIGN.md target of 150 MB/s of OTLP per ingester: 9 900
requests × 4 KiB is about 40 MB/s, from 9 MB/s at the baseline. The WAL is no
longer the limit; the flusher is.

## Review fixes in the same change set

- The WAL size the capacity check reads was published only after a commit, so
  a WAL that reached its cap stayed "full" after the checkpoint freed segments
  and the ingester refused writes until a restart. The checkpoint now publishes
  the size under the lock; a test fills a small WAL, flushes and writes again.
- The writer queue was unbounded: a slow disk would have let every in-flight
  request hold its decoded message and record outside the buffer and WAL
  limits. The queue now has a record limit with a bounded wait and a byte
  budget that refuses immediately, both answered as `RESOURCE_EXHAUSTED` and
  counted in `fusescope_wal_backpressure_total`.

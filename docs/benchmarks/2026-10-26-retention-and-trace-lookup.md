# 2026-10-26 · compact retention and trace-id pushdown

Same machine, disks, services and traffic shape as the
[baseline](2026-10-25-baseline.md); code at e6fb243 (hot-window retention
keeps the file's rows as written, 8 s by default; trace and span ids pushed
into the hot scan). Raw JSON and the histogram page in
[`2026-10-26-retention-and-trace-lookup/`](2026-10-26-retention-and-trace-lookup/).
Compare with the [parallel coalescer](2026-10-26-parallel-coalesce.md) run,
which had the same load and the same concurrent query suite.

Commands as before: `fs-bench load` for 30 s with `--metrics`, and
`fs-bench query --iterations 40` against the same tenant at the same time.

## Results

| run | req/s | spans/s | refused | flushes | rows per flush | total per flush | persisted at end | hot scans in window | hot scan p95 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 32 senders, before (12b85a4) | 7 345 | 88 138 | 2 648 | 13 | 201 776 | 969 ms | 735 MiB | 15 | ≤ 20 ms |
| 32 senders, after (e6fb243) | 10 419 | 125 028 | 760 | 18 | 202 450 | 980 ms | 571 MiB | 143 | ≤ 20 ms |
| 8 senders, before | 4 215 | 50 577 | 0 | 7 | 206 988 | 935 ms | 506 MiB | 32 | ≤ 5 ms |
| 8 senders, after | 4 169 | 50 027 | 0 | 7 | 207 588 | 1 052 ms | 293 MiB | 173 | ≤ 10 ms |

Queries during the load, p50 / p95 / max:

| query | 32 senders, before | 32 senders, after | 8 senders, before | 8 senders, after |
|---|---:|---:|---:|---:|
| trace by id | 908 ms / 2.12 s / 2.23 s | 196 / 384 / 429 ms | 751 ms / 1.39 s / 1.42 s | 141 / 175 / 343 ms |
| span list, 1 service, 15 min | 158 / 200 / 202 ms | 158 / 200 / 202 ms | 114 / 165 / 183 ms | 114 / 165 / 183 ms |

No hot scan timed out; no warning in the server log.

## Reading

**The trace lookup is 5 to 8 times faster under load.** The hot scan now
ships only the rows of the requested trace instead of every retained row,
so the 40 iterations of the query suite completed inside the 30 s window
(143 hot scans against 15 before) and `trace by id` p95 fell from 2.1 s to
384 ms at 32 senders. What remains is the file side: every L0 file whose
time range overlaps is still opened, which is roadmap B3, the trace-id
sidecar.

**Retention costs what the file costs.** 571 MiB at the end of the
32-sender run is 8 s of 125 k spans/s kept as the sorted batches the files
were written from, about 570 bytes per span for this traffic shape; before,
15 s of chunks and raw batches held 735 MiB at a lower rate. Refusals fell
from 2 648 to 760 and the per-tenant cap stopped being the limit.

**Throughput went up as a side effect.** Fewer refusals and a lighter hot
scan left the senders and the conversion path more of the machine:
125 k spans/s is the highest measured so far. Flush cost per row is
unchanged; Parquet encode is still 60% of it (roadmap A3).

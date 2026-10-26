# 2026-10-26 · cost of the gate under load

Same machine, disks, services and traffic shape as the
[baseline](2026-10-25-baseline.md); code at 163507f (gate held from append
to push only, merge scratch reserved inside the cap, hot-scan and checkpoint
histograms). Raw JSON and the histogram page in
[`2026-10-26-gate-costs/`](2026-10-26-gate-costs/). The OTLP port was 14317
because a collector held 4317 on this machine.

Commands, run together: `fs-bench load --endpoint 127.0.0.1:14317 --duration-secs 30 --concurrency C --metrics 127.0.0.1:9464`
and `fs-bench query --endpoint 127.0.0.1:8815 --iterations 40` against the
same tenant, after a 5 s warm-up so the queries found rows.

The flush cut, the checkpoint and the hot-window snapshot now take the gate
exclusively, which waits for every request between its append and its push.
This run measures what that wait costs while the ingester is loaded and
queries hit the hot window.

## Results

| senders | req/s | spans/s | errors | flushes | rows per flush | concat | sort | encode | total per flush |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 32 | 6 637 | 79 650 | 10 673 | 9 | 229 487 | 164 ms | 197 ms | 607 ms | 1 076 ms |
| 8 | 4 513 | 54 151 | 0 | 7 | 207 053 | 36 ms | 175 ms | 578 ms | 869 ms |

Gate-protected operations, over the whole session (warm-up and both runs,
402 snapshots and 20 checkpoints):

| operation | ≤ 1 ms | ≤ 2 ms | ≤ 5 ms | ≤ 20 ms | ≤ 50 ms | mean |
|---|---:|---:|---:|---:|---:|---:|
| hot-window snapshot, gate wait included | 89.6% | 95.0% | 97.3% | 99.5% | 100% | 0.67 ms |
| checkpoint, gate wait and I/O included | 5% | 5% | 15% | 100% | 100% | 9.0 ms |

During the 32-sender run alone the snapshot p95 was within 50 ms and the
checkpoint p95 within 20 ms; during the 8-sender run, 5 ms and 20 ms.

Queries during the load (p50 / p95):

| query | 32 senders | 8 senders |
|---|---:|---:|
| trace by id | 484 ms / 4.15 s | 778 ms / 1.47 s |
| span list, 1 service, 15 min | 135 / 144 ms | 114 / 132 ms |
| p99 per service, 24 h | 141 / 156 ms | 177 / 189 ms |
| dashboard, 5 min | 126 / 128 ms | 101 / 134 ms |
| top operations, 1 h | 157 / 166 ms | 181 / 192 ms |

## Reading

**The gate is cheap.** A snapshot waits for the requests already past their
append, which is one group commit; 95% of snapshots took 2 ms or less with
32 senders writing, and the worst was under 50 ms. Checkpoints are
dominated by their own fsync and segment removal, 9 ms on average.

**The hot window itself is not cheap.** Two hot scans timed out at the
querier's 2 s limit during the 32-sender run, and `trace by id` reached a
4 s p95. That is the scan after the snapshot: filtering 800 MiB of live and
retained rows for one trace id, then reading every file without a trace-id
index. The gate contributes under 50 ms of it. The trace-id index and a
narrower hot-window predicate are the next query-side items.

**Admission refused 10 673 requests at 32 senders** at the tenant cap and
the tenant buffer cap, as in the previous run; the coalescer still merges one
chunk at a time (1 093 batches, 81 ms each) and runs behind, so part of the
retention is raw batches. The budgets are now in place for merging groups in
parallel.

# Dashboard

`docs/ops/grafana/fusescope-engine.json` is a Grafana dashboard over the
engine's `/metrics`, served on the metrics port (9464 in the image and the
chart) next to `/healthz` and `/readyz`. Scrape every pod of every role;
the dashboard filters by the `instance` label Prometheus adds, so it needs
nothing else from the scrape configuration. Import it in Grafana and pick
the Prometheus data source.

| Row | What it shows |
|---|---|
| Ingest | requests and spans per second; every refusal (rejected spans, rate limit, capacity, WAL backpressure); WAL size, group commits and the mean share of time each ingester spends in fsync; buffer bytes by state |
| Flush and coalescer | flushes, failures, retries, superseded flushes and replay mismatches; each flush stage's time per flush; output bytes and rows; the largest writer footprint; unconfirmed files; coalescer activity and latency |
| Hot window and checkpoint | hot scan latency and volume, batches the trace index skipped, querier fan-outs and unavailable ingesters; checkpoint latency |
| Query | latency p50, p95 and p99 (`fusescope_query_seconds`); statements, errors and authentication refusals; prepared plans; trace index use; rows returned |
| Compactor | passes, inputs, outputs and bytes; failures, conflicts, lost leases and hours over the input cap; derived tables built and skipped; retention; the orphan collector |
| Security | failed and refused TLS handshakes; stale TLS files or JWT keys and their reload failures; uptime, whose drops are restarts |

`fusescope_query_seconds` covers every `DoGet` and `DoAction` of an
authenticated caller, from the request to the end of its stream, however
it ends, in buckets from 5 ms to 300 s. `query.timeout_secs` is at most
300 (60 by default), so every execution ends within a finite bucket and a
slow quantile is never clipped to a lower bound; requests
refused for their token or their ticket are left out so they do not pull
the quantiles down. `fusescope_query_rows_total` counts the rows those
executions sent.

`the_dashboard_queries_only_rendered_metrics` renders `/metrics` with every
component registered and fails if the dashboard queries a metric the
engine does not render, so a renamed metric breaks the build, not the
dashboard.

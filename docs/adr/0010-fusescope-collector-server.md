# 0010 · fusescope-collector-server is the collector

- Status: accepted
- Date: 2026-11-08
- Amends: [0006](0006-collector-exports-otlp.md)

## Context

ADR 0006 names `fusescope-otel-collector`, the AGPL-3.0 fork of the SigNoz
collector, as the collector that exports OTLP to the ingesters.
`fuseone-io/fusescope-collector-server` (Apache-2.0) replaces it: a FuseScope
distribution of the OpenTelemetry Collector built from upstream components,
with its own clean-room ClickHouse exporter and schema command. Its
[ADR 0003](https://github.com/fuseone-io/fusescope-collector-server/blob/main/docs/adr/0003-clickhouse-and-engine.md)
keeps ClickHouse a supported output next to the engine.

## Decision

- `fusescope-collector-server` is the collector of ADR 0006. It exports
  standard OTLP to the ingesters over mTLS with one `fst_` token per tenant,
  and writes ClickHouse and the engine together for the comparisons.
- The engine still owns its schema in `fs-schema`; the collector's ClickHouse
  schema is not an input to it.
- ClickHouse stays a supported product after the engine's cutover. The cutover
  removes ClickHouse from FuseScope's own production and makes Flight the
  default provider; `clickhousetelemetrystore` stays as an optional provider.
  Removing it takes a deprecation ADR, an announced window and a migration path.
- The clean-room rule of ADR 0008 does not change: the AGPL collector's source,
  including its git history, is never opened. `fusescope-collector-server` is
  Apache-2.0 and may be read.

## Consequences

The engine depends on the collector's phase 7 (engine output, dual-write) for
M1, ahead of its phase 6 cutover, and on phases 6 and 8 for its own cutover.
The ingesters' OTLP listener needs mTLS before dual-write can connect. The Go
backend keeps two telemetry providers with no end date.

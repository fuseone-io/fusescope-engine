# 0007 · Logical schema compatible with the Go backend, physical encoding free

- Status: accepted
- Date: 2026-10-24

## Context

The Go backend references over a hundred logical field names through
pkg/telemetryschema and the statementbuilder packages: timestamp, trace_id,
span_id, duration_nano, has_error, attributes_string, attributes_number,
attributes_bool, resources_string, resource_fingerprint, events, links, scope
and the promoted-attribute mechanism. It already separates logical fields from
physical columns. The ClickHouse tables also carry camelCase aliases (traceID,
durationNano, serviceName) for the legacy v2 tables and the legacy
query-service, which ADR 0005 does not port.

## Decision

The engine keeps the logical field names and semantics the Go backend uses, so
the DataFusion dialect of the statement builders is a syntax translation. The
physical encoding is chosen for Parquet:

- trace_id and span_id are FixedSizeBinary, not hex strings. The dialect
  converts literals; a hex() UDF serves the UI.
- events and links are List of Struct, not arrays of JSON strings.
- Series labels are a Map, not a JSON string.
- ts_bucket_start survives as a virtual partition column because the query
  builder emits that predicate and it maps to partition pruning.
- CamelCase aliases, the v2 tables and the env column of samples are dropped.
- Promoted attributes exist from day one for the fields the UI depends on; the
  compactor promotes more based on query statistics.

## Consequences

Phase 1 Go work shrinks to a dialect plus a storage mapping. Existing Go test
fixtures stay valid. Anything that only the legacy code paths used is gone and
must be rebuilt on querier v5 if still needed.

# OTLP to schema mapping

Clean-room specification (ADR 0008). Every rule names its source: the OTLP
protocol definitions (`opentelemetry/proto/*.proto`), the OpenTelemetry
semantic conventions (semconv), or observed data from the current pipeline.
Nothing here derives from the collector's exporter source.

## Identity and time

| Field | Rule | Source |
|---|---|---|
| trace_id | 16 bytes as given. Any other length rejects the span and counts `spans_rejected`. | OTLP `Span.trace_id` is defined as 16 bytes |
| span_id | 8 bytes as given. Any other length rejects the span. | OTLP `Span.span_id` is 8 bytes |
| parent_span_id | 8 bytes, or null when empty. Other lengths reject the span. | OTLP: empty parent means root span |
| timestamp | `start_time_unix_nano` as nanosecond UTC timestamp | OTLP |
| duration_nano | `end - start`, saturating at zero when end precedes start | OTLP; negative durations are observed from clock skew and clamped |
| ts_bucket_start | `floor(start_seconds / 1800) * 1800` | engine partition key; the Go query builder emits predicates on this name |
| flags | `Span.flags` as is | OTLP |
| is_remote | `""` when bit 0x100 is clear, else `"true"`/`"false"` from bit 0x200 | OTLP `SpanFlags` enum |

## Enumerations

| Field | Rule | Source |
|---|---|---|
| kind | OTLP enum value; unknown values keep the number, text becomes `Unspecified` | OTLP `SpanKind` |
| kind_string | `Unspecified`, `Internal`, `Server`, `Client`, `Producer`, `Consumer` | OTLP enum names, without the `SPAN_KIND_` prefix |
| status_code | `Status.code`, 0 when absent | OTLP `StatusCode` |
| status_code_string | `Unset`, `Ok`, `Error` | OTLP enum names |
| status_message | `Status.message`, empty when absent | OTLP |
| has_error | `status_code == 2` | OTLP: only `STATUS_CODE_ERROR` means error |

## Attributes

Span attributes are split by value type:

| OTLP value | Destination | Rendering |
|---|---|---|
| string | attributes_string | as is |
| int | attributes_number | f64; values above 2^53 lose precision, accepted |
| double | attributes_number | as is |
| bool | attributes_bool | as is |
| bytes | attributes_string | standard base64 |
| array, kvlist | attributes_string | JSON text |
| empty | dropped, counted | OTLP allows `AnyValue` with no value |

Resource, scope, event and link attributes go to string-only maps. Scalars
render with Rust `Display` (`3`, `0.5`, `true`), bytes as base64, arrays and
kvlists as JSON. Source for the three-way split and the string-only resource
map: the logical schema the Go backend already consumes (ADR 0007).

Limits (defaults in `limits.rs`): at most 256 attributes per set, keys up to
256 bytes, values truncated at 8 KiB on a UTF-8 boundary, 128 events and 128
links per span. Excess is dropped and counted, never rejected.

## Resource fingerprint

xxh3-64 over the resource's string map sorted by key, each key and value
prefixed with its byte length as little-endian u64. Engine internal; it groups rows of one resource and is
never compared with other systems' values.

## Promoted columns

A copy of selected attributes into native columns, for pruning and fast
filters. The attribute also stays in its map. Keys are tried in order so
current and legacy semconv names feed one column.

| Column | Scope | Keys (priority order) | Source |
|---|---|---|---|
| service_name | resource | service.name | semconv resource |
| deployment_environment | resource | deployment.environment.name, deployment.environment | semconv; the second is the pre-1.27 name |
| http_route | span | http.route | semconv HTTP |
| http_method | span | http.request.method, http.method | semconv HTTP stable, then pre-stable |
| response_status_code | span | http.response.status_code, http.status_code, rpc.grpc.status_code | semconv HTTP stable, pre-stable, gRPC |
| db_system | span | db.system.name, db.system | semconv database stable, pre-stable |
| db_name | span | db.namespace, db.name | semconv database stable, pre-stable |
| db_operation | span | db.operation.name, db.operation | semconv database stable, pre-stable |
| rpc_system, rpc_service, rpc_method | span | same names | semconv RPC |
| messaging_system | span | messaging.system | semconv messaging |
| messaging_operation | span | messaging.operation.type, messaging.operation.name, messaging.operation | semconv messaging stable, then legacy |
| peer_service | span | peer.service | semconv general |

Numeric sources (status codes) render as integers without a fraction. All
promoted columns are dictionary-encoded strings in schema v1; typed promotions
are a compactor concern.

Deferred: `http_url`, `http_host`, `external_http_url`, `external_http_method`
used by the external-calls views. They are derived from `url.full`,
`server.address` and span kind and belong to a later schema version.

## Compactor-derived trace data

These rules are evaluated over the spans of one L0-to-L1 compaction pass.
They derive only from the stored OTLP fields and semantic-convention
attributes above; they do not derive from an exporter implementation.

| Output | Rule | Source |
|---|---|---|
| trace_summary.start | least span `timestamp` for the trace | OTLP span start |
| trace_summary.end | greatest saturating `timestamp + duration_nano` | OTLP span start and end |
| trace_summary.num_spans | number of spans for the trace | OTLP trace membership |
| trace_summary.services | sorted distinct non-empty `service.name` values | semconv resource `service.name` |
| trace_summary.has_error | true when any span has `STATUS_CODE_ERROR` | OTLP status |
| direct service edge | a CLIENT parent in `src` with a SERVER child in `dst`, or a PRODUCER parent with a CONSUMER child, joined by `(trace_id, parent_span_id)` | OTLP parent and span identities and `SpanKind`; product service-map contract |
| inferred destination | for an unmatched CLIENT or PRODUCER, first non-empty value in `db.system`, `messaging.system`, `peer.service`, `server.address`, `net.peer.name` order | semconv database, messaging, peer and server attributes |
| graph minute | floor of the measured span start to a UTC minute | OTLP span start; engine aggregation window |
| graph count/error_count | one call, and one error when the measured child or inferred client/producer has error status | OTLP status; product service-map contract |
| graph digest | latency of that same measured span, in nanoseconds | OTLP span start and end |

A direct edge takes precedence over inference. The parent map is keyed by
both trace and span id, because span ids are only unique within a trace.
Pairs split across compaction passes or UTC hours do not form an edge.

## Events and links

| Field | Rule |
|---|---|
| events | list of `{timestamp, name, attributes}`; attributes as a string map |
| links | list of `{trace_id, span_id, trace_state, attributes}`; a link with ids of the wrong length is dropped and counted |

Dropped counts reported by the producer (`dropped_attributes_count`,
`dropped_events_count`, `dropped_links_count`) are not stored in v1.

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

## Derived span columns

`http_url`, `http_host`, `external_http_method` and `external_http_url` are
computed from the stored `kind` and attribute maps when a query reads them;
schema v1 stores none of them, so every v1 file and the hot window answer
them. Filters on them run above the scan and never prune files.

Sources: the OTLP protocol (`SpanKind`, `AnyValue`), the semconv HTTP, URL
and server attribute names, and controlled observation of the ClickHouse
pipeline: `tests/fixtures/derived_columns_observed.json`, recorded with
`scripts/derived-columns-corpus.sh`, holds each OTLP span exactly as sent
and the row written for it, with the collector and ClickHouse versions. The
case names below are its cases.

| Column | Rule | Observed in |
|---|---|---|
| http_url | the value of the last `url.full` or `http.url` attribute, in attribute order, any kind; `""` overrides an earlier value; a URL is never assembled from `url.scheme`, `server.address`, `url.path`, `url.query` or `http.target` | `*_url_full_and_http_url_conflict`, `*_http_url_and_url_full_conflict`, `*_url_full_then_http_url_empty`, `*_url_parts_*` |
| http_host | the value of the last `server.address`, `net.peer.name` or `http.host` attribute, any kind, kept as written (a `host:port` value keeps its port; `server.port` and `net.peer.port` are not appended); `""` overrides | `*_server_address_vs_*`, `*_net_peer_name_vs_*`, `*_http_host_vs_*`, `*_with_port`, `*_and_port`, `*_then_net_peer_name_empty` |
| http_host, client | a CLIENT span whose host attributes leave it empty takes the host of its first non-empty `url.full` or `http.url` value | `client_url_full_only`, `client_url_full_and_http_url_conflict`, `client_url_full_empty_then_http_url`, `client_url_full_then_server_address_empty` |
| external_http_method | CLIENT only: the value of the last `http.request.method` or `http.method` attribute, any type rendered as text (integral numbers without a fraction, booleans `true`/`false`), case kept; `""` overrides; `""` for every other kind | `client_method_*`, `kind_*_full` |
| external_http_url | CLIENT only: the host of the final `http_url`; `""` for every other kind | `client_url_full_*`, `kind_*_full` |
| a URL's host | without user info, port or IPv6 brackets, case kept; `""` when the URL has no `//` authority (no scheme, a relative path, `https://`); the whole URL when it does not parse (a scheme not starting with a letter, a port that is not a number) | `*_url_full_userinfo_upper`, `*_url_full_ipv6`, `*_url_full_ip`, `*_url_full_no_scheme`, `*_url_full_relative`, `*_url_full_scheme_only`, `*_url_full_bad_port`, `*_url_full_bad_scheme`, `*_url_full_empty_port` |

Not inputs, observed: `net.host.name`, `peer.service`, resource attributes
(`host.name`, `server.address`), the span kinds other than CLIENT for the
URL host fallback. An empty or missing `AnyValue` is no attribute; a
non-string URL or host value derives `""`.

Known divergence: v1 keeps string, number and bool attributes in separate
maps, so the order between a string and a non-string value is lost. Where
the pipeline let a later non-string value of an input key override an
earlier string one, the engine keeps the string
(`*_server_address_then_net_peer_name_int`, `*_url_full_then_http_url_int`,
`*_method_stable_then_legacy_int`); for the method, string values decide
and a number or bool counts only when no string method is present.

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

| top_level_operations row | one per `(service_name, name)` of a span with no parent, or whose parent is remote (`is_remote = "true"`); a span without a service, with a local parent (`"false"`) or whose remoteness is unknown (`""`) does not count | OTLP empty `parent_span_id` means root; `SpanFlags` bits 0x100 and 0x200 for the parent context's `is_remote` |
| top_level_operations.last_seen | greatest span `timestamp` of the pair in the pass | OTLP span start |

Top-level membership needs only the span itself: no parent lookup, so a
parent that is absent, in another hour or in another pass changes nothing,
and nothing is inferred when the flags do not say.

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

## Logs

Logs follow the attribute, resource, scope, fingerprint and limit rules above;
`deployment_environment` and `service_name` are promoted with the same keys.

| Field | Rule | Source |
|---|---|---|
| timestamp | `time_unix_nano`, else `observed_time_unix_nano`; a record with neither is rejected and counts `logs_rejected` | OTLP `LogRecord`: 0 means unknown, and one-timestamp recipients use the event time if present, otherwise the observed time |
| observed_timestamp | `observed_time_unix_nano`, else `timestamp` | OTLP `LogRecord` |
| ts_bucket_start | from `timestamp`, as for spans | engine partition key |
| id | ULID: big-endian Unix milliseconds of `timestamp`, then 10 bytes of xxh3 over the request's seed (16 random bytes the ingester logs with the request) and the record's position in the request, rejected records counted | engine internal; ordered as the ULID text the Go backend pages by, and the same on replay |
| trace_id, span_id | 16 and 8 bytes as given; null when absent, of another length or all zeroes | OTLP `LogRecord`: such ids mean the record is not associated with a trace or span |
| trace_flags | `flags & 0xff` | OTLP `LogRecordFlags`: the low byte holds the W3C trace flags, the rest is reserved |
| severity_number | as given; values outside 0 to 255 become 0 | OTLP `SeverityNumber` |
| severity_text | as given, not derived from the number | OTLP: the original string as known at the source |
| body | a string as is; other values rendered like string-only map values (scalars as text, bytes as base64, arrays and kvlists as JSON); empty when absent; truncated at `max_body_bytes` (64 KiB) on a UTF-8 boundary, counted in `bodies_truncated` | OTLP `LogRecord.body` is an `AnyValue` |

Not stored in v1: `event_name`, `dropped_attributes_count`, and the
structured body columns (`body_v2`, `body_promoted`) the Go backend reads only
behind its experimental `use_json_body` flag.

## Metrics

Sources: the OTLP metrics definitions (`metrics/v1/metrics.proto`,
`common/v1/common.proto`), the OpenTelemetry metrics data model, the
columns the Go backend reads (`time_series_v4`, `samples_v4`), and
**[obs]**: the clean-room write contract observed from a known corpus,
`fusescope-collector-server/docs/spec/clickhouse-metrics.md` and its
`observed/0.145.0` rows (Apache-2.0).

A data point becomes one or more series, each with one sample. These
rules are mapping version 2 (`fs_schema::metrics::MAPPING_VERSION`), which
each metric WAL record names; any change to the rows a request converts
into takes a new version. Version 1 is the same but records no label
origin: a record of it converts with every origin legacy.

| OTLP | Series | type | temporality | is_monotonic | unit | Source |
|---|---|---|---|---|---|---|
| Gauge | `<name>` | `Gauge` | `Unspecified` | false | the metric's | [obs] |
| Sum | `<name>` | `Sum` | the sum's | the sum's | the metric's | [obs]; temporality text from the OTLP enum without its prefix |
| Histogram | `<name>.bucket` per bound and `le="+Inf"`, value cumulative over `le` | `Histogram` | the histogram's | true | the metric's | [obs]; cumulative as Prometheus buckets are |
| | `<name>.count` | `Sum` | the histogram's | true | the metric's, else `1` | [obs] |
| | `<name>.sum`, when the point has a sum | `Sum` | the histogram's | true | the metric's | [obs] |
| | `<name>.min`, `<name>.max`, when present | `Gauge` | `Unspecified` | false | the metric's | [obs] |
| Summary | `<name>.quantile` per quantile, label `quantile` | `Summary` | `Unspecified` | false | the metric's | [obs] |
| | `<name>.count` | `Sum` | `Cumulative` | true | `1` | [obs] |
| | `<name>.sum` | `Sum` | `Cumulative` | true | the metric's | [obs] |
| Exponential histogram | none: each point refused, `exponential_histogram` | | | | | engine; deferred to M3 step 4 |

Every series' description is the metric's. A histogram with no bounds and
no counts, the proto's one exception, has one `+Inf` bucket holding its
count [obs `probe.histogram_no_minmax`].

**Values.** Int becomes Float64, values above 2^53 losing precision. A
NaN value, sum, min or max writes no sample and no series [obs
`probe.nan`]. A Gauge or Sum point flagged `NO_RECORDED_VALUE` writes value
0 and its flags, its value ignored [obs `probe.no_value`]; the Go backend
reads flag bit 0 as a stale marker.

**Refused points.** Refused whole before any row, reported in the
partial success and counted by reason (OTLP invariants unless noted):

- `bucket_counts` not one longer than `explicit_bounds`, a bound not
  finite or not strictly increasing, buckets not summing to `count`, a
  cumulative count past u64: `malformed_histogram`;
- a quantile outside `[0, 1]` or not strictly increasing, or a quantile
  value NaN or negative: `malformed_summary`;
- a histogram or summary flagged `NO_RECORDED_VALUE`: `stale_histogram`
  (engine: no observed rule says which series go stale);
- more bounds than `ingest.max_histogram_buckets`, quantiles than
  `ingest.max_summary_quantiles`, or labels than `ingest.max_series_labels`
  (engine limits);
- no `time_unix_nano`, a number point with no value, a metric with no
  name.

A request with more points than `ingest.max_metric_points_per_request` or
more samples than `ingest.max_metric_samples_per_request` is refused whole.

**Labels.** A `Map(Utf8, Utf8)` in key byte order: the resource's and the
scope's attributes; `__scope.name__`, `__scope.version__`,
`__scope.schema_url__` (the scope's schema URL), empty when absent; the
point's attributes; `le` or `quantile`; `__temporality__`; `__name__`, the
series name. Of one key the generated label wins, then the resource's,
the scope's, the point's [obs `probe.collide_*`]. Within a level a key
received twice keeps its last value. Keys and values follow the attribute
limits above (an empty or long key dropped, values cut at
`max_value_bytes`); a level of more attributes than
`ingest.max_series_labels` refuses its points.

**Canonical values** (engine): a string as received; an int in decimal; a
double as the shortest decimal that reads back to it, never in exponent
form, `-0` kept, `NaN`, `+Inf`, `-Inf`; a bool `true`/`false`; bytes
standard base64; an array or key-value list as JSON, arrays in order,
lists with a repeated key keeping its last value, a key with no value
ignored, keys in byte order at every level, doubles as above and
non-finite ones and bytes as JSON strings, an empty value `null`, strings
escaped as RFC 8259 requires and no further. `le` and `quantile` use the
double form [obs `probe.extreme_bounds`, `probe.extreme_quantiles`].

**Fingerprint, version 1** (engine; the observed one is not identifiable
[obs §4]): xxh3-64 over the domain `fs.metric.v1`; the hashes of the
resource level (its schema URL and attributes), the scope level (name,
version, schema URL, attributes) and the point level (attributes), each
over its kind, its extra strings, its attribute count and the pairs in key
byte order; then the series' name, type, unit, temporality,
monotonicity and generated `le` or `quantile`. Strings are length
prefixed, counts and lengths little-endian u64. Hashing each level apart
keeps series apart whose labels match only because a level shadows
another [obs `probe.collide_resource`]. Two declarations of one identity
that differ in type, unit, temporality or monotonicity become two series,
so the same labels may show under two fingerprints; `metrics.select`
refuses such a set rather than merge it.

**Rows.** A sample: name, temporality, fingerprint, `time_unix_nano` in
milliseconds, value, flags, and `_source_order`, 36 bytes: the WAL
instance, segment and offset of its record, big-endian, and its ordinal
among the request's samples, the larger winning a duplicate fingerprint
and millisecond. A series row: one per fingerprint and hour, the hour of
its samples; of differing descriptions the longest in bytes, then the
least in byte order. Not stored: `env`, `attrs`, `scope_attrs`,
`resource_attrs` and `__normalized`, which the Go backend does not read.

**Label origins** (mapping 2, engine): each label keeps the level it won
from, resource, scope or point, and the OTLP types its value was received
as (`Str`, `Int`, `Double`, `Bool`, `Slice`, `Map`, `Bytes`); the
generated `__scope.*__` are scope strings, `__name__`, `__temporality__`,
`le` and `quantile` point strings [obs §5, `attr_type`]. Only the winner
of a key is kept: a value shadowed by another level leaves no origin and
no metadata row, unlike the reference, which writes one for each level,
since only the winner can be queried. The level is part of a series'
identity, the types are not: Int `1` and Str `"1"` render and hash alike
and are one series whose types accumulate.

**Identity** (engine): a series is its name, fingerprint, type,
temporality, monotonicity, unit, labels and the level of each label; the
fingerprint is never used alone, samples are keyed by name, fingerprint
and time. Two rows of one name, fingerprint and hour merge only when
their identities agree, a legacy origin, from a row of series v1,
agreeing with any level and taking it; two identities under one key are
both kept, a collision counted once a key, and a reader that must choose
fails closed.

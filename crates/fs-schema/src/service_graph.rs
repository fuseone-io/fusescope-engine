//! Per-compaction-pass service graph contributions at one-minute resolution.
//!
//! Format v1 has one row per `(src, dst, dst_kind, minute)`: counts, error
//! count, and a versioned binary t-digest of latency in nanoseconds. A direct
//! edge joins a SERVER child to a CLIENT parent or a CONSUMER child to a
//! PRODUCER parent inside the same trace and pass. An unmatched CLIENT or
//! PRODUCER may yield an inferred database, queue, service or external edge.

use std::collections::{BTreeMap, HashSet};
use std::mem::size_of;
use std::sync::{Arc, OnceLock};

use arrow::array::{
    Array, AsArray, BinaryArray, StringArray, TimestampNanosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit, TimestampNanosecondType};
use arrow::record_batch::RecordBatch;
use datafusion_functions_aggregate_common::tdigest::{Centroid, TDigest};
use opentelemetry_proto::tonic::trace::v1::span::SpanKind;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::SchemaError;
use crate::spans::{TRACE_ID_BYTES, col};

/// Version this engine writes and reads.
pub const SCHEMA_VERSION: u32 = 1;
/// Value of the `fusescope.signal` metadata.
pub const SIGNAL: &str = "service_graph_1m";
/// Name in a query session.
pub const TABLE: &str = "service_graph_1m";
/// Compression bound used by every latency digest.
pub const DIGEST_MAX_SIZE: usize = 100;
const BTREE_ENTRY_OVERHEAD: usize = 4 * size_of::<usize>();
const EDGE_NAME_COPIES: usize = 2;
/// One span's map entry, B-tree slack, finish-time resolution key and latency.
pub const SPAN_ENTRY_BYTES: usize = size_of::<SpanKey>()
    + size_of::<SpanMeta>()
    + BTREE_ENTRY_OVERHEAD
    + 2 * size_of::<&SpanKey>()
    + size_of::<f64>();
#[cfg(target_pointer_width = "64")]
const _: () = assert!(SPAN_ENTRY_BYTES == 168);
/// One distinct edge through aggregation and the finished digest map.
pub const EDGE_ENTRY_BYTES: usize = size_of::<EdgeKey>()
    + size_of::<EdgeValues>()
    + size_of::<ServiceEdge>()
    + 2 * BTREE_ENTRY_OVERHEAD
    + 2 * DIGEST_MAX_SIZE * size_of::<Centroid>()
    + DIGEST_FIXED;
/// Hash-table and allocation metadata above one interned name's UTF-8 bytes.
pub const INTERNED_NAME_BYTES: usize = size_of::<Arc<str>>() + 4 * size_of::<usize>();

const MINUTE_NS: i64 = 60_000_000_000;
const SPAN_ID_BYTES: usize = 8;
const DIGEST_HEADER: &[u8; 8] = b"FSDGST\0\x01";
const DIGEST_FIXED: usize = 8 + 4 + 8 * 4 + 4;

/// Column names.
pub mod columns {
    /// Start of the UTC minute.
    pub const MINUTE: &str = "minute";
    /// Caller service.
    pub const SRC: &str = "src";
    /// Callee or inferred dependency.
    pub const DST: &str = "dst";
    /// `service`, `database`, `queue`, or `external`.
    pub const DST_KIND: &str = "dst_kind";
    /// Calls in this pass.
    pub const COUNT: &str = "count";
    /// Calls whose measured span has error status.
    pub const ERROR_COUNT: &str = "error_count";
    /// Versioned binary t-digest of latency in nanoseconds.
    pub const DIGEST: &str = "digest";
}

/// Kind of the destination node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DestinationKind {
    /// Another instrumented service.
    Service,
    /// A database inferred from `db.system`.
    Database,
    /// A broker or queue inferred from `messaging.system`.
    Queue,
    /// An external endpoint inferred from an address attribute.
    External,
}

impl DestinationKind {
    /// Stable value stored in the table.
    pub fn name(self) -> &'static str {
        match self {
            Self::Service => "service",
            Self::Database => "database",
            Self::Queue => "queue",
            Self::External => "external",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "service" => Some(Self::Service),
            "database" => Some(Self::Database),
            "queue" => Some(Self::Queue),
            "external" => Some(Self::External),
            _ => None,
        }
    }
}

/// A latency t-digest with a validated stable binary representation.
#[derive(Debug, Clone, PartialEq)]
pub struct LatencyDigest {
    digest: TDigest,
    count: u64,
}

impl LatencyDigest {
    /// Builds a digest from nanosecond values.
    pub fn from_values(values: Vec<f64>) -> Self {
        let count = u64::try_from(values.len()).unwrap_or(u64::MAX);
        Self {
            digest: TDigest::new(DIGEST_MAX_SIZE).merge_unsorted_f64(values),
            count,
        }
    }

    /// Merges digests without expanding their observations.
    pub fn merge<'a>(digests: impl IntoIterator<Item = &'a Self>) -> Self {
        let digests: Vec<_> = digests.into_iter().collect();
        Self {
            digest: TDigest::merge_digests(digests.iter().map(|d| &d.digest)),
            count: digests
                .iter()
                .fold(0u64, |count, digest| count.saturating_add(digest.count)),
        }
    }

    /// Estimated quantile in nanoseconds. `q` is clamped to `[0, 1]`.
    pub fn quantile(&self, q: f64) -> f64 {
        self.digest.estimate_quantile(q.clamp(0.0, 1.0))
    }

    /// Number of observations.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Sum of observations.
    pub fn sum(&self) -> f64 {
        self.digest.sum()
    }

    /// Minimum observation.
    pub fn min(&self) -> f64 {
        self.digest.min()
    }

    /// Maximum observation.
    pub fn max(&self) -> f64 {
        self.digest.max()
    }

    /// Heap and inline bytes held by the digest.
    pub fn size(&self) -> usize {
        self.digest.size()
    }

    /// Stable binary representation: header, compression, count, min, max,
    /// sum, centroid count, then little-endian `(mean, weight)` pairs.
    pub fn encode(&self) -> Vec<u8> {
        let centroids = self.digest.centroids();
        let mut out = Vec::with_capacity(DIGEST_FIXED + centroids.len() * 16);
        out.extend_from_slice(DIGEST_HEADER);
        out.extend_from_slice(
            &u32::try_from(self.digest.max_size())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        out.extend_from_slice(&self.count.to_le_bytes());
        out.extend_from_slice(&self.digest.min().to_le_bytes());
        out.extend_from_slice(&self.digest.max().to_le_bytes());
        out.extend_from_slice(&self.digest.sum().to_le_bytes());
        out.extend_from_slice(
            &u32::try_from(centroids.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        for centroid in centroids {
            out.extend_from_slice(&centroid.mean().to_le_bytes());
            out.extend_from_slice(&centroid.weight().to_le_bytes());
        }
        out
    }

    /// Decodes a digest without trusting its state.
    ///
    /// # Errors
    /// Wrong version or length, impossible counts, unsorted centroids, or
    /// non-positive weights.
    pub fn decode(bytes: &[u8]) -> Result<Self, SchemaError> {
        if bytes.get(..8) != Some(DIGEST_HEADER.as_slice()) || bytes.len() < DIGEST_FIXED {
            return Err(SchemaError::Layout("service graph digest header"));
        }
        let max_size = read_u32(bytes, 8)? as usize;
        let count = read_u64(bytes, 12)?;
        let min = read_f64(bytes, 20)?;
        let max = read_f64(bytes, 28)?;
        let sum = read_f64(bytes, 36)?;
        let centroids_len = read_u32(bytes, 44)? as usize;
        let expected = DIGEST_FIXED
            .checked_add(centroids_len.saturating_mul(16))
            .ok_or(SchemaError::Layout("service graph digest length"))?;
        if max_size == 0
            || max_size > 10_000
            || centroids_len > max_size
            || bytes.len() != expected
            || count == 0
            || count > (1u64 << 53)
            || !min.is_finite()
            || !max.is_finite()
            || !sum.is_finite()
        {
            return Err(SchemaError::Layout("service graph digest state"));
        }
        let mut centroids = Vec::with_capacity(centroids_len);
        for index in 0..centroids_len {
            let at = DIGEST_FIXED + index * 16;
            centroids.push(Centroid::new(
                read_f64(bytes, at)?,
                read_f64(bytes, at + 8)?,
            ));
        }
        let count_f64 = count_as_f64(count);
        let digest = TDigest::try_from_parts(max_size, sum, count_f64, max, min, centroids)
            .map_err(|_| SchemaError::Layout("service graph digest centroids"))?;
        let centroid_count = digest.centroids().iter().map(Centroid::weight).sum::<f64>();
        if (centroid_count - count_f64).abs() > f64::EPSILON * count_f64 {
            return Err(SchemaError::Layout("service graph digest count"));
        }
        Ok(Self { digest, count })
    }
}

/// Counts are refused above 2^53 before this conversion, so every integer
/// represented by the durable format stays exact in the t-digest state.
#[allow(clippy::cast_precision_loss)]
fn count_as_f64(count: u64) -> f64 {
    count as f64
}

fn read_u32(bytes: &[u8], at: usize) -> Result<u32, SchemaError> {
    let raw: [u8; 4] = bytes
        .get(at..at + 4)
        .ok_or(SchemaError::Layout("service graph digest truncated"))?
        .try_into()
        .map_err(|_| SchemaError::Layout("service graph digest u32"))?;
    Ok(u32::from_le_bytes(raw))
}

fn read_u64(bytes: &[u8], at: usize) -> Result<u64, SchemaError> {
    let raw: [u8; 8] = bytes
        .get(at..at + 8)
        .ok_or(SchemaError::Layout("service graph digest truncated"))?
        .try_into()
        .map_err(|_| SchemaError::Layout("service graph digest u64"))?;
    Ok(u64::from_le_bytes(raw))
}

fn read_f64(bytes: &[u8], at: usize) -> Result<f64, SchemaError> {
    let raw: [u8; 8] = bytes
        .get(at..at + 8)
        .ok_or(SchemaError::Layout("service graph digest truncated"))?
        .try_into()
        .map_err(|_| SchemaError::Layout("service graph digest f64"))?;
    Ok(f64::from_le_bytes(raw))
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct EdgeKey {
    src: Arc<str>,
    dst: Arc<str>,
    dst_kind: DestinationKind,
    minute: i64,
}

/// One edge contribution.
#[derive(Debug, Clone, PartialEq)]
pub struct ServiceEdge {
    /// Start of the minute, nanoseconds.
    pub minute: i64,
    /// Caller service.
    pub src: String,
    /// Destination node.
    pub dst: String,
    /// Destination kind.
    pub dst_kind: DestinationKind,
    /// Calls in the pass.
    pub count: u64,
    /// Calls with error status on the measured span.
    pub error_count: u64,
    /// Latency distribution in nanoseconds.
    pub digest: LatencyDigest,
}

/// The configured bound that caused a whole graph contribution to be skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LimitExceeded {
    /// Configuration field whose bound was crossed.
    pub limit: &'static str,
    /// Configured count or byte bound.
    pub cap: usize,
    /// Count or bytes that the next retained value would require.
    pub observed: usize,
    /// Estimated resident bytes at that point.
    pub estimated_bytes: usize,
}

#[derive(Debug, Default)]
struct EdgeValues {
    errors: u64,
    latencies: Vec<f64>,
}

impl EdgeValues {
    fn push(&mut self, duration: u64, error: bool) {
        self.latencies.push(duration_as_f64(duration));
        self.errors = self.errors.saturating_add(u64::from(error));
    }
}

/// The digest is approximate; durations above 2^53 nanoseconds may lose
/// sub-nanosecond precision but keep their scale and ordering.
#[allow(clippy::cast_precision_loss)]
fn duration_as_f64(duration: u64) -> f64 {
    duration as f64
}

/// A finished graph contribution.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ServiceGraph {
    rows: BTreeMap<EdgeKey, ServiceEdge>,
}

impl ServiceGraph {
    /// Rows in `(src, dst, dst_kind, minute)` order.
    pub fn iter(&self) -> impl Iterator<Item = &ServiceEdge> {
        self.rows.values()
    }

    /// Number of edges.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// True when no edge was found.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Earliest and latest minute.
    pub fn range(&self) -> Option<(i64, i64)> {
        self.rows.values().fold(None, |range, row| {
            Some(match range {
                None => (row.minute, row.minute),
                Some((first, last)) => (first.min(row.minute), last.max(row.minute)),
            })
        })
    }

    /// Merges partial graph rows, including their t-digests.
    pub fn merge(&mut self, other: Self) {
        for (key, edge) in other.rows {
            if let Some(known) = self.rows.get_mut(&key) {
                known.count = known.count.saturating_add(edge.count);
                known.error_count = known.error_count.saturating_add(edge.error_count);
                known.digest = LatencyDigest::merge([&known.digest, &edge.digest]);
            } else {
                self.rows.insert(key, edge);
            }
        }
    }

    /// Encodes the rows as one Arrow batch.
    ///
    /// # Errors
    /// Arrow refuses an inconsistent array.
    pub fn to_batch(&self) -> Result<RecordBatch, SchemaError> {
        let mut minutes = Vec::with_capacity(self.len());
        let mut sources = Vec::with_capacity(self.len());
        let mut destinations = Vec::with_capacity(self.len());
        let mut kinds = Vec::with_capacity(self.len());
        let mut counts = Vec::with_capacity(self.len());
        let mut errors = Vec::with_capacity(self.len());
        let mut digests = Vec::with_capacity(self.len());
        for row in self.rows.values() {
            minutes.push(row.minute);
            sources.push(row.src.as_str());
            destinations.push(row.dst.as_str());
            kinds.push(row.dst_kind.name());
            counts.push(row.count);
            errors.push(row.error_count);
            digests.push(row.digest.encode());
        }
        Ok(RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(TimestampNanosecondArray::from(minutes).with_timezone("UTC")),
                Arc::new(StringArray::from(sources)),
                Arc::new(StringArray::from(destinations)),
                Arc::new(StringArray::from(kinds)),
                Arc::new(UInt64Array::from(counts)),
                Arc::new(UInt64Array::from(errors)),
                Arc::new(BinaryArray::from_iter_values(digests)),
            ],
        )?)
    }

    fn from_batch(batch: &RecordBatch) -> Result<Self, SchemaError> {
        let schema = batch.schema();
        let column = |name: &str| -> Result<&arrow::array::ArrayRef, SchemaError> {
            Ok(batch.column(schema.index_of(name)?))
        };
        let minutes = column(columns::MINUTE)?
            .as_primitive_opt::<TimestampNanosecondType>()
            .ok_or(SchemaError::Layout("service graph minute"))?;
        let sources = column(columns::SRC)?
            .as_string_opt::<i32>()
            .ok_or(SchemaError::Layout("service graph src"))?;
        let destinations = column(columns::DST)?
            .as_string_opt::<i32>()
            .ok_or(SchemaError::Layout("service graph dst"))?;
        let kinds = column(columns::DST_KIND)?
            .as_string_opt::<i32>()
            .ok_or(SchemaError::Layout("service graph dst_kind"))?;
        let counts = column(columns::COUNT)?
            .as_primitive_opt::<arrow::datatypes::UInt64Type>()
            .ok_or(SchemaError::Layout("service graph count"))?;
        let errors = column(columns::ERROR_COUNT)?
            .as_primitive_opt::<arrow::datatypes::UInt64Type>()
            .ok_or(SchemaError::Layout("service graph error_count"))?;
        let digests = column(columns::DIGEST)?
            .as_binary_opt::<i32>()
            .ok_or(SchemaError::Layout("service graph digest"))?;
        let mut out = Self::default();
        for row in 0..batch.num_rows() {
            let minute = minutes.value(row);
            if minute.rem_euclid(MINUTE_NS) != 0 {
                return Err(SchemaError::Layout("service graph minute alignment"));
            }
            let kind = DestinationKind::parse(kinds.value(row))
                .ok_or(SchemaError::Layout("service graph destination kind"))?;
            let digest = LatencyDigest::decode(digests.value(row))?;
            let count = counts.value(row);
            if digest.count() != count || errors.value(row) > count {
                return Err(SchemaError::Layout("service graph counts"));
            }
            let key = EdgeKey {
                src: Arc::from(sources.value(row)),
                dst: Arc::from(destinations.value(row)),
                dst_kind: kind,
                minute,
            };
            let edge = ServiceEdge {
                minute,
                src: key.src.to_string(),
                dst: key.dst.to_string(),
                dst_kind: kind,
                count,
                error_count: errors.value(row),
                digest,
            };
            if let Some(known) = out.rows.get_mut(&key) {
                known.count = known.count.saturating_add(edge.count);
                known.error_count = known.error_count.saturating_add(edge.error_count);
                known.digest = LatencyDigest::merge([&known.digest, &edge.digest]);
            } else {
                out.rows.insert(key, edge);
            }
        }
        Ok(out)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct SpanKey {
    trace: [u8; TRACE_ID_BYTES],
    span: [u8; SPAN_ID_BYTES],
}

#[derive(Debug, Clone)]
struct Destination {
    name: Arc<str>,
    kind: DestinationKind,
}

#[derive(Debug, Clone)]
struct SpanMeta {
    parent: Option<SpanKey>,
    service: Option<Arc<str>>,
    kind: i32,
    minute: i64,
    duration: u64,
    error: bool,
    inferred: Option<Destination>,
}

#[derive(Clone, Copy)]
struct PendingSpan<'a> {
    parent: Option<SpanKey>,
    service: Option<&'a str>,
    kind: i32,
    minute: i64,
    duration: u64,
    error: bool,
    inferred: Option<(&'a str, DestinationKind)>,
}

/// Whole-pass span map used to resolve direct edges after every row arrived.
/// Crossing either the span or byte cap clears the map and permanently skips it.
#[derive(Debug, Clone)]
pub struct ServiceGraphBuilder {
    spans: BTreeMap<SpanKey, SpanMeta>,
    names: HashSet<Arc<str>>,
    estimated_bytes: usize,
    max_bytes: usize,
    exceeded: Option<LimitExceeded>,
}

impl Default for ServiceGraphBuilder {
    fn default() -> Self {
        Self {
            spans: BTreeMap::new(),
            names: HashSet::new(),
            estimated_bytes: 0,
            max_bytes: usize::MAX,
            exceeded: None,
        }
    }
}

impl ServiceGraphBuilder {
    /// Adds a span batch while the map stays within both configured caps.
    ///
    /// # Errors
    /// A required span column is absent or has another physical type.
    pub fn absorb(
        &mut self,
        batch: &RecordBatch,
        max_spans: usize,
        max_bytes: usize,
    ) -> Result<(), SchemaError> {
        if self.exceeded.is_some() {
            return Ok(());
        }
        self.max_bytes = self.max_bytes.min(max_bytes);
        let schema = batch.schema();
        let traces = batch
            .column(schema.index_of(col::TRACE_ID)?)
            .as_fixed_size_binary();
        let spans = batch
            .column(schema.index_of(col::SPAN_ID)?)
            .as_fixed_size_binary();
        let parents = batch
            .column(schema.index_of(col::PARENT_SPAN_ID)?)
            .as_fixed_size_binary();
        let timestamps = batch
            .column(schema.index_of(col::TIMESTAMP)?)
            .as_primitive_opt::<TimestampNanosecondType>()
            .ok_or(SchemaError::Layout("timestamp"))?;
        let durations = batch
            .column(schema.index_of(col::DURATION_NANO)?)
            .as_primitive_opt::<arrow::datatypes::UInt64Type>()
            .ok_or(SchemaError::Layout("duration_nano"))?;
        let kinds = batch
            .column(schema.index_of(col::KIND)?)
            .as_primitive_opt::<arrow::datatypes::Int8Type>()
            .ok_or(SchemaError::Layout("kind"))?;
        let errors = batch
            .column(schema.index_of(col::HAS_ERROR)?)
            .as_boolean_opt()
            .ok_or(SchemaError::Layout("has_error"))?;
        let services = string_column(batch, col::SERVICE_NAME)?;
        let databases = string_column(batch, "db_system")?;
        let messaging = string_column(batch, "messaging_system")?;
        let peers = string_column(batch, "peer_service")?;
        let attributes = batch
            .column(schema.index_of(col::ATTRIBUTES_STRING)?)
            .as_map_opt()
            .ok_or(SchemaError::Layout("attributes_string"))?;
        for row in 0..batch.num_rows() {
            let trace: [u8; TRACE_ID_BYTES] = traces
                .value(row)
                .try_into()
                .map_err(|_| SchemaError::Layout("trace_id"))?;
            let span: [u8; SPAN_ID_BYTES] = spans
                .value(row)
                .try_into()
                .map_err(|_| SchemaError::Layout("span_id"))?;
            let key = SpanKey { trace, span };
            let parent = if parents.is_null(row) {
                None
            } else {
                Some(SpanKey {
                    trace,
                    span: parents
                        .value(row)
                        .try_into()
                        .map_err(|_| SchemaError::Layout("parent_span_id"))?,
                })
            };
            let service = value_at(&services, row).filter(|v| !v.is_empty());
            let inferred = inferred_destination_ref(
                value_at(&databases, row),
                value_at(&messaging, row),
                value_at(&peers, row),
                map_value(attributes, row, &["server.address", "net.peer.name"]),
            );
            if !self.retain(
                key,
                PendingSpan {
                    parent,
                    service,
                    kind: i32::from(kinds.value(row)),
                    minute: timestamps.value(row).div_euclid(MINUTE_NS) * MINUTE_NS,
                    duration: durations.value(row),
                    error: errors.value(row),
                    inferred,
                },
                max_spans,
                max_bytes,
            ) {
                return Ok(());
            }
        }
        Ok(())
    }

    fn retain(
        &mut self,
        key: SpanKey,
        span: PendingSpan<'_>,
        max_spans: usize,
        max_bytes: usize,
    ) -> bool {
        let new_span = !self.spans.contains_key(&key);
        let mut charge = usize::from(new_span).saturating_mul(SPAN_ENTRY_BYTES);
        if let Some(name) = span.service.filter(|name| !self.names.contains(*name)) {
            charge = charge.saturating_add(name.len().saturating_add(INTERNED_NAME_BYTES));
        }
        if let Some((name, _)) = span
            .inferred
            .filter(|(name, _)| Some(*name) != span.service && !self.names.contains(*name))
        {
            charge = charge.saturating_add(name.len().saturating_add(INTERNED_NAME_BYTES));
        }
        let Some(next_bytes) = self.estimated_bytes.checked_add(charge) else {
            self.skip(LimitExceeded {
                limit: "service_graph_max_bytes",
                cap: max_bytes,
                observed: usize::MAX,
                estimated_bytes: usize::MAX,
            });
            return false;
        };
        if new_span && self.spans.len() >= max_spans {
            self.skip(LimitExceeded {
                limit: "service_graph_max_spans",
                cap: max_spans,
                observed: self.spans.len().saturating_add(1),
                estimated_bytes: next_bytes,
            });
            return false;
        }
        if next_bytes > max_bytes {
            self.skip(LimitExceeded {
                limit: "service_graph_max_bytes",
                cap: max_bytes,
                observed: next_bytes,
                estimated_bytes: next_bytes,
            });
            return false;
        }
        self.estimated_bytes = next_bytes;
        let service = span.service.map(|name| self.intern(name));
        let inferred = span.inferred.map(|(name, kind)| Destination {
            name: self.intern(name),
            kind,
        });
        self.spans.insert(
            key,
            SpanMeta {
                parent: span.parent,
                service,
                kind: span.kind,
                minute: span.minute,
                duration: span.duration,
                error: span.error,
                inferred,
            },
        );
        true
    }

    /// True when a cap was crossed and no partial graph may be emitted.
    pub fn skipped(&self) -> bool {
        self.exceeded.is_some()
    }

    /// The first bound crossed by this contribution.
    pub fn limit_exceeded(&self) -> Option<LimitExceeded> {
        self.exceeded
    }

    /// Conservative bytes charged before every allocation retained by the pass.
    pub fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }

    /// Resolves direct edges first, then inferred edges for unmatched client
    /// and producer spans. `None` means the cap was crossed.
    pub fn finish(self) -> Option<ServiceGraph> {
        self.finish_with_limit().0
    }

    /// Resolves the graph and returns the bound crossed during ingestion or
    /// finish-time allocation, if any.
    pub fn finish_with_limit(self) -> (Option<ServiceGraph>, Option<LimitExceeded>) {
        if self.exceeded.is_some() {
            return (None, self.exceeded);
        }
        let mut resolved: HashSet<&SpanKey> = HashSet::new();
        let mut edges: BTreeMap<EdgeKey, EdgeValues> = BTreeMap::new();
        let mut estimated_bytes = self.estimated_bytes;
        let mut exceeded = None;
        for child in self.spans.values() {
            let Some(parent_key) = child.parent else {
                continue;
            };
            let Some((parent_key, parent)) = self.spans.get_key_value(&parent_key) else {
                continue;
            };
            let direct = (parent.kind == SpanKind::Client as i32
                && child.kind == SpanKind::Server as i32)
                || (parent.kind == SpanKind::Producer as i32
                    && child.kind == SpanKind::Consumer as i32);
            if !direct {
                continue;
            }
            resolved.insert(parent_key);
            let (Some(src), Some(dst)) = (&parent.service, &child.service) else {
                continue;
            };
            if src == dst {
                continue;
            }
            if !push_edge(
                &mut edges,
                EdgeKey {
                    src: Arc::clone(src),
                    dst: Arc::clone(dst),
                    dst_kind: DestinationKind::Service,
                    minute: child.minute,
                },
                child.duration,
                child.error,
                &mut estimated_bytes,
                self.max_bytes,
                &mut exceeded,
            ) {
                return (None, exceeded);
            }
        }
        for (key, span) in &self.spans {
            if !matches!(span.kind, value if value == SpanKind::Client as i32 || value == SpanKind::Producer as i32)
                || resolved.contains(key)
            {
                continue;
            }
            let (Some(src), Some(dst)) = (&span.service, &span.inferred) else {
                continue;
            };
            if dst.name.is_empty()
                || (dst.kind == DestinationKind::Service && src.as_ref() == dst.name.as_ref())
            {
                continue;
            }
            if !push_edge(
                &mut edges,
                EdgeKey {
                    src: Arc::clone(src),
                    dst: Arc::clone(&dst.name),
                    dst_kind: dst.kind,
                    minute: span.minute,
                },
                span.duration,
                span.error,
                &mut estimated_bytes,
                self.max_bytes,
                &mut exceeded,
            ) {
                return (None, exceeded);
            }
        }
        let rows = edges
            .into_iter()
            .map(|(key, values)| {
                let count = values.latencies.len() as u64;
                let edge = ServiceEdge {
                    minute: key.minute,
                    src: key.src.to_string(),
                    dst: key.dst.to_string(),
                    dst_kind: key.dst_kind,
                    count,
                    error_count: values.errors,
                    digest: LatencyDigest::from_values(values.latencies),
                };
                (key, edge)
            })
            .collect();
        (Some(ServiceGraph { rows }), None)
    }

    fn intern(&mut self, name: &str) -> Arc<str> {
        if let Some(known) = self.names.get(name) {
            return Arc::clone(known);
        }
        let name: Arc<str> = Arc::from(name);
        self.names.insert(Arc::clone(&name));
        name
    }

    fn skip(&mut self, exceeded: LimitExceeded) {
        self.spans.clear();
        self.names.clear();
        self.estimated_bytes = 0;
        self.exceeded = Some(exceeded);
    }
}

fn push_edge(
    edges: &mut BTreeMap<EdgeKey, EdgeValues>,
    key: EdgeKey,
    duration: u64,
    error: bool,
    estimated_bytes: &mut usize,
    max_bytes: usize,
    exceeded: &mut Option<LimitExceeded>,
) -> bool {
    if !edges.contains_key(&key) {
        let names = key
            .src
            .len()
            .saturating_add(key.dst.len())
            .saturating_mul(EDGE_NAME_COPIES);
        let charge = EDGE_ENTRY_BYTES.saturating_add(names);
        let Some(next_bytes) = estimated_bytes.checked_add(charge) else {
            *exceeded = Some(LimitExceeded {
                limit: "service_graph_max_bytes",
                cap: max_bytes,
                observed: usize::MAX,
                estimated_bytes: usize::MAX,
            });
            return false;
        };
        if next_bytes > max_bytes {
            *exceeded = Some(LimitExceeded {
                limit: "service_graph_max_bytes",
                cap: max_bytes,
                observed: next_bytes,
                estimated_bytes: next_bytes,
            });
            return false;
        }
        *estimated_bytes = next_bytes;
    }
    edges.entry(key).or_default().push(duration, error);
    true
}

fn string_column(batch: &RecordBatch, name: &str) -> Result<StringArray, SchemaError> {
    let index = batch.schema().index_of(name)?;
    let cast = arrow::compute::cast(batch.column(index), &DataType::Utf8)?;
    cast.as_string_opt::<i32>()
        .cloned()
        .ok_or(SchemaError::Layout("service graph string column"))
}

fn value_at(values: &StringArray, row: usize) -> Option<&str> {
    (!values.is_null(row)).then(|| values.value(row))
}

fn map_value<'a>(map: &'a arrow::array::MapArray, row: usize, wanted: &[&str]) -> Option<&'a str> {
    let keys = map.keys().as_string_opt::<i32>()?;
    let values = map.values().as_string_opt::<i32>()?;
    let offsets = map.value_offsets();
    let (start, end) = (*offsets.get(row)?, *offsets.get(row + 1)?);
    for key in wanted {
        for entry in start..end {
            let entry = usize::try_from(entry).ok()?;
            if keys.value(entry) == *key {
                return Some(values.value(entry));
            }
        }
    }
    None
}

fn inferred_destination_ref<'a>(
    database: Option<&'a str>,
    messaging: Option<&'a str>,
    peer: Option<&'a str>,
    external: Option<&'a str>,
) -> Option<(&'a str, DestinationKind)> {
    [
        (database, DestinationKind::Database),
        (messaging, DestinationKind::Queue),
        (peer, DestinationKind::Service),
        (external, DestinationKind::External),
    ]
    .into_iter()
    .find_map(|(name, kind)| {
        name.filter(|value| !value.is_empty())
            .map(|name| (name, kind))
    })
}

/// Exact physical layout of format v1.
pub fn schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let metadata = [
                ("fusescope.signal".to_owned(), SIGNAL.to_owned()),
                (
                    "fusescope.schema_version".to_owned(),
                    SCHEMA_VERSION.to_string(),
                ),
            ];
            Arc::new(Schema::new_with_metadata(
                vec![
                    Field::new(
                        columns::MINUTE,
                        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                        false,
                    ),
                    Field::new(columns::SRC, DataType::Utf8, false),
                    Field::new(columns::DST, DataType::Utf8, false),
                    Field::new(columns::DST_KIND, DataType::Utf8, false),
                    Field::new(columns::COUNT, DataType::UInt64, false),
                    Field::new(columns::ERROR_COUNT, DataType::UInt64, false),
                    Field::new(columns::DIGEST, DataType::Binary, false),
                ],
                metadata.into_iter().collect(),
            ))
        })
        .clone()
}

/// Refuses another signal, version, physical type, order or nullability.
///
/// # Errors
/// The schema is not exactly format v1.
pub fn check_schema(found: &Schema) -> Result<(), SchemaError> {
    let expected = schema();
    let metadata = found.metadata();
    let version = SCHEMA_VERSION.to_string();
    let valid = metadata.get("fusescope.signal").map(String::as_str) == Some(SIGNAL)
        && metadata.get("fusescope.schema_version").map(String::as_str) == Some(version.as_str())
        && found.fields() == expected.fields();
    if !valid {
        return Err(SchemaError::Layout("service_graph_1m format v1"));
    }
    Ok(())
}

/// Encodes one pass's contribution as Parquet.
///
/// # Errors
/// Arrow or Parquet failure.
pub fn encode(graph: &ServiceGraph) -> Result<Vec<u8>, SchemaError> {
    let batch = graph.to_batch()?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(
            crate::parquet::ZSTD_LEVEL,
        )?))
        .build();
    let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), Some(properties))?;
    writer.write(&batch)?;
    Ok(writer.into_inner()?)
}

/// Decodes and validates a Parquet contribution.
///
/// # Errors
/// It is not a `service_graph_1m` format-v1 file or a digest is corrupt.
pub fn decode(bytes: bytes::Bytes) -> Result<ServiceGraph, SchemaError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    check_schema(builder.schema())?;
    let mut out = ServiceGraph::default();
    for batch in builder.build()? {
        out.merge(ServiceGraph::from_batch(&batch?)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::span::SpanKind;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};

    use super::*;
    use crate::limits::Limits;
    use crate::spans::SpanBatchBuilder;

    fn kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(value.into())),
            }),
            ..KeyValue::default()
        }
    }

    fn resource(service: &str, spans: Vec<Span>) -> ResourceSpans {
        ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv("service.name", service)],
                ..Resource::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans,
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn span(
        trace: u8,
        id: u8,
        parent: Option<u8>,
        kind: SpanKind,
        start: u64,
        duration: u64,
        error: bool,
        attributes: Vec<KeyValue>,
    ) -> Span {
        Span {
            trace_id: vec![trace; 16],
            span_id: vec![id; 8],
            parent_span_id: parent.map_or_else(Vec::new, |p| vec![p; 8]),
            name: "op".into(),
            kind: kind as i32,
            start_time_unix_nano: start,
            end_time_unix_nano: start + duration,
            status: error.then_some(Status {
                code: 2,
                ..Status::default()
            }),
            attributes,
            ..Span::default()
        }
    }

    fn batch() -> RecordBatch {
        let minute = 120_000_000_000;
        let request = ExportTraceServiceRequest {
            resource_spans: vec![
                resource(
                    "api",
                    vec![
                        span(1, 1, None, SpanKind::Client, minute + 1, 50, false, vec![]),
                        span(
                            2,
                            3,
                            None,
                            SpanKind::Producer,
                            minute + 2,
                            70,
                            true,
                            vec![kv("messaging.system", "kafka")],
                        ),
                        span(
                            3,
                            4,
                            None,
                            SpanKind::Client,
                            minute + 3,
                            90,
                            false,
                            vec![kv("server.address", "example.test")],
                        ),
                    ],
                ),
                resource(
                    "worker",
                    vec![span(
                        1,
                        2,
                        Some(1),
                        SpanKind::Server,
                        minute + 10,
                        100,
                        true,
                        vec![],
                    )],
                ),
            ],
        };
        let mut builder = SpanBatchBuilder::new(Limits::default());
        builder.append_request(&request).unwrap();
        builder.finish().unwrap()
    }

    #[test]
    fn direct_and_inferred_edges_round_trip_with_latency_and_errors() {
        let mut builder = ServiceGraphBuilder::default();
        builder.absorb(&batch(), 10, usize::MAX).unwrap();
        let graph = builder.finish().unwrap();
        let rows: Vec<_> = graph.iter().collect();
        assert_eq!(rows.len(), 3);
        let direct = rows.iter().find(|edge| edge.dst == "worker").unwrap();
        assert_eq!(
            (direct.src.as_str(), direct.dst_kind),
            ("api", DestinationKind::Service)
        );
        assert_eq!((direct.count, direct.error_count), (1, 1));
        assert_eq!(direct.digest.quantile(0.99), 100.0);
        let queue = rows.iter().find(|edge| edge.dst == "kafka").unwrap();
        assert_eq!(queue.dst_kind, DestinationKind::Queue);
        assert_eq!((queue.count, queue.error_count), (1, 1));
        let external = rows.iter().find(|edge| edge.dst == "example.test").unwrap();
        assert_eq!(external.dst_kind, DestinationKind::External);

        let bytes = encode(&graph).unwrap();
        assert_eq!(decode(bytes.into()).unwrap(), graph);
    }

    #[test]
    fn committed_format_v1_fixture_remains_readable() {
        let mut builder = ServiceGraphBuilder::default();
        builder.absorb(&batch(), 10, usize::MAX).unwrap();
        let expected = builder.finish().unwrap();
        let bytes = bytes::Bytes::from_static(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/service_graph_v1.parquet"
        )));
        assert_eq!(decode(bytes).unwrap(), expected);
    }

    #[test]
    fn digest_round_trip_and_merge_preserve_quantiles_and_measures() {
        let first = LatencyDigest::from_values((1..=100).map(f64::from).collect());
        let second = LatencyDigest::from_values((101..=200).map(f64::from).collect());
        let first_back = LatencyDigest::decode(&first.encode()).unwrap();
        assert_eq!(first_back, first);
        let merged = LatencyDigest::merge([&first, &second]);
        assert_eq!(merged.count(), 200);
        assert_eq!(
            (merged.min(), merged.max(), merged.sum()),
            (1.0, 200.0, 20_100.0)
        );
        assert!((merged.quantile(0.99) - 198.0).abs() <= 3.0);
        let mut corrupt = merged.encode();
        *corrupt.first_mut().unwrap() ^= 1;
        assert!(LatencyDigest::decode(&corrupt).is_err());
    }

    #[test]
    fn crossing_the_span_cap_skips_the_whole_graph() {
        let mut builder = ServiceGraphBuilder::default();
        builder.absorb(&batch(), 3, usize::MAX).unwrap();
        assert!(builder.skipped());
        assert!(builder.finish().is_none());
    }

    #[test]
    fn duplicate_span_identities_do_not_spend_the_cap_twice() {
        let batch = batch();
        let mut builder = ServiceGraphBuilder::default();
        builder.absorb(&batch, 4, usize::MAX).unwrap();
        builder.absorb(&batch, 4, usize::MAX).unwrap();
        assert!(!builder.skipped());
        assert_eq!(builder.finish().unwrap().len(), 3);
    }

    #[test]
    fn byte_cap_is_charged_before_the_first_span_is_retained() {
        let mut builder = ServiceGraphBuilder::default();
        builder
            .absorb(&batch(), usize::MAX, SPAN_ENTRY_BYTES - 1)
            .unwrap();
        assert!(builder.skipped());
        assert_eq!(builder.estimated_bytes(), 0);
        assert_eq!(
            builder.limit_exceeded(),
            Some(LimitExceeded {
                limit: "service_graph_max_bytes",
                cap: SPAN_ENTRY_BYTES - 1,
                observed: SPAN_ENTRY_BYTES + INTERNED_NAME_BYTES + "api".len(),
                estimated_bytes: SPAN_ENTRY_BYTES + INTERNED_NAME_BYTES + "api".len(),
            })
        );
    }

    #[test]
    fn estimated_bytes_follow_the_retained_layout() {
        let mut builder = ServiceGraphBuilder::default();
        builder.absorb(&batch(), usize::MAX, usize::MAX).unwrap();
        assert_eq!(
            SPAN_ENTRY_BYTES,
            size_of::<SpanKey>()
                + size_of::<SpanMeta>()
                + BTREE_ENTRY_OVERHEAD
                + 2 * size_of::<&SpanKey>()
                + size_of::<f64>()
        );
        let names = builder
            .names
            .iter()
            .map(|name| INTERNED_NAME_BYTES + name.len())
            .sum::<usize>();
        assert_eq!(
            builder.estimated_bytes(),
            builder.spans.len() * SPAN_ENTRY_BYTES + names
        );
    }

    #[test]
    fn finish_charges_each_distinct_edge_once() {
        let mut edges = BTreeMap::new();
        let mut estimated = 0;
        let mut exceeded = None;
        let key = EdgeKey {
            src: Arc::from("api"),
            dst: Arc::from("worker"),
            dst_kind: DestinationKind::Service,
            minute: MINUTE_NS,
        };
        assert!(push_edge(
            &mut edges,
            key.clone(),
            10,
            false,
            &mut estimated,
            usize::MAX,
            &mut exceeded,
        ));
        let once = EDGE_ENTRY_BYTES + EDGE_NAME_COPIES * ("api".len() + "worker".len());
        assert_eq!(estimated, once);
        assert!(push_edge(
            &mut edges,
            key,
            20,
            true,
            &mut estimated,
            usize::MAX,
            &mut exceeded,
        ));
        assert_eq!(estimated, once);
        assert_eq!(edges.values().next().unwrap().latencies.len(), 2);

        let mut limited = BTreeMap::new();
        let mut limited_bytes = 0;
        let mut limited_by = None;
        assert!(!push_edge(
            &mut limited,
            EdgeKey {
                src: Arc::from("api"),
                dst: Arc::from("worker"),
                dst_kind: DestinationKind::Service,
                minute: MINUTE_NS,
            },
            10,
            false,
            &mut limited_bytes,
            once - 1,
            &mut limited_by,
        ));
        assert_eq!(
            limited_by,
            Some(LimitExceeded {
                limit: "service_graph_max_bytes",
                cap: once - 1,
                observed: once,
                estimated_bytes: once,
            })
        );
        assert_eq!(limited.len(), 0);
    }

    #[test]
    fn another_version_is_refused() {
        let expected = schema();
        let mut metadata = expected.metadata().clone();
        metadata.insert("fusescope.schema_version".into(), "2".into());
        let future = Schema::new_with_metadata(expected.fields().clone(), metadata);
        assert!(check_schema(&future).is_err());
    }
}

//! One output file of a merge being written: its Parquet bytes, its
//! sidecar and its catalog row, within the bounds of [`MergeConfig`].

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, SchemaRef, TimestampNanosecondType};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use fs_catalog::{FileId, Level, NewFile, Signal, TenantId, TimeRange};
use fs_index::TraceIndex;
use fs_schema::parquet::{l2_writer_properties, log_writer_properties, writer_properties};
use fs_schema::spans::{TRACE_ID_BYTES, col, trace_ids};
use object_store::PutPayload;
use object_store::path::Path;
use parquet::arrow::ArrowWriter;

use crate::CompactError;
use crate::merge::MergeConfig;

/// One output file ready to upload with its catalog row and sidecar.
pub struct Output {
    /// Catalog row.
    pub file: NewFile,
    /// Parquet bytes.
    pub bytes: PutPayload,
    /// Where the sidecar goes and its bytes: the `trace_id` sidecar of a
    /// span file, the text sidecar of a log file; `None` when its cap was
    /// passed, so the file is scanned.
    pub index: Option<(String, Bytes)>,
}

/// Bytes a service name holds in the set besides its characters.
const SERVICE_OVERHEAD: usize = 48;

/// Bytes of one `(trace, file)` entry of a `trace_id` sidecar.
const TRACE_ENTRY_BYTES: usize = TRACE_ID_BYTES + 4;

/// Copies of the entries `finish` holds at once: the sorted entries, the
/// encoded body and the framed sidecar.
const TRACE_FINISH_COPIES: usize = 3;

/// Bytes of each buffer an output is written into: a sixty-fourth of
/// its memory budget, within these.
const MIN_CHUNK_BYTES: usize = 64 << 10;
const MAX_CHUNK_BYTES: usize = 4 << 20;

/// Bytes one data page holds in the page indexes until the footer: its
/// location and unencoded size, its minimum and maximum truncated to 64
/// bytes, its null count and flag, and its level histograms, a counter
/// per level.
fn page_index_bytes(max_def: i16, max_rep: i16) -> usize {
    let levels = usize::try_from(max_def).unwrap_or(0) + usize::try_from(max_rep).unwrap_or(0) + 2;
    32 + 2 * 64 + 9 + 8 * levels
}

/// Bytes one column chunk holds in the footer besides its pages: its
/// path, encodings, offsets and statistics truncated to 64 bytes.
const CHUNK_META_BYTES: usize = 1024;

/// Bytes of the footer whatever its row groups: the schema and the
/// key-value metadata, the Arrow schema among them.
const FOOTER_FIXED_BYTES: usize = 64 << 10;

/// Copies of the indexes and footer at once as an output finishes: the
/// indexes held, the footer encoded, and the metadata built from them.
const FOOTER_COPIES: usize = 3;

/// An output's bytes in buffers of one size, so growing never copies
/// what is written, as a doubling buffer would.
pub(crate) struct Chunks {
    done: Vec<Bytes>,
    current: Vec<u8>,
    size: usize,
    len: usize,
}

impl std::io::Write for Chunks {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.current.len() == self.current.capacity() {
            let full = std::mem::replace(&mut self.current, Vec::with_capacity(self.size));
            if !full.is_empty() {
                self.done.push(Bytes::from(full));
            }
        }
        let n = buf.len().min(self.current.capacity() - self.current.len());
        self.current
            .extend_from_slice(buf.get(..n).unwrap_or_default());
        self.len += n;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Chunks {
    fn new(size: usize) -> Self {
        Self {
            done: Vec::new(),
            current: Vec::new(),
            size,
            len: 0,
        }
    }

    /// The most the buffers hold: what is written and a whole buffer
    /// besides, the one being filled or the next.
    pub(crate) fn held(&self) -> usize {
        self.len + self.size
    }

    /// The bytes, their length and their SHA-256.
    fn finish(mut self) -> (PutPayload, usize, [u8; 32]) {
        use sha2::Digest;
        if !self.current.is_empty() {
            self.done
                .push(Bytes::from(std::mem::take(&mut self.current)));
        }
        let mut hash = sha2::Sha256::new();
        for chunk in &self.done {
            hash.update(chunk);
        }
        (
            self.done.into_iter().collect(),
            self.len,
            hash.finalize().into(),
        )
    }
}

/// What the page indexes and footer of `groups` hold, by their page
/// counts.
fn index_bytes(groups: &[parquet::file::metadata::RowGroupMetaData]) -> usize {
    use parquet::basic::PageType;
    groups
        .iter()
        .flat_map(parquet::file::metadata::RowGroupMetaData::columns)
        .map(|c| {
            let pages = c.page_encoding_stats().map_or(
                usize::try_from(c.num_values()).unwrap_or(usize::MAX),
                |stats| {
                    stats
                        .iter()
                        .filter(|s| {
                            matches!(s.page_type, PageType::DATA_PAGE | PageType::DATA_PAGE_V2)
                        })
                        .map(|s| usize::try_from(s.count).unwrap_or(usize::MAX))
                        .sum()
                },
            );
            let descr = c.column_descr();
            pages
                .saturating_mul(page_index_bytes(
                    descr.max_def_level(),
                    descr.max_rep_level(),
                ))
                .saturating_add(CHUNK_META_BYTES)
        })
        .fold(0, usize::saturating_add)
}

/// What writing a row group adds besides its rows, for one writer.
struct RowGroupCost {
    /// The bloom filters of a row group, sized as the Parquet split-block
    /// filter sizes them for each column's distinct values and
    /// false-positive rate, before folding.
    blooms: usize,
    /// Leaf columns.
    leaves: usize,
    /// The largest index entry of a page of any leaf.
    page_entry: usize,
    page_bytes: usize,
    page_rows: usize,
}

impl RowGroupCost {
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    fn new(
        schema: &SchemaRef,
        properties: &parquet::file::properties::WriterProperties,
    ) -> Result<Self, CompactError> {
        let descr = parquet::arrow::ArrowSchemaConverter::new().convert(schema)?;
        let blooms = descr
            .columns()
            .iter()
            .filter_map(|c| properties.bloom_filter_properties(c.path()))
            .map(|b| {
                let bits = -8.0 * b.ndv() as f64 / (1.0 - b.fpp().powf(1.0 / 8.0)).ln();
                ((bits / 8.0) as usize)
                    .clamp(32, 128 << 20)
                    .next_power_of_two()
                    + 64
            })
            .sum();
        let page_entry = descr
            .columns()
            .iter()
            .map(|c| page_index_bytes(c.max_def_level(), c.max_rep_level()))
            .max()
            .unwrap_or(0);
        Ok(Self {
            blooms,
            leaves: descr.num_columns(),
            page_entry,
            page_bytes: properties.data_page_size_limit().max(1),
            page_rows: properties.data_page_row_count_limit().max(1),
        })
    }

    /// For a row group of `rows` rows whose values take `payload` bytes:
    /// its bloom filters, built and written, and its page indexes. A page
    /// closes past the page size or row limit, so a column has at most
    /// those pages, one at a dictionary fallback and its last; a value
    /// encodes to no more than its payload and a four-byte index.
    fn of(&self, rows: usize, payload: usize) -> usize {
        let encoded = payload.saturating_add(rows.saturating_mul(4 * self.leaves));
        let pages = (encoded / self.page_bytes)
            .saturating_add(self.leaves.saturating_mul(rows / self.page_rows + 2));
        (2 * self.blooms).saturating_add(FOOTER_COPIES * pages.saturating_mul(self.page_entry))
    }
}

/// One output file being written.
pub(crate) struct OutputBuilder {
    signal: Signal,
    level: Level,
    id: FileId,
    path: Path,
    writer: ArrowWriter<Chunks>,
    text: Option<fs_index::TextIndexBuilder>,
    pub(crate) rows: usize,
    ts_min: i64,
    ts_max: i64,
    services: BTreeSet<String>,
    /// More distinct services than the cap: the row lists none, unknown.
    services_over: bool,
    traces: Vec<[u8; TRACE_ID_BYTES]>,
    /// More trace ids than the cap: the file is published without a
    /// `trace_id` sidecar.
    traces_over: bool,
    max_trace_bytes: usize,
    max_services: usize,
    /// The row groups whose indexes `index_bytes` counts.
    indexed: usize,
    index_bytes: usize,
    cost: RowGroupCost,
}

impl OutputBuilder {
    /// The output `id` of `level` at its `path`.
    pub(crate) fn new(
        schema: &SchemaRef,
        signal: Signal,
        id: FileId,
        path: Path,
        config: &MergeConfig,
    ) -> Result<Self, CompactError> {
        let row_group_rows = config.row_group_rows;
        let logs = signal == Signal::Logs;
        let properties = match (config.level, logs) {
            (Level::L2, _) => l2_writer_properties(logs, row_group_rows)?,
            (_, true) => log_writer_properties(row_group_rows)?,
            (_, false) => writer_properties(row_group_rows)?,
        };
        let cost = RowGroupCost::new(schema, &properties)?;
        let chunk = (config.output_bytes / 64).clamp(MIN_CHUNK_BYTES, MAX_CHUNK_BYTES);
        let writer =
            ArrowWriter::try_new(Chunks::new(chunk), Arc::clone(schema), Some(properties))?;
        // Grows with the row groups; a pass's outputs are not sized ahead.
        let text = logs
            .then(|| fs_index::TextIndexBuilder::new(path.as_ref(), config.text_index, 0, 0))
            .transpose()?;
        Ok(Self {
            signal,
            level: config.level,
            id,
            path,
            writer,
            text,
            rows: 0,
            ts_min: i64::MAX,
            ts_max: i64::MIN,
            services: BTreeSet::new(),
            services_over: false,
            traces: Vec::new(),
            traces_over: false,
            max_trace_bytes: config.max_trace_index_bytes,
            max_services: config.max_services_per_file,
            indexed: 0,
            index_bytes: 0,
            cost,
        })
    }

    pub(crate) fn write(&mut self, group: &RecordBatch) -> Result<(), CompactError> {
        let schema = group.schema();
        let ts = group
            .column(schema.index_of(col::TIMESTAMP)?)
            .as_primitive::<TimestampNanosecondType>();
        self.ts_min = self
            .ts_min
            .min(arrow::compute::min(ts).unwrap_or(self.ts_min));
        self.ts_max = self
            .ts_max
            .max(arrow::compute::max(ts).unwrap_or(self.ts_max));
        self.note_services(group)?;
        if self.signal == Signal::Traces && !self.traces_over {
            if let Some(ids) = trace_ids(group) {
                self.traces.extend(ids);
            }
            self.bound_traces();
        }
        self.writer.write(group)?;
        self.writer.flush()?;
        if let Some(text) = self.text.as_mut() {
            let bodies = group
                .column(schema.index_of(fs_schema::logs::col::BODY)?)
                .as_string::<i32>();
            text.add_row_group((0..bodies.len()).map(|i| bodies.value(i).as_bytes()))?;
        }
        self.rows += group.num_rows();
        let groups = self.writer.flushed_row_groups();
        self.index_bytes = self
            .index_bytes
            .saturating_add(index_bytes(groups.get(self.indexed..).unwrap_or_default()));
        self.indexed = groups.len();
        Ok(())
    }

    fn note_services(&mut self, group: &RecordBatch) -> Result<(), CompactError> {
        if self.services_over {
            return Ok(());
        }
        let service = arrow::compute::cast(
            group.column(group.schema().index_of(col::SERVICE_NAME)?),
            &DataType::Utf8,
        )?;
        for value in service.as_string::<i32>().iter().flatten() {
            if !self.services.contains(value) {
                self.services.insert(value.to_owned());
            }
        }
        if self.services.len() > self.max_services {
            self.services_over = true;
            self.services = BTreeSet::new();
        }
        Ok(())
    }

    /// Drops the trace ids, and the sidecar with them, once they pass
    /// their cap even without repeats.
    fn bound_traces(&mut self) {
        if self.traces.len() * TRACE_ID_BYTES <= self.max_trace_bytes {
            return;
        }
        self.traces.sort_unstable();
        self.traces.dedup();
        if self.traces.len() * TRACE_ID_BYTES > self.max_trace_bytes {
            self.traces_over = true;
            self.traces = Vec::new();
        }
    }

    pub(crate) fn bytes_written(&self) -> usize {
        self.writer.bytes_written()
    }

    /// Bytes the output holds, and what finishing it allocates: its
    /// buffers, the row group in progress, the page indexes and the
    /// footer with the copies of them closing makes, the service names,
    /// the trace ids with the sorted entries and two encoded copies
    /// `finish` makes of them, and the text sidecar, which may double once
    /// as it closes.
    pub(crate) fn memory(&self) -> usize {
        let services: usize = self
            .services
            .iter()
            .map(|s| s.len() + SERVICE_OVERHEAD)
            .sum();
        let traces = self.traces.capacity() * TRACE_ID_BYTES
            + self.traces.len() * TRACE_ENTRY_BYTES * TRACE_FINISH_COPIES;
        let text = self
            .text
            .as_ref()
            .map_or(0, fs_index::TextIndexBuilder::allocated)
            * 2;
        let footer = FOOTER_COPIES * (self.index_bytes + FOOTER_FIXED_BYTES);
        self.writer.inner().held() + self.writer.memory_size() + footer + services + traces + text
    }

    /// What writing one more row group of `rows` rows whose values take
    /// `payload` bytes holds besides them and their encoding, as
    /// [`RowGroupCost::of`].
    pub(crate) fn row_group_overhead(&self, rows: usize, payload: usize) -> usize {
        self.cost.of(rows, payload)
    }

    pub(crate) fn finish(mut self, tenant: TenantId) -> Result<Output, CompactError> {
        let (bytes, size, content_sha256) = self.writer.into_inner()?.finish();
        let (id, path) = (self.id, self.path);
        let no_sidecar = || CompactError::Merge("output path has no sidecar");
        let index = if let Some(text) = self.text {
            let index_path =
                fs_index::text_index::sidecar_path(path.as_ref()).ok_or_else(no_sidecar)?;
            Some((index_path, Bytes::from(text.finish(content_sha256)?)))
        } else if self.signal == Signal::Traces && !self.traces_over {
            let index_path = fs_index::sidecar_path(path.as_ref()).ok_or_else(no_sidecar)?;
            self.traces.sort_unstable();
            self.traces.dedup();
            let index = TraceIndex::new(
                vec![path.to_string()],
                self.traces.iter().map(|t| (*t, 0u32)),
            )?;
            Some((index_path, Bytes::from(index.encode())))
        } else {
            None
        };
        let file = NewFile {
            id,
            origin: id,
            tenant_id: tenant,
            signal: self.signal,
            level: self.level,
            path: path.to_string(),
            size_bytes: i64::try_from(size).unwrap_or(i64::MAX),
            row_count: i64::try_from(self.rows).unwrap_or(i64::MAX),
            range: TimeRange {
                start: Utc.timestamp_nanos(self.ts_min),
                end: Utc.timestamp_nanos(self.ts_max),
            },
            services: self.services.into_iter().collect(),
            summary: None,
            content_sha256: Some(content_sha256),
            usage_accounted: true,
        };
        Ok(Output { file, bytes, index })
    }
}

#[cfg(test)]
mod tests;

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

/// Bytes of each buffer an output is written into.
const CHUNK_BYTES: usize = 4 << 20;

/// An output's bytes in buffers of [`CHUNK_BYTES`], so growing never
/// copies what is written, as a doubling buffer would.
#[derive(Default)]
pub(crate) struct Chunks {
    done: Vec<Bytes>,
    current: Vec<u8>,
}

impl std::io::Write for Chunks {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.current.len() == self.current.capacity() {
            let full = std::mem::replace(&mut self.current, Vec::with_capacity(CHUNK_BYTES));
            if !full.is_empty() {
                self.done.push(Bytes::from(full));
            }
        }
        let n = buf.len().min(self.current.capacity() - self.current.len());
        self.current
            .extend_from_slice(buf.get(..n).unwrap_or_default());
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Chunks {
    /// Bytes the buffers hold, written or not.
    pub(crate) fn capacity(&self) -> usize {
        self.done.iter().map(Bytes::len).sum::<usize>() + self.current.capacity()
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
        let len = self.done.iter().map(Bytes::len).sum();
        (self.done.into_iter().collect(), len, hash.finalize().into())
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
        let writer = ArrowWriter::try_new(Chunks::default(), Arc::clone(schema), Some(properties))?;
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
    /// buffers, the row group in progress, the service names, the trace
    /// ids with the sorted entries and two encoded copies `finish` makes
    /// of them, and the text sidecar, which may double once as it closes.
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
        self.writer.inner().capacity() + self.writer.memory_size() + services + traces + text
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

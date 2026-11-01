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
use object_store::path::Path;
use parquet::arrow::ArrowWriter;

use crate::CompactError;
use crate::merge::MergeConfig;

/// One output file ready to upload with its catalog row and sidecar.
pub struct Output {
    /// Catalog row.
    pub file: NewFile,
    /// Parquet bytes.
    pub bytes: Bytes,
    /// Where the sidecar goes and its bytes: the `trace_id` sidecar of a
    /// span file, the text sidecar of a log file; `None` when its cap was
    /// passed, so the file is scanned.
    pub index: Option<(String, Bytes)>,
}

/// One output file being written.
pub(crate) struct OutputBuilder {
    signal: Signal,
    level: Level,
    id: FileId,
    path: Path,
    writer: ArrowWriter<Vec<u8>>,
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
        let writer = ArrowWriter::try_new(Vec::new(), Arc::clone(schema), Some(properties))?;
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

    /// Bytes the output holds: what is encoded, the row group in progress,
    /// the trace ids and the text sidecar.
    pub(crate) fn memory(&self) -> usize {
        self.writer.bytes_written()
            + self.writer.memory_size()
            + self.traces.capacity() * TRACE_ID_BYTES
            + self
                .text
                .as_ref()
                .map_or(0, fs_index::TextIndexBuilder::allocated)
    }

    pub(crate) fn finish(mut self, tenant: TenantId) -> Result<Output, CompactError> {
        let bytes = Bytes::from(self.writer.into_inner()?);
        let (id, path) = (self.id, self.path);
        let content_sha256 = crate::objects::sha256(&bytes);
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
            size_bytes: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
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

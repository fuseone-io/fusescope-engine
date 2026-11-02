//! The series rows a selector reads: from each series file of the window,
//! the rows its postings allow when it has usable ones, else every row of
//! the names asked for, and from the hot window. Every row is a series in
//! one hour; rows outside the window's hours are never visited.

use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{AsArray, BooleanArray};
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter, RowSelection};
use fs_catalog::{FileRecord, Level};
use fs_index::postings::Postings;
use fs_schema::metrics::{SeriesRow, col, series_row_sizes, series_rows, series_schema_version};
use futures_util::TryStreamExt;
use object_store::ObjectStore;
use object_store::path::Path;
use roaring::RoaringBitmap;

use super::matchers::Matcher;
use super::request::Window;
use crate::QueryError;
use crate::derived_table::Footer;
use crate::metric_index::MetricIndexCache;

/// Rows a series file is read in.
const BATCH_ROWS: usize = 8192;

/// Bytes of one selector of a row selection, and of the range it comes
/// from.
const SELECTOR_BYTES: usize = 48;

/// The label every series row carries with its name.
const NAME_LABEL: &str = "__name__";

/// Which rows of a source to read.
#[derive(Clone)]
pub(crate) enum Narrow {
    /// Rows that may pass these matchers: by postings, those of the label
    /// each label-requiring matcher accepts; by name, those its `__name__`
    /// matchers accept.
    Matchers(Arc<[Matcher]>),
    /// Rows of these names.
    Names(Arc<HashSet<Box<str>>>),
}

impl Narrow {
    fn passes(&self, name: &str) -> bool {
        match self {
            Self::Matchers(matchers) => matchers
                .iter()
                .filter(|m| &*m.name == NAME_LABEL)
                .all(|m| m.matches(name)),
            Self::Names(names) => names.contains(name),
        }
    }

    /// Whether postings can narrow a file's rows.
    fn uses_postings(&self) -> bool {
        match self {
            Self::Matchers(matchers) => matchers.iter().any(Matcher::requires_label),
            Self::Names(_) => true,
        }
    }

    fn tests_names(&self) -> bool {
        match self {
            Self::Matchers(matchers) => matchers.iter().any(|m| &*m.name == NAME_LABEL),
            Self::Names(_) => true,
        }
    }

    /// The rows of a file its postings allow and their charge, three
    /// times the bitmaps combined at twice their encoded bytes: the rows so
    /// far, the union being built and their intersection. The charge is
    /// reserved before any of them is built and held until the caller
    /// drops it with the rows. `None` when the postings do not narrow it.
    pub(super) fn rows(
        &self,
        postings: &Postings,
        reservation: &MemoryReservation,
    ) -> Result<Option<(RoaringBitmap, MemoryReservation)>, QueryError> {
        let groups = self.groups();
        if groups == 0 {
            return Ok(None);
        }
        let mut bytes = 0usize;
        for group in 0..groups {
            self.each_bitmap(postings, group, &mut |bitmap| {
                bytes = bytes.saturating_add(bitmap.serialized_size().saturating_mul(2));
            });
        }
        let charge = bytes.saturating_mul(3);
        reservation.try_grow(charge)?;
        let held = reservation.split(charge);
        let mut rows: Option<RoaringBitmap> = None;
        for group in 0..groups {
            let mut union = RoaringBitmap::new();
            self.each_bitmap(postings, group, &mut |bitmap| union |= bitmap);
            rows = Some(match rows {
                Some(rows) => rows & union,
                None => union,
            });
        }
        Ok(rows.map(|rows| (rows, held)))
    }

    /// The bitmaps that intersect: one group a label-requiring matcher, or
    /// the one of the names.
    fn groups(&self) -> usize {
        match self {
            Self::Matchers(matchers) => matchers.iter().filter(|m| m.requires_label()).count(),
            Self::Names(_) => 1,
        }
    }

    /// Hands `f` each bitmap of `group`, whose union it accepts.
    fn each_bitmap(&self, postings: &Postings, group: usize, f: &mut dyn FnMut(&RoaringBitmap)) {
        match self {
            Self::Matchers(matchers) => {
                if let Some(matcher) = matchers.iter().filter(|m| m.requires_label()).nth(group) {
                    accepted(postings, matcher, f);
                }
            }
            Self::Names(names) => names
                .iter()
                .filter_map(|n| postings.rows(NAME_LABEL, n))
                .for_each(f),
        }
    }
}

/// Hands `f` the bitmap of each value of `matcher`'s label it accepts.
fn accepted(postings: &Postings, matcher: &Matcher, f: &mut dyn FnMut(&RoaringBitmap)) {
    if let Some(value) = matcher.equal_to() {
        postings.rows(&matcher.name, value).into_iter().for_each(f);
        return;
    }
    for (key, values) in postings.keys().filter(|(key, _)| *key == &*matcher.name) {
        values
            .filter(|value| matcher.matches(value))
            .filter_map(|value| postings.rows(key, value))
            .for_each(&mut *f);
    }
}

/// Where a selector's series rows come from.
pub(crate) struct Sources<'a> {
    pub(crate) store: Arc<dyn ObjectStore>,
    /// The series files of the window the hot window does not serve.
    pub(crate) files: Vec<FileRecord>,
    /// The hot window's series rows.
    pub(crate) hot: Vec<RecordBatch>,
    /// What `hot` is charged to, for as long as it is held.
    pub(crate) _hot_held: Arc<MemoryReservation>,
    pub(crate) postings: &'a MetricIndexCache,
    pub(crate) window: Window,
}

/// What reading a source came to.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Visited {
    /// Files left unread because their postings hold no row asked for.
    pub(crate) pruned: u64,
}

impl Sources<'_> {
    /// Hands every row of the window `narrow` keeps to `visit`, which
    /// reserves what it keeps; the rows of a batch are reserved while they
    /// are visited.
    ///
    /// # Errors
    /// A file that does not read as series, a store failure, a refused
    /// reservation, or what `visit` returns.
    pub(crate) async fn visit(
        &self,
        narrow: &Narrow,
        reservation: &mut MemoryReservation,
        visit: &mut impl FnMut(SeriesRow, &mut MemoryReservation) -> Result<(), QueryError>,
    ) -> Result<Visited, QueryError> {
        let mut visited = Visited::default();
        for file in &self.files {
            if !self.visit_file(file, narrow, reservation, visit).await? {
                visited.pruned += 1;
            }
        }
        for batch in &self.hot {
            self.visit_batch((batch, "hot window"), narrow, reservation, visit)?;
        }
        Ok(visited)
    }

    /// False when the file's postings leave nothing to read.
    async fn visit_file(
        &self,
        file: &FileRecord,
        narrow: &Narrow,
        reservation: &mut MemoryReservation,
        visit: &mut impl FnMut(SeriesRow, &mut MemoryReservation) -> Result<(), QueryError>,
    ) -> Result<bool, QueryError> {
        let parquet = |e| QueryError::DataFusion(DataFusionError::from(e));
        let reader = Footer {
            store: Arc::clone(&self.store),
            path: Path::from(file.path.as_str()),
            size: u64::try_from(file.size_bytes).unwrap_or(0),
        };
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(parquet)?;
        let schema = Arc::clone(builder.schema());
        let version = series_schema_version(&schema).map_err(|source| QueryError::MetricFile {
            path: file.path.clone(),
            source,
        })?;
        let total = usize::try_from(builder.metadata().file_metadata().num_rows()).unwrap_or(0);
        let mut builder = builder.with_batch_size(BATCH_ROWS);
        let selected = reservation.new_empty();
        if file.level != Level::L0 && narrow.uses_postings() {
            if let Some(postings) = self.postings.postings(file, version).await {
                if let Some((rows, held)) = narrow.rows(&postings, reservation)? {
                    if rows.is_empty() {
                        return Ok(false);
                    }
                    selected.try_grow(runs(&rows).saturating_mul(SELECTOR_BYTES))?;
                    let selection =
                        RowSelection::from_consecutive_ranges(consecutive(&rows), total);
                    drop((rows, held));
                    builder = builder.with_row_selection(selection);
                }
            }
        }
        if narrow.tests_names() {
            let index = schema.index_of(col::METRIC_NAME)?;
            let mask = ProjectionMask::roots(builder.parquet_schema(), [index]);
            let test = narrow.clone();
            let predicate = ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
                let names =
                    arrow::compute::cast(batch.column(0), &arrow::datatypes::DataType::Utf8)?;
                Ok(names
                    .as_string::<i32>()
                    .iter()
                    .map(|n| Some(n.is_some_and(|n| test.passes(n))))
                    .collect::<BooleanArray>())
            });
            builder = builder.with_row_filter(RowFilter::new(vec![Box::new(predicate)]));
        }
        let mut stream = builder.build().map_err(parquet)?;
        while let Some(batch) = stream.try_next().await.map_err(parquet)? {
            let batch = RecordBatch::try_new(Arc::clone(&schema), batch.columns().to_vec())?;
            self.visit_batch((&batch, &file.path), narrow, reservation, visit)?;
        }
        drop(selected);
        Ok(true)
    }

    fn visit_batch(
        &self,
        (batch, origin): (&RecordBatch, &str),
        narrow: &Narrow,
        reservation: &mut MemoryReservation,
        visit: &mut impl FnMut(SeriesRow, &mut MemoryReservation) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        let unreadable = |source| QueryError::MetricFile {
            path: origin.into(),
            source,
        };
        let bytes: usize = series_row_sizes(batch).map_err(unreadable)?.iter().sum();
        reservation.try_grow(bytes)?;
        let rows = series_rows(batch).map_err(unreadable)?;
        for row in rows {
            let in_window =
                row.unix_milli >= self.window.first_hour && row.unix_milli <= self.window.end;
            if in_window && narrow.passes(&row.name) {
                visit(row, reservation)?;
            }
        }
        reservation.shrink(bytes);
        Ok(())
    }
}

/// How many runs of consecutive rows `rows` holds.
fn runs(rows: &RoaringBitmap) -> usize {
    let mut count = 0;
    let mut last: Option<u32> = None;
    for row in rows {
        if last.is_none_or(|last| last.checked_add(1) != Some(row)) {
            count += 1;
        }
        last = Some(row);
    }
    count
}

/// The runs of consecutive rows of `rows`, in order.
fn consecutive(rows: &RoaringBitmap) -> impl Iterator<Item = Range<usize>> + '_ {
    let mut iter = rows.iter().map(|r| r as usize).peekable();
    std::iter::from_fn(move || {
        let start = iter.next()?;
        let mut end = start + 1;
        while iter.peek() == Some(&end) {
            iter.next();
            end += 1;
        }
        Some(start..end)
    })
}

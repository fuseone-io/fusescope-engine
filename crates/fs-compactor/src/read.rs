//! Reading a merge input within a byte budget.
//!
//! A bounded read plans chunks of rows from the file's page index before
//! fetching any: what reading a chunk holds is bounded by the pages it
//! touches in every column, compressed as fetched, decompressed one at a
//! time and decoded, by the unencoded size the index records for each
//! page, and by the dictionary of each column, sized from its page
//! header. A chunk is fetched and decoded on its own, so the bound holds
//! whatever the width of the rows before it. A file without the unencoded
//! sizes cannot be bounded and is refused.

use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use fs_catalog::FileRecord;
use futures_util::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, RowSelection, RowSelector,
};
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use parquet::basic::Type as PhysicalType;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, RowGroupMetaData};

use crate::CompactError;

mod object;

use object::FETCHES;
pub(crate) use object::ObjectReader;
#[cfg(test)]
use object::touching_runs;

/// Bytes a decoded value holds besides its payload: its offset, its
/// levels and its validity bit.
const VALUE_OVERHEAD: usize = 16;

/// Bytes reading any batch costs per leaf column, whatever its rows: the
/// arrays, decoders and page readers.
const LEAF_OVERHEAD: usize = 8 * 1024;

/// Bytes a row holds in a loaded slot besides its columns: its service
/// key, timestamp and size.
pub(crate) const SLOT_ROW_BYTES: usize = 40;

/// Bytes read to size a dictionary page from its header.
const DICTIONARY_HEADER_BYTES: u64 = 64;

/// Rows `rows` of row group `group`, and the most bytes reading them
/// holds, the decoded batch included.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Chunk {
    pub(crate) group: usize,
    pub(crate) rows: Range<usize>,
    pub(crate) bytes: usize,
}

/// One leaf column of one row group, as reading a range of its rows
/// costs.
struct Leaf {
    /// First row of each page.
    first_rows: Vec<usize>,
    /// What each page costs whatever rows of it are read, summed from the
    /// first page: compressed, decompressed and its variable payload
    /// decoded, with its values when the leaf is nested.
    cost_before: Vec<usize>,
    /// What each row read costs when the leaf is flat, one value a row.
    per_row: usize,
    /// The dictionary: compressed, decompressed and decoded.
    dictionary: usize,
}

impl Leaf {
    fn page_of(&self, row: usize) -> usize {
        self.first_rows
            .partition_point(|first| *first <= row)
            .saturating_sub(1)
    }

    fn cost(&self, rows: &Range<usize>) -> usize {
        let first = self.page_of(rows.start);
        let last = self.page_of(rows.end.saturating_sub(1).max(rows.start));
        let pages = self
            .cost_before
            .get(last + 1)
            .zip(self.cost_before.get(first))
            .map_or(usize::MAX, |(end, start)| end.saturating_sub(*start));
        pages
            .saturating_add(self.dictionary)
            .saturating_add(self.per_row.saturating_mul(rows.len()))
    }
}

/// What reading any range of rows of a file costs, from its page index.
pub(crate) struct Layout {
    groups: Vec<(usize, Vec<Leaf>)>,
    /// The file's metadata as held while it is read.
    pub(crate) metadata_bytes: usize,
    /// The least any chunk of the file can cost: the costliest single row.
    pub(crate) floor: usize,
}

impl Layout {
    /// The layout of `metadata`, read with its page index, sizing each
    /// dictionary from the header `reader` fetches; `coerce` when the
    /// file's columns are cast on reading, and `copied` the columns copied
    /// once more after, each a further decoding.
    ///
    /// # Errors
    /// A file without a page index or without the unencoded size of its
    /// byte-array pages, or a store failure.
    pub(crate) async fn load(
        reader: &ObjectReader,
        metadata: &ParquetMetaData,
        coerce: bool,
        copied: &[&str],
    ) -> Result<Self, CompactError> {
        let offsets = metadata
            .offset_index()
            .ok_or(CompactError::Merge("an input without an offset index"))?;
        let dictionaries = dictionary_sizes(reader, metadata).await?;
        let mut groups = Vec::with_capacity(metadata.num_row_groups());
        let mut index_bytes = 0usize;
        for (g, group) in metadata.row_groups().iter().enumerate() {
            let rows = usize::try_from(group.num_rows()).unwrap_or(0);
            let mut leaves = Vec::with_capacity(group.num_columns());
            for c in 0..group.num_columns() {
                let dictionary = dictionaries.get(g).and_then(|d| d.get(c)).copied();
                let path = group.column(c).column_path().string();
                let decodes =
                    2 + usize::from(coerce) + usize::from(copied.contains(&path.as_str()));
                let leaf = leaf(metadata, offsets, group, (g, c), rows, decodes, dictionary)?;
                index_bytes += leaf.first_rows.len() * 2 * std::mem::size_of::<usize>();
                leaves.push(leaf);
            }
            groups.push((rows, leaves));
        }
        let mut layout = Self {
            groups,
            metadata_bytes: metadata.memory_size().saturating_add(index_bytes),
            floor: 0,
        };
        layout.floor = layout.minimal_cost();
        Ok(layout)
    }

    /// What reading `rows` of row group `group` holds at most.
    fn cost(&self, group: usize, rows: &Range<usize>) -> usize {
        self.groups.get(group).map_or(usize::MAX, |(_, leaves)| {
            leaves
                .iter()
                .fold(LEAF_OVERHEAD.saturating_mul(leaves.len()), |sum, leaf| {
                    sum.saturating_add(leaf.cost(rows))
                })
                .saturating_add(SLOT_ROW_BYTES.saturating_mul(rows.len()))
        })
    }

    /// Where the pages of any leaf of `group` start, and its end.
    fn cuts(&self, group: usize) -> Vec<usize> {
        let Some((rows, leaves)) = self.groups.get(group) else {
            return Vec::new();
        };
        let mut cuts: Vec<usize> = leaves
            .iter()
            .flat_map(|l| l.first_rows.iter().copied())
            .filter(|r| r < rows)
            .chain([0, *rows])
            .collect();
        cuts.sort_unstable();
        cuts.dedup();
        cuts
    }

    fn minimal_cost(&self) -> usize {
        (0..self.groups.len())
            .flat_map(|g| {
                let cuts = self.cuts(g);
                let len = cuts.len().saturating_sub(1);
                cuts.into_iter()
                    .take(len)
                    .map(move |r| (g, r))
                    .collect::<Vec<_>>()
            })
            .map(|(g, r)| self.cost(g, &(r..r + 1)))
            .max()
            .unwrap_or(0)
    }

    /// Chunks of at most `max_rows` rows, each costing at most `budget`
    /// unless a single row costs more, in file order. A chunk costs no
    /// less for having more rows, so each takes as many as fit.
    pub(crate) fn plan(&self, budget: usize, max_rows: usize) -> VecDeque<Chunk> {
        let mut chunks = VecDeque::new();
        for (group, (rows, _)) in self.groups.iter().enumerate() {
            let mut start = 0;
            while start < *rows {
                let fits = |stop: usize| self.cost(group, &(start..stop)) <= budget;
                let (mut low, mut high) = (start + 1, (start + max_rows.max(1)).min(*rows));
                while low < high {
                    let mid = low + (high - low).div_ceil(2);
                    if fits(mid) {
                        low = mid;
                    } else {
                        high = mid - 1;
                    }
                }
                chunks.push_back(Chunk {
                    group,
                    rows: start..low,
                    bytes: self.cost(group, &(start..low)),
                });
                start = low;
            }
        }
        chunks
    }
}

fn leaf(
    metadata: &ParquetMetaData,
    offsets: &parquet::file::metadata::ParquetOffsetIndex,
    group: &RowGroupMetaData,
    (g, c): (usize, usize),
    rows: usize,
    decodes: usize,
    dictionary: Option<usize>,
) -> Result<Leaf, CompactError> {
    let chunk = group.column(c);
    let descr = chunk.column_descr();
    let index = offsets
        .get(g)
        .and_then(|o| o.get(c))
        .ok_or(CompactError::Merge("an input without an offset index"))?;
    let pages = index.page_locations();
    let byte_array = chunk.column_type() == PhysicalType::BYTE_ARRAY;
    let unencoded = index.unencoded_byte_array_data_bytes();
    if byte_array && unencoded.is_none_or(|u| u.len() != pages.len()) {
        return Err(CompactError::Merge(
            "an input without the unencoded size of its pages",
        ));
    }
    let width = match chunk.column_type() {
        PhysicalType::BOOLEAN => 1,
        PhysicalType::INT32 | PhysicalType::FLOAT => 4,
        PhysicalType::INT64 | PhysicalType::DOUBLE => 8,
        PhysicalType::INT96 => 12,
        PhysicalType::FIXED_LEN_BYTE_ARRAY => usize::try_from(descr.type_length()).unwrap_or(0),
        PhysicalType::BYTE_ARRAY => 0,
    };
    let per_value = width + VALUE_OVERHEAD;
    let flat = descr.max_rep_level() == 0;
    let chunk_levels = usize::try_from(chunk.num_values()).unwrap_or(usize::MAX);
    let decompressed_cap = usize::try_from(chunk.uncompressed_size()).unwrap_or(usize::MAX);
    let column_index = metadata
        .column_index()
        .and_then(|i| i.get(g))
        .and_then(|i| i.get(c));
    let mut first_rows = Vec::with_capacity(pages.len());
    let mut cost_before = Vec::with_capacity(pages.len() + 1);
    let mut sum = 0usize;
    cost_before.push(0);
    for (p, page) in pages.iter().enumerate() {
        let first = usize::try_from(page.first_row_index).unwrap_or(0);
        let next = pages
            .get(p + 1)
            .map_or(rows, |n| usize::try_from(n.first_row_index).unwrap_or(rows));
        let levels = column_index
            .and_then(|i| i.definition_level_histogram(p))
            .map_or(
                if flat {
                    next.saturating_sub(first)
                } else {
                    chunk_levels
                },
                |h| h.iter().map(|n| usize::try_from(*n).unwrap_or(0)).sum(),
            );
        let payload = unencoded
            .and_then(|u| u.get(p))
            .map_or(0, |b| usize::try_from(*b).unwrap_or(usize::MAX));
        let values = levels.saturating_mul(per_value);
        let compressed = usize::try_from(page.compressed_page_size).unwrap_or(usize::MAX);
        let decompressed = payload.saturating_add(values).min(decompressed_cap);
        let output = payload.saturating_add(if flat { 0 } else { values });
        sum = sum
            .saturating_add(compressed)
            .saturating_add(decompressed)
            .saturating_add(output.saturating_mul(decodes));
        first_rows.push(first);
        cost_before.push(sum);
    }
    let dictionary = dictionary.unwrap_or(0);
    Ok(Leaf {
        first_rows,
        cost_before,
        per_row: if flat { per_value * decodes } else { 0 },
        dictionary,
    })
}

/// What the dictionary of each leaf of each row group holds while read:
/// its compressed bytes, decompressed and decoded, sized from its page
/// header, or from the column chunk when the header cannot be read.
async fn dictionary_sizes(
    reader: &ObjectReader,
    metadata: &ParquetMetaData,
) -> Result<Vec<Vec<usize>>, CompactError> {
    let offsets = metadata
        .offset_index()
        .ok_or(CompactError::Merge("an input without an offset index"))?;
    let mut spans = Vec::new();
    for (g, group) in metadata.row_groups().iter().enumerate() {
        for c in 0..group.num_columns() {
            let chunk = group.column(c);
            let (start, _) = chunk.byte_range();
            let first = offsets
                .get(g)
                .and_then(|o| o.get(c))
                .and_then(|i| i.page_locations().first())
                .and_then(|p| u64::try_from(p.offset).ok())
                .unwrap_or(start);
            if first > start {
                spans.push(((g, c), start..first));
            }
        }
    }
    let headers: Vec<Bytes> = futures_util::stream::iter(spans.iter().cloned())
        .map(|(_, span)| {
            let end = span.end.min(span.start + DICTIONARY_HEADER_BYTES);
            reader.store.get_range(&reader.path, span.start..end)
        })
        .buffered(FETCHES)
        .try_collect()
        .await?;
    let mut sizes: Vec<Vec<usize>> = metadata
        .row_groups()
        .iter()
        .map(|g| vec![0; g.num_columns()])
        .collect();
    for (((g, c), span), header) in spans.iter().zip(headers) {
        let chunk = metadata.row_group(*g).column(*c);
        let compressed = usize::try_from(span.end - span.start).unwrap_or(usize::MAX);
        let decompressed = dictionary_page_size(&header)
            .unwrap_or_else(|| usize::try_from(chunk.uncompressed_size()).unwrap_or(usize::MAX));
        // Decoded, a byte-array dictionary holds its values and an offset
        // for each, at most as many as its four-byte length prefixes.
        let decoded = decompressed.saturating_mul(2);
        if let Some(size) = sizes.get_mut(*g).and_then(|s| s.get_mut(*c)) {
            *size = compressed
                .saturating_add(decompressed)
                .saturating_add(decoded);
        }
    }
    Ok(sizes)
}

/// The uncompressed size a dictionary page header records: a Thrift
/// compact struct whose first fields are the page type, an `i32` that is
/// 2 for a dictionary page, and the uncompressed size, an `i32`.
fn dictionary_page_size(header: &[u8]) -> Option<usize> {
    let mut bytes = header.iter().copied();
    let mut field = |delta: u8| -> Option<i64> {
        // A field header: the id delta in the high nibble, 5 for i32.
        (bytes.next()? == (delta << 4 | 5)).then_some(())?;
        let mut value = 0u64;
        for shift in (0..35).step_by(7) {
            let byte = bytes.next()?;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Some(i64::try_from(value >> 1).ok()? ^ -i64::try_from(value & 1).ok()?);
            }
        }
        None
    };
    (field(1)? == 2).then_some(())?;
    usize::try_from(field(1)?).ok()
}

/// The batches of one input.
pub(crate) enum Input {
    /// The whole file in batches of a number of rows, unbounded.
    Rows(ParquetRecordBatchStream<ObjectReader>),
    /// Planned chunks, one stream at a time.
    Planned {
        reader: ObjectReader,
        metadata: ArrowReaderMetadata,
        chunks: VecDeque<Chunk>,
        current: Option<(ParquetRecordBatchStream<ObjectReader>, usize)>,
    },
}

impl Input {
    /// `file` in batches of at most `rows` rows, unbounded.
    ///
    /// # Errors
    /// Store or Parquet failure opening the file.
    pub(crate) async fn rows(
        store: Arc<dyn ObjectStore>,
        file: &FileRecord,
        rows: usize,
    ) -> Result<Self, CompactError> {
        let reader = object_reader(store, &file.path, file.size_bytes, false);
        let stream = ParquetRecordBatchStreamBuilder::new(reader)
            .await?
            .with_batch_size(rows.max(1))
            .build()?;
        Ok(Self::Rows(stream))
    }

    /// The file at `path`, `size` bytes, opened for a bounded read, with
    /// its layout to plan it with; read as `schema`, `copied` as
    /// [`Layout::load`].
    ///
    /// # Errors
    /// As [`Layout::load`].
    pub(crate) async fn bounded(
        store: Arc<dyn ObjectStore>,
        (path, size): (&str, i64),
        schema: &arrow::datatypes::SchemaRef,
        copied: &[&str],
    ) -> Result<(Self, Layout), CompactError> {
        let mut reader = object_reader(store, path, size, true);
        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
        let metadata = ArrowReaderMetadata::load_async(&mut reader, options).await?;
        let coerce = metadata.schema().fields() != schema.fields();
        let layout = Layout::load(&reader, metadata.metadata(), coerce, copied).await?;
        let input = Self::Planned {
            reader,
            metadata,
            chunks: VecDeque::new(),
            current: None,
        };
        Ok((input, layout))
    }

    /// Reads the file in `chunks`, planned from its layout.
    pub(crate) fn with_chunks(mut self, planned: VecDeque<Chunk>) -> Self {
        if let Self::Planned { chunks, .. } = &mut self {
            *chunks = planned;
        }
        self
    }

    /// The next batch, with what reading it holds at most until the next
    /// call, the batch included; 0 for an unbounded read. `None` once the
    /// file is read.
    ///
    /// # Errors
    /// Store or Parquet failure.
    pub(crate) async fn next(&mut self) -> Result<Option<(RecordBatch, usize)>, CompactError> {
        match self {
            Self::Rows(stream) => Ok(stream.next().await.transpose()?.map(|b| (b, 0))),
            Self::Planned {
                reader,
                metadata,
                chunks,
                current,
            } => loop {
                if let Some((stream, bytes)) = current.as_mut() {
                    if let Some(batch) = stream.next().await.transpose()? {
                        return Ok(Some((batch, *bytes)));
                    }
                    *current = None;
                }
                let Some(chunk) = chunks.pop_front() else {
                    return Ok(None);
                };
                let selection = RowSelection::from(vec![
                    RowSelector::skip(chunk.rows.start),
                    RowSelector::select(chunk.rows.len()),
                ]);
                let stream = ParquetRecordBatchStreamBuilder::new_with_metadata(
                    reader.clone(),
                    metadata.clone(),
                )
                .with_row_groups(vec![chunk.group])
                .with_row_selection(selection)
                .with_batch_size(chunk.rows.len().max(1))
                .build()?;
                *current = Some((stream, chunk.bytes));
            },
        }
    }
}

fn object_reader(store: Arc<dyn ObjectStore>, path: &str, size: i64, exact: bool) -> ObjectReader {
    ObjectReader {
        store,
        path: Path::from(path),
        size: u64::try_from(size).unwrap_or(0),
        exact,
    }
}

#[cfg(test)]
mod tests;

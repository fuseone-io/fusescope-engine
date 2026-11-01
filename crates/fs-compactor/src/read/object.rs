//! Ranged reads of one object for the Parquet readers.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

/// Ranges fetched at once.
pub(super) const FETCHES: usize = 8;

/// Reads one object of known size through ranged gets. `exact` fetches
/// each run of touching ranges on its own, never the gap between two,
/// so what a read fetches is what it asked for.
#[derive(Clone)]
pub(crate) struct ObjectReader {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) path: Path,
    pub(crate) size: u64,
    pub(crate) exact: bool,
}

fn to_parquet_err(e: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(e))
}

impl AsyncFileReader for ObjectReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
        self.store
            .get_range(&self.path, range)
            .map_err(to_parquet_err)
            .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, Result<Vec<Bytes>, ParquetError>> {
        async move {
            if !self.exact {
                return self
                    .store
                    .get_ranges(&self.path, &ranges)
                    .await
                    .map_err(to_parquet_err);
            }
            let runs = touching_runs(&ranges);
            let fetched: Vec<Bytes> = futures_util::stream::iter(runs.iter().cloned())
                .map(|run| self.store.get_range(&self.path, run))
                .buffered(FETCHES)
                .try_collect()
                .await
                .map_err(to_parquet_err)?;
            ranges
                .iter()
                .map(|range| {
                    runs.iter()
                        .zip(&fetched)
                        .find(|(run, _)| run.start <= range.start && range.end <= run.end)
                        .and_then(|(run, bytes)| {
                            let start = usize::try_from(range.start - run.start).ok()?;
                            let end = usize::try_from(range.end - run.start).ok()?;
                            bytes.get(start..end).map(|_| bytes.slice(start..end))
                        })
                        .ok_or_else(|| ParquetError::General("a range outside its run".into()))
                })
                .collect()
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>, ParquetError>> {
        async move {
            let size = self.size;
            let metadata = ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .load_and_finish(self, size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

/// `ranges` sorted and merged where they touch or overlap.
pub(super) fn touching_runs(ranges: &[Range<u64>]) -> Vec<Range<u64>> {
    let mut sorted: Vec<Range<u64>> = ranges.to_vec();
    sorted.sort_by_key(|r| r.start);
    let mut runs: Vec<Range<u64>> = Vec::with_capacity(sorted.len());
    for range in sorted {
        match runs.last_mut() {
            Some(last) if range.start <= last.end => last.end = last.end.max(range.end),
            _ => runs.push(range),
        }
    }
    runs
}

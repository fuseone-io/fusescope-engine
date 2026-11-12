//! `metadata.keys` over every row of a tenant, not only the hours the
//! compactor closed.
//!
//! The derived keys table holds the rows of compacted hours. The rows it
//! does not hold yet are the visible L0 files of the signal and the hot
//! window, and the catalog lists those L0 files and that table in one
//! snapshot, as a compaction swaps them in one transaction: nothing is
//! counted twice nor left between them, a late L0 of an old hour included.
//! The L0 files and the hot window are read as the `spans` or `logs` table
//! reads them, the hot window excluding the files it still serves, under
//! the tenant's byte budget. Their keys are counted hour by hour on what
//! the derived table left of that hour's `max_keys_hour`, as the compactor
//! will count them, so a key is listed exactly when a complete read admits
//! it. A read that exceeds the budget or misses an ingester fails; only a
//! complete listing is kept, for `recent_keys_ttl`.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::AsArray;
use arrow::compute::filter_record_batch;
use arrow::datatypes::TimestampNanosecondType;
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use fs_catalog::{FileRecord, Signal, TenantId};
use fs_schema::attribute_keys::{KeyStats, KeyTable};
use object_store::ObjectStoreExt;

use super::{Querier, QueryWarnings};
use crate::QueryError;
use crate::signal_table::SignalTable;

const HOUR_NS: i64 = 3_600_000_000_000;

/// Complete listings of attribute keys, one a tenant and table, kept for a
/// time and bounded by count and bytes, the oldest dropped first.
pub(super) struct KeysCache {
    ttl: Duration,
    max_entries: usize,
    max_bytes: usize,
    inner: Mutex<Kept>,
}

#[derive(Default)]
struct Kept {
    entries: HashMap<(TenantId, KeyTable), (RecordBatch, Instant, usize)>,
    order: VecDeque<(TenantId, KeyTable)>,
    bytes: usize,
}

impl KeysCache {
    pub(super) fn new(ttl: Duration, max_entries: usize, max_bytes: usize) -> Self {
        Self {
            ttl,
            max_entries,
            max_bytes,
            inner: Mutex::new(Kept::default()),
        }
    }

    pub(super) fn get(&self, tenant: TenantId, table: KeyTable) -> Option<RecordBatch> {
        let kept = self.inner.lock().ok()?;
        let (batch, at, _) = kept.entries.get(&(tenant, table))?;
        (at.elapsed() < self.ttl).then(|| batch.clone())
    }

    /// Keeps `batch`, dropping the oldest listings until it fits; a listing
    /// larger than the whole cache, or a zero TTL, keeps nothing.
    pub(super) fn put(&self, tenant: TenantId, table: KeyTable, batch: RecordBatch) {
        let size = batch.get_array_memory_size();
        if self.ttl.is_zero() || self.max_entries == 0 || size > self.max_bytes {
            return;
        }
        let Ok(mut kept) = self.inner.lock() else {
            return;
        };
        let key = (tenant, table);
        if let Some((_, _, old)) = kept.entries.remove(&key) {
            kept.bytes -= old;
            kept.order.retain(|k| *k != key);
        }
        while kept.entries.len() >= self.max_entries || kept.bytes + size > self.max_bytes {
            let Some(oldest) = kept.order.pop_front() else {
                break;
            };
            if let Some((_, _, bytes)) = kept.entries.remove(&oldest) {
                kept.bytes -= bytes;
            }
        }
        kept.bytes += size;
        kept.order.push_back(key);
        kept.entries.insert(key, (batch, Instant::now(), size));
    }
}

impl Querier {
    /// Every attribute key of `tenant`'s `table`, compacted hours and recent
    /// rows together, in the table's file schema.
    ///
    /// # Errors
    /// Unknown tenant, catalog or bucket failure, a derived table that does
    /// not decode, recent rows over the tenant's byte budget
    /// ([`QueryError::OverBudget`]) or an ingester that does not answer
    /// ([`QueryError::HotUnavailable`]).
    pub(super) async fn listed_keys(
        &self,
        tenant: TenantId,
        table: KeyTable,
        ctx: &SessionContext,
    ) -> Result<RecordBatch, QueryError> {
        if let Some(batch) = self.keys_cache.get(tenant, table) {
            return Ok(batch);
        }
        let record = self
            .catalog
            .tenant(tenant)
            .await?
            .ok_or(QueryError::UnknownTenant)?;
        let (rows, keys) = match table {
            KeyTable::Spans => (Signal::Traces, Signal::AttributeKeys),
            KeyTable::Logs => (Signal::Logs, Signal::LogAttributeKeys),
        };
        let snapshot = self.catalog.keys_snapshot(tenant, rows, keys).await?;
        let derived = self.derived_keys(table, &snapshot.keys).await?;
        let budget = u64::try_from(record.bytes_budget).unwrap_or(u64::MAX);
        let recent = self
            .recent_rows(tenant, table, snapshot.l0, budget, ctx)
            .await?;
        let max_keys = usize::try_from(record.max_keys_hour).unwrap_or(0);
        let counting = |source| QueryError::DerivedTable {
            path: format!("{} of recent rows", table.name()),
            source,
        };
        let stats = absorb_by_hour(derived, &recent, max_keys).map_err(|e| match e {
            Counted::Schema(source) => counting(source),
            Counted::Query(query) => query,
        })?;
        let batch = stats.to_batch().map_err(counting)?;
        self.keys_cache.put(tenant, table, batch.clone());
        Ok(batch)
    }

    /// The derived table of `files`, one in a steady catalog, decoded and
    /// merged, with nothing counted as new.
    async fn derived_keys(
        &self,
        table: KeyTable,
        files: &[FileRecord],
    ) -> Result<KeyStats, QueryError> {
        let mut stats = KeyStats::new(table);
        for file in files {
            let bytes = self
                .store
                .get(&object_store::path::Path::from(file.path.as_str()))
                .await
                .map_err(|e| QueryError::DataFusion(DataFusionError::ObjectStore(Box::new(e))))?
                .bytes()
                .await
                .map_err(|e| QueryError::DataFusion(DataFusionError::ObjectStore(Box::new(e))))?;
            let unreadable = |source| QueryError::DerivedTable {
                path: file.path.clone(),
                source,
            };
            stats
                .merge(fs_schema::attribute_keys::decode(bytes, table).map_err(unreadable)?)
                .map_err(unreadable)?;
        }
        Ok(stats.settled())
    }

    /// The timestamp and map columns of `l0` and of the hot window, as the
    /// row table reads them: the hot window first, the files it serves left
    /// out, an ingester that does not answer failing the read.
    async fn recent_rows(
        &self,
        tenant: TenantId,
        table: KeyTable,
        l0: Vec<FileRecord>,
        budget: u64,
        ctx: &SessionContext,
    ) -> Result<Vec<RecordBatch>, QueryError> {
        let cache = Arc::clone(&self.cache);
        let rows = match table {
            KeyTable::Spans => SignalTable::spans(tenant, cache, self.store_url.clone(), budget),
            KeyTable::Logs => SignalTable::logs(tenant, cache, self.store_url.clone(), budget),
        }
        .with_hot_window(Arc::clone(&self.hot), Arc::new(QueryWarnings::default()))
        .requiring_hot_window()
        .over_files(Arc::new(l0));
        let mut columns = vec!["timestamp"];
        columns.extend(table.map_columns().iter().map(|(name, _, _)| *name));
        let frame = ctx.read_table(Arc::new(rows))?.select_columns(&columns)?;
        frame.collect().await.map_err(typed)
    }
}

/// `derived` with the keys of `rows` counted, each hour on what `derived`
/// left of its budget of `max_keys` new keys, hours in order.
fn absorb_by_hour(
    derived: KeyStats,
    rows: &[RecordBatch],
    max_keys: usize,
) -> Result<KeyStats, Counted> {
    let mut by_hour: BTreeMap<i64, Vec<RecordBatch>> = BTreeMap::new();
    for batch in rows {
        for (hour, part) in split_by_hour(batch).map_err(Counted::Query)? {
            by_hour.entry(hour).or_default().push(part);
        }
    }
    let mut stats = derived;
    for (hour, parts) in by_hour {
        let left = max_keys.saturating_sub(stats.admitted_in(hour));
        stats = stats.settled();
        for part in &parts {
            stats.absorb(part, hour, left).map_err(Counted::Schema)?;
        }
    }
    Ok(stats.settled())
}

/// Why counting the recent rows' keys failed.
enum Counted {
    Schema(fs_schema::SchemaError),
    Query(QueryError),
}

/// `batch`'s rows by the hour of their timestamp.
fn split_by_hour(batch: &RecordBatch) -> Result<Vec<(i64, RecordBatch)>, QueryError> {
    let timestamps = batch
        .column_by_name("timestamp")
        .and_then(|c| c.as_primitive_opt::<TimestampNanosecondType>())
        .ok_or(QueryError::Arrow(arrow::error::ArrowError::SchemaError(
            "recent rows without their timestamp".into(),
        )))?;
    let mut hours: Vec<i64> = timestamps
        .iter()
        .flatten()
        .map(|t| t - t.rem_euclid(HOUR_NS))
        .collect();
    hours.sort_unstable();
    hours.dedup();
    let mut out = Vec::with_capacity(hours.len());
    for hour in hours {
        let mask = arrow::array::BooleanArray::from_iter(
            timestamps
                .iter()
                .map(|t| Some(t.is_some_and(|t| t - t.rem_euclid(HOUR_NS) == hour))),
        );
        out.push((hour, filter_record_batch(batch, &mask)?));
    }
    Ok(out)
}

/// A query error the row table raised inside DataFusion, as itself.
fn typed(e: DataFusionError) -> QueryError {
    match e {
        DataFusionError::External(inner) => match inner.downcast::<QueryError>() {
            Ok(query) => *query,
            Err(other) => QueryError::DataFusion(DataFusionError::External(other)),
        },
        other => QueryError::DataFusion(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    fn listing(rows: u64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));
        let column: ArrayRef = Arc::new(UInt64Array::from_iter_values(0..rows));
        RecordBatch::try_new(schema, vec![column]).unwrap()
    }

    fn tenant() -> TenantId {
        TenantId(uuid::Uuid::new_v4())
    }

    #[test]
    fn a_listing_is_kept_for_its_ttl() {
        let cache = KeysCache::new(Duration::from_millis(50), 4, 1 << 20);
        let t = tenant();
        cache.put(t, KeyTable::Spans, listing(3));
        assert_eq!(cache.get(t, KeyTable::Spans).map(|b| b.num_rows()), Some(3));
        assert!(cache.get(t, KeyTable::Logs).is_none(), "per table");
        std::thread::sleep(Duration::from_millis(60));
        assert!(cache.get(t, KeyTable::Spans).is_none());
    }

    #[test]
    fn the_oldest_listing_goes_first_and_a_too_large_one_is_not_kept() {
        let size = listing(1000).get_array_memory_size();
        let cache = KeysCache::new(Duration::from_secs(60), 2, 2 * size);
        let (a, b, c) = (tenant(), tenant(), tenant());
        cache.put(a, KeyTable::Spans, listing(1000));
        cache.put(b, KeyTable::Spans, listing(1000));
        cache.put(c, KeyTable::Spans, listing(1000));
        assert!(
            cache.get(a, KeyTable::Spans).is_none(),
            "the oldest is dropped"
        );
        assert!(cache.get(b, KeyTable::Spans).is_some());
        assert!(cache.get(c, KeyTable::Spans).is_some());
        let tight = KeysCache::new(Duration::from_secs(60), 2, size - 1);
        tight.put(a, KeyTable::Spans, listing(1000));
        assert!(
            tight.get(a, KeyTable::Spans).is_none(),
            "larger than the cache"
        );
        let off = KeysCache::new(Duration::ZERO, 2, 2 * size);
        off.put(a, KeyTable::Spans, listing(1));
        assert!(
            off.get(a, KeyTable::Spans).is_none(),
            "a zero TTL keeps nothing"
        );
    }
}

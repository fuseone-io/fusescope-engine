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

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::AsArray;
use arrow::compute::filter_record_batch;
use arrow::datatypes::TimestampNanosecondType;
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::prelude::SessionContext;
use fs_catalog::{FileRecord, Signal, TenantId};
use fs_schema::attribute_keys::{KeyStats, KeyTable};
use futures_util::StreamExt;
use object_store::ObjectStoreExt;

use super::{Querier, QueryWarnings};
use crate::QueryError;
use crate::signal_table::SignalTable;

const HOUR_NS: i64 = 3_600_000_000_000;

/// What one counted key costs beyond its text, its entry in the maps and
/// its sightings, as the memory pool is charged for it.
const KEY_ENTRY_BYTES: usize = 128;

/// What one row of the final listing costs beyond its key's text: its
/// source, type, count and three timestamps, and the offsets.
const LISTED_ROW_BYTES: usize = 96;

/// A complete listing and its charge in the memory pool, released when the
/// last holder lets go: the cache that keeps it, the waiters of its fill or
/// a stream of it still being read.
#[derive(Clone)]
pub(super) struct Listing {
    pub(super) batch: RecordBatch,
    held: Arc<MemoryReservation>,
}

/// The outcome of one fill, shared by every request that missed while it
/// ran.
type Filled = Result<Listing, Arc<QueryError>>;

/// A fill under way, which the misses that join it wait on.
type Flight = Arc<tokio::sync::OnceCell<Filled>>;

/// What a miss finds, under one lock: a fill to join or start, or the
/// listing a fill published since the miss.
pub(super) enum Joined {
    Cached(Listing),
    Fill(Flight),
}

/// Complete listings of attribute keys, one a tenant and table, kept for a
/// time and bounded by count and bytes, the oldest dropped first.
pub(super) struct KeysCache {
    ttl: Duration,
    max_entries: usize,
    max_bytes: usize,
    inner: Mutex<Kept>,
    /// The fill under way for a tenant and table, if any; it is removed
    /// once it lands, so a later miss fills again.
    inflight: Mutex<HashMap<(TenantId, KeyTable), Flight>>,
}

#[derive(Default)]
struct Kept {
    entries: HashMap<(TenantId, KeyTable), (Listing, Instant, usize)>,
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
            inflight: Mutex::new(HashMap::new()),
        }
    }

    /// Under the lock of the fills: the fill under way for `tenant` and
    /// `table`, else the listing in the cache, else a new fill. A fill
    /// publishes before it is forgotten, so a miss that finds no fill finds
    /// what the last one published.
    pub(super) fn join(&self, tenant: TenantId, table: KeyTable) -> Joined {
        let Ok(mut inflight) = self.inflight.lock() else {
            return Joined::Fill(Arc::new(tokio::sync::OnceCell::new()));
        };
        if let Some(flight) = inflight.get(&(tenant, table)) {
            return Joined::Fill(Arc::clone(flight));
        }
        if let Some(listing) = self.get(tenant, table) {
            return Joined::Cached(listing);
        }
        let flight: Flight = Arc::default();
        inflight.insert((tenant, table), Arc::clone(&flight));
        Joined::Fill(flight)
    }

    /// Forgets `flight` once it landed, unless a later one replaced it.
    pub(super) fn landed(&self, tenant: TenantId, table: KeyTable, flight: &Flight) {
        if let Ok(mut inflight) = self.inflight.lock() {
            if inflight
                .get(&(tenant, table))
                .is_some_and(|current| Arc::ptr_eq(current, flight))
            {
                inflight.remove(&(tenant, table));
            }
        }
    }

    fn get(&self, tenant: TenantId, table: KeyTable) -> Option<Listing> {
        let kept = self.inner.lock().ok()?;
        let (listing, at, _) = kept.entries.get(&(tenant, table))?;
        (at.elapsed() < self.ttl).then(|| listing.clone())
    }

    /// Keeps `listing`, dropping the oldest listings until it fits; a
    /// listing larger than the whole cache, or a zero TTL, keeps nothing.
    pub(super) fn put(&self, tenant: TenantId, table: KeyTable, listing: Listing) {
        let size = listing.batch.get_array_memory_size();
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
        kept.entries.insert(key, (listing, Instant::now(), size));
    }
}

impl Querier {
    /// Every attribute key of `tenant`'s `table`, compacted hours and recent
    /// rows together, in the table's file schema. Concurrent misses share
    /// one fill and its outcome, an error as much as a listing.
    ///
    /// # Errors
    /// Unknown tenant, catalog or bucket failure, a derived table that does
    /// not decode, recent rows over the tenant's byte budget
    /// ([`QueryError::OverBudget`]), an ingester that does not answer
    /// ([`QueryError::HotUnavailable`]), or a listing the memory pool cannot
    /// hold.
    pub(super) async fn listed_keys(
        &self,
        tenant: TenantId,
        table: KeyTable,
        ctx: &SessionContext,
    ) -> Result<Listing, QueryError> {
        let flight = match self.keys_cache.join(tenant, table) {
            Joined::Cached(listing) => return Ok(listing),
            Joined::Fill(flight) => flight,
        };
        let filled = flight
            .get_or_init(|| async {
                let filled = self.fill_keys(tenant, table, ctx).await;
                if let Ok(listing) = &filled {
                    self.keys_cache.put(tenant, table, listing.clone());
                }
                filled.map_err(Arc::new)
            })
            .await
            .clone();
        self.keys_cache.landed(tenant, table, &flight);
        filled.map_err(|e| unshared(&e))
    }

    /// One fill: the snapshot, the derived table and the recent rows,
    /// counted as they stream. Every growth is charged to the memory pool
    /// before it happens, and the listing keeps the charge for its batch.
    async fn fill_keys(
        &self,
        tenant: TenantId,
        table: KeyTable,
        ctx: &SessionContext,
    ) -> Result<Listing, QueryError> {
        self.stats.recent_keys_fills.fetch_add(1, Ordering::Relaxed);
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
        let mut held = MemoryConsumer::new("metadata.keys").register(&self.runtime.memory_pool);
        let mut stats = self.derived_keys(table, &snapshot.keys, &mut held).await?;
        let budget = u64::try_from(record.bytes_budget).unwrap_or(u64::MAX);
        let max_keys = usize::try_from(record.max_keys_hour).unwrap_or(0);
        let counting = |source| QueryError::DerivedTable {
            path: format!("{} of recent rows", table.name()),
            source,
        };
        let mut recent = self
            .recent_rows(tenant, table, snapshot.l0, budget, ctx)
            .await?;
        while let Some(batch) = recent.next().await {
            let batch = batch.map_err(typed)?;
            held.try_resize(held_bytes(&stats) + growth_bound(&batch, table))?;
            stats = absorb_by_hour(stats, &batch, max_keys).map_err(|e| match e {
                Counted::Schema(source) => counting(source),
                Counted::Query(query) => query,
            })?;
            held.try_resize(held_bytes(&stats))?;
        }
        held.try_resize(held_bytes(&stats) + listed_bound(&stats))?;
        let batch = stats.to_batch().map_err(counting)?;
        drop(stats);
        held.try_resize(batch.get_array_memory_size())?;
        Ok(Listing {
            batch,
            held: Arc::new(held),
        })
    }

    /// The derived table of `files`, one in a steady catalog, decoded and
    /// merged, with nothing counted as new; each object and the table as it
    /// grows are charged to `held` first.
    async fn derived_keys(
        &self,
        table: KeyTable,
        files: &[FileRecord],
        held: &mut MemoryReservation,
    ) -> Result<KeyStats, QueryError> {
        let mut stats = KeyStats::new(table);
        for file in files {
            let object = usize::try_from(file.size_bytes).unwrap_or(usize::MAX);
            let keys = usize::try_from(file.row_count).unwrap_or(usize::MAX);
            held.try_resize(
                held_bytes(&stats)
                    .saturating_add(object)
                    .saturating_add(keys.saturating_mul(KEY_ENTRY_BYTES + LISTED_ROW_BYTES)),
            )?;
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
            let decoded = fs_schema::attribute_keys::decode(bytes, table).map_err(unreadable)?;
            held.try_resize(held_bytes(&stats) + held_bytes(&decoded))?;
            stats.merge(decoded).map_err(unreadable)?;
        }
        held.try_resize(held_bytes(&stats))?;
        Ok(stats.settled())
    }

    /// The timestamp and map columns of `l0` and of the hot window, as the
    /// row table reads them, streamed: the hot window first, the files it
    /// serves left out, an ingester that does not answer failing the read.
    async fn recent_rows(
        &self,
        tenant: TenantId,
        table: KeyTable,
        l0: Vec<FileRecord>,
        budget: u64,
        ctx: &SessionContext,
    ) -> Result<SendableRecordBatchStream, QueryError> {
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
        frame.execute_stream().await.map_err(typed)
    }
}

/// The most `batch` can add to the counted keys: every map entry of its
/// rows a new key, its text and an entry.
fn growth_bound(batch: &RecordBatch, table: KeyTable) -> usize {
    table
        .map_columns()
        .iter()
        .filter_map(|(name, _, _)| batch.column_by_name(name)?.as_map_opt())
        .map(|map| {
            let text = map
                .keys()
                .as_string_opt::<i32>()
                .map_or(0, |keys| keys.value_data().len());
            text + arrow::array::Array::len(map.entries()) * KEY_ENTRY_BYTES
        })
        .sum()
}

/// What the listing of `stats` will hold once built.
fn listed_bound(stats: &KeyStats) -> usize {
    stats
        .iter()
        .map(|(_, key, _, _)| key.len() + LISTED_ROW_BYTES)
        .sum()
}

/// A stream of a listing's rows that keeps the listing's charge in the
/// memory pool until it is dropped.
pub(super) struct HeldStream {
    inner: SendableRecordBatchStream,
    _held: Arc<MemoryReservation>,
}

impl HeldStream {
    pub(super) fn new(inner: SendableRecordBatchStream, listing: &Listing) -> Self {
        Self {
            inner,
            _held: Arc::clone(&listing.held),
        }
    }
}

impl futures_util::Stream for HeldStream {
    type Item = datafusion::error::Result<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().inner.as_mut().poll_next(cx)
    }
}

impl datafusion::execution::RecordBatchStream for HeldStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }
}

/// What `stats` holds in memory, as the pool is charged for it: each key
/// and a fixed allowance for its entry.
fn held_bytes(stats: &KeyStats) -> usize {
    stats
        .iter()
        .map(|(_, key, _, _)| key.len() + KEY_ENTRY_BYTES)
        .sum()
}

/// `stats` with the keys of `batch` counted, each hour of its rows on what
/// is left of that hour's budget of `max_keys` new keys, the derived
/// table's admissions and the recent ones before it included.
fn absorb_by_hour(
    mut stats: KeyStats,
    batch: &RecordBatch,
    max_keys: usize,
) -> Result<KeyStats, Counted> {
    for (hour, part) in split_by_hour(batch).map_err(Counted::Query)? {
        let left = max_keys.saturating_sub(stats.admitted_in(hour));
        stats = stats.settled();
        stats.absorb(&part, hour, left).map_err(Counted::Schema)?;
    }
    Ok(stats.settled())
}

/// `err`, shared by a fill's waiters, as each of them returns it: the kinds
/// a status is chosen by keep their kind, the others their message.
fn unshared(err: &QueryError) -> QueryError {
    match err {
        QueryError::OverBudget { bytes, budget } => QueryError::OverBudget {
            bytes: *bytes,
            budget: *budget,
        },
        QueryError::HotUnavailable(ingesters) => QueryError::HotUnavailable(ingesters.clone()),
        QueryError::UnknownTenant => QueryError::UnknownTenant,
        QueryError::Timeout(after) => QueryError::Timeout(*after),
        QueryError::TooManyRows(rows) => QueryError::TooManyRows(*rows),
        QueryError::DataFusion(DataFusionError::ResourcesExhausted(why)) => {
            QueryError::DataFusion(DataFusionError::ResourcesExhausted(why.clone()))
        }
        other => QueryError::DataFusion(DataFusionError::Execution(other.to_string())),
    }
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

    fn listed(rows: u64) -> Listing {
        let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> =
            Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default());
        Listing {
            batch: listing(rows),
            held: Arc::new(MemoryConsumer::new("test").register(&pool)),
        }
    }

    fn tenant() -> TenantId {
        TenantId(uuid::Uuid::new_v4())
    }

    #[test]
    fn a_miss_joins_the_flight_under_way_or_reads_the_cache_it_published() {
        let cache = KeysCache::new(Duration::from_secs(60), 4, 1 << 20);
        let t = tenant();
        let Joined::Fill(first) = cache.join(t, KeyTable::Spans) else {
            panic!("an empty cache starts a fill");
        };
        let Joined::Fill(second) = cache.join(t, KeyTable::Spans) else {
            panic!("a fill under way is joined");
        };
        assert!(Arc::ptr_eq(&first, &second));
        cache.put(t, KeyTable::Spans, listed(3));
        cache.landed(t, KeyTable::Spans, &first);
        assert!(
            matches!(cache.join(t, KeyTable::Spans), Joined::Cached(ref l) if l.batch.num_rows() == 3),
            "a miss after the fill landed reads what it published, and fills nothing"
        );
    }

    #[test]
    fn a_listing_is_kept_for_its_ttl() {
        let cache = KeysCache::new(Duration::from_millis(50), 4, 1 << 20);
        let t = tenant();
        cache.put(t, KeyTable::Spans, listed(3));
        assert_eq!(
            cache.get(t, KeyTable::Spans).map(|l| l.batch.num_rows()),
            Some(3)
        );
        assert!(cache.get(t, KeyTable::Logs).is_none(), "per table");
        std::thread::sleep(Duration::from_millis(60));
        assert!(cache.get(t, KeyTable::Spans).is_none());
    }

    #[test]
    fn the_oldest_listing_goes_first_and_a_too_large_one_is_not_kept() {
        let size = listing(1000).get_array_memory_size();
        let cache = KeysCache::new(Duration::from_secs(60), 2, 2 * size);
        let (a, b, c) = (tenant(), tenant(), tenant());
        cache.put(a, KeyTable::Spans, listed(1000));
        cache.put(b, KeyTable::Spans, listed(1000));
        cache.put(c, KeyTable::Spans, listed(1000));
        assert!(
            cache.get(a, KeyTable::Spans).is_none(),
            "the oldest is dropped"
        );
        assert!(cache.get(b, KeyTable::Spans).is_some());
        assert!(cache.get(c, KeyTable::Spans).is_some());
        let tight = KeysCache::new(Duration::from_secs(60), 2, size - 1);
        tight.put(a, KeyTable::Spans, listed(1000));
        assert!(
            tight.get(a, KeyTable::Spans).is_none(),
            "larger than the cache"
        );
        let off = KeysCache::new(Duration::ZERO, 2, 2 * size);
        off.put(a, KeyTable::Spans, listed(1));
        assert!(
            off.get(a, KeyTable::Spans).is_none(),
            "a zero TTL keeps nothing"
        );
    }
}

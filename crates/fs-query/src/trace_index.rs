//! Cache of `trace_id` sidecars, consulted when a query pins trace ids.
//!
//! A sidecar is immutable and written before its file is registered, so a
//! decoded one is kept until the byte budget evicts it, and a file whose
//! sidecar is missing, damaged, too large or naming other files is
//! remembered as such. Either way the file is scanned; the sidecar only
//! ever removes files from a scan.
//!
//! The budget is a ceiling: bytes held plus bytes of downloads in flight
//! never exceed it. A download is reserved against the budget from its
//! announced size before a byte is read, evicting idle entries to make
//! room; one that cannot fit is not read and its file is scanned. One
//! download per file is in flight at a time; a concurrent query scans
//! the file instead of waiting.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use fs_catalog::{FileId, FileRecord};
use fs_index::TraceIndex;
use futures_util::future::join_all;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::querier::QueryStats;

/// Sidecar fetches in flight for one query.
const FETCH_PARALLELISM: usize = 8;

/// Bytes charged for remembering that a file has no usable sidecar.
const MISS_BYTES: usize = 64;

/// Shared sidecar cache.
pub struct TraceIndexCache {
    store: Arc<dyn ObjectStore>,
    max_bytes: usize,
    stats: Option<Arc<QueryStats>>,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    entries: HashMap<FileId, Slot>,
    /// Bytes of the entries held.
    bytes: usize,
    /// Bytes reserved for downloads in flight.
    inflight: usize,
    fetching: HashSet<FileId>,
    clock: u64,
}

struct Slot {
    index: Option<Arc<TraceIndex>>,
    bytes: usize,
    used: u64,
}

impl TraceIndexCache {
    /// Cache over `store` holding at most `max_bytes` of decoded sidecars.
    /// Zero disables the sidecars: every file a query keeps is scanned.
    pub fn new(store: Arc<dyn ObjectStore>, max_bytes: usize) -> Self {
        Self {
            store,
            max_bytes,
            stats: None,
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Counts fetches, misses and pruned files in `stats`.
    #[must_use]
    pub fn with_stats(mut self, stats: Arc<QueryStats>) -> Self {
        self.stats = Some(stats);
        self
    }

    /// True when sidecars are consulted at all.
    pub fn enabled(&self) -> bool {
        self.max_bytes > 0
    }

    /// Bytes of decoded sidecars held.
    pub async fn bytes(&self) -> usize {
        self.inner.lock().await.bytes
    }

    /// Bytes held plus bytes reserved for downloads in flight.
    pub async fn committed_bytes(&self) -> usize {
        let inner = self.inner.lock().await;
        inner.bytes + inner.inflight
    }

    /// The subset of `files` that may hold one of `traces`: those whose
    /// sidecar lists one in them, plus those without a usable sidecar.
    pub async fn files_holding<'a>(
        &self,
        files: Vec<&'a FileRecord>,
        traces: &[[u8; 16]],
    ) -> Vec<&'a FileRecord> {
        if !self.enabled() || traces.is_empty() {
            return files;
        }
        let before = files.len();
        let mut kept = Vec::with_capacity(files.len());
        for chunk in files.chunks(FETCH_PARALLELISM) {
            let indexes = join_all(chunk.iter().map(|file| self.lookup(file))).await;
            for (file, index) in chunk.iter().zip(indexes) {
                if index.is_none_or(|index| index.file_holds(&file.path, traces)) {
                    kept.push(*file);
                }
            }
        }
        if let Some(stats) = &self.stats {
            stats.trace_index_pruned.fetch_add(
                (before - kept.len()) as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        kept
    }

    /// The file's sidecar, fetched on a first sight. `None` when the file
    /// has none this engine can use, or none it can hold right now.
    async fn lookup(&self, file: &FileRecord) -> Option<Arc<TraceIndex>> {
        if let Some(index) = self.cached(file.id).await {
            return index;
        }
        match self.fetch(file).await {
            Fetched::Index(index, reserved) => {
                let bytes = index.heap_bytes();
                let kept = if bytes <= self.max_bytes {
                    Some(index)
                } else {
                    warn!(file = %file.id.0, bytes, budget = self.max_bytes, "trace index larger than the budget; the file is scanned");
                    None
                };
                let charged = if kept.is_some() { bytes } else { MISS_BYTES };
                self.insert(file.id, kept.clone(), charged, reserved).await;
                kept
            }
            Fetched::Unusable(reserved) => {
                self.insert(file.id, None, MISS_BYTES, reserved).await;
                None
            }
            Fetched::Unavailable(reserved) => {
                self.release(file.id, reserved).await;
                None
            }
        }
    }

    /// A cache hit, or `None` when the file was never looked up.
    async fn cached(&self, id: FileId) -> Option<Option<Arc<TraceIndex>>> {
        let mut inner = self.inner.lock().await;
        inner.clock += 1;
        let clock = inner.clock;
        let slot = inner.entries.get_mut(&id)?;
        slot.used = clock;
        Some(slot.index.clone())
    }

    /// Marks a download of `id` as in flight. False when one already is.
    async fn begin(&self, id: FileId) -> bool {
        self.inner.lock().await.fetching.insert(id)
    }

    /// Reserves `size` bytes of the budget for the download in flight,
    /// evicting idle entries to make room. The download stays marked until
    /// released whatever the answer.
    async fn reserve(&self, size: usize) -> Reserve {
        let mut inner = self.inner.lock().await;
        if size > self.max_bytes {
            return Reserve::TooLarge;
        }
        while inner.bytes + inner.inflight + size > self.max_bytes {
            if !evict_one(&mut inner) {
                break;
            }
        }
        if inner.bytes + inner.inflight + size > self.max_bytes {
            return Reserve::Busy;
        }
        inner.inflight += size;
        Reserve::Fits
    }

    /// Returns a reservation without an entry.
    async fn release(&self, id: FileId, reserved: usize) {
        let mut inner = self.inner.lock().await;
        inner.inflight -= reserved;
        inner.fetching.remove(&id);
    }

    /// Replaces the reservation of `id` with an entry of `bytes`, evicting
    /// idle entries so the budget still holds.
    async fn insert(
        &self,
        id: FileId,
        index: Option<Arc<TraceIndex>>,
        bytes: usize,
        reserved: usize,
    ) {
        let mut inner = self.inner.lock().await;
        inner.inflight -= reserved;
        inner.fetching.remove(&id);
        inner.clock += 1;
        let used = inner.clock;
        if let Some(old) = inner.entries.insert(id, Slot { index, bytes, used }) {
            inner.bytes -= old.bytes;
        }
        inner.bytes += bytes;
        while inner.bytes + inner.inflight > self.max_bytes {
            if !evict_one(&mut inner) {
                break;
            }
        }
    }

    async fn fetch(&self, file: &FileRecord) -> Fetched {
        let Some(path) = fs_index::sidecar_path(&file.path) else {
            return self.miss(0);
        };
        if !self.begin(file.id).await {
            return Fetched::Unavailable(0);
        }
        let mut reserved = 0;
        if let Some(stats) = &self.stats {
            stats
                .trace_index_fetches
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let result = match self.store.get(&Path::from(path)).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => {
                debug!(file = %file.id.0, "trace index missing; the file is scanned");
                return self.miss(reserved);
            }
            Err(e) => return self.unavailable(file, &e, reserved),
        };
        let size = usize::try_from(result.meta.size).unwrap_or(usize::MAX);
        match self.reserve(size).await {
            Reserve::Fits => reserved = size,
            Reserve::TooLarge => {
                warn!(file = %file.id.0, size, budget = self.max_bytes, "trace index larger than the budget; the file is scanned");
                return self.miss(reserved);
            }
            Reserve::Busy => {
                debug!(file = %file.id.0, size, "trace index budget busy; the file is scanned this time");
                self.count_miss();
                return Fetched::Unavailable(reserved);
            }
        }
        let bytes = match result.bytes().await {
            Ok(bytes) => bytes,
            Err(e) => return self.unavailable(file, &e, reserved),
        };
        match TraceIndex::decode(&bytes) {
            Ok(index) if index.covers(&file.path) => Fetched::Index(Arc::new(index), reserved),
            Ok(_) => {
                warn!(file = %file.id.0, "trace index names other files; the file is scanned");
                self.miss(reserved)
            }
            Err(e) => {
                warn!(file = %file.id.0, error = %e, "trace index unreadable; the file is scanned");
                self.miss(reserved)
            }
        }
    }

    fn miss(&self, reserved: usize) -> Fetched {
        self.count_miss();
        Fetched::Unusable(reserved)
    }

    /// A store failure is not remembered: the next query tries again.
    fn unavailable(
        &self,
        file: &FileRecord,
        error: &object_store::Error,
        reserved: usize,
    ) -> Fetched {
        warn!(file = %file.id.0, error = %error, "trace index fetch failed; the file is scanned");
        self.count_miss();
        Fetched::Unavailable(reserved)
    }

    fn count_miss(&self) {
        if let Some(stats) = &self.stats {
            stats
                .trace_index_misses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

/// Whether a download's bytes fit the budget.
#[derive(Debug, PartialEq, Eq)]
enum Reserve {
    Fits,
    /// Larger than the whole budget: never fits, remembered as a miss.
    TooLarge,
    /// Held by entries or downloads in use right now: tried again next time.
    Busy,
}

/// Evicts the least recently used entry. False when there is none.
fn evict_one(inner: &mut Inner) -> bool {
    let Some((&victim, _)) = inner.entries.iter().min_by_key(|(_, s)| s.used) else {
        return false;
    };
    if let Some(slot) = inner.entries.remove(&victim) {
        inner.bytes -= slot.bytes;
    }
    true
}

/// What a fetch produced, with the bytes still reserved for it.
enum Fetched {
    Index(Arc<TraceIndex>, usize),
    Unusable(usize),
    Unavailable(usize),
}

impl std::fmt::Debug for TraceIndexCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraceIndexCache")
            .field("max_bytes", &self.max_bytes)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[allow(clippy::indexing_slicing)]
mod tests {
    use chrono::Utc;
    use fs_catalog::{FileState, Level, Signal, TenantId, TimeRange};
    use object_store::memory::InMemory;

    use super::*;

    fn record(n: u128) -> FileRecord {
        let now = Utc::now();
        FileRecord {
            id: FileId(uuid::Uuid::from_u128(n)),
            tenant_id: TenantId(uuid::Uuid::nil()),
            signal: Signal::Traces,
            level: Level::L0,
            path: format!("t/x/traces/l0/dt=2026-10-24/hr=14/{n}.parquet"),
            size_bytes: 1,
            row_count: 1,
            range: TimeRange {
                start: now,
                end: now,
            },
            services: vec![],
            summary: None,
            state: FileState::Visible,
            created_at: now,
            quarantined_at: None,
        }
    }

    async fn put_sidecar(
        store: &InMemory,
        at: &FileRecord,
        covering: &FileRecord,
        traces: &[[u8; 16]],
    ) -> (usize, usize) {
        let index = TraceIndex::new(
            vec![covering.path.clone()],
            traces.iter().map(|t| (*t, 0u32)),
        )
        .unwrap();
        let path = fs_index::sidecar_path(&at.path).unwrap();
        let encoded = index.encode();
        let size = encoded.len();
        store.put(&Path::from(path), encoded.into()).await.unwrap();
        (index.heap_bytes(), size)
    }

    fn stats(cache: &TraceIndexCache) -> (u64, u64, u64) {
        let s = cache.stats.as_ref().unwrap();
        let o = std::sync::atomic::Ordering::Relaxed;
        (
            s.trace_index_fetches.load(o),
            s.trace_index_misses.load(o),
            s.trace_index_pruned.load(o),
        )
    }

    #[tokio::test]
    async fn the_byte_budget_evicts_the_least_recently_used_sidecar() {
        let store = Arc::new(InMemory::new());
        let files: Vec<FileRecord> = (1..=3).map(record).collect();
        let (mut heap, mut size) = (0, 0);
        for (n, file) in (1u8..).zip(&files) {
            (heap, size) = put_sidecar(&store, file, file, &[[n; 16]]).await;
        }
        // Room for two decoded sidecars while a third downloads only after
        // one of them goes.
        let budget = heap + size;
        let cache = TraceIndexCache::new(store, budget).with_stats(Arc::new(QueryStats::default()));

        for (n, file) in (1u8..).zip(&files) {
            let kept = cache.files_holding(vec![file], &[[n; 16]]).await;
            assert_eq!(kept.len(), 1);
            assert!(cache.bytes().await <= budget);
        }
        assert_eq!(cache.bytes().await, heap * 2, "two of the three are held");
        assert_eq!(stats(&cache), (3, 0, 0));
        assert_eq!(
            cache.committed_bytes().await,
            cache.bytes().await,
            "nothing left in flight"
        );

        // The last two looked up survive; the first is fetched again.
        cache.files_holding(vec![&files[0]], &[[1u8; 16]]).await;
        assert_eq!(stats(&cache).0, 4);
        cache.files_holding(vec![&files[2]], &[[3u8; 16]]).await;
        assert_eq!(stats(&cache).0, 4);
    }

    #[tokio::test]
    async fn a_busy_budget_scans_the_file_and_is_not_remembered() {
        let store = Arc::new(InMemory::new());
        let file = record(1);
        let (_, size) = put_sidecar(&store, &file, &file, &[[1u8; 16]]).await;
        let cache =
            TraceIndexCache::new(store, size * 2).with_stats(Arc::new(QueryStats::default()));
        // Another query's download holds most of the budget.
        let other = FileId(uuid::Uuid::from_u128(9));
        assert!(cache.begin(other).await);
        assert_eq!(cache.reserve(size + 1).await, Reserve::Fits);

        let kept = cache.files_holding(vec![&file], &[[7u8; 16]]).await;
        assert_eq!(kept.len(), 1, "scanned while the budget is busy");
        assert_eq!(stats(&cache), (1, 1, 0));
        assert_eq!(
            cache.committed_bytes().await,
            size + 1,
            "nothing of it stayed"
        );

        cache.release(other, size + 1).await;
        let kept = cache.files_holding(vec![&file], &[[7u8; 16]]).await;
        assert_eq!(kept.len(), 0, "fetched and pruned once there is room");
        assert_eq!(stats(&cache), (2, 1, 1));
    }

    #[tokio::test]
    async fn a_sidecar_larger_than_the_budget_is_neither_read_nor_kept() {
        let store = Arc::new(InMemory::new());
        let big = record(1);
        let small = record(2);
        let traces: Vec<[u8; 16]> = (0u8..200).map(|n| [n; 16]).collect();
        put_sidecar(&store, &big, &big, &traces).await;
        let (_, one) = put_sidecar(&store, &small, &small, &[[7u8; 16]]).await;
        let cache =
            TraceIndexCache::new(store, one * 2).with_stats(Arc::new(QueryStats::default()));

        let kept = cache.files_holding(vec![&big, &small], &[[9u8; 16]]).await;
        assert_eq!(
            kept.len(),
            1,
            "the big file is scanned, the small one pruned"
        );
        assert_eq!(kept[0].id, big.id);
        assert_eq!(stats(&cache), (2, 1, 1));
        assert!(cache.bytes().await <= one * 2);
        assert!(cache.committed_bytes().await <= one * 2);

        // Remembered: the big sidecar is not fetched again.
        cache.files_holding(vec![&big], &[[9u8; 16]]).await;
        assert_eq!(stats(&cache).0, 2);
    }

    #[tokio::test]
    async fn a_sidecar_naming_another_file_is_a_miss() {
        let store = Arc::new(InMemory::new());
        let file = record(1);
        let other = record(2);
        put_sidecar(&store, &file, &other, &[[1u8; 16]]).await;
        let cache =
            TraceIndexCache::new(store, 1 << 20).with_stats(Arc::new(QueryStats::default()));
        let kept = cache.files_holding(vec![&file], &[[1u8; 16]]).await;
        assert_eq!(kept.len(), 1, "scanned, not trusted");
        assert_eq!(stats(&cache), (1, 1, 0));
    }

    #[tokio::test]
    async fn reservations_bound_the_bytes_in_flight_and_singleflight_a_file() {
        let store = Arc::new(InMemory::new());
        let cache = TraceIndexCache::new(store, 100);
        let a = FileId(uuid::Uuid::from_u128(1));
        let b = FileId(uuid::Uuid::from_u128(2));
        assert!(cache.begin(a).await);
        assert!(!cache.begin(a).await, "one download per file");
        assert_eq!(cache.reserve(60).await, Reserve::Fits);
        assert!(cache.begin(b).await);
        assert_eq!(
            cache.reserve(50).await,
            Reserve::Busy,
            "would cross the ceiling with a in flight"
        );
        assert_eq!(cache.reserve(40).await, Reserve::Fits);
        assert_eq!(cache.committed_bytes().await, 100);
        cache.release(a, 60).await;
        assert_eq!(cache.committed_bytes().await, 40);
        assert!(cache.begin(a).await, "released downloads may start again");
        assert_eq!(
            cache.reserve(101).await,
            Reserve::TooLarge,
            "never larger than the budget"
        );
        cache.insert(b, None, MISS_BYTES, 40).await;
        assert_eq!(cache.committed_bytes().await, MISS_BYTES);
        assert_eq!(
            cache.reserve(100 - MISS_BYTES).await,
            Reserve::Fits,
            "the budget is the ceiling"
        );
    }

    #[tokio::test]
    async fn zero_bytes_disables_the_sidecars() {
        let store = Arc::new(InMemory::new());
        let file = record(1);
        put_sidecar(&store, &file, &file, &[[1u8; 16]]).await;
        let cache = TraceIndexCache::new(store, 0);
        assert!(!cache.enabled());
        let kept = cache.files_holding(vec![&file], &[[9u8; 16]]).await;
        assert_eq!(kept.len(), 1, "nothing consulted, nothing pruned");
    }
}

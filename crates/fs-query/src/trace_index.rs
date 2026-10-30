//! Cache of `trace_id` sidecars, consulted when a query pins trace ids.
//!
//! A sidecar is immutable and written before its file is registered, so a
//! decoded one is kept until the byte budget evicts it, and a file whose
//! sidecar is missing, damaged, too large or naming other files is
//! remembered as such. Either way the file is scanned; the sidecar only
//! ever removes files from a scan.
//!
//! The budget is a ceiling on what the cache makes the process hold:
//! decoded sidecars alive anywhere, including ones evicted from the map
//! but still used by a query, plus the encoded bytes and the heap of
//! downloads in flight. A download holds a lease: it is marked per file so
//! one is in flight at a time, reserves the announced object size before a
//! byte is read and the decoded heap, measured from the frame, before a
//! byte is decoded, evicting idle entries to make room. Dropping the lease,
//! on a timeout or a cancelled query, returns everything it reserved. A
//! download that cannot fit is not read; its file is scanned.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use fs_catalog::{FileId, FileRecord};
use fs_index::TraceIndex;
use futures_util::future::join_all;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use tracing::{debug, warn};

use crate::querier::QueryStats;

/// Sidecar fetches in flight for one query.
const FETCH_PARALLELISM: usize = 8;

/// Bytes charged for remembering that a file has no usable sidecar.
const MISS_BYTES: usize = 64;

/// Shared sidecar cache.
pub struct TraceIndexCache {
    store: Arc<dyn ObjectStore>,
    stats: Option<Arc<QueryStats>>,
    shared: Arc<Shared>,
}

struct Shared {
    max_bytes: usize,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    entries: HashMap<FileId, Slot>,
    /// Bytes of decoded sidecars alive and of misses remembered.
    bytes: usize,
    /// Bytes reserved by downloads in flight.
    inflight: usize,
    fetching: HashSet<FileId>,
    clock: u64,
}

struct Slot {
    index: Option<Arc<Held>>,
    /// Bytes charged to the slot itself: a remembered miss. A decoded
    /// index charges its own bytes for as long as it is alive.
    charged: usize,
    used: u64,
}

/// A decoded sidecar that holds its bytes against the budget until the
/// last query using it lets go.
pub struct Held {
    index: TraceIndex,
    bytes: usize,
    /// Weak, so entries never keep the cache alive.
    shared: Weak<Shared>,
}

impl std::ops::Deref for Held {
    type Target = TraceIndex;

    fn deref(&self) -> &TraceIndex {
        &self.index
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.upgrade() {
            shared.lock().bytes -= self.bytes;
        }
    }
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl TraceIndexCache {
    /// Cache over `store` holding at most `max_bytes` of decoded sidecars.
    /// Zero disables the sidecars: every file a query keeps is scanned.
    pub fn new(store: Arc<dyn ObjectStore>, max_bytes: usize) -> Self {
        Self {
            store,
            stats: None,
            shared: Arc::new(Shared {
                max_bytes,
                inner: Mutex::new(Inner::default()),
            }),
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
        self.shared.max_bytes > 0
    }

    /// Bytes of decoded sidecars alive and misses remembered.
    pub fn bytes(&self) -> usize {
        self.shared.lock().bytes
    }

    /// Bytes alive plus bytes reserved by downloads in flight.
    pub fn committed_bytes(&self) -> usize {
        let inner = self.shared.lock();
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
    async fn lookup(&self, file: &FileRecord) -> Option<Arc<Held>> {
        match self.cached(file.id) {
            Some(Entry::Index(index)) => return Some(index),
            Some(Entry::Miss) => return None,
            None => {}
        }
        let Some(mut lease) = Lease::begin(&self.shared, file.id) else {
            debug!(file = %file.id.0, "trace index download in flight elsewhere; the file is scanned this time");
            self.count_miss();
            return None;
        };
        match self.fetch(file, &mut lease).await {
            Fetched::Index(index, heap) => lease.commit(Some(index), heap),
            Fetched::Unusable => {
                lease.commit(None, MISS_BYTES);
                None
            }
            Fetched::Unavailable => None,
        }
    }

    /// A cache hit, or `None` when the file was never looked up.
    fn cached(&self, id: FileId) -> Option<Entry> {
        let mut inner = self.shared.lock();
        inner.clock += 1;
        let clock = inner.clock;
        let slot = inner.entries.get_mut(&id)?;
        slot.used = clock;
        Some(slot.index.clone().map_or(Entry::Miss, Entry::Index))
    }

    async fn fetch(&self, file: &FileRecord, lease: &mut Lease) -> Fetched {
        let Some(path) = fs_index::sidecar_path(&file.path) else {
            return self.miss();
        };
        if let Some(stats) = &self.stats {
            stats
                .trace_index_fetches
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let result = match self.store.get(&Path::from(path)).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => {
                debug!(file = %file.id.0, "trace index missing; the file is scanned");
                return self.miss();
            }
            Err(e) => return self.unavailable(file, &e),
        };
        let size = usize::try_from(result.meta.size).unwrap_or(usize::MAX);
        match lease.reserve(size) {
            Reserve::Fits => {}
            Reserve::TooLarge => {
                warn!(file = %file.id.0, size, budget = self.shared.max_bytes, "trace index larger than the budget; the file is scanned");
                return self.miss();
            }
            Reserve::Busy => return self.busy(file, size),
        }
        let bytes = match result.bytes().await {
            Ok(bytes) => bytes,
            Err(e) => return self.unavailable(file, &e),
        };
        let cost = match TraceIndex::decode_cost(&bytes) {
            Ok(cost) => cost,
            Err(e) => return self.unreadable(file, &e),
        };
        let decoding = cost.retained + cost.scratch;
        match lease.reserve(decoding) {
            Reserve::Fits => {}
            Reserve::TooLarge => {
                warn!(file = %file.id.0, decoding, budget = self.shared.max_bytes, "trace index decodes beyond the budget; the file is scanned");
                return self.miss();
            }
            Reserve::Busy => return self.busy(file, decoding),
        }
        match TraceIndex::decode(&bytes) {
            Ok(index) if index.covers(&file.path) => Fetched::Index(index, cost.retained),
            Ok(_) => {
                warn!(file = %file.id.0, "trace index names other files; the file is scanned");
                self.miss()
            }
            Err(e) => self.unreadable(file, &e),
        }
    }

    fn miss(&self) -> Fetched {
        self.count_miss();
        Fetched::Unusable
    }

    fn unreadable(&self, file: &FileRecord, error: &fs_index::IndexError) -> Fetched {
        warn!(file = %file.id.0, error = %error, "trace index unreadable; the file is scanned");
        self.miss()
    }

    /// A busy budget is not remembered: the next query tries again.
    fn busy(&self, file: &FileRecord, bytes: usize) -> Fetched {
        debug!(file = %file.id.0, bytes, "trace index budget busy; the file is scanned this time");
        self.count_miss();
        Fetched::Unavailable
    }

    /// A store failure is not remembered: the next query tries again.
    fn unavailable(&self, file: &FileRecord, error: &object_store::Error) -> Fetched {
        warn!(file = %file.id.0, error = %error, "trace index fetch failed; the file is scanned");
        self.count_miss();
        Fetched::Unavailable
    }

    fn count_miss(&self) {
        if let Some(stats) = &self.stats {
            stats
                .trace_index_misses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

/// Evicts the least recently used entry from the map: `None` when there
/// is none, else the decoded index it held, if any. That index keeps
/// charging its bytes until every query using it lets go, so the caller
/// drops it outside the lock.
fn evict_one(inner: &mut Inner) -> Option<Entry> {
    let (&victim, _) = inner.entries.iter().min_by_key(|(_, s)| s.used)?;
    let slot = inner.entries.remove(&victim)?;
    inner.bytes -= slot.charged;
    Some(slot.index.map_or(Entry::Miss, Entry::Index))
}

/// What a slot holds.
enum Entry {
    Index(Arc<Held>),
    /// The file has no sidecar this engine can use.
    Miss,
}

/// Evicts until `extra` more bytes fit or nothing is left to evict. An
/// evicted index charges its bytes until dropped, which happens outside
/// the lock, so the bytes an eviction frees are counted here: those of
/// indexes no query still uses.
fn make_room(inner: &mut Inner, max_bytes: usize, extra: usize) -> Evicted {
    let mut evicted = Evicted::default();
    while inner.bytes - evicted.freed + inner.inflight + extra > max_bytes {
        match evict_one(inner) {
            Some(Entry::Index(held)) => {
                if Arc::strong_count(&held) == 1 {
                    evicted.freed += held.bytes;
                }
                evicted.held.push(held);
            }
            Some(Entry::Miss) => {}
            None => break,
        }
    }
    evicted
}

/// Indexes evicted under the lock, dropped after it.
#[derive(Default)]
struct Evicted {
    held: Vec<Arc<Held>>,
    /// Bytes that drop will return.
    freed: usize,
}

impl Evicted {
    fn fits(&self, inner: &Inner, max_bytes: usize, extra: usize) -> bool {
        inner.bytes - self.freed + inner.inflight + extra <= max_bytes
    }
}

/// One download's claim on the cache: the per-file mark and the bytes it
/// reserved. Dropped, it returns both; committed, it becomes an entry.
struct Lease {
    shared: Arc<Shared>,
    id: FileId,
    reserved: usize,
}

impl Lease {
    /// Marks a download of `id` as in flight. `None` when one already is.
    fn begin(shared: &Arc<Shared>, id: FileId) -> Option<Self> {
        if !shared.lock().fetching.insert(id) {
            return None;
        }
        Some(Self {
            shared: Arc::clone(shared),
            id,
            reserved: 0,
        })
    }

    /// Reserves `size` more bytes of the budget, evicting idle entries to
    /// make room.
    fn reserve(&mut self, size: usize) -> Reserve {
        let evicted;
        {
            let mut inner = self.shared.lock();
            if size > self.shared.max_bytes {
                return Reserve::TooLarge;
            }
            evicted = make_room(&mut inner, self.shared.max_bytes, size);
            if !evicted.fits(&inner, self.shared.max_bytes, size) {
                drop(inner);
                drop(evicted);
                return Reserve::Busy;
            }
            inner.inflight += size;
            self.reserved += size;
        }
        drop(evicted);
        Reserve::Fits
    }

    /// Turns the lease into an entry charging `charged` bytes: a decoded
    /// index that charges them while alive, or a remembered miss.
    fn commit(mut self, index: Option<TraceIndex>, charged: usize) -> Option<Arc<Held>> {
        let held = index.map(|index| {
            Arc::new(Held {
                index,
                bytes: charged,
                shared: Arc::downgrade(&self.shared),
            })
        });
        let slot_charge = if held.is_some() { 0 } else { charged };
        let evicted;
        {
            let mut inner = self.shared.lock();
            inner.inflight -= self.reserved;
            inner.fetching.remove(&self.id);
            self.reserved = 0;
            inner.clock += 1;
            let used = inner.clock;
            let mut old = Vec::new();
            if let Some(previous) = inner.entries.insert(
                self.id,
                Slot {
                    index: held.clone(),
                    charged: slot_charge,
                    used,
                },
            ) {
                inner.bytes -= previous.charged;
                old.extend(previous.index);
            }
            inner.bytes += charged;
            evicted = make_room(&mut inner, self.shared.max_bytes, 0);
            drop(inner);
            drop(old);
        }
        drop(evicted);
        held
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut inner = self.shared.lock();
        inner.inflight -= self.reserved;
        inner.fetching.remove(&self.id);
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

enum Fetched {
    Index(TraceIndex, usize),
    Unusable,
    Unavailable,
}

impl std::fmt::Debug for TraceIndexCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraceIndexCache")
            .field("max_bytes", &self.shared.max_bytes)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[allow(clippy::indexing_slicing)]
mod tests {
    use std::time::Duration;

    use chrono::Utc;
    use fs_catalog::{FileState, Level, Signal, TenantId, TimeRange};
    use object_store::memory::InMemory;
    use object_store::throttle::{ThrottleConfig, ThrottledStore};

    use super::*;

    fn record(n: u128) -> FileRecord {
        let now = Utc::now();
        FileRecord {
            id: FileId(uuid::Uuid::from_u128(n)),
            origin: FileId(uuid::Uuid::from_u128(n)),
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
            content_sha256: None,
            quarantined_at: None,
        }
    }

    /// Writes the sidecar of `at` naming `covering`; returns (heap, encoded) bytes.
    async fn put_sidecar(
        store: &dyn ObjectStore,
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
        let cost = TraceIndex::decode_cost(&encoded).unwrap();
        store
            .put(&Path::from(path), encoded.clone().into())
            .await
            .unwrap();
        // Bytes a download of it needs at its peak: the object, the
        // retained heap and the decoder's scratch.
        (cost.retained, encoded.len() + cost.scratch)
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

    fn cache(store: Arc<dyn ObjectStore>, max_bytes: usize) -> TraceIndexCache {
        TraceIndexCache::new(store, max_bytes).with_stats(Arc::new(QueryStats::default()))
    }

    #[tokio::test]
    async fn the_byte_budget_evicts_the_least_recently_used_sidecar() {
        let store = Arc::new(InMemory::new());
        let files: Vec<FileRecord> = (1..=3).map(record).collect();
        let (mut heap, mut size) = (0, 0);
        for (n, file) in (1u8..).zip(&files) {
            (heap, size) = put_sidecar(store.as_ref(), file, file, &[[n; 16]]).await;
        }
        // Two decoded sidecars stay while a third downloads and decodes only
        // after one of them goes.
        let budget = heap + size + heap;
        let cache = cache(store, budget);

        for (n, file) in (1u8..).zip(&files) {
            let kept = cache.files_holding(vec![file], &[[n; 16]]).await;
            assert_eq!(kept.len(), 1);
            assert!(cache.committed_bytes() <= budget);
        }
        assert_eq!(cache.bytes(), heap * 2, "two of the three are held");
        assert_eq!(stats(&cache), (3, 0, 0));
        assert_eq!(
            cache.committed_bytes(),
            cache.bytes(),
            "nothing left in flight"
        );

        // The last two looked up survive; the first is fetched again.
        cache.files_holding(vec![&files[0]], &[[1u8; 16]]).await;
        assert_eq!(stats(&cache).0, 4);
        cache.files_holding(vec![&files[2]], &[[3u8; 16]]).await;
        assert_eq!(stats(&cache).0, 4);
    }

    #[tokio::test]
    async fn a_sidecar_larger_than_the_budget_is_neither_read_nor_kept() {
        let store = Arc::new(InMemory::new());
        let big = record(1);
        let small = record(2);
        let traces: Vec<[u8; 16]> = (0u8..200).map(|n| [n; 16]).collect();
        put_sidecar(store.as_ref(), &big, &big, &traces).await;
        let (heap, size) = put_sidecar(store.as_ref(), &small, &small, &[[7u8; 16]]).await;
        let budget = (heap + size) * 2;
        let cache = cache(store, budget);

        let kept = cache.files_holding(vec![&big, &small], &[[9u8; 16]]).await;
        assert_eq!(
            kept.len(),
            1,
            "the big file is scanned, the small one pruned"
        );
        assert_eq!(kept[0].id, big.id);
        assert_eq!(stats(&cache), (2, 1, 1));
        assert!(cache.committed_bytes() <= budget);

        // Remembered: the big sidecar is not fetched again.
        cache.files_holding(vec![&big], &[[9u8; 16]]).await;
        assert_eq!(stats(&cache).0, 2);
    }

    #[tokio::test]
    async fn a_busy_budget_scans_the_file_and_is_not_remembered() {
        let store = Arc::new(InMemory::new());
        let file = record(1);
        let (heap, size) = put_sidecar(store.as_ref(), &file, &file, &[[1u8; 16]]).await;
        let cache = cache(store, (heap + size) * 2);
        // Another query's download holds most of the budget.
        let mut other = Lease::begin(&cache.shared, FileId(uuid::Uuid::from_u128(9))).unwrap();
        assert_eq!(other.reserve(heap + size + 1), Reserve::Fits);

        let kept = cache.files_holding(vec![&file], &[[7u8; 16]]).await;
        assert_eq!(kept.len(), 1, "scanned while the budget is busy");
        assert_eq!(stats(&cache), (1, 1, 0));
        assert_eq!(
            cache.committed_bytes(),
            heap + size + 1,
            "nothing of it stayed"
        );

        drop(other);
        assert_eq!(cache.committed_bytes(), 0);
        let kept = cache.files_holding(vec![&file], &[[7u8; 16]]).await;
        assert_eq!(kept.len(), 0, "fetched and pruned once there is room");
        assert_eq!(stats(&cache), (2, 1, 1));
    }

    #[tokio::test]
    async fn a_sidecar_naming_another_file_is_a_miss() {
        let store = Arc::new(InMemory::new());
        let file = record(1);
        let other = record(2);
        put_sidecar(store.as_ref(), &file, &other, &[[1u8; 16]]).await;
        let cache = cache(store, 1 << 20);
        let kept = cache.files_holding(vec![&file], &[[1u8; 16]]).await;
        assert_eq!(kept.len(), 1, "scanned, not trusted");
        assert_eq!(stats(&cache), (1, 1, 0));
    }

    #[tokio::test]
    async fn a_lease_returns_its_mark_and_bytes_when_dropped() {
        let store = Arc::new(InMemory::new());
        let cache = cache(store, 100);
        let a = FileId(uuid::Uuid::from_u128(1));
        let b = FileId(uuid::Uuid::from_u128(2));
        let mut lease_a = Lease::begin(&cache.shared, a).unwrap();
        assert!(
            Lease::begin(&cache.shared, a).is_none(),
            "one download per file"
        );
        assert_eq!(lease_a.reserve(60), Reserve::Fits);
        let mut lease_b = Lease::begin(&cache.shared, b).unwrap();
        assert_eq!(
            lease_b.reserve(50),
            Reserve::Busy,
            "would cross the ceiling with a in flight"
        );
        assert_eq!(lease_b.reserve(40), Reserve::Fits);
        assert_eq!(cache.committed_bytes(), 100);
        assert_eq!(
            lease_b.reserve(101),
            Reserve::TooLarge,
            "never larger than the budget"
        );

        drop(lease_a);
        assert_eq!(cache.committed_bytes(), 40, "a returned its 60 bytes");
        assert!(Lease::begin(&cache.shared, a).is_some(), "and its mark");
        lease_b.commit(None, MISS_BYTES);
        assert_eq!(
            cache.committed_bytes(),
            MISS_BYTES,
            "a miss charges its own bytes"
        );
        let mut again = Lease::begin(&cache.shared, b).unwrap();
        assert_eq!(
            again.reserve(100 - MISS_BYTES),
            Reserve::Fits,
            "the budget is the ceiling"
        );
    }

    #[tokio::test]
    async fn an_evicted_sidecar_charges_its_bytes_until_the_last_query_lets_go() {
        let store = Arc::new(InMemory::new());
        let one = record(1);
        let two = record(2);
        let (heap, size) = put_sidecar(store.as_ref(), &one, &one, &[[1u8; 16]]).await;
        put_sidecar(store.as_ref(), &two, &two, &[[2u8; 16]]).await;
        // Room for one decoded sidecar and one download.
        let cache = cache(store, heap + size);
        let held = cache.lookup(&one).await.unwrap();
        assert_eq!(cache.bytes(), heap);

        // Decoding `two` needs the bytes of `one`, which a query still uses:
        // `one` leaves the map but keeps charging, so `two` finds no room.
        assert!(
            cache.lookup(&two).await.is_none(),
            "busy while one is in use"
        );
        assert_eq!(
            cache.shared.lock().entries.len(),
            0,
            "one was evicted from the map"
        );
        assert_eq!(cache.bytes(), heap, "and still counts");
        assert_eq!(stats(&cache), (2, 1, 0));

        drop(held);
        assert_eq!(cache.bytes(), 0, "the last user returned the bytes");
        assert!(cache.lookup(&two).await.is_some());
        assert_eq!(cache.bytes(), heap);
        assert_eq!(stats(&cache).0, 3, "the busy lookup was not remembered");
    }

    #[tokio::test]
    async fn a_cancelled_download_leaves_nothing_behind() {
        let memory = InMemory::new();
        let file = record(1);
        let (_, peak) = put_sidecar(&memory, &file, &file, &[[1u8; 16]]).await;
        let object = memory
            .head(&Path::from(fs_index::sidecar_path(&file.path).unwrap()))
            .await
            .unwrap()
            .size;
        let object = usize::try_from(object).unwrap();
        let slow = ThrottledStore::new(
            memory,
            ThrottleConfig {
                wait_get_per_byte: Duration::from_millis(50),
                ..ThrottleConfig::default()
            },
        );
        let cache = Arc::new(cache(Arc::new(slow), peak * 4));

        let query = tokio::spawn({
            let cache = Arc::clone(&cache);
            let file = file.clone();
            async move { cache.lookup(&file).await.is_some() }
        });
        // Let it pass the reservation and start reading the body.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while cache.committed_bytes() == 0 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            cache.committed_bytes(),
            object,
            "the object size is reserved mid-download"
        );
        query.abort();
        assert!(query.await.unwrap_err().is_cancelled());
        assert_eq!(cache.committed_bytes(), 0, "the lease went with the future");
        assert!(
            cache.shared.lock().fetching.is_empty(),
            "and so did the mark"
        );

        let kept = cache.files_holding(vec![&file], &[[1u8; 16]]).await;
        assert_eq!(kept.len(), 1, "a fresh download works and finds the trace");
        assert_eq!(stats(&cache).0, 2);
        assert_eq!(cache.bytes(), cache.committed_bytes());
    }

    #[tokio::test]
    async fn entries_do_not_keep_the_cache_alive() {
        let store = Arc::new(InMemory::new());
        let file = record(1);
        put_sidecar(store.as_ref(), &file, &file, &[[1u8; 16]]).await;
        let cache = cache(store, 1 << 20);
        let held = cache.lookup(&file).await.unwrap();
        let shared = Arc::downgrade(&cache.shared);
        drop(cache);
        assert!(
            shared.upgrade().is_none(),
            "an entry in use does not hold the cache"
        );
        drop(held);
    }

    #[tokio::test]
    async fn zero_bytes_disables_the_sidecars() {
        let store = Arc::new(InMemory::new());
        let file = record(1);
        put_sidecar(store.as_ref(), &file, &file, &[[1u8; 16]]).await;
        let cache = TraceIndexCache::new(store, 0);
        assert!(!cache.enabled());
        let kept = cache.files_holding(vec![&file], &[[9u8; 16]]).await;
        assert_eq!(kept.len(), 1, "nothing consulted, nothing pruned");
    }
}

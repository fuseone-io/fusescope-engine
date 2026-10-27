//! Cache of `trace_id` sidecars, consulted when a query pins trace ids.
//!
//! A sidecar is immutable and written before its file is registered, so a
//! decoded one is kept until the byte budget evicts it, and a file whose
//! sidecar is missing or damaged is remembered as such. Either way the file
//! is scanned; the sidecar only ever removes files from a scan.

use std::collections::HashMap;
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
    bytes: usize,
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

    /// The subset of `files` that may hold one of `traces`: those whose
    /// sidecar lists one, plus those without a usable sidecar.
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
                if index.is_none_or(|index| index.contains_any(traces)) {
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
    /// has none this engine can use.
    async fn lookup(&self, file: &FileRecord) -> Option<Arc<TraceIndex>> {
        if let Some(index) = self.cached(file.id).await {
            return index;
        }
        let fetched = self.fetch(file).await;
        let (index, bytes) = match fetched {
            Fetched::Index(index) => {
                let bytes = index.heap_bytes();
                (Some(index), bytes)
            }
            Fetched::Unusable => (None, MISS_BYTES),
            Fetched::Unavailable => return None,
        };
        self.insert(file.id, index.clone(), bytes).await;
        index
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

    async fn insert(&self, id: FileId, index: Option<Arc<TraceIndex>>, bytes: usize) {
        let mut inner = self.inner.lock().await;
        inner.clock += 1;
        let used = inner.clock;
        if let Some(old) = inner.entries.insert(id, Slot { index, bytes, used }) {
            inner.bytes -= old.bytes;
        }
        inner.bytes += bytes;
        while inner.bytes > self.max_bytes && inner.entries.len() > 1 {
            let Some((&victim, _)) = inner.entries.iter().min_by_key(|(_, s)| s.used) else {
                break;
            };
            if let Some(slot) = inner.entries.remove(&victim) {
                inner.bytes -= slot.bytes;
            }
        }
    }

    async fn fetch(&self, file: &FileRecord) -> Fetched {
        let Some(path) = fs_index::sidecar_path(&file.path) else {
            return Fetched::Unusable;
        };
        if let Some(stats) = &self.stats {
            stats
                .trace_index_fetches
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let bytes = match self.store.get(&Path::from(path)).await {
            Ok(result) => match result.bytes().await {
                Ok(bytes) => bytes,
                Err(e) => return self.unavailable(file, &e),
            },
            Err(object_store::Error::NotFound { .. }) => {
                debug!(file = %file.id.0, "trace index missing; the file is scanned");
                return self.miss();
            }
            Err(e) => return self.unavailable(file, &e),
        };
        match TraceIndex::decode(&bytes) {
            Ok(index) => Fetched::Index(Arc::new(index)),
            Err(e) => {
                warn!(file = %file.id.0, error = %e, "trace index unreadable; the file is scanned");
                self.miss()
            }
        }
    }

    fn miss(&self) -> Fetched {
        if let Some(stats) = &self.stats {
            stats
                .trace_index_misses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        Fetched::Unusable
    }

    /// A store failure is not remembered: the next query tries again.
    fn unavailable(&self, file: &FileRecord, error: &object_store::Error) -> Fetched {
        warn!(file = %file.id.0, error = %error, "trace index fetch failed; the file is scanned");
        self.miss();
        Fetched::Unavailable
    }
}

enum Fetched {
    Index(Arc<TraceIndex>),
    Unusable,
    Unavailable,
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

    async fn put_sidecar(store: &InMemory, file: &FileRecord, traces: &[[u8; 16]]) {
        let index =
            TraceIndex::new(vec![file.path.clone()], traces.iter().map(|t| (*t, 0u32))).unwrap();
        let path = fs_index::sidecar_path(&file.path).unwrap();
        store
            .put(&Path::from(path), index.encode().into())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn the_byte_budget_evicts_the_least_recently_used_sidecar() {
        let store = Arc::new(InMemory::new());
        let files: Vec<FileRecord> = (1..=3).map(record).collect();
        for (n, file) in (1u8..).zip(&files) {
            put_sidecar(&store, file, &[[n; 16]]).await;
        }
        let one = TraceIndex::new(vec![files[0].path.clone()], [([1u8; 16], 0)])
            .unwrap()
            .heap_bytes();
        let cache = TraceIndexCache::new(store, one * 2);

        let kept = cache
            .files_holding(files.iter().collect(), &[[2u8; 16]])
            .await;
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].id, files[1].id);
        assert!(
            cache.bytes().await <= one * 2,
            "the budget holds two of the three"
        );

        // The last two looked up survive; the first is fetched again.
        let stats = Arc::new(QueryStats::default());
        let cache = TraceIndexCache {
            stats: Some(Arc::clone(&stats)),
            ..cache
        };
        cache.files_holding(vec![&files[0]], &[[1u8; 16]]).await;
        assert_eq!(
            stats
                .trace_index_fetches
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        cache.files_holding(vec![&files[2]], &[[3u8; 16]]).await;
        assert_eq!(
            stats
                .trace_index_fetches
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn zero_bytes_disables_the_sidecars() {
        let store = Arc::new(InMemory::new());
        let file = record(1);
        put_sidecar(&store, &file, &[[1u8; 16]]).await;
        let cache = TraceIndexCache::new(store, 0);
        assert!(!cache.enabled());
        let kept = cache.files_holding(vec![&file], &[[9u8; 16]]).await;
        assert_eq!(kept.len(), 1, "nothing consulted, nothing pruned");
    }
}

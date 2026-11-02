//! Cache of the label postings of metric series files, consulted when a
//! metric selector carries a matcher that rules out an absent label.
//!
//! The protocol is the trace index's (`sidecar_cache`): one download per
//! file in flight, the object size reserved before a byte is read and the
//! decoded heap and scratch, measured from the frame, before a byte is
//! decoded. Postings missing, damaged, too large or bound to another
//! object are remembered as such and the file is scanned; postings only
//! ever narrow which rows of a file are read.

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

use fs_catalog::FileRecord;
use fs_index::postings::{Binding, Postings, sidecar_path};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use tracing::{debug, warn};

use crate::querier::QueryStats;
use crate::sidecar_cache::{Entry, Lease, MISS_BYTES, Reserve, Shared};

/// A decoded postings sidecar, charged to the cache while alive.
pub type HeldPostings = crate::sidecar_cache::Held<Postings>;

/// Shared postings cache.
pub struct MetricIndexCache {
    store: Arc<dyn ObjectStore>,
    stats: Option<Arc<QueryStats>>,
    shared: Arc<Shared<Postings>>,
}

impl MetricIndexCache {
    /// Most bytes the cache may hold, whatever it is configured with.
    pub const MAX_BYTES: usize = 1 << 30;

    /// Cache over `store` holding at most `max_bytes`, clamped to
    /// [`MetricIndexCache::MAX_BYTES`]. Zero disables the postings.
    pub fn new(store: Arc<dyn ObjectStore>, max_bytes: usize) -> Self {
        Self {
            store,
            stats: None,
            shared: Shared::new(max_bytes.min(Self::MAX_BYTES)),
        }
    }

    /// Counts fetches and misses in `stats`.
    #[must_use]
    pub fn with_stats(mut self, stats: Arc<QueryStats>) -> Self {
        self.stats = Some(stats);
        self
    }

    /// True when postings are consulted at all.
    pub fn enabled(&self) -> bool {
        self.shared.max_bytes > 0
    }

    /// Bytes of decoded postings alive and misses remembered.
    pub fn bytes(&self) -> usize {
        self.shared.bytes()
    }

    /// Bytes alive plus bytes reserved by downloads in flight.
    pub fn committed_bytes(&self) -> usize {
        self.shared.committed_bytes()
    }

    /// The postings of the series `file`, of series schema
    /// `schema_version`, fetched on a first sight. `None` when the file
    /// has none this engine can use, or none it can hold right now.
    pub async fn postings(
        &self,
        file: &FileRecord,
        schema_version: u32,
    ) -> Option<Arc<HeldPostings>> {
        if !self.enabled() {
            return None;
        }
        match self.shared.cached(file.id) {
            Some(Entry::Index(postings)) => return Some(postings),
            Some(Entry::Miss) => return None,
            None => {}
        }
        let Some(mut lease) = Lease::begin(&self.shared, file.id) else {
            debug!(file = %file.id.0, "postings download in flight elsewhere; the file is scanned this time");
            self.count_miss();
            return None;
        };
        match self.fetch(file, schema_version, &mut lease).await {
            Fetched::Postings(postings, heap) => lease.commit(Some(postings), heap),
            Fetched::Unusable => {
                lease.commit(None, MISS_BYTES);
                None
            }
            Fetched::Unavailable => None,
        }
    }

    async fn fetch(
        &self,
        file: &FileRecord,
        schema_version: u32,
        lease: &mut Lease<Postings>,
    ) -> Fetched {
        let (Some(path), Some(sha256), Ok(row_count)) = (
            sidecar_path(&file.path),
            file.content_sha256,
            u32::try_from(file.row_count),
        ) else {
            return self.miss();
        };
        let binding = Binding {
            path: file.path.clone(),
            sha256,
            schema_version,
            row_count,
        };
        if let Some(stats) = &self.stats {
            stats.metric_index_fetches.fetch_add(1, Relaxed);
        }
        let result = match self.store.get(&Path::from(path)).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => {
                debug!(file = %file.id.0, "postings missing; the file is scanned");
                return self.miss();
            }
            Err(e) => return self.unavailable(file, &e),
        };
        let size = usize::try_from(result.meta.size).unwrap_or(usize::MAX);
        if let Some(fetched) = self.reserve(file, lease, size, "postings larger than the budget") {
            return fetched;
        }
        let bytes = match result.bytes().await {
            Ok(bytes) => bytes,
            Err(e) => return self.unavailable(file, &e),
        };
        let cost = match Postings::decode_cost(&bytes) {
            Ok(cost) => cost,
            Err(e) => return self.unreadable(file, &e),
        };
        let decoding = cost.retained + cost.scratch;
        if let Some(fetched) =
            self.reserve(file, lease, decoding, "postings decode beyond the budget")
        {
            return fetched;
        }
        match Postings::decode(&bytes, &binding) {
            Ok(postings) => Fetched::Postings(postings, cost.retained),
            Err(e) => self.unreadable(file, &e),
        }
    }

    /// `None` when `bytes` fit; else what the fetch ends in.
    fn reserve(
        &self,
        file: &FileRecord,
        lease: &mut Lease<Postings>,
        bytes: usize,
        too_large: &str,
    ) -> Option<Fetched> {
        match lease.reserve(bytes) {
            Reserve::Fits => None,
            Reserve::TooLarge => {
                warn!(file = %file.id.0, bytes, budget = self.shared.max_bytes, "{too_large}; the file is scanned");
                Some(self.miss())
            }
            Reserve::Busy => {
                debug!(file = %file.id.0, bytes, "postings budget busy; the file is scanned this time");
                self.count_miss();
                Some(Fetched::Unavailable)
            }
        }
    }

    fn miss(&self) -> Fetched {
        self.count_miss();
        Fetched::Unusable
    }

    fn unreadable(&self, file: &FileRecord, error: &fs_index::IndexError) -> Fetched {
        warn!(file = %file.id.0, error = %error, "postings unreadable; the file is scanned");
        self.miss()
    }

    /// A store failure is not remembered: the next query tries again.
    fn unavailable(&self, file: &FileRecord, error: &object_store::Error) -> Fetched {
        warn!(file = %file.id.0, error = %error, "postings fetch failed; the file is scanned");
        self.count_miss();
        Fetched::Unavailable
    }

    fn count_miss(&self) {
        if let Some(stats) = &self.stats {
            stats.metric_index_misses.fetch_add(1, Relaxed);
        }
    }
}

enum Fetched {
    Postings(Postings, usize),
    Unusable,
    Unavailable,
}

impl std::fmt::Debug for MetricIndexCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricIndexCache")
            .field("max_bytes", &self.shared.max_bytes)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;

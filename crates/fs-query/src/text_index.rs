//! Cache of text sidecars, consulted when a log query searches the body.
//!
//! A sidecar is trusted only when it names the file's path, the digest the
//! catalog row carries and the row count of the row, and when its row
//! groups match the file's footer, read once as the sidecar enters the
//! cache. A file without a digest, or whose sidecar is missing, damaged,
//! too large or bound to other bytes, is remembered as such and read whole.
//! Downloads, decoding and the footer are reserved against the budget of
//! [`crate::sidecar_cache`] before a byte is read.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::parquet::file::metadata::{FooterTail, ParquetMetaDataReader};
use fs_catalog::FileRecord;
use fs_index::TextIndex;
use fs_index::text_index::Probe;
use futures_util::future::join_all;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use tracing::{debug, warn};

use crate::file_index::{FileIndex, FileSelection, Selection};
use crate::querier::QueryStats;
use crate::sidecar_cache::{Entry, Lease, MISS_BYTES, Reserve, Shared};

/// Sidecar fetches in flight for one query.
const FETCH_PARALLELISM: usize = 8;

/// Bytes reserved per footer byte while it is decoded and compared.
const FOOTER_DECODE_FACTOR: usize = 4;

/// Bytes of the Parquet footer's length and magic.
const FOOTER_TAIL: u64 = 8;

type Index = TextIndex<Bytes>;
type Held = crate::sidecar_cache::Held<Index>;

/// Shared text sidecar cache.
pub struct TextIndexCache {
    store: Arc<dyn ObjectStore>,
    stats: Option<Arc<QueryStats>>,
    shared: Arc<Shared<Index>>,
}

impl TextIndexCache {
    /// Cache over `store` holding at most `max_bytes`. Zero disables the
    /// sidecars: every file a search keeps is read whole.
    pub fn new(store: Arc<dyn ObjectStore>, max_bytes: usize) -> Self {
        Self {
            store,
            stats: None,
            shared: Shared::new(max_bytes),
        }
    }

    /// Counts fetches and misses in `stats`.
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
        self.shared.bytes()
    }

    /// Bytes alive plus bytes reserved by downloads in flight.
    pub fn committed_bytes(&self) -> usize {
        self.shared.committed_bytes()
    }

    /// The file's sidecar, fetched and checked on a first sight. `None`
    /// when it has none this engine can use, or none it can hold now.
    async fn lookup(&self, file: &FileRecord) -> Option<Arc<Held>> {
        match self.shared.cached(file.id) {
            Some(Entry::Index(index)) => return Some(index),
            Some(Entry::Miss) => return None,
            None => {}
        }
        let Some(mut lease) = Lease::begin(&self.shared, file.id) else {
            debug!(file = %file.id.0, "text index download in flight elsewhere; the file is read whole this time");
            self.count_miss();
            return None;
        };
        match self.fetch(file, &mut lease).await {
            Fetched::Index(index, heap) => lease.commit(Some(index), heap),
            Fetched::Unusable(why) => {
                debug!(file = %file.id.0, why, "text index unusable; the file is read whole");
                self.count_miss();
                lease.commit(None, MISS_BYTES);
                None
            }
            Fetched::Unavailable => {
                self.count_miss();
                None
            }
        }
    }

    async fn fetch(&self, file: &FileRecord, lease: &mut Lease<Index>) -> Fetched {
        let Some(path) = fs_index::text_index::sidecar_path(&file.path) else {
            return Fetched::Unusable("no sidecar path");
        };
        let Some(digest) = file.content_sha256 else {
            return Fetched::Unusable("the catalog row has no digest");
        };
        self.count_fetch();
        let result = match self.store.get(&Path::from(path)).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Fetched::Unusable("missing"),
            Err(e) => return Self::unavailable(file, &e),
        };
        let size = usize::try_from(result.meta.size).unwrap_or(usize::MAX);
        if let Some(refused) = self.reserve(lease, file, size) {
            return refused;
        }
        let bytes = match result.bytes().await {
            Ok(bytes) => bytes,
            Err(e) => return Self::unavailable(file, &e),
        };
        let Ok(heap) = Index::decode_cost(&bytes) else {
            return Fetched::Unusable("damaged");
        };
        if let Some(refused) = self.reserve(lease, file, heap) {
            return refused;
        }
        let Ok(index) = Index::decode(bytes) else {
            return Fetched::Unusable("damaged");
        };
        let rows: u64 = index.rows().sum();
        if index.path() != file.path
            || *index.content_sha256() != digest
            || i64::try_from(rows).ok() != Some(file.row_count)
        {
            return Fetched::Unusable("bound to another file");
        }
        match self.footer_rows(file, lease).await {
            Ok(footer) if footer.iter().copied().eq(index.rows()) => {
                let retained = index_bytes(&index);
                Fetched::Index(index, retained)
            }
            Ok(_) => Fetched::Unusable("row groups differ from the footer"),
            Err(refused) => refused,
        }
    }

    /// Rows of each row group of the file, read from its footer.
    async fn footer_rows(
        &self,
        file: &FileRecord,
        lease: &mut Lease<Index>,
    ) -> Result<Vec<u64>, Fetched> {
        let path = Path::from(file.path.as_str());
        let size = u64::try_from(file.size_bytes).unwrap_or(0);
        if size < FOOTER_TAIL {
            return Err(Fetched::Unusable("file shorter than a footer"));
        }
        let tail = self
            .store
            .get_range(&path, size - FOOTER_TAIL..size)
            .await
            .map_err(|e| Self::unavailable(file, &e))?;
        let tail: [u8; 8] = tail
            .as_ref()
            .try_into()
            .map_err(|_| Fetched::Unusable("footer"))?;
        let footer = FooterTail::try_new(&tail).map_err(|_| Fetched::Unusable("footer"))?;
        let len = footer.metadata_length() as u64;
        if footer.is_encrypted_footer() || len + FOOTER_TAIL > size {
            return Err(Fetched::Unusable("footer"));
        }
        let decoding = usize::try_from(len)
            .unwrap_or(usize::MAX)
            .saturating_mul(FOOTER_DECODE_FACTOR);
        if let Some(refused) = self.reserve(lease, file, decoding) {
            return Err(refused);
        }
        let start = size - FOOTER_TAIL - len;
        let raw = self
            .store
            .get_range(&path, start..size - FOOTER_TAIL)
            .await
            .map_err(|e| Self::unavailable(file, &e))?;
        let metadata = ParquetMetaDataReader::decode_metadata(&raw)
            .map_err(|_| Fetched::Unusable("footer"))?;
        Ok(metadata
            .row_groups()
            .iter()
            .map(|g| u64::try_from(g.num_rows()).unwrap_or(0))
            .collect())
    }

    /// `None` when `bytes` more fit the lease, else what the fetch becomes.
    fn reserve(
        &self,
        lease: &mut Lease<Index>,
        file: &FileRecord,
        bytes: usize,
    ) -> Option<Fetched> {
        match lease.reserve(bytes) {
            Reserve::Fits => None,
            Reserve::TooLarge => {
                warn!(file = %file.id.0, bytes, budget = self.shared.max_bytes, "text index larger than the budget; the file is read whole");
                Some(Fetched::Unusable("too large"))
            }
            Reserve::Busy => {
                debug!(file = %file.id.0, bytes, "text index budget busy; the file is read whole this time");
                Some(Fetched::Unavailable)
            }
        }
    }

    /// A store failure is not remembered: the next query tries again.
    fn unavailable(file: &FileRecord, error: &object_store::Error) -> Fetched {
        warn!(file = %file.id.0, error = %error, "text index fetch failed; the file is read whole");
        Fetched::Unavailable
    }

    fn count_fetch(&self) {
        if let Some(stats) = &self.stats {
            stats
                .text_index_fetches
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn count_miss(&self) {
        if let Some(stats) = &self.stats {
            stats
                .text_index_misses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    async fn select_one(&self, file: &FileRecord, probe: &Probe) -> FileSelection {
        let Some(index) = self.lookup(file).await else {
            return FileSelection {
                file: file.id,
                selection: Selection::All,
                row_groups: None,
            };
        };
        let count = index.row_groups();
        let groups = index.select(probe);
        let selection = match groups.len() {
            0 => Selection::Skip,
            n if n == count => Selection::All,
            _ => Selection::RowGroups(groups.into()),
        };
        FileSelection {
            file: file.id,
            selection,
            row_groups: Some(count),
        }
    }
}

#[async_trait]
impl FileIndex for TextIndexCache {
    async fn select(&self, files: &[FileRecord], probe: &Probe) -> Vec<FileSelection> {
        if !self.enabled() || !probe.prunes() {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(files.len());
        for chunk in files.chunks(FETCH_PARALLELISM) {
            out.extend(join_all(chunk.iter().map(|file| self.select_one(file, probe))).await);
        }
        out
    }
}

/// What a decoded sidecar keeps: its bytes and its descriptors.
fn index_bytes(index: &Index) -> usize {
    index.encoded_len() + index.heap_bytes()
}

enum Fetched {
    Index(Index, usize),
    /// Remembered: the file is read whole until the entry is evicted.
    Unusable(&'static str),
    /// Not remembered: tried again by the next query.
    Unavailable,
}

impl std::fmt::Debug for TextIndexCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TextIndexCache")
            .field("max_bytes", &self.shared.max_bytes)
            .finish_non_exhaustive()
    }
}

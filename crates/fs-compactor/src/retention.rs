//! Retention by tenant. Files whose rows all ended before the tenant's
//! cut, a UTC day boundary on the catalog's clock, leave the catalog
//! under the leader's fence, so many per pass; their objects follow at
//! the purge, as after a compaction, so a reader that opened one finishes.
//! A crash anywhere leaves the catalog ahead of the bucket, never behind
//! it, and the next pass continues with what is left.
//!
//! The `attribute_keys` table of a tenant is a count over every row ever
//! compacted; once the tenant's expired files are gone it still lists what
//! they carried, so it is rebuilt from the L1 files that remain, with the
//! admissions of the table before, and swapped in under the fence, the
//! table before quarantined with it. A rebuild that does not commit, for
//! any reason, leaves the table before in place and the new table an
//! orphan. A tenant whose remaining L1 files exceed the read cap keeps its
//! table and is reported once.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeZone, Utc};
use fs_catalog::{
    Catalog, CatalogError, Compaction, ExpiredFiles, FileRecord, LeaseFence, Level, Signal,
    StaleTable, TenantId,
};
use fs_schema::attribute_keys::{KeyStats, READ_COLUMNS};
use futures_util::StreamExt;
use object_store::ObjectStore;
use object_store::path::Path;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use tracing::{info, warn};

use crate::CompactError;
use crate::derived::{read_table, table_file};
use crate::merge::ObjectReader;
use crate::objects::{CountsOrphans, OrphanGuard, create, take_injected};

/// Bounds of retention.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionConfig {
    /// Most files one pass quarantines.
    pub files_per_pass: i64,
    /// Most bytes of L1 files one `attribute_keys` rebuild reads; a
    /// tenant whose remaining L1 files exceed it keeps its table.
    pub rebuild_max_bytes: u64,
    /// Rows per batch a rebuild decodes at a time.
    pub batch_rows: usize,
    /// Make this many rebuild swaps fail artificially after the table is
    /// uploaded. Zero in production; tests use it to crash before the swap.
    pub inject_swap_failures: u32,
}

/// Counters of retention.
#[derive(Debug, Default)]
pub struct RetentionStats {
    /// Passes that quarantined files.
    pub passes: AtomicU64,
    /// Files quarantined.
    pub expired: AtomicU64,
    /// Passes refused because the lease had changed hands.
    pub lease_lost: AtomicU64,
    /// Passes that failed for any other reason.
    pub failures: AtomicU64,
    /// `attribute_keys` tables rebuilt and swapped in, or removed when
    /// nothing remained.
    pub rebuilds: AtomicU64,
    /// Tenants passed over because their remaining L1 files exceed the
    /// read cap.
    pub rebuild_skipped: AtomicU64,
    /// Rebuild swaps refused because the lease had changed hands.
    pub rebuild_lease_lost: AtomicU64,
    /// Rebuild swaps refused because the table before was no longer
    /// visible.
    pub rebuild_conflicts: AtomicU64,
    /// Rebuilds that failed before or at the swap for any other reason.
    pub rebuild_failures: AtomicU64,
    /// Tables uploaded by rebuilds that did not publish them.
    pub rebuild_orphaned: AtomicU64,
}

impl CountsOrphans for RetentionStats {
    fn count_orphans(&self, created: u64) {
        self.rebuild_orphaned.fetch_add(created, Ordering::Relaxed);
    }
}

/// The retention side of the compactor.
pub struct Retention {
    catalog: Catalog,
    store: Arc<dyn ObjectStore>,
    root: Path,
    config: RetentionConfig,
    stats: Arc<RetentionStats>,
    inject_swap_failures: AtomicU32,
    /// Tenants already reported as exceeding the read cap.
    reported: Mutex<HashSet<TenantId>>,
}

impl Retention {
    /// Retention over `catalog` and `store`, writing rebuilt tables under
    /// `root`.
    pub fn new(
        catalog: Catalog,
        store: Arc<dyn ObjectStore>,
        root: Path,
        config: RetentionConfig,
        stats: Arc<RetentionStats>,
    ) -> Self {
        let inject = config.inject_swap_failures;
        Self {
            catalog,
            store,
            root,
            config,
            stats,
            inject_swap_failures: AtomicU32::new(inject),
            reported: Mutex::new(HashSet::new()),
        }
    }

    /// The counters.
    pub fn stats(&self) -> Arc<RetentionStats> {
        Arc::clone(&self.stats)
    }

    /// Quarantines up to `files_per_pass` span files beyond their
    /// tenant's cut, oldest first, under `fence`; `None` when no file is
    /// beyond its cut.
    ///
    /// # Errors
    /// A lost lease, or a catalog failure. The counters record which.
    pub async fn expire_once(
        &self,
        fence: &LeaseFence,
    ) -> Result<Option<ExpiredFiles>, CompactError> {
        let result = self
            .catalog
            .expire_files_fenced(Signal::Traces, self.config.files_per_pass, fence)
            .await;
        match &result {
            Ok(Some(expired)) => {
                self.stats.passes.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .expired
                    .fetch_add(expired.files.len() as u64, Ordering::Relaxed);
                info!(
                    role = "compactor",
                    tenant = %expired.tenant_id.0,
                    cutoff = %expired.cutoff,
                    files = expired.files.len(),
                    "expired files quarantined"
                );
            }
            Ok(None) => {}
            Err(CatalogError::LeaseLost { .. }) => {
                self.stats.lease_lost.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.stats.failures.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(result?)
    }

    /// Rebuilds the `attribute_keys` table of the tenant whose table lists
    /// the earliest sighting before its cut, from the L1 files that remain
    /// at or after it, and swaps it in under `fence`. A tenant whose
    /// remaining L1 files exceed the read cap is passed over, reported
    /// once, and the next is tried. `None` when no table is stale.
    ///
    /// # Errors
    /// A refused swap (conflict or lost lease), or a store, Parquet or
    /// catalog failure, the table before or an L1 file unreadable
    /// included. The counters record which.
    pub async fn rebuild_once(
        &self,
        fence: &LeaseFence,
    ) -> Result<Option<Compaction>, CompactError> {
        let mut skip = 0;
        let (stale, inputs) = loop {
            let Some(stale) = self
                .catalog
                .stale_derived_table(Signal::AttributeKeys, Signal::Traces, skip)
                .await?
            else {
                return Ok(None);
            };
            let inputs = self
                .catalog
                .visible_files_from(stale.tenant_id, Signal::Traces, Level::L1, stale.cutoff)
                .await?;
            let bytes = inputs
                .iter()
                .map(|f| u64::try_from(f.size_bytes).unwrap_or(u64::MAX))
                .fold(0u64, u64::saturating_add);
            if bytes <= self.config.rebuild_max_bytes {
                break (stale, inputs);
            }
            self.report_over_cap(stale.tenant_id, bytes);
            skip += 1;
        };
        let result = self.rebuild(&stale, &inputs, fence).await;
        match &result {
            Ok(_) => {
                self.stats.rebuilds.fetch_add(1, Ordering::Relaxed);
            }
            Err(CompactError::Catalog(CatalogError::LeaseLost { .. })) => {
                self.stats
                    .rebuild_lease_lost
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(CompactError::Catalog(CatalogError::CompactionConflict { .. })) => {
                self.stats.rebuild_conflicts.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.stats.rebuild_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
        result.map(Some)
    }

    fn report_over_cap(&self, tenant: TenantId, bytes: u64) {
        let first_time = self
            .reported
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(tenant);
        if first_time {
            self.stats.rebuild_skipped.fetch_add(1, Ordering::Relaxed);
            warn!(
                role = "compactor",
                tenant = %tenant.0,
                bytes,
                cap = self.config.rebuild_max_bytes,
                "the tenant's remaining L1 files exceed the rebuild cap; its attribute_keys table is kept as it is"
            );
        }
    }

    async fn rebuild(
        &self,
        stale: &StaleTable,
        inputs: &[FileRecord],
        fence: &LeaseFence,
    ) -> Result<Compaction, CompactError> {
        let tenant = stale.tenant_id;
        let before = read_table(self.store.as_ref(), &stale.table.path)
            .await
            .inspect_err(|e| {
                warn!(
                    role = "compactor",
                    tenant = %tenant.0,
                    path = %stale.table.path,
                    error = %e,
                    "attribute_keys table unreadable; rebuild aborted"
                );
            })?;
        let mut keys = KeyStats::default();
        for file in inputs {
            self.absorb(&mut keys, &before, file).await?;
        }
        let keys = keys.settled();
        let mut guard = OrphanGuard {
            stats: Arc::clone(&self.stats),
            tenant,
            work: "attribute_keys rebuild".to_owned(),
            created: 0,
            armed: true,
        };
        let mut outputs = Vec::new();
        if let Some((file, bytes)) = table_file(&self.root, tenant, this_hour(), &keys)? {
            create(self.store.as_ref(), &file.path, bytes).await?;
            guard.created += 1;
            outputs.push(file);
        }
        if take_injected(&self.inject_swap_failures) {
            return Err(CompactError::Injected("swap"));
        }
        let done = self
            .catalog
            .swap_files_fenced(
                tenant,
                Signal::Traces,
                &[],
                &[(Signal::AttributeKeys, stale.table.id)],
                &outputs,
                Some(fence),
            )
            .await?;
        guard.armed = false;
        info!(
            role = "compactor",
            tenant = %tenant.0,
            cutoff = %stale.cutoff,
            inputs = inputs.len(),
            keys = keys.len(),
            "attribute_keys table rebuilt"
        );
        Ok(done)
    }

    /// Counts the keys of `file` that `before` knows into `keys`, reading
    /// the timestamp and attribute columns only, one batch at a time.
    async fn absorb(
        &self,
        keys: &mut KeyStats,
        before: &KeyStats,
        file: &FileRecord,
    ) -> Result<(), CompactError> {
        let reader = ObjectReader {
            store: Arc::clone(&self.store),
            path: Path::from(file.path.as_str()),
            size: u64::try_from(file.size_bytes).unwrap_or(0),
        };
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
        let columns = builder.parquet_schema();
        let leaves: Vec<usize> = (0..columns.num_columns())
            .filter(|&i| {
                columns
                    .column(i)
                    .path()
                    .parts()
                    .first()
                    .is_some_and(|root| READ_COLUMNS.contains(&root.as_str()))
            })
            .collect();
        let mask = ProjectionMask::leaves(columns, leaves);
        let mut stream = builder
            .with_projection(mask)
            .with_batch_size(self.config.batch_rows)
            .build()?;
        while let Some(batch) = stream.next().await {
            keys.absorb_known(&batch?, before)?;
        }
        Ok(())
    }
}

/// The start of the current hour, the partition a rebuilt table goes in.
fn this_hour() -> DateTime<Utc> {
    let now = Utc::now();
    Utc.timestamp_opt(now.timestamp().div_euclid(3600) * 3600, 0)
        .single()
        .unwrap_or(now)
}

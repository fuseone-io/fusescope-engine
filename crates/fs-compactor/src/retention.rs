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
//! orphan, and the tenant is held back for the rest of the tick so the
//! others are tried. A tenant whose remaining L1 files exceed the input
//! cap, or whose rebuild would hold more than the memory budget by the
//! worst case the limits allow, keeps its table and is reported once.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeZone, Utc};
use fs_catalog::{
    Catalog, CatalogError, Compaction, ExpiredFiles, FileRecord, LeaseFence, Level, RebuildInput,
    Signal, StaleTable, TenantId,
};
use fs_schema::attribute_keys::{KeyStats, READ_COLUMNS};
use fs_schema::limits::Limits;
use futures_util::StreamExt;
use object_store::ObjectStore;
use object_store::path::Path;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use tracing::{info, warn};

use crate::CompactError;
use crate::derived::{read_table, table_file};
use crate::merge::ObjectReader;
use crate::objects::{CountsOrphans, OrphanGuard, create, take_injected};

/// Rows a rebuild decodes from an L1 file at a time: under the default
/// limits one row of attribute maps may reach several megabytes, so a
/// few rows bound the batch.
pub const REBUILD_BATCH_ROWS: usize = 16;

/// Bytes a decoded key row holds beyond its key: the map node, the string
/// header and the sightings.
const KEY_ROW_OVERHEAD: u64 = 128;

/// Bytes a table row holds in the output arrays, and again in the Parquet
/// bytes, beyond its key: six fixed columns and the offsets.
const OUTPUT_ROW_OVERHEAD: u64 = 56;

/// Inputs listed per catalog round trip; the budget sees each page.
const INPUT_PAGE: i64 = 1000;

/// Bounds of retention.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionConfig {
    /// Most files one pass quarantines.
    pub files_per_pass: i64,
    /// Most bytes of L1 files one `attribute_keys` rebuild reads; a
    /// tenant whose remaining L1 files exceed it keeps its table.
    pub rebuild_max_input_bytes: u64,
    /// Most bytes one rebuild may hold at its peak, by
    /// [`rebuild_footprint`]; a tenant whose rebuild would exceed it
    /// keeps its table.
    pub rebuild_max_bytes: u64,
    /// Rows per batch a rebuild decodes at a time.
    pub batch_rows: usize,
    /// The limits the rows were written under, which bound a row.
    pub limits: Limits,
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
    /// Tenants already reported as exceeding a cap.
    reported: Mutex<HashSet<TenantId>>,
    /// Tenants whose rebuild failed this tick, passed over until the next.
    failed: Mutex<HashSet<TenantId>>,
}

/// Bytes a rebuild of a table of `table_bytes` and `table_rows` from L1
/// files no larger than `largest_input`, listed in `list_bytes`, holds at
/// its peak, by the worst case `config.limits` allow: the list of inputs,
/// the table before as fetched and as decoded, the rebuilt table, its
/// arrays and its Parquet bytes, the column chunks of one input and one
/// decoded batch of `batch_rows` spans with every attribute map full.
#[must_use]
pub fn rebuild_footprint(
    table_bytes: u64,
    table_rows: u64,
    largest_input: u64,
    list_bytes: u64,
    config: &RetentionConfig,
) -> u64 {
    let key = config.limits.max_key_bytes as u64;
    let value = config.limits.max_value_bytes as u64;
    let attributes = config.limits.max_attributes as u64;
    let key_row = key + KEY_ROW_OVERHEAD;
    let output_row = key + OUTPUT_ROW_OVERHEAD;
    let string_maps = 3 * attributes * (key + value);
    let number_map = attributes * (key + 8);
    let bool_map = attributes * (key + 1);
    let span_row = string_maps + number_map + bool_map + 8 + 5 * 4;
    list_bytes
        .saturating_add(table_bytes)
        .saturating_add(table_rows.saturating_mul(2 * key_row))
        .saturating_add(table_rows.saturating_mul(2 * output_row))
        .saturating_add(largest_input)
        .saturating_add((config.batch_rows as u64).saturating_mul(span_row))
}

/// What a rebuild would read and hold, added up page by page as the
/// catalog lists the inputs, against the caps; the first cap exceeded is
/// remembered.
struct Budget<'a> {
    config: &'a RetentionConfig,
    table_bytes: u64,
    table_rows: u64,
    input_bytes: u64,
    largest: u64,
    list_bytes: u64,
    over: Option<(&'static str, u64, u64)>,
}

impl<'a> Budget<'a> {
    fn new(config: &'a RetentionConfig, table: &FileRecord) -> Self {
        Self {
            config,
            table_bytes: u64::try_from(table.size_bytes).unwrap_or(u64::MAX),
            table_rows: u64::try_from(table.row_count).unwrap_or(u64::MAX),
            input_bytes: 0,
            largest: 0,
            list_bytes: 0,
            over: None,
        }
    }

    /// Adds `page` and tells whether everything so far fits.
    fn admit(&mut self, page: &[RebuildInput]) -> bool {
        for input in page {
            let size = u64::try_from(input.size_bytes).unwrap_or(u64::MAX);
            self.input_bytes = self.input_bytes.saturating_add(size);
            self.largest = self.largest.max(size);
            let record = input.path.len() as u64 + std::mem::size_of::<RebuildInput>() as u64;
            self.list_bytes = self.list_bytes.saturating_add(record);
        }
        self.fits()
    }

    fn fits(&mut self) -> bool {
        let footprint = rebuild_footprint(
            self.table_bytes,
            self.table_rows,
            self.largest,
            self.list_bytes,
            self.config,
        );
        if self.input_bytes > self.config.rebuild_max_input_bytes {
            self.over = Some((
                "input",
                self.input_bytes,
                self.config.rebuild_max_input_bytes,
            ));
        } else if footprint > self.config.rebuild_max_bytes {
            self.over = Some(("memory", footprint, self.config.rebuild_max_bytes));
        }
        self.over.is_none()
    }
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
            failed: Mutex::new(HashSet::new()),
        }
    }

    /// The counters.
    pub fn stats(&self) -> Arc<RetentionStats> {
        Arc::clone(&self.stats)
    }

    /// Tries again, from the next rebuild on, the tenants whose rebuild
    /// failed; called at the start of a tick.
    pub fn forget_failures(&self) {
        self.failed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
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
    /// remaining L1 files exceed the input cap or whose rebuild would
    /// exceed the memory budget is passed over, reported once, and the
    /// next is tried; so is one whose rebuild failed since the last
    /// [`forget_failures`](Self::forget_failures). `None` when no table
    /// is left to rebuild.
    ///
    /// # Errors
    /// A refused swap (conflict or lost lease), or a store, Parquet or
    /// catalog failure, the table before or an L1 file unreadable
    /// included. The counters record which, and the tenant is held back.
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
            skip += 1;
            if self.held_back(stale.tenant_id) {
                continue;
            }
            let mut budget = Budget::new(&self.config, &stale.table);
            let inputs = self
                .catalog
                .rebuild_inputs(
                    stale.tenant_id,
                    Signal::Traces,
                    Level::L1,
                    stale.cutoff,
                    INPUT_PAGE,
                    |page| budget.admit(page),
                )
                .await?;
            let Some(inputs) = inputs.filter(|_| budget.fits()) else {
                let (what, bytes, cap) = budget.over.unwrap_or(("memory", 0, 0));
                self.report_over_cap(stale.tenant_id, what, bytes, cap);
                continue;
            };
            break (stale, inputs);
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
                self.failed
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(stale.tenant_id);
            }
        }
        result.map(Some)
    }

    fn held_back(&self, tenant: TenantId) -> bool {
        self.failed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&tenant)
    }

    fn report_over_cap(&self, tenant: TenantId, what: &'static str, bytes: u64, cap: u64) {
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
                what,
                bytes,
                cap,
                "the tenant's attribute_keys rebuild exceeds its cap; the table is kept as it is"
            );
        }
    }

    async fn rebuild(
        &self,
        stale: &StaleTable,
        inputs: &[RebuildInput],
        fence: &LeaseFence,
    ) -> Result<Compaction, CompactError> {
        let tenant = stale.tenant_id;
        let keys = if inputs.is_empty() {
            KeyStats::default()
        } else {
            self.count_remaining(stale, inputs).await?
        };
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

    /// The table before, with the sightings of `inputs` alone.
    async fn count_remaining(
        &self,
        stale: &StaleTable,
        inputs: &[RebuildInput],
    ) -> Result<KeyStats, CompactError> {
        let before = read_table(self.store.as_ref(), &stale.table.path)
            .await
            .inspect_err(|e| {
                warn!(
                    role = "compactor",
                    tenant = %stale.tenant_id.0,
                    path = %stale.table.path,
                    error = %e,
                    "attribute_keys table unreadable; rebuild aborted"
                );
            })?;
        let mut keys = KeyStats::default();
        for file in inputs {
            self.absorb(&mut keys, &before, file).await?;
        }
        Ok(keys.settled())
    }

    /// Counts the keys of `file` that `before` knows into `keys`, reading
    /// the timestamp and attribute columns only, one batch at a time.
    async fn absorb(
        &self,
        keys: &mut KeyStats,
        before: &KeyStats,
        file: &RebuildInput,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn config(batch_rows: usize) -> RetentionConfig {
        RetentionConfig {
            files_per_pass: 1,
            rebuild_max_input_bytes: 1,
            rebuild_max_bytes: 1,
            batch_rows,
            limits: Limits::default(),
            inject_swap_failures: 0,
        }
    }

    #[test]
    fn the_footprint_adds_every_resident_under_the_default_limits() {
        let none = config(0);
        assert_eq!(rebuild_footprint(0, 0, 0, 0, &none), 0);
        assert_eq!(
            rebuild_footprint(1_000, 0, 0, 0, &none),
            1_000,
            "the table as fetched"
        );
        assert_eq!(
            rebuild_footprint(0, 0, 5_000, 0, &none),
            5_000,
            "one input's chunks"
        );
        assert_eq!(
            rebuild_footprint(0, 0, 0, 300, &none),
            300,
            "the list of inputs"
        );
        assert_eq!(
            rebuild_footprint(0, 1, 0, 0, &none),
            2 * (256 + 128) + 2 * (256 + 56),
            "a key row decoded twice, in arrays and in bytes"
        );
        assert_eq!(
            rebuild_footprint(0, 0, 0, 0, &config(1)),
            3 * 256 * (256 + 8 * 1024) + 256 * (256 + 8) + 256 * (256 + 1) + 8 + 20,
            "one span with every map full"
        );
        assert_eq!(
            rebuild_footprint(u64::MAX, 1, 1, 1, &config(1)),
            u64::MAX,
            "saturates"
        );
    }
}

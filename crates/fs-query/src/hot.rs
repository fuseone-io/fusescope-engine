//! Querier side of the hot window: fan out to live ingesters, merge what
//! they hold in memory, and learn which files to skip.

use std::collections::HashSet;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use chrono::{Duration as ChronoDuration, Utc};
use fs_catalog::{Catalog, FileId, TenantId};
use fs_ingest::hot::{HotScanRequest, scan};
use tracing::{debug, warn};

use crate::prune::Pruning;

/// Settings of the hot-window fan-out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HotWindowConfig {
    /// Ask ingesters at all.
    pub enabled: bool,
    /// Queries whose range ends earlier than now minus this skip the fan-out.
    pub recent: Duration,
    /// Per-ingester time limit; a slow ingester makes the result partial.
    pub timeout: Duration,
    /// Ingesters silent for longer are not asked.
    pub stale_after: Duration,
}

impl Default for HotWindowConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            recent: Duration::from_secs(120),
            timeout: Duration::from_secs(2),
            stale_after: Duration::from_secs(10),
        }
    }
}

impl HotWindowConfig {
    /// True when the pruned range may include rows not yet flushed.
    pub fn touches_recent(&self, pruning: &Pruning) -> bool {
        if !self.enabled {
            return false;
        }
        let now = Utc::now();
        let recent_start =
            now - ChronoDuration::from_std(self.recent).unwrap_or(ChronoDuration::zero());
        let ends_recent = pruning
            .ts_max
            .is_none_or(|max| max >= recent_start.timestamp_nanos_opt().unwrap_or(i64::MAX));
        let starts_before_now = pruning
            .ts_min
            .is_none_or(|min| min <= now.timestamp_nanos_opt().unwrap_or(i64::MAX));
        ends_recent && starts_before_now
    }
}

/// What the fan-out produced.
#[derive(Debug, Default)]
pub struct HotResult {
    /// Rows from every ingester that answered.
    pub batches: Vec<RecordBatch>,
    /// Files whose rows came from an ingester; the file scan must skip them.
    pub excluded_files: HashSet<FileId>,
    /// Ingesters that were alive but did not answer in time.
    pub unavailable: Vec<String>,
}

/// Decides whether a query touches the recent window and runs the fan-out.
pub struct HotWindow {
    catalog: Catalog,
    config: HotWindowConfig,
}

impl HotWindow {
    /// Fan-out over `catalog`'s live ingesters.
    pub fn new(catalog: Catalog, config: HotWindowConfig) -> Self {
        Self { catalog, config }
    }

    /// True when the pruned range may include rows not yet flushed.
    pub fn touches_recent(&self, pruning: &Pruning) -> bool {
        self.config.touches_recent(pruning)
    }

    /// Asks every live ingester for `tenant`'s rows matching `pruning`,
    /// projected to `columns`.
    ///
    /// # Errors
    /// Catalog failure while listing ingesters. Ingester failures are not
    /// errors; they are reported in [`HotResult::unavailable`].
    pub async fn scan(
        &self,
        tenant: TenantId,
        pruning: &Pruning,
        columns: Option<Vec<String>>,
    ) -> Result<HotResult, fs_catalog::CatalogError> {
        let stale = ChronoDuration::from_std(self.config.stale_after)
            .unwrap_or(ChronoDuration::seconds(10));
        let ingesters = self.catalog.live_ingesters(stale).await?;
        if ingesters.is_empty() {
            return Ok(HotResult::default());
        }
        let request = HotScanRequest {
            tenant,
            ts_min: pruning.ts_min,
            ts_max: pruning.ts_max,
            services: pruning.services.clone(),
            columns,
        };
        let calls = ingesters.iter().map(|ingester| {
            let request = request.clone();
            let timeout = self.config.timeout;
            async move {
                (
                    ingester.id.clone(),
                    scan(&ingester.address, &request, timeout).await,
                )
            }
        });
        let mut result = HotResult::default();
        for (id, outcome) in futures_util::future::join_all(calls).await {
            match outcome {
                Ok(response) => {
                    result.excluded_files.extend(response.persisted_files);
                    result.batches.extend(response.batches);
                }
                Err(e) => {
                    warn!(ingester = %id, error = %e, "hot window scan failed; result is partial");
                    result.unavailable.push(id);
                }
            }
        }
        debug!(
            tenant = %tenant.0,
            ingesters = ingesters.len(),
            rows = result.batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            excluded_files = result.excluded_files.len(),
            unavailable = result.unavailable.len(),
            "hot window"
        );
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(enabled: bool) -> HotWindowConfig {
        HotWindowConfig {
            enabled,
            ..HotWindowConfig::default()
        }
    }

    #[test]
    fn recent_detection() {
        let now = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let hour = 3_600_000_000_000_i64;
        assert!(
            config(true).touches_recent(&Pruning::default()),
            "unbounded queries touch the window"
        );
        assert!(config(true).touches_recent(&Pruning {
            ts_min: Some(now - hour),
            ts_max: None,
            services: None
        }));
        assert!(!config(true).touches_recent(&Pruning {
            ts_min: Some(now - 2 * hour),
            ts_max: Some(now - hour),
            services: None
        }));
        assert!(
            !config(true).touches_recent(&Pruning {
                ts_min: Some(now + hour),
                ts_max: None,
                services: None
            }),
            "future only"
        );
        assert!(!config(false).touches_recent(&Pruning::default()));
    }
}

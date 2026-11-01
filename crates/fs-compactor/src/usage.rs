//! Usage counted once: the `usage_1h` contributions of a pass and the
//! accounting pass, which counts files no compaction pass will take.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use fs_catalog::{FileId, FileRecord, LeaseFence, Level, NewFile, Signal, TenantId, TimeRange};
use fs_schema::usage::UsageCounts;
use futures_util::StreamExt;
use object_store::path::Path;
use tracing::info;

use crate::CompactError;
use crate::compact::Compactor;
use crate::merge::{batches, partitioned_path, usage_signal};
use crate::objects::OrphanGuard;

const HOUR_NANOS: i64 = 3_600_000_000_000;

/// One contribution per hour `counts` covers, with its catalog row, every
/// one counted.
///
/// # Errors
/// Encoding failure.
pub(crate) fn usage_files(
    root: &Path,
    tenant: TenantId,
    counts: &UsageCounts,
) -> Result<Vec<(NewFile, Bytes)>, CompactError> {
    let mut out = Vec::new();
    for hour in counts.hours() {
        let bytes = Bytes::from(fs_schema::usage::encode_hour(counts, hour)?);
        let id = FileId::new();
        let start = Utc.timestamp_nanos(hour);
        let path = partitioned_path(root, tenant, Signal::Usage1h.path_name(), start, id);
        let services = counts
            .of_hour(hour)
            .filter_map(|(service, ..)| service.map(str::to_owned))
            .collect();
        out.push((
            NewFile {
                id,
                origin: id,
                tenant_id: tenant,
                signal: Signal::Usage1h,
                level: Level::L1,
                path: path.to_string(),
                size_bytes: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
                row_count: i64::try_from(counts.of_hour(hour).count()).unwrap_or(i64::MAX),
                range: TimeRange {
                    start,
                    end: Utc.timestamp_nanos(hour + HOUR_NANOS - 1),
                },
                services,
                summary: None,
                content_sha256: Some(crate::objects::sha256(&bytes)),
                usage_accounted: true,
            },
            bytes,
        ));
    }
    Ok(out)
}

/// What one accounting pass counted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Accounted {
    /// Owner.
    pub tenant_id: TenantId,
    /// Signal of the files counted.
    pub signal: Signal,
    /// The hour of their `ts_min`.
    pub hour: DateTime<Utc>,
    /// The files marked counted.
    pub files: Vec<FileId>,
    /// Contribution files published.
    pub contributions: usize,
}

impl Compactor {
    /// Counts the oldest cold hour of span or log files whose usage is not
    /// counted and that no compaction pass takes: L1 and L2 files written
    /// before usage was counted, and L0 files of a signal whose compaction
    /// is off. The files are read and left in place; their contributions
    /// are published and the files marked counted in one fenced
    /// transaction. `None` when nothing qualifies.
    ///
    /// # Errors
    /// Lease lost, conflict, or a store, Parquet or catalog failure.
    pub async fn account_once(
        &self,
        fence: &LeaseFence,
    ) -> Result<Option<Accounted>, CompactError> {
        let compacted: &[Signal] = if self.config.compact_logs {
            &[Signal::Traces, Signal::Logs]
        } else {
            &[Signal::Traces]
        };
        let mut skip = 0;
        let (candidate, taken) = loop {
            let Some(candidate) = self
                .catalog
                .accounting_inputs(compacted, self.config.hour_close_delay, skip)
                .await?
            else {
                return Ok(None);
            };
            let taken = self.taken(&candidate.files);
            if taken >= 1 {
                break (candidate, taken);
            }
            skip += 1;
        };
        let files = candidate.files.get(..taken).unwrap_or(&[]);
        let tenant = candidate.tenant_id;
        let counts = self.count_files(candidate.signal, files).await?;
        let mut guard = OrphanGuard {
            stats: Arc::clone(&self.stats),
            tenant,
            work: format!("usage of {}", candidate.hour),
            created: 0,
            armed: true,
        };
        let root = self.root.clone();
        let prepared =
            tokio::task::spawn_blocking(move || usage_files(&root, tenant, &counts)).await??;
        let mut puts = 0usize;
        let mut outputs = Vec::with_capacity(prepared.len());
        for (file, bytes) in prepared {
            self.put_once(&file.path, bytes, &mut puts).await?;
            guard.created += 1;
            outputs.push(file);
        }
        let ids: Vec<FileId> = files.iter().map(|f| f.id).collect();
        self.catalog
            .account_files_fenced(tenant, candidate.signal, &ids, &outputs, fence)
            .await?;
        guard.armed = false;
        self.stats
            .accounted
            .fetch_add(ids.len() as u64, Ordering::Relaxed);
        self.stats
            .usage_contributions
            .fetch_add(outputs.len() as u64, Ordering::Relaxed);
        info!(
            role = "compactor",
            tenant = %tenant.0,
            signal = ?candidate.signal,
            hour = %candidate.hour,
            files = ids.len(),
            contributions = outputs.len(),
            "usage counted"
        );
        Ok(Some(Accounted {
            tenant_id: tenant,
            signal: candidate.signal,
            hour: candidate.hour,
            files: ids,
            contributions: outputs.len(),
        }))
    }

    /// The usage of every row of `files`, read one batch at a time.
    async fn count_files(
        &self,
        signal: Signal,
        files: &[FileRecord],
    ) -> Result<UsageCounts, CompactError> {
        let usage = usage_signal(signal).ok_or(CompactError::Merge("a signal without usage"))?;
        let mut counts = UsageCounts::new(usage);
        for file in files {
            let mut stream =
                batches(Arc::clone(&self.store), file, self.config.row_group_rows).await?;
            while let Some(batch) = stream.next().await.transpose()? {
                counts.absorb(&batch)?;
                if counts.len() > self.config.usage_max_entries {
                    return Err(CompactError::Merge(
                        "usage pairs over compactor.usage_max_entries",
                    ));
                }
            }
        }
        Ok(counts)
    }
}

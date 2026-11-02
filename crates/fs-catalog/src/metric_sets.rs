//! What the compactor asks the catalog of metric file sets: the next hour
//! or day to compact or account, read in one snapshot, and the fenced
//! transactions that publish what it made of them.
//!
//! A set is two files, samples and series, taken, quarantined, published
//! and accounted together. A set found in part, or whose two files disagree
//! on whether they were accounted, is no state a crash leaves: every call
//! fails closed on one.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::catalog::{Confirm, insert_file};
use crate::types::{FileRecord, FileRow, FileSetId, FileState, Level, NewFile, Signal, TenantId};
use crate::{Catalog, CatalogError, Compaction, LeaseFence};

/// One set: its id and its two files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricSet {
    /// The set.
    pub set: FileSetId,
    /// Its samples file.
    pub samples: FileRecord,
    /// Its series file.
    pub series: FileRecord,
}

/// The sets of one tenant and hour, or day, a pass takes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricInputs {
    /// Owner.
    pub tenant_id: TenantId,
    /// Start of the UTC hour, or day for L2, by the files' `ts_min`.
    pub start: DateTime<Utc>,
    /// The sets, oldest registration first.
    pub sets: Vec<MetricSet>,
}

/// What a pass takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricPass {
    /// L0 sets of a cold hour, into L1.
    Compact,
    /// L0 sets of a cold hour not accounted yet, left in place.
    Account,
    /// L1 sets of a closed day without L0, into L2.
    Merge,
}

const HOUR_BIN: &str = "date_bin('1 hour', ts_min, timestamptz '1970-01-01 00:00:00+00')";
const DAY_BIN: &str = "date_bin('1 day', ts_min, timestamptz '1970-01-01 00:00:00+00')";

/// Columns of a file row, as [`FileRow`] reads them.
const FILE_COLUMNS: &str = "id, origin, tenant_id, signal, level, path, size_bytes, row_count,
    ts_min, ts_max, services, summary, content_sha256, state, created_at, quarantined_at,
    usage_accounted";

impl Catalog {
    /// The `skip`-th oldest unit `pass` takes, across tenants, with its
    /// sets, read in one repeatable-read snapshot:
    ///
    /// - [`MetricPass::Compact`]: an hour whose visible L0 metric files all
    ///   ended, and were confirmed, more than `closed_for` ago;
    /// - [`MetricPass::Account`]: such an hour holding a set not accounted,
    ///   its sets not accounted only;
    /// - [`MetricPass::Merge`]: a UTC day that ended more than
    ///   `closed_for` ago with no visible L0 metric file and at least one
    ///   L1 set, every L1 file of it ended and confirmed that long ago; its
    ///   L1 sets, beside L2 ones or not, so a day closes over several
    ///   passes.
    ///
    /// # Errors
    /// [`CatalogError::PartialFileSet`] or [`CatalogError::InvalidFileSet`]
    /// for a set in part or whose files disagree on their accounting, or a
    /// database failure.
    pub async fn metric_inputs(
        &self,
        pass: MetricPass,
        closed_for: std::time::Duration,
        skip: i64,
    ) -> Result<Option<MetricInputs>, CatalogError> {
        let mut tx = self.pool().begin().await?;
        sqlx::query("set transaction isolation level repeatable read")
            .execute(&mut *tx)
            .await?;
        let unit: Option<(TenantId, DateTime<Utc>)> = sqlx::query_as(&unit_query(pass))
            .bind(Signal::MetricSamples.code())
            .bind(Signal::MetricSeries.code())
            .bind(FileState::Visible.code())
            .bind(Level::L0.code())
            .bind(Level::L1.code())
            .bind(closed_for.as_secs_f64())
            .bind(skip)
            .fetch_optional(&mut *tx)
            .await?;
        let Some((tenant_id, start)) = unit else {
            tx.rollback().await?;
            return Ok(None);
        };
        let (bin, level) = match pass {
            MetricPass::Merge => (DAY_BIN, Level::L1),
            MetricPass::Compact | MetricPass::Account => (HOUR_BIN, Level::L0),
        };
        let rows = sqlx::query_as::<_, SetRow>(&format!(
            "select {FILE_COLUMNS}, file_set from engine_file
             where tenant_id = $1 and signal in ($2, $3) and level = $4 and state = $5
               and {bin} = $6
             order by created_at, id"
        ))
        .bind(tenant_id)
        .bind(Signal::MetricSamples.code())
        .bind(Signal::MetricSeries.code())
        .bind(level.code())
        .bind(FileState::Visible.code())
        .bind(start)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        let mut sets = sets_of(rows)?;
        if pass == MetricPass::Account {
            sets.retain(|s| !s.samples.usage_accounted);
        }
        Ok(Some(MetricInputs {
            tenant_id,
            start,
            sets,
        }))
    }

    /// In one transaction under `fence`: quarantines both files of every
    /// set of `inputs`, registers each of `outputs` as a set, confirmed,
    /// and inserts `derived`, the contributions the pass made, confirmed.
    ///
    /// # Errors
    /// [`CatalogError::LeaseLost`] without the fence, a
    /// [`CatalogError::CompactionConflict`] when an input file of `tenant`
    /// is no longer visible, [`CatalogError::InvalidFileSet`] for an output
    /// that is not a pair of one hour, or one day at L2, or a database
    /// failure. Nothing changes then.
    pub async fn swap_file_sets_fenced(
        &self,
        tenant: TenantId,
        inputs: &[FileSetId],
        outputs: &[(FileSetId, [NewFile; 2])],
        derived: &[NewFile],
        fence: &LeaseFence,
    ) -> Result<Compaction, CatalogError> {
        for (set, files) in outputs {
            crate::file_set::check_pair(*set, files)?;
        }
        let mut tx = self.pool().begin().await?;
        if !crate::lease::fence_holds(&mut tx, fence).await? {
            tx.rollback().await?;
            return Err(lease_lost(fence));
        }
        let quarantined: Vec<(crate::FileId,)> = sqlx::query_as(
            "update engine_file set state = $3, quarantined_at = now()
             where tenant_id = $1 and file_set = any($2) and state = $4
             returning id",
        )
        .bind(tenant)
        .bind(inputs)
        .bind(FileState::Quarantined.code())
        .bind(FileState::Visible.code())
        .fetch_all(&mut *tx)
        .await?;
        let expected = inputs.len() * 2;
        if quarantined.len() != expected {
            tx.rollback().await?;
            return Err(CatalogError::CompactionConflict {
                expected,
                found: quarantined.len(),
            });
        }
        let mut published = Vec::with_capacity(outputs.len() * 2 + derived.len());
        for (set, files) in outputs {
            for file in files {
                published.push(
                    insert_file(&mut tx, file, Confirm::OnInsert, Some(*set))
                        .await?
                        .id,
                );
            }
        }
        for file in derived {
            published.push(
                insert_file(&mut tx, file, Confirm::OnInsert, None)
                    .await?
                    .id,
            );
        }
        let inputs: Vec<crate::FileId> = quarantined.into_iter().map(|(id,)| id).collect();
        let id = Uuid::now_v7();
        sqlx::query(
            "insert into engine_compaction (id, tenant_id, signal, inputs, outputs) values ($1, $2, $3, $4, $5)",
        )
        .bind(id)
        .bind(tenant)
        .bind(Signal::MetricSamples.code())
        .bind(&inputs)
        .bind(&published)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Compaction {
            id,
            inputs,
            outputs: published,
        })
    }

    /// In one transaction under `fence`: marks both files of every set of
    /// `sets` accounted, their usage and metadata counted, and inserts
    /// `derived`, the contributions that counted them, confirmed. The
    /// files stay where they are.
    ///
    /// # Errors
    /// [`CatalogError::LeaseLost`] without the fence, a
    /// [`CatalogError::CompactionConflict`] when a file of a set is no
    /// longer visible or already accounted, or a database failure. Nothing
    /// changes then.
    pub async fn account_file_sets_fenced(
        &self,
        tenant: TenantId,
        sets: &[FileSetId],
        derived: &[NewFile],
        fence: &LeaseFence,
    ) -> Result<(), CatalogError> {
        let mut tx = self.pool().begin().await?;
        if !crate::lease::fence_holds(&mut tx, fence).await? {
            tx.rollback().await?;
            return Err(lease_lost(fence));
        }
        let marked = sqlx::query(
            "update engine_file set usage_accounted = true
             where tenant_id = $1 and file_set = any($2) and state = $3 and not usage_accounted",
        )
        .bind(tenant)
        .bind(sets)
        .bind(FileState::Visible.code())
        .execute(&mut *tx)
        .await?
        .rows_affected();
        let expected = sets.len() * 2;
        if marked != expected as u64 {
            tx.rollback().await?;
            return Err(CatalogError::CompactionConflict {
                expected,
                found: usize::try_from(marked).unwrap_or(usize::MAX),
            });
        }
        for file in derived {
            insert_file(&mut tx, file, Confirm::OnInsert, None).await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

fn lease_lost(fence: &LeaseFence) -> CatalogError {
    CatalogError::LeaseLost {
        name: fence.name.clone(),
        holder: fence.holder.clone(),
        generation: fence.generation,
    }
}

/// The query of the next unit of `pass`: `$1`, `$2` the metric signals,
/// `$3` visible, `$4` L0, `$5` L1, `$6` the close delay, `$7` the skip.
fn unit_query(pass: MetricPass) -> String {
    let cold = |level: &str| {
        format!(
            "count(*) filter (where level = {level}) >= 1
                and max(ts_max) filter (where level = {level}) < now() - make_interval(secs => $6)
                and count(confirmed_at) filter (where level = {level})
                    = count(*) filter (where level = {level})
                and max(confirmed_at) filter (where level = {level})
                    < now() - make_interval(secs => $6)"
        )
    };
    match pass {
        MetricPass::Compact | MetricPass::Account => {
            let open = if pass == MetricPass::Account {
                "and bool_or(not usage_accounted)"
            } else {
                ""
            };
            format!(
                "select tenant_id, {HOUR_BIN} as start from engine_file
                 where signal in ($1, $2) and state = $3 and level = $4
                 group by tenant_id, start
                 having {} {open}
                 order by start, tenant_id
                 offset $7 limit 1",
                cold("$4")
            )
        }
        MetricPass::Merge => format!(
            "select tenant_id, {DAY_BIN} as start from engine_file
             where signal in ($1, $2) and state = $3
             group by tenant_id, start
             having {DAY_BIN} + interval '1 day' < now() - make_interval(secs => $6)
                and count(*) filter (where level = $4) = 0
                and {}
             order by start, tenant_id
             offset $7 limit 1",
            cold("$5")
        ),
    }
}

/// A file row with its set.
#[derive(sqlx::FromRow)]
struct SetRow {
    #[sqlx(flatten)]
    file: FileRow,
    file_set: Option<Uuid>,
}

/// The sets `rows` make, in the order of their first file; every set
/// whole and agreeing on its accounting.
fn sets_of(rows: Vec<SetRow>) -> Result<Vec<MetricSet>, CatalogError> {
    let mut order: Vec<FileSetId> = Vec::new();
    let mut files: BTreeMap<Uuid, (Option<FileRecord>, Option<FileRecord>)> = BTreeMap::new();
    for row in rows {
        let set = row.file_set.ok_or(CatalogError::InvalidValue {
            column: "file_set",
            value: 0,
        })?;
        let file = FileRecord::try_from(row.file)?;
        let entry = files.entry(set).or_insert_with(|| {
            order.push(FileSetId(set));
            (None, None)
        });
        let slot = if file.signal == Signal::MetricSamples {
            &mut entry.0
        } else {
            &mut entry.1
        };
        if slot.replace(file).is_some() {
            return Err(CatalogError::InvalidFileSet {
                set,
                reason: "two files of one role",
            });
        }
    }
    order
        .into_iter()
        .map(|set| match files.remove(&set.0) {
            Some((Some(samples), Some(series))) => {
                if samples.usage_accounted != series.usage_accounted {
                    return Err(CatalogError::InvalidFileSet {
                        set: set.0,
                        reason: "files that disagree on their accounting",
                    });
                }
                Ok(MetricSet {
                    set,
                    samples,
                    series,
                })
            }
            _ => Err(CatalogError::PartialFileSet { set: set.0 }),
        })
        .collect()
}

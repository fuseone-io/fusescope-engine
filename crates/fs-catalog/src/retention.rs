//! What retention asks the catalog: the files beyond a tenant's
//! `retention_days`, quarantined under the fence, and the derived table
//! left listing sightings from days that are gone.
//!
//! The cut is a UTC day boundary on the database clock: the start of the
//! current day minus the tenant's `retention_days`. A file is beyond it
//! when every row of it ended before, so whole hour partitions of whole
//! days go and a file at the boundary stays.

use chrono::{DateTime, Utc};

use crate::lease::{LeaseFence, fence_holds};
use crate::types::{FileRecord, FileRow, FileState, Level, Signal, TenantId};
use crate::{Catalog, CatalogError};

/// Files of one tenant quarantined by one retention call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpiredFiles {
    /// Owner.
    pub tenant_id: TenantId,
    /// The tenant's cut when the files were quarantined.
    pub cutoff: DateTime<Utc>,
    /// Oldest first, quarantined.
    pub files: Vec<FileRecord>,
}

/// A derived table whose earliest sighting is before its tenant's cut.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaleTable {
    /// Owner.
    pub tenant_id: TenantId,
    /// The visible table.
    pub table: FileRecord,
    /// The tenant's cut when the table was found.
    pub cutoff: DateTime<Utc>,
}

/// The tenant's cut, `t` being `engine_tenant`; UTC whatever the
/// session's time zone.
const CUTOFF: &str = "date_bin('1 day', now(), timestamptz '1970-01-01 00:00:00+00') \
                      - make_interval(days => t.retention_days)";

impl Catalog {
    /// Quarantines, under `fence`, up to `limit` visible files of `signal`
    /// beyond their tenant's cut, oldest first, all of the tenant whose
    /// expired file is the oldest; `None` when no file is beyond its cut.
    /// Calling again continues with what is left, so a caller that
    /// stopped anywhere resumes by calling again. The objects stay for
    /// the purge, as after a swap.
    ///
    /// # Errors
    /// Lease lost, database failure, or a row with values outside the
    /// domain.
    pub async fn expire_files_fenced(
        &self,
        signal: Signal,
        limit: i64,
        fence: &LeaseFence,
    ) -> Result<Option<ExpiredFiles>, CatalogError> {
        let mut tx = self.pool().begin().await?;
        if !fence_holds(&mut tx, fence).await? {
            tx.rollback().await?;
            return Err(CatalogError::LeaseLost {
                name: fence.name.clone(),
                holder: fence.holder.clone(),
                generation: fence.generation,
            });
        }
        let oldest: Option<(TenantId, DateTime<Utc>)> = sqlx::query_as(&format!(
            "select f.tenant_id, {CUTOFF} as cutoff
             from engine_file f join engine_tenant t on t.id = f.tenant_id
             where f.signal = $1 and f.state = $2 and f.ts_max < {CUTOFF}
             order by f.ts_max, f.id
             limit 1"
        ))
        .bind(signal.code())
        .bind(FileState::Visible.code())
        .fetch_optional(&mut *tx)
        .await?;
        let Some((tenant_id, cutoff)) = oldest else {
            tx.rollback().await?;
            return Ok(None);
        };
        let rows = sqlx::query_as::<_, FileRow>(
            "update engine_file set state = $1, quarantined_at = now()
             where id in (
                 select id from engine_file
                 where tenant_id = $2 and signal = $3 and state = $4 and ts_max < $5
                 order by ts_max, id
                 limit $6
             )
             returning id, origin, tenant_id, signal, level, path, size_bytes, row_count, ts_min, ts_max,
                       services, summary, state, created_at, quarantined_at",
        )
        .bind(FileState::Quarantined.code())
        .bind(tenant_id)
        .bind(signal.code())
        .bind(FileState::Visible.code())
        .bind(cutoff)
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        let mut files = rows
            .into_iter()
            .map(FileRecord::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        files.sort_by_key(|f| (f.range.end, f.id));
        Ok(Some(ExpiredFiles {
            tenant_id,
            cutoff,
            files,
        }))
    }

    /// The `skip`-th visible table of `derived` whose earliest sighting is
    /// before its tenant's cut while no visible file of `data` of the
    /// tenant is beyond it: a table to rebuild from the files that remain.
    /// Earliest sighting first.
    ///
    /// # Errors
    /// Database failure, or a row with values outside the domain.
    pub async fn stale_derived_table(
        &self,
        derived: Signal,
        data: Signal,
        skip: i64,
    ) -> Result<Option<StaleTable>, CatalogError> {
        let row: Option<(DateTime<Utc>, FileRow)> = sqlx::query_as::<_, StaleRow>(&format!(
            "select {CUTOFF} as cutoff,
                    f.id, f.origin, f.tenant_id, f.signal, f.level, f.path, f.size_bytes, f.row_count,
                    f.ts_min, f.ts_max, f.services, f.summary, f.state, f.created_at, f.quarantined_at
             from engine_file f join engine_tenant t on t.id = f.tenant_id
             where f.signal = $1 and f.state = $2 and f.ts_min < {CUTOFF}
               and not exists (
                   select 1 from engine_file e
                   where e.tenant_id = f.tenant_id and e.signal = $3 and e.state = $2
                     and e.ts_max < {CUTOFF}
               )
             order by f.ts_min, f.id
             offset $4
             limit 1"
        ))
        .bind(derived.code())
        .bind(FileState::Visible.code())
        .bind(data.code())
        .bind(skip)
        .fetch_optional(self.pool())
        .await?
        .map(|r| (r.cutoff, r.file));
        let Some((cutoff, row)) = row else {
            return Ok(None);
        };
        let table = FileRecord::try_from(row)?;
        Ok(Some(StaleTable {
            tenant_id: table.tenant_id,
            table,
            cutoff,
        }))
    }

    /// Visible files of one tenant, signal and level whose rows end at or
    /// after `since`, oldest first: what remains once retention has cut
    /// there.
    ///
    /// # Errors
    /// Database failure, or a row with values outside the domain.
    pub async fn visible_files_from(
        &self,
        tenant: TenantId,
        signal: Signal,
        level: Level,
        since: DateTime<Utc>,
    ) -> Result<Vec<FileRecord>, CatalogError> {
        let rows = sqlx::query_as::<_, FileRow>(
            "select id, origin, tenant_id, signal, level, path, size_bytes, row_count, ts_min, ts_max,
                    services, summary, state, created_at, quarantined_at
             from engine_file
             where tenant_id = $1 and signal = $2 and level = $3 and state = $4 and ts_max >= $5
             order by ts_min, id",
        )
        .bind(tenant)
        .bind(signal.code())
        .bind(level.code())
        .bind(FileState::Visible.code())
        .bind(since)
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(FileRecord::try_from).collect()
    }
}

#[derive(sqlx::FromRow)]
struct StaleRow {
    cutoff: DateTime<Utc>,
    #[sqlx(flatten)]
    file: FileRow,
}

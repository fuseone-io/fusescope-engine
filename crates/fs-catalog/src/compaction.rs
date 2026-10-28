//! What the compactor asks the catalog: which hour to compact next and
//! which files make it up.

use chrono::{DateTime, Utc};
use sqlx::FromRow;

use crate::types::{FileRecord, FileRow, FileState, Level, Signal, TenantId};
use crate::{Catalog, CatalogError};

/// An hour of one tenant with enough L0 files to compact.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct CompactionCandidate {
    /// Owner.
    pub tenant_id: TenantId,
    /// Start of the hour, by the files' `ts_min`.
    pub hour: DateTime<Utc>,
    /// Visible L0 files in it.
    pub files: i64,
}

impl Catalog {
    /// The oldest hour, across tenants, whose visible L0 files of `signal`
    /// number at least `min_files` and all ended more than `closed_for`
    /// ago on the database clock. `None` when nothing qualifies.
    ///
    /// # Errors
    /// Database failure.
    pub async fn compaction_candidate(
        &self,
        signal: Signal,
        closed_for: std::time::Duration,
        min_files: i64,
    ) -> Result<Option<CompactionCandidate>, CatalogError> {
        let row = sqlx::query_as::<_, CompactionCandidate>(
            "select tenant_id, date_trunc('hour', ts_min) as hour, count(*) as files
             from engine_file
             where signal = $1 and level = $2 and state = $3
               and ts_max < now() - make_interval(secs => $4)
             group by tenant_id, hour
             having count(*) >= $5
             order by hour, tenant_id
             limit 1",
        )
        .bind(signal.code())
        .bind(Level::L0.code())
        .bind(FileState::Visible.code())
        .bind(closed_for.as_secs_f64())
        .bind(min_files)
        .fetch_optional(self.pool())
        .await?;
        Ok(row)
    }

    /// Visible L0 files of `tenant` and `signal` whose `ts_min` falls in
    /// the hour starting at `hour`, oldest registration first.
    ///
    /// # Errors
    /// Database failure.
    pub async fn l0_files_in_hour(
        &self,
        tenant: TenantId,
        signal: Signal,
        hour: DateTime<Utc>,
    ) -> Result<Vec<FileRecord>, CatalogError> {
        let rows = sqlx::query_as::<_, FileRow>(
            "select id, tenant_id, signal, level, path, size_bytes, row_count, ts_min, ts_max,
                    services, summary, state, created_at, quarantined_at
             from engine_file
             where tenant_id = $1 and signal = $2 and level = $3 and state = $4
               and date_trunc('hour', ts_min) = $5
             order by created_at, id",
        )
        .bind(tenant)
        .bind(signal.code())
        .bind(Level::L0.code())
        .bind(FileState::Visible.code())
        .bind(hour)
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(FileRecord::try_from).collect()
    }
}

//! What the compactor asks the catalog: the next hour to compact and the
//! files that make it up, in one statement so both come from one snapshot.

use chrono::{DateTime, Timelike, Utc};

use crate::types::{FileRecord, FileRow, FileState, Level, Signal, TenantId};
use crate::{Catalog, CatalogError};

/// An hour of one tenant with its visible L0 files, all cold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionInputs {
    /// Owner.
    pub tenant_id: TenantId,
    /// Start of the hour, by the files' `ts_min`.
    pub hour: DateTime<Utc>,
    /// Every visible L0 file of the hour, oldest registration first.
    pub files: Vec<FileRecord>,
}

impl Catalog {
    /// The oldest hour, across tenants, whose visible L0 files of `signal`
    /// number at least `min_files` and are all cold: every one ended and
    /// was registered more than `closed_for` ago on the database clock, so
    /// nothing in the hour is still served by a hot window or about to be
    /// joined by late data. Candidate and files come from one statement,
    /// so a file registered meanwhile is neither counted nor returned.
    ///
    /// # Errors
    /// Database failure.
    pub async fn compaction_inputs(
        &self,
        signal: Signal,
        closed_for: std::time::Duration,
        min_files: i64,
    ) -> Result<Option<CompactionInputs>, CatalogError> {
        let rows = sqlx::query_as::<_, FileRow>(
            "with hours as (
                 select tenant_id, date_trunc('hour', ts_min) as hour
                 from engine_file
                 where signal = $1 and level = $2 and state = $3
                 group by tenant_id, hour
                 having count(*) >= $4
                    and max(ts_max) < now() - make_interval(secs => $5)
                    and max(created_at) < now() - make_interval(secs => $5)
                 order by hour, tenant_id
                 limit 1
             )
             select f.id, f.tenant_id, f.signal, f.level, f.path, f.size_bytes, f.row_count,
                    f.ts_min, f.ts_max, f.services, f.summary, f.state, f.created_at,
                    f.quarantined_at
             from engine_file f
             join hours h on f.tenant_id = h.tenant_id and date_trunc('hour', f.ts_min) = h.hour
             where f.signal = $1 and f.level = $2 and f.state = $3
             order by f.created_at, f.id",
        )
        .bind(signal.code())
        .bind(Level::L0.code())
        .bind(FileState::Visible.code())
        .bind(min_files)
        .bind(closed_for.as_secs_f64())
        .fetch_all(self.pool())
        .await?;
        let files: Vec<FileRecord> = rows
            .into_iter()
            .map(FileRecord::try_from)
            .collect::<Result<_, _>>()?;
        let Some(first) = files.first() else {
            return Ok(None);
        };
        let hour = first
            .range
            .start
            .with_minute(0)
            .and_then(|t| t.with_second(0))
            .and_then(|t| t.with_nanosecond(0))
            .unwrap_or(first.range.start);
        Ok(Some(CompactionInputs {
            tenant_id: first.tenant_id,
            hour,
            files,
        }))
    }
}

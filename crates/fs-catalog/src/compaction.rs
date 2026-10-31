//! What the compactor asks the catalog: the next hour to compact and the
//! files that make it up, read in one snapshot.

use chrono::{DateTime, Utc};

use crate::types::{FileRecord, FileRow, FileState, Level, Signal, TenantId};
use crate::{Catalog, CatalogError};

/// An hour of one tenant and signal with its visible L0 files, all cold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionInputs {
    /// Owner.
    pub tenant_id: TenantId,
    /// Signal of every file.
    pub signal: Signal,
    /// Start of the hour in UTC, by the files' `ts_min`.
    pub hour: DateTime<Utc>,
    /// Every visible L0 file of the hour, oldest registration first.
    pub files: Vec<FileRecord>,
}

/// Hours are UTC, whatever the session's time zone.
const HOUR_BIN: &str = "date_bin('1 hour', ts_min, timestamptz '1970-01-01 00:00:00+00')";

impl Catalog {
    /// The `skip`-th oldest hour, across tenants and `signals`, spans
    /// before logs within an hour, whose visible L0 files of one signal
    /// number at least `min_files` and are all cold: every one
    /// ended, and was confirmed by its writer, more than `closed_for` ago on
    /// the database clock, so nothing in the hour is still served by a hot
    /// window or about to be joined by late data. A file not yet confirmed
    /// keeps its hour open. Hour and files are read in one
    /// repeatable-read snapshot, so a file registered meanwhile is neither
    /// counted nor returned. `skip` lets a caller pass over hours it cannot
    /// take on.
    ///
    /// # Errors
    /// Database failure.
    pub async fn compaction_inputs(
        &self,
        signals: &[Signal],
        closed_for: std::time::Duration,
        min_files: i64,
        skip: i64,
    ) -> Result<Option<CompactionInputs>, CatalogError> {
        let mut tx = self.pool().begin().await?;
        sqlx::query("set transaction isolation level repeatable read")
            .execute(&mut *tx)
            .await?;
        let codes: Vec<i16> = signals.iter().map(|s| s.code()).collect();
        let hour: Option<(TenantId, i16, DateTime<Utc>)> = sqlx::query_as(&format!(
            "select tenant_id, signal, {HOUR_BIN} as hour
             from engine_file
             where signal = any($1) and level = $2 and state = $3
             group by tenant_id, signal, hour
             having count(*) >= $4
                and max(ts_max) < now() - make_interval(secs => $5)
                and count(confirmed_at) = count(*)
                and max(confirmed_at) < now() - make_interval(secs => $5)
             order by hour, signal, tenant_id
             offset $6
             limit 1"
        ))
        .bind(&codes)
        .bind(Level::L0.code())
        .bind(FileState::Visible.code())
        .bind(min_files)
        .bind(closed_for.as_secs_f64())
        .bind(skip)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((tenant_id, code, hour)) = hour else {
            tx.rollback().await?;
            return Ok(None);
        };
        let signal = Signal::from_code(code)?;
        let rows = sqlx::query_as::<_, FileRow>(&format!(
            "select id, origin, tenant_id, signal, level, path, size_bytes, row_count, ts_min, ts_max,
                    services, summary, content_sha256, state, created_at, quarantined_at
             from engine_file
             where tenant_id = $1 and signal = $2 and level = $3 and state = $4
               and {HOUR_BIN} = $5
             order by created_at, id"
        ))
        .bind(tenant_id)
        .bind(signal.code())
        .bind(Level::L0.code())
        .bind(FileState::Visible.code())
        .bind(hour)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        let files = rows
            .into_iter()
            .map(FileRecord::try_from)
            .collect::<Result<_, _>>()?;
        Ok(Some(CompactionInputs {
            tenant_id,
            signal,
            hour,
            files,
        }))
    }
}

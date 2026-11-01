//! Usage counted once: the files whose rows are not yet in `usage_1h` and
//! the fenced transaction that counts them.

use chrono::{DateTime, Utc};

use crate::compaction::CompactionInputs;
use crate::lease::fence_holds;
use crate::types::FileRow;
use crate::{
    Catalog, CatalogError, FileId, FileRecord, FileState, LeaseFence, Level, NewFile, Signal,
    TenantId,
};

/// Hours are UTC, whatever the session's time zone.
const HOUR_BIN: &str = "date_bin('1 hour', ts_min, timestamptz '1970-01-01 00:00:00+00')";

/// Uncounted files the accounting pass takes: spans or logs, visible, and
/// not L0 files of a signal in `$1`, which its compaction counts.
const UNCOUNTED: &str = "signal in (1, 2) and state = $2 and not usage_accounted
                         and not (level = $3 and signal = any($1))";

impl Catalog {
    /// The `skip`-th oldest hour, across tenants and signals, of visible
    /// span or log files whose usage is not counted, all cold as
    /// [`Catalog::compaction_inputs`] defines it, leaving out the L0 files
    /// of `compacted`, whose L0 to L1 pass counts them; with its files,
    /// read in the same repeatable-read snapshot.
    ///
    /// # Errors
    /// Database failure.
    pub async fn accounting_inputs(
        &self,
        compacted: &[Signal],
        closed_for: std::time::Duration,
        skip: i64,
    ) -> Result<Option<CompactionInputs>, CatalogError> {
        let mut tx = self.pool().begin().await?;
        sqlx::query("set transaction isolation level repeatable read")
            .execute(&mut *tx)
            .await?;
        let codes: Vec<i16> = compacted.iter().map(|s| s.code()).collect();
        let hour: Option<(TenantId, i16, DateTime<Utc>)> = sqlx::query_as(&format!(
            "select tenant_id, signal, {HOUR_BIN} as hour
             from engine_file
             where {UNCOUNTED}
             group by tenant_id, signal, hour
             having max(ts_max) < now() - make_interval(secs => $4)
                and count(confirmed_at) = count(*)
                and max(confirmed_at) < now() - make_interval(secs => $4)
             order by hour, signal, tenant_id
             offset $5
             limit 1"
        ))
        .bind(&codes)
        .bind(FileState::Visible.code())
        .bind(Level::L0.code())
        .bind(closed_for.as_secs_f64())
        .bind(skip)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((tenant_id, code, hour)) = hour else {
            tx.rollback().await?;
            return Ok(None);
        };
        let rows = sqlx::query_as::<_, FileRow>(&format!(
            "select id, origin, tenant_id, signal, level, path, size_bytes, row_count, ts_min, ts_max,
                    services, summary, content_sha256, state, created_at, quarantined_at, usage_accounted
             from engine_file
             where {UNCOUNTED} and tenant_id = $4 and signal = $5 and {HOUR_BIN} = $6
             order by created_at, id"
        ))
        .bind(&codes)
        .bind(FileState::Visible.code())
        .bind(Level::L0.code())
        .bind(tenant_id)
        .bind(code)
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
            signal: Signal::from_code(code)?,
            hour,
            files,
        }))
    }

    /// Marks `counted`, visible files of `tenant` and `signal` not counted
    /// yet, as counted and registers `outputs`, their usage contributions,
    /// in one transaction under `fence`. A file already counted, gone or
    /// of another tenant or signal changes nothing and is a
    /// [`CatalogError::CompactionConflict`].
    ///
    /// # Errors
    /// Lease lost, conflict, or database failure.
    pub async fn account_files_fenced(
        &self,
        tenant: TenantId,
        signal: Signal,
        counted: &[FileId],
        outputs: &[NewFile],
        fence: &LeaseFence,
    ) -> Result<(), CatalogError> {
        let mut tx = self.pool().begin().await?;
        if !fence_holds(&mut tx, fence).await? {
            tx.rollback().await?;
            return Err(CatalogError::LeaseLost {
                name: fence.name.clone(),
                holder: fence.holder.clone(),
                generation: fence.generation,
            });
        }
        let marked = sqlx::query(
            "update engine_file set usage_accounted = true
             where id = any($1) and tenant_id = $2 and signal = $3 and state = $4
               and not usage_accounted",
        )
        .bind(counted)
        .bind(tenant)
        .bind(signal.code())
        .bind(FileState::Visible.code())
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if usize::try_from(marked).ok() != Some(counted.len()) {
            tx.rollback().await?;
            return Err(CatalogError::CompactionConflict {
                expected: counted.len(),
                found: usize::try_from(marked).unwrap_or(usize::MAX),
            });
        }
        for file in outputs {
            crate::catalog::insert_counted(&mut tx, file).await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

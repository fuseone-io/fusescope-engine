//! The two files of a metric flush, registered and confirmed as one.

use chrono::{DateTime, Duration, Timelike, Utc};

use crate::CatalogError;
use crate::catalog::{Catalog, Confirm, insert_file};
use crate::types::{FileId, FileRecord, FileSetId, MetricRole, NewFile};

impl Catalog {
    /// Registers `files`, the samples then the series file of one tenant
    /// and hour, as set `set`, both or neither. Idempotent: a set already
    /// registered with the same content returns its rows untouched.
    ///
    /// # Errors
    /// [`CatalogError::InvalidFileSet`] for files that are not such a pair,
    /// [`CatalogError::PartialFileSet`] when only one of them is
    /// registered, [`CatalogError::FileMismatch`] when either is registered
    /// with other content or under another set, an object doomed by the
    /// orphan sweep, or a database failure.
    pub async fn register_file_set(
        &self,
        set: FileSetId,
        files: &[NewFile; 2],
    ) -> Result<[FileRecord; 2], CatalogError> {
        check_pair(set, files)?;
        let ids: Vec<FileId> = files.iter().map(|f| f.id).collect();
        let mut tx = self.pool().begin().await?;
        let existing: Vec<(FileId, Option<FileSetId>)> =
            sqlx::query_as("select id, file_set from engine_file where id = any($1)")
                .bind(&ids)
                .fetch_all(&mut *tx)
                .await?;
        if existing.len() == 1 {
            tx.rollback().await?;
            return Err(CatalogError::PartialFileSet { set: set.0 });
        }
        if let Some((id, _)) = existing.iter().find(|(_, s)| *s != Some(set)) {
            tx.rollback().await?;
            return Err(CatalogError::FileMismatch {
                id: id.0,
                field: "file_set",
            });
        }
        let [samples, series] = files;
        let samples = insert_file(&mut tx, samples, Confirm::ByWriter, Some(set)).await?;
        let series = insert_file(&mut tx, series, Confirm::ByWriter, Some(set)).await?;
        tx.commit().await?;
        Ok([samples, series])
    }

    /// Makes both files of `set` eligible for compaction, stamped on the
    /// database clock now, as [`Catalog::confirm_file`] does one file.
    ///
    /// # Errors
    /// Database failure.
    pub async fn confirm_file_set(&self, set: FileSetId) -> Result<(), CatalogError> {
        sqlx::query(
            "update engine_file set confirmed_at = clock_timestamp()
             where file_set = $1 and confirmed_at is null",
        )
        .bind(set)
        .execute(self.pool())
        .await?;
        Ok(())
    }
}

/// `files` are a samples then a series file of one tenant, both within one
/// hour, under distinct ids.
fn check_pair(set: FileSetId, files: &[NewFile; 2]) -> Result<(), CatalogError> {
    let invalid = |reason| Err(CatalogError::InvalidFileSet { set: set.0, reason });
    let [samples, series] = files;
    if samples.signal != MetricRole::Samples.signal()
        || series.signal != MetricRole::Series.signal()
    {
        return invalid("not a samples then a series file");
    }
    if samples.tenant_id != series.tenant_id {
        return invalid("files of two tenants");
    }
    if samples.id == series.id {
        return invalid("one id for both files");
    }
    let hour = hour_of(samples.range.start);
    let end = hour + Duration::hours(1);
    let within =
        |f: &NewFile| f.range.start >= hour && f.range.end < end && f.range.start <= f.range.end;
    if !within(samples) || !within(series) {
        return invalid("files beyond one hour");
    }
    Ok(())
}

fn hour_of(at: DateTime<Utc>) -> DateTime<Utc> {
    at.with_minute(0)
        .and_then(|t| t.with_second(0))
        .and_then(|t| t.with_nanosecond(0))
        .unwrap_or(at)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use uuid::Uuid;

    use super::*;
    use crate::types::TenantId;

    #[test]
    fn the_first_intent_names_its_set_and_files_the_same_every_time() {
        let instance = Uuid::from_bytes([1; 16]);
        let tenant = TenantId(Uuid::from_bytes([2; 16]));
        let hour = Utc.with_ymd_and_hms(2026, 10, 30, 10, 0, 0).unwrap();
        let at = (7, 4096, hour);
        let set = FileSetId::derived(instance, tenant, 7, 4096, hour);
        let samples = FileId::derived_metric(instance, tenant, at, MetricRole::Samples);
        let series = FileId::derived_metric(instance, tenant, at, MetricRole::Series);
        assert_eq!(set.0.to_string(), PINNED[0]);
        assert_eq!(samples.0.to_string(), PINNED[1]);
        assert_eq!(series.0.to_string(), PINNED[2]);
        let later = (7, 4096, hour + Duration::hours(1));
        assert_ne!(
            FileId::derived_metric(instance, tenant, later, MetricRole::Samples),
            samples
        );
    }

    const PINNED: [&str; 3] = [
        "66827d74-db72-5a2b-b54d-37ee02a13643",
        "cb28f0de-84a2-5263-8e87-64c6ea950c17",
        "231c85de-b9a3-56c3-955d-d21a75d7d171",
    ];
}

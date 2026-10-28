//! Objects in the bucket without a file row, remembered between sweeps so
//! one is deleted only after it has been an orphan for the whole TTL, and
//! marked before deletion so a registration cannot race the delete.
//!
//! The protocol is a row per path in `engine_orphan`. The sweep records a
//! sighting, and once the sighting is older than the TTL it marks the row
//! `deleting` with an update that is refused when a file row for the path,
//! or for any file the path could belong to, exists; the update takes the
//! row lock. A registration locks the orphan rows of the paths it writes
//! and is refused when one is marked. The two serialise on the lock: the
//! one that commits first decides.

use std::collections::HashSet;

use crate::{Catalog, CatalogError};

/// Paths per membership query.
const CHUNK: usize = 1000;

impl Catalog {
    /// Which of `paths` are the path of a file row, in any state.
    ///
    /// # Errors
    /// Database failure.
    pub async fn registered_paths(
        &self,
        paths: &[String],
    ) -> Result<HashSet<String>, CatalogError> {
        let mut found = HashSet::new();
        for chunk in paths.chunks(CHUNK) {
            let rows: Vec<(String,)> =
                sqlx::query_as("select path from engine_file where path = any($1)")
                    .bind(chunk)
                    .fetch_all(self.pool())
                    .await?;
            found.extend(rows.into_iter().map(|(p,)| p));
        }
        Ok(found)
    }

    /// Remembers `paths` as seen without a row; a path already remembered
    /// keeps its first sighting.
    ///
    /// # Errors
    /// Database failure.
    pub async fn orphans_seen(&self, paths: &[String]) -> Result<(), CatalogError> {
        for chunk in paths.chunks(CHUNK) {
            sqlx::query(
                "insert into engine_orphan (path) select unnest($1::text[])
                 on conflict (path) do nothing",
            )
            .bind(chunk)
            .execute(self.pool())
            .await?;
        }
        Ok(())
    }

    /// Up to `limit` remembered orphans, not yet marked, first seen more
    /// than `ttl` ago on the database clock, oldest sighting first.
    ///
    /// # Errors
    /// Database failure.
    pub async fn orphans_due(
        &self,
        ttl: std::time::Duration,
        limit: i64,
    ) -> Result<Vec<String>, CatalogError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "select path from engine_orphan
             where not deleting and first_seen < now() - make_interval(secs => $1)
             order by first_seen, path
             limit $2",
        )
        .bind(ttl.as_secs_f64())
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        Ok(rows.into_iter().map(|(p,)| p).collect())
    }

    /// Marks `path` for deletion unless a file row exists for any of
    /// `owners`, the paths a file owning the object would have. True when
    /// marked: the object may be deleted and no registration of it will
    /// commit; false when a row exists, or the path is not remembered.
    ///
    /// The orphan row is locked first, then the file rows are read by a
    /// statement of its own, so a registration that held the lock has
    /// committed and its row is seen; a `where not exists` on the update
    /// alone would keep the snapshot from before the wait.
    ///
    /// # Errors
    /// Database failure.
    pub async fn orphan_doom(&self, path: &str, owners: &[String]) -> Result<bool, CatalogError> {
        let mut tx = self.pool().begin().await?;
        let remembered: Option<(String,)> = sqlx::query_as(
            "select path from engine_orphan where path = $1 and not deleting for update",
        )
        .bind(path)
        .fetch_optional(&mut *tx)
        .await?;
        if remembered.is_none() {
            tx.rollback().await?;
            return Ok(false);
        }
        let owned: Option<(String,)> =
            sqlx::query_as("select path from engine_file where path = any($1) limit 1")
                .bind(owners)
                .fetch_optional(&mut *tx)
                .await?;
        if owned.is_some() {
            tx.rollback().await?;
            return Ok(false);
        }
        sqlx::query("update engine_orphan set deleting = true where path = $1")
            .bind(path)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Up to `limit` paths marked for deletion: a sweep that stopped
    /// between marking and deleting finishes them first.
    ///
    /// # Errors
    /// Database failure.
    pub async fn orphans_doomed(&self, limit: i64) -> Result<Vec<String>, CatalogError> {
        let rows: Vec<(String,)> =
            sqlx::query_as("select path from engine_orphan where deleting order by path limit $1")
                .bind(limit)
                .fetch_all(self.pool())
                .await?;
        Ok(rows.into_iter().map(|(p,)| p).collect())
    }

    /// Forgets `paths`: deleted, or seen with a row after all.
    ///
    /// # Errors
    /// Database failure.
    pub async fn orphans_forget(&self, paths: &[String]) -> Result<(), CatalogError> {
        for chunk in paths.chunks(CHUNK) {
            sqlx::query("delete from engine_orphan where path = any($1)")
                .bind(chunk)
                .execute(self.pool())
                .await?;
        }
        Ok(())
    }
}

/// Refuses, inside `tx`, to register a file whose path or sidecar path is
/// marked for deletion, and locks those orphan rows until `tx` ends so
/// the sweep cannot mark them meanwhile. One statement: a row locked by
/// the sweep is read as the sweep left it once the wait is over.
pub(crate) async fn refuse_doomed(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    path: &str,
) -> Result<(), CatalogError> {
    let mut paths = vec![path.to_owned()];
    paths.extend(fs_index::sidecar_path(path));
    let rows: Vec<(String, bool)> =
        sqlx::query_as("select path, deleting from engine_orphan where path = any($1) for update")
            .bind(&paths)
            .fetch_all(&mut **tx)
            .await?;
    if let Some((doomed, _)) = rows.into_iter().find(|(_, deleting)| *deleting) {
        return Err(CatalogError::ObjectDoomed { path: doomed });
    }
    Ok(())
}

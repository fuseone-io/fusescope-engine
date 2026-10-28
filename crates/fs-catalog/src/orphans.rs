//! Objects in the bucket without a file row, remembered between sweeps so
//! one is deleted only after it has been an orphan for the whole TTL, and
//! marked before deletion under a lock every registration takes too.
//!
//! The protocol is a row per path in `engine_orphan` and a transaction
//! advisory lock per path. The sweep records a sighting, and once the
//! sighting is older than the TTL it marks the row `deleting` in a
//! transaction that takes the path's lock first and reads the file rows
//! after, so a registration that held the lock has committed and its row
//! is seen. A registration takes the locks of the path and its sidecar in
//! its own transaction and is refused when either row is marked. The lock
//! is not the row's, so it serialises the two even before any orphan row
//! exists. A mark is never undone by another sweeper: one that finds the
//! row already marked leaves it for whoever marked it, or deletes it
//! itself, both of which end the same way.

use std::collections::HashSet;

use crate::{Catalog, CatalogError};

/// Paths per membership query.
const CHUNK: usize = 1000;

/// What marking a path for deletion found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Doom {
    /// Marked now: the object may be deleted, no registration will commit.
    Marked,
    /// Marked by an earlier pass or another sweeper; still to be deleted.
    AlreadyMarked,
    /// A file row owns the object: not an orphan, forget it.
    Owned,
    /// The path is not remembered.
    Missing,
}

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
    /// `owners`, the paths a file owning the object would have.
    ///
    /// The path's lock is taken first, then the file rows are read by a
    /// statement of its own, so a registration that held the lock has
    /// committed and its row is seen; a `where not exists` on the update
    /// alone would keep the snapshot from before the wait.
    ///
    /// # Errors
    /// Database failure.
    pub async fn orphan_doom(&self, path: &str, owners: &[String]) -> Result<Doom, CatalogError> {
        let mut tx = self.pool().begin().await?;
        lock_path(&mut tx, path).await?;
        let remembered: Option<(bool,)> =
            sqlx::query_as("select deleting from engine_orphan where path = $1 for update")
                .bind(path)
                .fetch_optional(&mut *tx)
                .await?;
        let outcome = match remembered {
            None => Doom::Missing,
            Some((true,)) => Doom::AlreadyMarked,
            Some((false,)) => {
                let owned: Option<(String,)> =
                    sqlx::query_as("select path from engine_file where path = any($1) limit 1")
                        .bind(owners)
                        .fetch_optional(&mut *tx)
                        .await?;
                if owned.is_some() {
                    Doom::Owned
                } else {
                    sqlx::query("update engine_orphan set deleting = true where path = $1")
                        .bind(path)
                        .execute(&mut *tx)
                        .await?;
                    Doom::Marked
                }
            }
        };
        tx.commit().await?;
        Ok(outcome)
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

/// Takes the transaction advisory lock of `path`, held until `tx` ends.
async fn lock_path(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    path: &str,
) -> Result<(), CatalogError> {
    sqlx::query("select pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(path)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Refuses, inside `tx`, to register a file whose path or sidecar path is
/// marked for deletion. Takes the locks of both paths, in that order, and
/// holds them until `tx` ends, so the sweep cannot mark either meanwhile.
pub(crate) async fn refuse_doomed(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    path: &str,
) -> Result<(), CatalogError> {
    let mut paths = vec![path.to_owned()];
    paths.extend(fs_index::sidecar_path(path));
    for p in &paths {
        lock_path(tx, p).await?;
    }
    let doomed: Option<(String,)> =
        sqlx::query_as("select path from engine_orphan where path = any($1) and deleting limit 1")
            .bind(&paths)
            .fetch_optional(&mut **tx)
            .await?;
    if let Some((doomed,)) = doomed {
        return Err(CatalogError::ObjectDoomed { path: doomed });
    }
    Ok(())
}

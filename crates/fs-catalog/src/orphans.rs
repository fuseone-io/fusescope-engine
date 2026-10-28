//! Objects in the bucket without a file row, remembered between sweeps so
//! one is deleted only after it has been an orphan for the whole TTL.

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

    /// Remembered orphans first seen more than `ttl` ago on the database
    /// clock.
    ///
    /// # Errors
    /// Database failure.
    pub async fn orphans_due(&self, ttl: std::time::Duration) -> Result<Vec<String>, CatalogError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "select path from engine_orphan
             where first_seen < now() - make_interval(secs => $1)
             order by first_seen, path",
        )
        .bind(ttl.as_secs_f64())
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

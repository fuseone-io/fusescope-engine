//! Named leases: one holder at a time, until a TTL runs out.
//!
//! A lease is taken, renewed and released by single statements evaluated
//! on the database clock, so instances with skewed clocks agree on who
//! holds it. The holder renews within the TTL; one that stops renewing
//! loses the lease to the next taker once it expires.

use chrono::{DateTime, Utc};
use sqlx::FromRow;

use crate::{Catalog, CatalogError};

/// The lease the active compactor holds.
pub const COMPACTOR_LEASE: &str = "compactor";

/// A lease row.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct Lease {
    /// Name.
    pub name: String,
    /// Who holds it.
    pub holder: String,
    /// When the current holder first took it.
    pub acquired_at: DateTime<Utc>,
    /// When it lapses unless renewed.
    pub expires_at: DateTime<Utc>,
}

impl Catalog {
    /// Takes `name` for `holder` for `ttl` from now, or renews it when
    /// `holder` already has it. True when `holder` holds the lease after
    /// the call; false when someone else holds it unexpired.
    ///
    /// # Errors
    /// Database failure.
    pub async fn acquire_lease(
        &self,
        name: &str,
        holder: &str,
        ttl: std::time::Duration,
    ) -> Result<bool, CatalogError> {
        let taken = sqlx::query(
            "insert into engine_lease (name, holder, acquired_at, expires_at)
             values ($1, $2, now(), now() + make_interval(secs => $3))
             on conflict (name) do update set
                 holder = excluded.holder,
                 expires_at = excluded.expires_at,
                 acquired_at = case
                     when engine_lease.holder = excluded.holder then engine_lease.acquired_at
                     else now()
                 end
             where engine_lease.holder = excluded.holder or engine_lease.expires_at < now()",
        )
        .bind(name)
        .bind(holder)
        .bind(ttl.as_secs_f64())
        .execute(self.pool())
        .await?
        .rows_affected();
        Ok(taken == 1)
    }

    /// Gives `name` up when `holder` has it. True when a lease was released.
    ///
    /// # Errors
    /// Database failure.
    pub async fn release_lease(&self, name: &str, holder: &str) -> Result<bool, CatalogError> {
        let released = sqlx::query("delete from engine_lease where name = $1 and holder = $2")
            .bind(name)
            .bind(holder)
            .execute(self.pool())
            .await?
            .rows_affected();
        Ok(released == 1)
    }

    /// The lease row, expired or not.
    ///
    /// # Errors
    /// Database failure.
    pub async fn lease(&self, name: &str) -> Result<Option<Lease>, CatalogError> {
        let row = sqlx::query_as::<_, Lease>(
            "select name, holder, acquired_at, expires_at from engine_lease where name = $1",
        )
        .bind(name)
        .fetch_optional(self.pool())
        .await?;
        Ok(row)
    }
}

//! Named leases: one holder at a time, until a TTL runs out.
//!
//! A lease is taken, renewed and released by single statements evaluated
//! on the database clock, so instances with skewed clocks agree on who
//! holds it. The holder renews within the TTL; one that stops renewing
//! loses the lease to the next taker once it expires. Every change of
//! hands bumps the generation, which a holder quotes as a fence on the
//! writes it makes under the lease.

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
    /// Grows by one each time the lease changes hands.
    pub generation: i64,
}

/// What a holder quotes to prove it still holds a lease: the fence a
/// write under the lease is checked against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseFence {
    /// Lease name.
    pub name: String,
    /// Holder.
    pub holder: String,
    /// Generation the holder took the lease at.
    pub generation: i64,
}

impl Catalog {
    /// Takes `name` for `holder` for `ttl` from now, or renews it when
    /// `holder` already has it. The generation when `holder` holds the
    /// lease after the call; `None` when someone else holds it unexpired.
    ///
    /// # Errors
    /// Database failure.
    pub async fn acquire_lease(
        &self,
        name: &str,
        holder: &str,
        ttl: std::time::Duration,
    ) -> Result<Option<i64>, CatalogError> {
        let row: Option<(i64,)> = sqlx::query_as(
            "insert into engine_lease (name, holder, acquired_at, expires_at, generation)
             values ($1, $2, now(), now() + make_interval(secs => $3), 1)
             on conflict (name) do update set
                 holder = excluded.holder,
                 expires_at = excluded.expires_at,
                 acquired_at = case
                     when engine_lease.holder = excluded.holder then engine_lease.acquired_at
                     else now()
                 end,
                 generation = case
                     when engine_lease.holder = excluded.holder then engine_lease.generation
                     else engine_lease.generation + 1
                 end
             where engine_lease.holder = excluded.holder or engine_lease.expires_at <= now()
             returning generation",
        )
        .bind(name)
        .bind(holder)
        .bind(ttl.as_secs_f64())
        .fetch_optional(self.pool())
        .await?;
        Ok(row.map(|(generation,)| generation))
    }

    /// Gives `name` up when `holder` has it, by letting it expire now so
    /// the generation keeps counting. True when a lease was released.
    ///
    /// # Errors
    /// Database failure.
    pub async fn release_lease(&self, name: &str, holder: &str) -> Result<bool, CatalogError> {
        let released = sqlx::query(
            "update engine_lease set expires_at = now()
             where name = $1 and holder = $2 and expires_at > now()",
        )
        .bind(name)
        .bind(holder)
        .execute(self.pool())
        .await?
        .rows_affected();
        Ok(released == 1)
    }

    /// The lease row, expired or not; a released lease shows as expired.
    ///
    /// # Errors
    /// Database failure.
    pub async fn lease(&self, name: &str) -> Result<Option<Lease>, CatalogError> {
        let row = sqlx::query_as::<_, Lease>(
            "select name, holder, acquired_at, expires_at, generation from engine_lease where name = $1",
        )
        .bind(name)
        .fetch_optional(self.pool())
        .await?;
        Ok(row)
    }
}

/// True inside `tx` when `fence` still names the unexpired holder and
/// generation of its lease.
pub(crate) async fn fence_holds(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    fence: &LeaseFence,
) -> Result<bool, CatalogError> {
    let row: Option<(i64,)> = sqlx::query_as(
        "select generation from engine_lease
         where name = $1 and holder = $2 and generation = $3 and expires_at > now()",
    )
    .bind(&fence.name)
    .bind(&fence.holder)
    .bind(fence.generation)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(row.is_some())
}

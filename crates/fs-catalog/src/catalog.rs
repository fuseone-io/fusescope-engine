//! Catalog operations.

use chrono::{DateTime, Duration, Utc};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::CatalogError;
use crate::token::{TenantToken, TokenScope, generate_token, hash_token, looks_like_token};
use crate::types::{
    Compaction, FileId, FileRecord, FileRow, FileState, Ingester, NewFile, NewTenant, PurgedFile,
    Signal, Tenant, TenantId, TimeRange,
};
use crate::watch::Watcher;

/// How long to wait for a pooled connection before failing.
pub const ACQUIRE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Handle to the catalog database. Cheap to clone.
#[derive(Clone)]
pub struct Catalog {
    pool: PgPool,
}

impl Catalog {
    /// Connects with a pool of at most `max_connections`.
    ///
    /// # Errors
    /// Connection failure.
    pub async fn connect(url: &str, max_connections: u32) -> Result<Self, CatalogError> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(ACQUIRE_TIMEOUT)
            .connect(url)
            .await?;
        Ok(Self { pool })
    }

    /// Wraps an existing pool.
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// The underlying pool, for callers that share the connection.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Applies pending migrations. Safe to call on every start.
    ///
    /// # Errors
    /// Migration failure.
    pub async fn migrate(&self) -> Result<(), CatalogError> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    /// Creates a tenant.
    ///
    /// # Errors
    /// Duplicate name, or database failure.
    pub async fn create_tenant(&self, tenant: &NewTenant) -> Result<Tenant, CatalogError> {
        let row = sqlx::query_as::<_, Tenant>(
            "insert into engine_tenant (id, name, retention_days, max_keys_hour, bytes_budget, qps_limit)
             values ($1, $2, $3, $4, $5, $6)
             returning id, name, retention_days, max_keys_hour, bytes_budget, qps_limit, created_at, deleted_at",
        )
        .bind(TenantId::new())
        .bind(&tenant.name)
        .bind(tenant.retention_days)
        .bind(tenant.max_keys_hour)
        .bind(tenant.bytes_budget)
        .bind(tenant.qps_limit)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    /// Looks up a tenant.
    ///
    /// # Errors
    /// Database failure.
    pub async fn tenant(&self, id: TenantId) -> Result<Option<Tenant>, CatalogError> {
        let row = sqlx::query_as::<_, Tenant>(
            "select id, name, retention_days, max_keys_hour, bytes_budget, qps_limit, created_at, deleted_at
             from engine_tenant where id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// Registers a file as visible. Called by the ingester after the object is
    /// fully written to the bucket.
    ///
    /// Idempotent on `file.id`: registering the same id again returns the
    /// existing row untouched, so a flush retried after a crash between upload
    /// and registration does not create a second file.
    ///
    /// # Errors
    /// Duplicate path under a different id, unknown tenant, or database failure.
    pub async fn register_file(&self, file: &NewFile) -> Result<FileRecord, CatalogError> {
        let mut tx = self.pool.begin().await?;
        let record = insert_file(&mut tx, file).await?;
        tx.commit().await?;
        Ok(record)
    }

    /// Visible files of one tenant and signal whose time span overlaps `range`,
    /// oldest first.
    ///
    /// # Errors
    /// Database failure, or a row with values outside the domain.
    pub async fn visible_files(
        &self,
        tenant: TenantId,
        signal: Signal,
        range: TimeRange,
    ) -> Result<Vec<FileRecord>, CatalogError> {
        let rows = sqlx::query_as::<_, FileRow>(
            "select id, tenant_id, signal, level, path, size_bytes, row_count, ts_min, ts_max,
                    services, summary, state, created_at, quarantined_at
             from engine_file
             where tenant_id = $1 and signal = $2 and state = $3 and ts_min <= $5 and ts_max >= $4
             order by ts_min, id",
        )
        .bind(tenant)
        .bind(signal.code())
        .bind(FileState::Visible.code())
        .bind(range.start)
        .bind(range.end)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(FileRecord::try_from).collect()
    }

    /// Every visible file of one tenant and signal, oldest first. Used to warm
    /// querier caches; `visible_files` serves bounded lookups.
    ///
    /// # Errors
    /// Database failure, or a row with values outside the domain.
    pub async fn all_visible_files(
        &self,
        tenant: TenantId,
        signal: Signal,
    ) -> Result<Vec<FileRecord>, CatalogError> {
        let rows = sqlx::query_as::<_, FileRow>(
            "select id, tenant_id, signal, level, path, size_bytes, row_count, ts_min, ts_max,
                    services, summary, state, created_at, quarantined_at
             from engine_file
             where tenant_id = $1 and signal = $2 and state = $3
             order by ts_min, id",
        )
        .bind(tenant)
        .bind(signal.code())
        .bind(FileState::Visible.code())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(FileRecord::try_from).collect()
    }

    /// Latest `ts_max` among visible files of a tenant and signal: how far the
    /// bucket covers. The querier asks ingesters only for data after this.
    ///
    /// # Errors
    /// Database failure.
    pub async fn covered_until(
        &self,
        tenant: TenantId,
        signal: Signal,
    ) -> Result<Option<DateTime<Utc>>, CatalogError> {
        let row: (Option<DateTime<Utc>>,) = sqlx::query_as(
            "select max(ts_max) from engine_file where tenant_id = $1 and signal = $2 and state = $3",
        )
        .bind(tenant)
        .bind(signal.code())
        .bind(FileState::Visible.code())
        .fetch_one(&self.pool)
        .await?;
        Ok(row.0)
    }

    /// Atomically quarantines `inputs` and makes `outputs` visible.
    ///
    /// Every input must still be visible and belong to `tenant` and `signal`;
    /// otherwise nothing changes and [`CatalogError::CompactionConflict`] is
    /// returned so the caller can discard its outputs.
    ///
    /// # Errors
    /// Conflict as above, or database failure.
    pub async fn swap_files(
        &self,
        tenant: TenantId,
        signal: Signal,
        inputs: &[FileId],
        outputs: &[NewFile],
    ) -> Result<Compaction, CatalogError> {
        self.swap_files_fenced(tenant, signal, inputs, outputs, None)
            .await
    }

    /// [`swap_files`](Self::swap_files) that first checks, in the same
    /// transaction, that `fence` still names the unexpired holder and
    /// generation of its lease; otherwise nothing changes and
    /// [`CatalogError::LeaseLost`] is returned.
    ///
    /// # Errors
    /// Lease lost, conflict, or database failure.
    pub async fn swap_files_fenced(
        &self,
        tenant: TenantId,
        signal: Signal,
        inputs: &[FileId],
        outputs: &[NewFile],
        fence: Option<&crate::LeaseFence>,
    ) -> Result<Compaction, CatalogError> {
        let mut tx = self.pool.begin().await?;
        if let Some(fence) = fence {
            if !crate::lease::fence_holds(&mut tx, fence).await? {
                tx.rollback().await?;
                return Err(CatalogError::LeaseLost {
                    name: fence.name.clone(),
                    holder: fence.holder.clone(),
                    generation: fence.generation,
                });
            }
        }
        let quarantined: Vec<(FileId,)> = sqlx::query_as(
            "update engine_file set state = $4, quarantined_at = now()
             where id = any($1) and tenant_id = $2 and signal = $3 and state = $5
             returning id",
        )
        .bind(inputs)
        .bind(tenant)
        .bind(signal.code())
        .bind(FileState::Quarantined.code())
        .bind(FileState::Visible.code())
        .fetch_all(&mut *tx)
        .await?;
        if quarantined.len() != inputs.len() {
            tx.rollback().await?;
            return Err(CatalogError::CompactionConflict {
                expected: inputs.len(),
                found: quarantined.len(),
            });
        }
        let mut output_ids = Vec::with_capacity(outputs.len());
        for file in outputs {
            output_ids.push(insert_file(&mut tx, file).await?.id);
        }
        let id = Uuid::now_v7();
        sqlx::query(
            "insert into engine_compaction (id, tenant_id, signal, inputs, outputs) values ($1, $2, $3, $4, $5)",
        )
        .bind(id)
        .bind(tenant)
        .bind(signal.code())
        .bind(inputs)
        .bind(&output_ids)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Compaction {
            id,
            inputs: inputs.to_vec(),
            outputs: output_ids,
        })
    }

    /// Removes up to `limit` files quarantined for longer than `older_than` and
    /// returns their paths. The caller deletes the objects afterwards; an
    /// object that outlives its row is an orphan the compactor sweeps later.
    ///
    /// # Errors
    /// Database failure.
    pub async fn purge_quarantined(
        &self,
        older_than: std::time::Duration,
        limit: i64,
    ) -> Result<Vec<PurgedFile>, CatalogError> {
        // The cutoff is the database's clock, as the quarantine time was,
        // so a compactor with a skewed clock cannot purge early.
        let rows = sqlx::query_as::<_, PurgedFile>(
            "delete from engine_file
             where id in (
                 select id from engine_file
                 where state = $1 and quarantined_at <= now() - make_interval(secs => $2)
                 order by quarantined_at
                 limit $3
             )
             returning id, path",
        )
        .bind(FileState::Quarantined.code())
        .bind(older_than.as_secs_f64())
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Records that an ingester is alive at `address`.
    ///
    /// # Errors
    /// Database failure.
    pub async fn heartbeat(&self, ingester: &str, address: &str) -> Result<(), CatalogError> {
        sqlx::query(
            "insert into engine_ingester (id, address, last_heartbeat) values ($1, $2, now())
             on conflict (id) do update set address = excluded.address, last_heartbeat = now()",
        )
        .bind(ingester)
        .bind(address)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Ingesters whose heartbeat is newer than `stale_after`.
    ///
    /// # Errors
    /// Database failure.
    pub async fn live_ingesters(
        &self,
        stale_after: Duration,
    ) -> Result<Vec<Ingester>, CatalogError> {
        let cutoff = Utc::now() - stale_after;
        let rows = sqlx::query_as::<_, Ingester>(
            "select id, address, last_heartbeat from engine_ingester where last_heartbeat > $1 order by id",
        )
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Issues an ingest token for `tenant`. The plaintext is returned once and
    /// never stored.
    ///
    /// # Errors
    /// Unknown tenant, no randomness, or database failure.
    pub async fn create_token(
        &self,
        tenant: TenantId,
        name: &str,
        scope: TokenScope,
    ) -> Result<(String, TenantToken), CatalogError> {
        let token = generate_token()?;
        let hash = hash_token(&token);
        let row = sqlx::query_as::<_, TenantToken>(
            "insert into engine_tenant_token (token_hash, tenant_id, name, scope) values ($1, $2, $3, $4)
             returning tenant_id, name, scope, created_at, revoked_at",
        )
        .bind(hash.as_slice())
        .bind(tenant)
        .bind(name)
        .bind(scope)
        .fetch_one(&self.pool)
        .await?;
        Ok((token, row))
    }

    /// The tenant a token belongs to, if the token exists with `scope`, is not
    /// revoked and the tenant is not deleted.
    ///
    /// # Errors
    /// Database failure.
    pub async fn resolve_token(
        &self,
        token: &str,
        scope: TokenScope,
    ) -> Result<Option<TenantId>, CatalogError> {
        if !looks_like_token(token) {
            return Ok(None);
        }
        let hash = hash_token(token);
        let row: Option<(TenantId,)> = sqlx::query_as(
            "select t.tenant_id from engine_tenant_token t
             join engine_tenant n on n.id = t.tenant_id
             where t.token_hash = $1 and t.scope = $2 and t.revoked_at is null and n.deleted_at is null",
        )
        .bind(hash.as_slice())
        .bind(scope)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.0))
    }

    /// Revokes a token by its hash. Returns whether a live token was revoked.
    ///
    /// # Errors
    /// Database failure.
    pub async fn revoke_token(&self, hash: &[u8; 32]) -> Result<bool, CatalogError> {
        let result = sqlx::query(
            "update engine_tenant_token set revoked_at = now() where token_hash = $1 and revoked_at is null",
        )
        .bind(hash.as_slice())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Tokens of a tenant, newest first, without secrets.
    ///
    /// # Errors
    /// Database failure.
    pub async fn tokens(&self, tenant: TenantId) -> Result<Vec<TenantToken>, CatalogError> {
        let rows = sqlx::query_as::<_, TenantToken>(
            "select tenant_id, name, scope, created_at, revoked_at from engine_tenant_token
             where tenant_id = $1 order by created_at desc",
        )
        .bind(tenant)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Subscribes to file changes. See [`Watcher`].
    ///
    /// # Errors
    /// Connection failure.
    pub async fn watch(&self) -> Result<Watcher, CatalogError> {
        Watcher::connect(&self.pool).await
    }
}

async fn insert_file(
    tx: &mut Transaction<'_, Postgres>,
    file: &NewFile,
) -> Result<FileRecord, CatalogError> {
    let inserted = sqlx::query_as::<_, FileRow>(
        "insert into engine_file
             (id, tenant_id, signal, level, path, size_bytes, row_count, ts_min, ts_max, services, summary, state)
         values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
         on conflict (id) do nothing
         returning id, tenant_id, signal, level, path, size_bytes, row_count, ts_min, ts_max,
                   services, summary, state, created_at, quarantined_at",
    )
    .bind(file.id)
    .bind(file.tenant_id)
    .bind(file.signal.code())
    .bind(file.level.code())
    .bind(&file.path)
    .bind(file.size_bytes)
    .bind(file.row_count)
    .bind(file.range.start)
    .bind(file.range.end)
    .bind(&file.services)
    .bind(&file.summary)
    .bind(FileState::Visible.code())
    .fetch_optional(&mut **tx)
    .await?;
    let row = if let Some(row) = inserted {
        row
    } else {
        let existing = sqlx::query_as::<_, FileRow>(
            "select id, tenant_id, signal, level, path, size_bytes, row_count, ts_min, ts_max,
                    services, summary, state, created_at, quarantined_at
             from engine_file where id = $1",
        )
        .bind(file.id)
        .fetch_one(&mut **tx)
        .await?;
        verify_same_content(file, &existing)?;
        existing
    };
    FileRecord::try_from(row)
}

/// A second registration under one id must describe the same bytes.
fn verify_same_content(file: &NewFile, existing: &FileRow) -> Result<(), CatalogError> {
    let mismatch = |field| {
        Err(CatalogError::FileMismatch {
            id: file.id.0,
            field,
        })
    };
    if existing.tenant_id != file.tenant_id {
        return mismatch("tenant");
    }
    if existing.signal != file.signal.code() {
        return mismatch("signal");
    }
    if existing.level != file.level.code() {
        return mismatch("level");
    }
    if existing.path != file.path {
        return mismatch("path");
    }
    if existing.size_bytes != file.size_bytes {
        return mismatch("size_bytes");
    }
    if existing.row_count != file.row_count {
        return mismatch("row_count");
    }
    if existing.ts_min != file.range.start || existing.ts_max != file.range.end {
        return mismatch("time range");
    }
    if existing.services != file.services {
        return mismatch("services");
    }
    if existing.summary != file.summary {
        return mismatch("summary");
    }
    Ok(())
}

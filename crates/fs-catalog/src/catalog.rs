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

    /// The identity this catalog was given once, when it was created; an
    /// engine marks the bucket prefix it writes with it.
    ///
    /// # Errors
    /// Database failure, or a catalog not migrated to carry an identity.
    pub async fn identity(&self) -> Result<Uuid, CatalogError> {
        let row: (Uuid,) = sqlx::query_as("select id from engine_catalog")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.0)
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
    /// The file is not eligible for compaction until the caller
    /// [confirms](Self::confirm_file) it, once the rows it serves from memory
    /// are retained for the registration: the compactor's delay then counts
    /// from after that retention began, however long the commit took.
    ///
    /// # Errors
    /// Duplicate path under a different id, unknown tenant, or database failure.
    pub async fn register_file(&self, file: &NewFile) -> Result<FileRecord, CatalogError> {
        let mut tx = self.pool.begin().await?;
        let record = insert_file(&mut tx, file, Confirm::ByWriter).await?;
        tx.commit().await?;
        Ok(record)
    }

    /// Makes a registered file eligible for compaction once the compactor's
    /// delay has passed, stamped on the database clock now. Idempotent: a
    /// file already confirmed keeps its stamp.
    ///
    /// # Errors
    /// Database failure.
    pub async fn confirm_file(&self, id: FileId) -> Result<(), CatalogError> {
        sqlx::query(
            "update engine_file set confirmed_at = clock_timestamp()
             where id = $1 and confirmed_at is null",
        )
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
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
            "select id, origin, tenant_id, signal, level, path, size_bytes, row_count, ts_min, ts_max,
                    services, summary, content_sha256, state, created_at, quarantined_at
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
            "select id, origin, tenant_id, signal, level, path, size_bytes, row_count, ts_min, ts_max,
                    services, summary, content_sha256, state, created_at, quarantined_at
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
        self.swap_files_fenced(tenant, signal, inputs, &[], outputs, None)
            .await
    }

    /// [`swap_files`](Self::swap_files) that first checks, in the same
    /// transaction, that `fence` still names the unexpired holder and
    /// generation of its lease; otherwise nothing changes and
    /// [`CatalogError::LeaseLost`] is returned. `replaced` names rows of
    /// other signals the outputs supersede, derived tables rebuilt from the
    /// inputs; they are quarantined with the inputs under the same rule and
    /// recorded with them.
    ///
    /// # Errors
    /// Lease lost, conflict, or database failure.
    pub async fn swap_files_fenced(
        &self,
        tenant: TenantId,
        signal: Signal,
        inputs: &[FileId],
        replaced: &[(Signal, FileId)],
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
        let mut quarantined = quarantine(&mut tx, tenant, signal, inputs).await?;
        for (signal, id) in replaced {
            quarantined += quarantine(&mut tx, tenant, *signal, std::slice::from_ref(id)).await?;
        }
        let expected = inputs.len() + replaced.len();
        if quarantined != expected {
            tx.rollback().await?;
            return Err(CatalogError::CompactionConflict {
                expected,
                found: quarantined,
            });
        }
        let inputs: Vec<FileId> = inputs
            .iter()
            .copied()
            .chain(replaced.iter().map(|(_, id)| *id))
            .collect();
        let mut output_ids = Vec::with_capacity(outputs.len());
        for file in outputs {
            output_ids.push(insert_file(&mut tx, file, Confirm::OnInsert).await?.id);
        }
        let id = Uuid::now_v7();
        sqlx::query(
            "insert into engine_compaction (id, tenant_id, signal, inputs, outputs) values ($1, $2, $3, $4, $5)",
        )
        .bind(id)
        .bind(tenant)
        .bind(signal.code())
        .bind(&inputs)
        .bind(&output_ids)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Compaction {
            id,
            inputs,
            outputs: output_ids,
        })
    }

    /// Removes up to `limit` files quarantined for longer than `older_than`,
    /// on the database clock so a compactor with a skewed clock cannot
    /// purge early, and returns them. Each row goes in a transaction that
    /// holds the locks of its path and sidecar and marks both for
    /// deletion, so from the commit on no registration of the path commits,
    /// a replay included; the caller deletes the objects under their
    /// claims, which leave tombstones, and one it cannot delete is
    /// finished by the orphan sweep.
    ///
    /// # Errors
    /// Database failure.
    pub async fn purge_quarantined(
        &self,
        older_than: std::time::Duration,
        limit: i64,
    ) -> Result<Vec<PurgedFile>, CatalogError> {
        let due = sqlx::query_as::<_, PurgedFile>(
            "select id, path from engine_file
             where state = $1 and quarantined_at <= now() - make_interval(secs => $2)
             order by quarantined_at, id
             limit $3",
        )
        .bind(FileState::Quarantined.code())
        .bind(older_than.as_secs_f64())
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        let mut purged = Vec::with_capacity(due.len());
        for file in due {
            if self.purge_one(&file, older_than).await? {
                purged.push(file);
            }
        }
        Ok(purged)
    }

    /// Removes one quarantined row under the locks of its paths and marks
    /// them; false when the row was no longer there to purge.
    async fn purge_one(
        &self,
        file: &PurgedFile,
        older_than: std::time::Duration,
    ) -> Result<bool, CatalogError> {
        let mut paths = vec![file.path.clone()];
        paths.extend(fs_index::sidecar_path(&file.path));
        let mut tx = self.pool.begin().await?;
        for path in &paths {
            crate::orphans::lock_path(&mut tx, path).await?;
        }
        let removed = sqlx::query(
            "delete from engine_file
             where id = $1 and state = $2 and quarantined_at <= now() - make_interval(secs => $3)",
        )
        .bind(file.id)
        .bind(FileState::Quarantined.code())
        .bind(older_than.as_secs_f64())
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if removed == 0 {
            tx.rollback().await?;
            return Ok(false);
        }
        sqlx::query(
            "insert into engine_orphan (path, deleting) select unnest($1::text[]), true
             on conflict (path) do update
             set deleting = true, deleted_at = null, attempts = 0, retry_after = now()",
        )
        .bind(&paths)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
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

/// When a registration becomes eligible for compaction.
#[derive(Clone, Copy)]
enum Confirm {
    /// With the row: nothing serves the file before it is visible.
    OnInsert,
    /// After the commit, by the writer, once its hot retention has begun.
    ByWriter,
}

/// Quarantines the visible `ids` of `tenant` and `signal`; how many were.
async fn quarantine(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant: TenantId,
    signal: Signal,
    ids: &[FileId],
) -> Result<usize, CatalogError> {
    let quarantined: Vec<(FileId,)> = sqlx::query_as(
        "update engine_file set state = $4, quarantined_at = now()
         where id = any($1) and tenant_id = $2 and signal = $3 and state = $5
         returning id",
    )
    .bind(ids)
    .bind(tenant)
    .bind(signal.code())
    .bind(FileState::Quarantined.code())
    .bind(FileState::Visible.code())
    .fetch_all(&mut **tx)
    .await?;
    Ok(quarantined.len())
}

async fn insert_file(
    tx: &mut Transaction<'_, Postgres>,
    file: &NewFile,
    confirm: Confirm,
) -> Result<FileRecord, CatalogError> {
    crate::orphans::refuse_doomed(tx, &file.path).await?;
    let inserted = sqlx::query_as::<_, FileRow>(
        "insert into engine_file
             (id, tenant_id, signal, level, path, size_bytes, row_count, ts_min, ts_max, services, summary, state, origin,
              confirmed_at, content_sha256)
         values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                 case when $14 then clock_timestamp() end, $15)
         on conflict (id) do nothing
         returning id, origin, tenant_id, signal, level, path, size_bytes, row_count, ts_min, ts_max,
                   services, summary, content_sha256, state, created_at, quarantined_at",
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
    .bind(file.origin)
    .bind(matches!(confirm, Confirm::OnInsert))
    .bind(file.content_sha256.map(|digest| digest.to_vec()))
    .fetch_optional(&mut **tx)
    .await?;
    let row = if let Some(row) = inserted {
        row
    } else {
        let existing = sqlx::query_as::<_, FileRow>(
            "select id, origin, tenant_id, signal, level, path, size_bytes, row_count, ts_min, ts_max,
                    services, summary, content_sha256, state, created_at, quarantined_at
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
    if existing.origin != file.origin {
        return mismatch("origin");
    }
    if let (Some(known), Some(ours)) = (&existing.content_sha256, &file.content_sha256) {
        if known.as_slice() != ours {
            return mismatch("content_sha256");
        }
    }
    Ok(())
}

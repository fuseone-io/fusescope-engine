//! Catalog operations.

use chrono::{DateTime, Duration, Utc};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::CatalogError;
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
    /// # Errors
    /// Duplicate path, unknown tenant, or database failure.
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
        let mut tx = self.pool.begin().await?;
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
        older_than: Duration,
        limit: i64,
    ) -> Result<Vec<PurgedFile>, CatalogError> {
        let cutoff = Utc::now() - older_than;
        let rows = sqlx::query_as::<_, PurgedFile>(
            "delete from engine_file
             where id in (
                 select id from engine_file
                 where state = $1 and quarantined_at <= $2
                 order by quarantined_at
                 limit $3
             )
             returning id, path",
        )
        .bind(FileState::Quarantined.code())
        .bind(cutoff)
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
    let row = sqlx::query_as::<_, FileRow>(
        "insert into engine_file
             (id, tenant_id, signal, level, path, size_bytes, row_count, ts_min, ts_max, services, summary, state)
         values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
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
    .fetch_one(&mut **tx)
    .await?;
    FileRecord::try_from(row)
}

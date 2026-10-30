//! Configuration: a TOML file plus environment overrides.
//!
//! Secrets (catalog URL with password, bucket credentials) are read from the
//! environment into [`Secrets`], which never prints them. The file may carry a
//! catalog URL for development, and the environment always wins.

use std::fmt;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::ServerError;
use crate::role::Role;

/// Whole configuration file.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Process identity and role.
    #[serde(default)]
    pub server: ServerConfig,
    /// Catalog database.
    #[serde(default)]
    pub catalog: CatalogConfig,
    /// Object storage.
    pub bucket: BucketConfig,
    /// Write-ahead log of the ingester role.
    #[serde(default)]
    pub wal: WalConfig,
    /// OTLP ingest of the ingester role.
    #[serde(default)]
    pub ingest: IngestSection,
    /// Query limits of the querier role.
    #[serde(default)]
    pub query: QuerySection,
    /// Compactor role.
    #[serde(default)]
    pub compactor: CompactorConfig,
    /// Logging.
    #[serde(default)]
    pub log: LogConfig,
    /// mTLS of the Flight listeners and their clients.
    #[serde(default)]
    pub tls: crate::tls::TlsSection,
    /// How Flight SQL clients authenticate.
    #[serde(default)]
    pub auth: crate::auth::AuthSection,
}

/// Process identity and role.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Role to run when `--role` is not given.
    #[serde(default = "default_role")]
    pub role: Role,
    /// Stable node id (pod name). Defaults to the `HOSTNAME` variable or `node-0`.
    pub node_id: Option<String>,
    /// Address advertised to the catalog for hot-window scans.
    #[serde(default = "default_flight_addr")]
    pub flight_addr: String,
    /// Prometheus `/metrics`, `/healthz` and `/readyz` listener. Empty
    /// string disables it.
    #[serde(default = "default_metrics_addr")]
    pub metrics_addr: String,
    /// Seconds between probes of the catalog and the bucket for `/readyz`.
    #[serde(default = "default_health_interval")]
    pub health_interval_secs: u64,
    /// Seconds a stop waits for the roles to finish what they hold before
    /// abandoning it.
    #[serde(default = "default_drain_timeout")]
    pub drain_timeout_secs: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            role: default_role(),
            node_id: None,
            flight_addr: default_flight_addr(),
            metrics_addr: default_metrics_addr(),
            health_interval_secs: default_health_interval(),
            drain_timeout_secs: default_drain_timeout(),
        }
    }
}

fn default_metrics_addr() -> String {
    "0.0.0.0:9464".to_owned()
}

fn default_health_interval() -> u64 {
    10
}

fn default_drain_timeout() -> u64 {
    30
}

fn default_role() -> Role {
    Role::All
}

fn default_flight_addr() -> String {
    "0.0.0.0:8815".to_owned()
}

/// Catalog database.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogConfig {
    /// Connection URL. Prefer `FS_CATALOG_URL` in the environment.
    pub url: Option<String>,
    /// Pool size.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// Apply migrations on start.
    #[serde(default = "default_true")]
    pub migrate: bool,
}

impl Default for CatalogConfig {
    fn default() -> Self {
        Self {
            url: None,
            max_connections: default_max_connections(),
            migrate: true,
        }
    }
}

fn default_max_connections() -> u32 {
    8
}

fn default_true() -> bool {
    true
}

/// Object storage backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BucketKind {
    /// S3 and S3-compatible stores (AWS, RustFS, Ceph, GCS interop).
    S3,
    /// A directory on local disk. Development and tests only.
    Local,
}

/// Object storage.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BucketConfig {
    /// Backend.
    pub kind: BucketKind,
    /// Bucket name (`s3`).
    pub name: Option<String>,
    /// Endpoint URL for S3-compatible stores. Unset means AWS.
    pub endpoint: Option<String>,
    /// Region.
    pub region: Option<String>,
    /// Permit `http://` endpoints. Development only.
    #[serde(default)]
    pub allow_http: bool,
    /// Root directory (`local`).
    pub path: Option<PathBuf>,
    /// Key prefix inside the bucket, without leading slash.
    #[serde(default)]
    pub prefix: Option<String>,
    /// Mark a prefix that already holds objects but no catalog marker as
    /// this catalog's, the operator's explicit decision; one start with it
    /// is enough. Off, such a prefix refuses to start.
    #[serde(default)]
    pub adopt_prefix: bool,
}

/// Write-ahead log.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalConfig {
    /// Directory for segments.
    #[serde(default = "default_wal_dir")]
    pub dir: PathBuf,
    /// Segment rotation size.
    #[serde(default = "default_segment_max_bytes")]
    pub segment_max_bytes: u64,
    /// Largest record.
    #[serde(default = "default_max_record_bytes")]
    pub max_record_bytes: usize,
    /// Fsync policy.
    #[serde(default)]
    pub sync: SyncPolicy,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            dir: default_wal_dir(),
            segment_max_bytes: default_segment_max_bytes(),
            max_record_bytes: default_max_record_bytes(),
            sync: SyncPolicy::default(),
        }
    }
}

fn default_wal_dir() -> PathBuf {
    PathBuf::from("./data/wal")
}

fn default_segment_max_bytes() -> u64 {
    64 * 1024 * 1024
}

fn default_max_record_bytes() -> usize {
    16 * 1024 * 1024
}

/// Fsync policy, mirrors [`fs_wal::SyncPolicy`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SyncPolicy {
    /// Fsync once per accepted request.
    #[default]
    Batch,
    /// Fsync on every append.
    Always,
}

impl From<SyncPolicy> for fs_wal::SyncPolicy {
    fn from(policy: SyncPolicy) -> Self {
        match policy {
            SyncPolicy::Batch => fs_wal::SyncPolicy::Batch,
            SyncPolicy::Always => fs_wal::SyncPolicy::Always,
        }
    }
}

/// OTLP ingest.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestSection {
    /// gRPC listen address.
    #[serde(default = "default_ingest_listen")]
    pub listen: String,
    /// Largest accepted gRPC message.
    #[serde(default = "default_max_message_bytes")]
    pub max_message_bytes: usize,
    /// Flush a tenant buffer at this many rows.
    #[serde(default = "default_flush_rows")]
    pub flush_rows: usize,
    /// Flush a tenant buffer at this many bytes.
    #[serde(default = "default_flush_bytes")]
    pub flush_bytes: usize,
    /// Flush a tenant buffer once its oldest record is this old.
    #[serde(default = "default_flush_interval")]
    pub flush_interval_secs: u64,
    /// Merge a tenant's complete request batches into one chunk once this
    /// many are waiting. At least 2.
    #[serde(default = "default_coalesce_batches")]
    pub coalesce_batches: usize,
    /// Most counted bytes merged into one chunk at a time: Arrow memory plus
    /// the bound of each batch's trace set.
    #[serde(default = "default_coalesce_bytes")]
    pub coalesce_bytes: usize,
    /// Requests converted to Arrow at the same time. Defaults to the cores.
    #[serde(default = "fs_ingest::config::default_parallelism")]
    pub max_conversions: usize,
    /// Chunks built at the same time. Defaults to half the cores.
    #[serde(default = "fs_ingest::config::default_coalesces")]
    pub max_coalesces: usize,
    /// Files written at the same time.
    #[serde(default = "default_max_flushes")]
    pub max_flushes: usize,
    /// Most calendar hours one cut flushes, at most 64; rows of later hours
    /// wait for the next cut.
    #[serde(default = "default_max_hours_per_cut")]
    pub max_hours_per_cut: usize,
    /// Refuse a request whose spans fall in more calendar hours than this.
    #[serde(default = "default_max_hours_per_batch")]
    pub max_hours_per_batch: usize,
    /// Refuse a tenant's request when its unflushed rows would fall in more
    /// calendar hours than this. At least `max_hours_per_batch`.
    #[serde(default = "default_max_live_hours")]
    pub max_live_hours: usize,
    /// Registration confirmations attempted at the same time, off the flusher.
    #[serde(default = "default_max_confirms")]
    pub max_confirms: usize,
    /// Seconds one confirmation call may take before it counts as failed;
    /// also bounds what a stop waits for the confirmations due.
    #[serde(default = "default_confirm_timeout")]
    pub confirm_timeout_secs: u64,
    /// Rows per Parquet row group; the writer holds one at a time.
    #[serde(default = "default_row_group_rows")]
    pub row_group_rows: usize,
    /// Refuse a tenant's data when its live and flushing rows would exceed this.
    #[serde(default = "default_max_buffer_bytes")]
    pub max_buffer_bytes: usize,
    /// Refuse a tenant's data when its rows in every state, hot-window rows
    /// included, would exceed this. Keeps one tenant from holding the whole
    /// `max_memory_bytes`.
    #[serde(default = "default_max_tenant_bytes")]
    pub max_tenant_bytes: usize,
    /// Refuse data when the WAL on disk exceeds this.
    #[serde(default = "default_wal_max_bytes")]
    pub wal_max_bytes: u64,
    /// Refuse data when live, flushing and hot-window rows together exceed this.
    #[serde(default = "default_max_memory_bytes")]
    pub max_memory_bytes: usize,
    /// Per-tenant throughput limit in bytes per second. Unset disables it.
    pub tenant_bytes_per_sec: Option<u64>,
    /// Accept unauthenticated requests as this tenant id. Development only.
    pub anonymous_tenant: Option<uuid::Uuid>,
    /// Internal Arrow Flight address serving the hot window to queriers.
    #[serde(default = "default_hot_listen")]
    pub hot_listen: String,
    /// Address queriers use to reach this ingester's hot window. In
    /// Kubernetes set it to the pod IP through `FS_INGEST_HOT_ADVERTISE`.
    #[serde(default = "default_hot_advertise")]
    pub hot_advertise: String,
    /// Seconds flushed rows stay served from memory after their file is
    /// registered. Must exceed the querier's file cache TTL.
    #[serde(default = "default_persisted_retain")]
    pub persisted_retain_secs: u64,
    /// Most records in one WAL group-commit batch.
    #[serde(default = "default_wal_batch_records")]
    pub wal_batch_records: usize,
    /// Most payload bytes in one WAL group-commit batch.
    #[serde(default = "default_wal_batch_bytes")]
    pub wal_batch_bytes: usize,
    /// Milliseconds the first record of a batch waits for company. Zero
    /// batches only what is already queued.
    #[serde(default)]
    pub wal_linger_ms: u64,
    /// Most records waiting for the WAL writer; beyond it requests wait and
    /// then get RESOURCE_EXHAUSTED.
    #[serde(default = "default_wal_queue_records")]
    pub wal_queue_records: usize,
    /// Most payload bytes waiting for the WAL writer; beyond it requests get
    /// RESOURCE_EXHAUSTED immediately.
    #[serde(default = "default_wal_queue_bytes")]
    pub wal_queue_bytes: usize,
    /// Milliseconds a request waits for a WAL queue slot.
    #[serde(default = "default_wal_queue_wait_ms")]
    pub wal_queue_wait_ms: u64,
}

impl Default for IngestSection {
    fn default() -> Self {
        Self {
            listen: default_ingest_listen(),
            max_message_bytes: default_max_message_bytes(),
            flush_rows: default_flush_rows(),
            flush_bytes: default_flush_bytes(),
            flush_interval_secs: default_flush_interval(),
            coalesce_batches: default_coalesce_batches(),
            coalesce_bytes: default_coalesce_bytes(),
            max_conversions: fs_ingest::config::default_parallelism(),
            max_coalesces: fs_ingest::config::default_coalesces(),
            max_flushes: default_max_flushes(),
            max_hours_per_cut: default_max_hours_per_cut(),
            max_hours_per_batch: default_max_hours_per_batch(),
            max_live_hours: default_max_live_hours(),
            max_confirms: default_max_confirms(),
            confirm_timeout_secs: default_confirm_timeout(),
            row_group_rows: default_row_group_rows(),
            max_buffer_bytes: default_max_buffer_bytes(),
            max_tenant_bytes: default_max_tenant_bytes(),
            wal_max_bytes: default_wal_max_bytes(),
            max_memory_bytes: default_max_memory_bytes(),
            tenant_bytes_per_sec: None,
            anonymous_tenant: None,
            hot_listen: default_hot_listen(),
            hot_advertise: default_hot_advertise(),
            persisted_retain_secs: default_persisted_retain(),
            wal_batch_records: default_wal_batch_records(),
            wal_batch_bytes: default_wal_batch_bytes(),
            wal_linger_ms: 0,
            wal_queue_records: default_wal_queue_records(),
            wal_queue_bytes: default_wal_queue_bytes(),
            wal_queue_wait_ms: default_wal_queue_wait_ms(),
        }
    }
}

fn default_wal_batch_records() -> usize {
    1024
}

fn default_wal_batch_bytes() -> usize {
    16 * 1024 * 1024
}

fn default_wal_queue_records() -> usize {
    4096
}

fn default_wal_queue_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_wal_queue_wait_ms() -> u64 {
    2000
}

fn default_hot_listen() -> String {
    "0.0.0.0:8816".to_owned()
}

fn default_hot_advertise() -> String {
    "127.0.0.1:8816".to_owned()
}

fn default_persisted_retain() -> u64 {
    8
}

impl IngestSection {
    /// Converts to the ingest crate's configuration.
    ///
    /// # Errors
    /// Unparsable listen address.
    pub fn to_ingest_config(&self) -> Result<fs_ingest::IngestConfig, ServerError> {
        let listen = self.listen.parse().map_err(|e| {
            ServerError::InvalidConfig(format!("ingest.listen {:?}: {e}", self.listen))
        })?;
        Ok(fs_ingest::IngestConfig {
            listen,
            max_message_bytes: self.max_message_bytes,
            flush_rows: self.flush_rows,
            flush_bytes: self.flush_bytes,
            flush_interval: std::time::Duration::from_secs(self.flush_interval_secs),
            coalesce_batches: self.coalesce_batches,
            coalesce_bytes: self.coalesce_bytes,
            max_conversions: self.max_conversions,
            max_coalesces: self.max_coalesces,
            max_flushes: self.max_flushes,
            max_hours_per_cut: self.max_hours_per_cut,
            max_hours_per_batch: self.max_hours_per_batch,
            max_live_hours: self.max_live_hours,
            max_confirms: self.max_confirms,
            confirm_timeout: std::time::Duration::from_secs(self.confirm_timeout_secs),
            row_group_rows: self.row_group_rows,
            max_buffer_bytes: self.max_buffer_bytes,
            max_tenant_bytes: self.max_tenant_bytes,
            wal_max_bytes: self.wal_max_bytes,
            max_memory_bytes: self.max_memory_bytes,
            tenant_bytes_per_sec: self.tenant_bytes_per_sec,
            anonymous_tenant: self.anonymous_tenant.map(fs_catalog::TenantId),
            hot_listen: Some(self.hot_listen.parse().map_err(|e| {
                ServerError::InvalidConfig(format!("ingest.hot_listen {:?}: {e}", self.hot_listen))
            })?),
            persisted_retain: std::time::Duration::from_secs(self.persisted_retain_secs),
            group_commit: fs_ingest::walwriter::GroupCommitConfig {
                max_records: self.wal_batch_records,
                max_bytes: self.wal_batch_bytes,
                linger: std::time::Duration::from_millis(self.wal_linger_ms),
                max_inflight_records: self.wal_queue_records,
                max_inflight_bytes: self.wal_queue_bytes,
                queue_wait: std::time::Duration::from_millis(self.wal_queue_wait_ms),
            },
            ..fs_ingest::IngestConfig::default()
        })
    }
}

fn default_ingest_listen() -> String {
    "0.0.0.0:4317".to_owned()
}

fn default_max_message_bytes() -> usize {
    16 * 1024 * 1024
}

fn default_flush_rows() -> usize {
    1_000_000
}

fn default_flush_bytes() -> usize {
    128 * 1024 * 1024
}

fn default_flush_interval() -> u64 {
    30
}

fn default_coalesce_batches() -> usize {
    512
}

fn default_max_hours_per_cut() -> usize {
    fs_ingest::IngestConfig::default().max_hours_per_cut
}

fn default_max_hours_per_batch() -> usize {
    fs_ingest::IngestConfig::default().max_hours_per_batch
}

fn default_max_live_hours() -> usize {
    fs_ingest::IngestConfig::default().max_live_hours
}

fn default_max_confirms() -> usize {
    fs_ingest::IngestConfig::default().max_confirms
}

fn default_confirm_timeout() -> u64 {
    fs_ingest::IngestConfig::default().confirm_timeout.as_secs()
}

fn default_max_flushes() -> usize {
    2
}

fn default_row_group_rows() -> usize {
    fs_ingest::flush::DEFAULT_ROW_GROUP_ROWS
}

fn default_coalesce_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_max_buffer_bytes() -> usize {
    512 * 1024 * 1024
}

fn default_max_tenant_bytes() -> usize {
    1024 * 1024 * 1024
}

fn default_max_memory_bytes() -> usize {
    2 * 1024 * 1024 * 1024
}

fn default_wal_max_bytes() -> u64 {
    4 * 1024 * 1024 * 1024
}

/// Query limits.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuerySection {
    /// Memory the querier may use for execution before spilling or failing.
    #[serde(default = "default_query_memory")]
    pub memory_bytes: usize,
    /// Wall-clock limit per query.
    #[serde(default = "default_query_timeout")]
    pub timeout_secs: u64,
    /// Maximum rows returned per query.
    #[serde(default = "default_query_max_rows")]
    pub max_rows: usize,
    /// Directory for spill files. Unset disables spilling.
    pub spill_dir: Option<PathBuf>,
    /// Decoded `trace_id` sidecars kept in memory for trace lookups. Zero
    /// disables the sidecars and every lookup opens every overlapping file.
    #[serde(default = "default_trace_index_cache_bytes")]
    pub trace_index_cache_bytes: usize,
    /// Most plans kept between `GetFlightInfo` and `DoGet`; beyond it the
    /// oldest is dropped and its ticket plans again.
    #[serde(default = "default_prepared_plans")]
    pub prepared_plans: usize,
    /// Most bytes the kept plans may retain together, as the querier
    /// estimates them; the oldest are dropped to make room.
    #[serde(default = "default_prepared_bytes")]
    pub prepared_bytes: usize,
    /// Seconds a prepared plan waits for its `DoGet` before it is dropped.
    #[serde(default = "default_prepared_ttl")]
    pub prepared_ttl_secs: u64,
}

impl Default for QuerySection {
    fn default() -> Self {
        Self {
            memory_bytes: default_query_memory(),
            timeout_secs: default_query_timeout(),
            max_rows: default_query_max_rows(),
            spill_dir: None,
            trace_index_cache_bytes: default_trace_index_cache_bytes(),
            prepared_plans: default_prepared_plans(),
            prepared_bytes: default_prepared_bytes(),
            prepared_ttl_secs: default_prepared_ttl(),
        }
    }
}

impl QuerySection {
    /// Converts to the query crate's limits.
    pub fn to_limits(&self) -> fs_query::QueryLimits {
        fs_query::QueryLimits {
            memory_bytes: self.memory_bytes,
            timeout: std::time::Duration::from_secs(self.timeout_secs),
            max_rows: self.max_rows,
            trace_index_cache_bytes: self.trace_index_cache_bytes,
            prepared_plans: self.prepared_plans,
            prepared_bytes: self.prepared_bytes,
            prepared_ttl: std::time::Duration::from_secs(self.prepared_ttl_secs),
            ..fs_query::QueryLimits::default()
        }
    }
}

fn default_trace_index_cache_bytes() -> usize {
    256 * 1024 * 1024
}

fn default_prepared_plans() -> usize {
    fs_query::QueryLimits::default().prepared_plans
}

fn default_prepared_bytes() -> usize {
    fs_query::QueryLimits::default().prepared_bytes
}

fn default_prepared_ttl() -> u64 {
    fs_query::QueryLimits::default().prepared_ttl.as_secs()
}

fn default_query_memory() -> usize {
    2 * 1024 * 1024 * 1024
}

fn default_query_timeout() -> u64 {
    60
}

fn default_query_max_rows() -> usize {
    1_000_000
}

/// Compactor role.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactorConfig {
    /// How often quarantined files are purged.
    #[serde(default = "default_purge_interval")]
    pub purge_interval_secs: u64,
    /// How long a quarantined file stays readable before deletion.
    #[serde(default = "default_quarantine_ttl")]
    pub quarantine_ttl_secs: u64,
    /// Files purged per pass.
    #[serde(default = "default_purge_batch")]
    pub purge_batch: i64,
    /// How long the compactor lease lasts without a renewal; renewed every
    /// third of it. Another instance takes over this long after a crash.
    #[serde(default = "default_lease_ttl")]
    pub lease_ttl_secs: u64,
    /// How often closed hours are looked at for L0 to L1 compaction.
    #[serde(default = "default_compaction_interval")]
    pub compaction_interval_secs: u64,
    /// An hour is compacted only once every file in it ended this long ago.
    #[serde(default = "default_hour_close_delay")]
    pub hour_close_delay_secs: u64,
    /// An L1 file closes once it reaches this many bytes.
    #[serde(default = "default_l1_target_bytes")]
    pub l1_target_bytes: usize,
    /// Most L0 files one compaction pass merges; memory holds one row group
    /// of each.
    #[serde(default = "default_max_inputs_per_pass")]
    pub max_inputs_per_pass: usize,
    /// Most input bytes one compaction pass merges.
    #[serde(default = "default_max_input_bytes")]
    pub max_input_bytes: u64,
    /// Most distinct traces retained for one pass's `trace_summary`.
    #[serde(default = "default_trace_summary_max_traces")]
    pub trace_summary_max_traces: usize,
    /// Conservative bytes retained for one pass's `trace_summary` builder.
    #[serde(default = "default_trace_summary_max_bytes")]
    pub trace_summary_max_bytes: usize,
    /// Most spans retained to resolve one pass's `service_graph_1m`.
    #[serde(default = "default_service_graph_max_spans")]
    pub service_graph_max_spans: usize,
    /// Conservative bytes retained for one pass's `service_graph_1m` builder.
    #[serde(default = "default_service_graph_max_bytes")]
    pub service_graph_max_bytes: usize,
    /// How often the bucket is swept for objects without a catalog row.
    #[serde(default = "default_orphan_interval")]
    pub orphan_interval_secs: u64,
    /// How long an object must have been seen without a row before it is
    /// deleted; longer than any upload waits for its registration.
    #[serde(default = "default_orphan_ttl")]
    pub orphan_ttl_secs: u64,
    /// A sweep stops walking once it has listed this many objects, after
    /// finishing the partition (tenant, level, day) it is in; the next
    /// sweep continues from there. A goal, overshot by at most one
    /// partition, not a ceiling.
    #[serde(default = "default_sweep_objects")]
    pub sweep_objects_per_pass: usize,
    /// Most objects one sweep deletes.
    #[serde(default = "default_sweep_deletes")]
    pub sweep_deletes_per_pass: usize,
    /// How long a marked object whose delete failed waits before the next
    /// try, times the number of failures so far, up to six times.
    #[serde(default = "default_sweep_delete_backoff")]
    pub sweep_delete_backoff_secs: u64,
    /// How often files beyond their tenant's `retention_days` are looked
    /// for; the cut is a UTC day boundary on the catalog's clock.
    #[serde(default = "default_retention_interval")]
    pub retention_interval_secs: u64,
    /// Most files one retention pass quarantines.
    #[serde(default = "default_retention_files_per_pass")]
    pub retention_files_per_pass: i64,
    /// Most bytes of L1 files one `attribute_keys` rebuild reads after
    /// retention; a tenant above it keeps its table.
    #[serde(default = "default_rebuild_max_input_bytes")]
    pub rebuild_max_input_bytes: u64,
    /// Most bytes one `attribute_keys` rebuild may hold at its peak, by
    /// the worst case the ingest limits allow for the table before, the
    /// rebuilt table, one L1 file's column chunks and one decoded batch;
    /// a tenant above it keeps its table.
    #[serde(default = "default_rebuild_max_bytes")]
    pub rebuild_max_bytes: u64,
    /// Most stale `attribute_keys` tables one retention tick examines, a
    /// passed-over one counting as one, at most 256.
    #[serde(default = "default_rebuilds_per_tick")]
    pub rebuilds_per_tick: usize,
    /// How long a tenant whose rebuild failed waits before it is tried
    /// again, times its failures so far, up to six times.
    #[serde(default = "default_rebuild_backoff")]
    pub rebuild_backoff_secs: u64,
}

impl Default for CompactorConfig {
    fn default() -> Self {
        Self {
            purge_interval_secs: default_purge_interval(),
            quarantine_ttl_secs: default_quarantine_ttl(),
            purge_batch: default_purge_batch(),
            lease_ttl_secs: default_lease_ttl(),
            compaction_interval_secs: default_compaction_interval(),
            hour_close_delay_secs: default_hour_close_delay(),
            l1_target_bytes: default_l1_target_bytes(),
            max_inputs_per_pass: default_max_inputs_per_pass(),
            max_input_bytes: default_max_input_bytes(),
            trace_summary_max_traces: default_trace_summary_max_traces(),
            trace_summary_max_bytes: default_trace_summary_max_bytes(),
            service_graph_max_spans: default_service_graph_max_spans(),
            service_graph_max_bytes: default_service_graph_max_bytes(),
            orphan_interval_secs: default_orphan_interval(),
            orphan_ttl_secs: default_orphan_ttl(),
            sweep_objects_per_pass: default_sweep_objects(),
            sweep_deletes_per_pass: default_sweep_deletes(),
            sweep_delete_backoff_secs: default_sweep_delete_backoff(),
            retention_interval_secs: default_retention_interval(),
            retention_files_per_pass: default_retention_files_per_pass(),
            rebuild_max_input_bytes: default_rebuild_max_input_bytes(),
            rebuild_max_bytes: default_rebuild_max_bytes(),
            rebuilds_per_tick: default_rebuilds_per_tick(),
            rebuild_backoff_secs: default_rebuild_backoff(),
        }
    }
}

impl CompactorConfig {
    /// Converts to the compactor crate's settings; outputs use the
    /// ingester's row group size so L0 and L1 files share one layout.
    pub fn to_compactor(&self, row_group_rows: usize) -> fs_compactor::CompactorConfig {
        fs_compactor::CompactorConfig {
            purge_interval: std::time::Duration::from_secs(self.purge_interval_secs),
            quarantine_ttl: std::time::Duration::from_secs(self.quarantine_ttl_secs),
            purge_batch: self.purge_batch,
            lease_ttl: std::time::Duration::from_secs(self.lease_ttl_secs),
            compaction_interval: std::time::Duration::from_secs(self.compaction_interval_secs),
            compaction: fs_compactor::CompactionConfig {
                hour_close_delay: std::time::Duration::from_secs(self.hour_close_delay_secs),
                l1_target_bytes: self.l1_target_bytes,
                max_inputs_per_pass: self.max_inputs_per_pass,
                max_input_bytes: self.max_input_bytes,
                trace_summary_max_traces: self.trace_summary_max_traces,
                trace_summary_max_bytes: self.trace_summary_max_bytes,
                service_graph_max_spans: self.service_graph_max_spans,
                service_graph_max_bytes: self.service_graph_max_bytes,
                row_group_rows,
                inject_swap_failures: 0,
                inject_put_failure_at: None,
                inject_put_hold: None,
            },
            orphan_interval: std::time::Duration::from_secs(self.orphan_interval_secs),
            sweep: fs_compactor::SweepConfig {
                ttl: std::time::Duration::from_secs(self.orphan_ttl_secs),
                objects_per_pass: self.sweep_objects_per_pass,
                deletes_per_pass: self.sweep_deletes_per_pass,
                delete_backoff: std::time::Duration::from_secs(self.sweep_delete_backoff_secs),
                inject_delete_failure: None,
            },
            retention_interval: std::time::Duration::from_secs(self.retention_interval_secs),
            retention: fs_compactor::RetentionConfig {
                files_per_pass: self.retention_files_per_pass,
                rebuild_max_input_bytes: self.rebuild_max_input_bytes,
                rebuild_max_bytes: self.rebuild_max_bytes,
                batch_rows: fs_compactor::retention::REBUILD_BATCH_ROWS,
                limits: fs_schema::limits::Limits::default(),
                rebuilds_per_tick: self.rebuilds_per_tick,
                rebuild_backoff: std::time::Duration::from_secs(self.rebuild_backoff_secs),
                inject_swap_failures: 0,
                inject_list_failures: 0,
            },
        }
    }
}

fn default_retention_interval() -> u64 {
    3600
}

fn default_retention_files_per_pass() -> i64 {
    1000
}

fn default_rebuild_max_input_bytes() -> u64 {
    256 * 1024 * 1024 * 1024
}

fn default_rebuild_max_bytes() -> u64 {
    2 * 1024 * 1024 * 1024
}

fn default_rebuilds_per_tick() -> usize {
    64
}

fn default_rebuild_backoff() -> u64 {
    3600
}

fn default_orphan_interval() -> u64 {
    3600
}

fn default_orphan_ttl() -> u64 {
    3600
}

fn default_sweep_objects() -> usize {
    100_000
}

fn default_sweep_deletes() -> usize {
    10_000
}

fn default_sweep_delete_backoff() -> u64 {
    600
}

fn default_lease_ttl() -> u64 {
    30
}

fn default_compaction_interval() -> u64 {
    60
}

fn default_hour_close_delay() -> u64 {
    900
}

fn default_l1_target_bytes() -> usize {
    512 * 1024 * 1024
}

fn default_max_inputs_per_pass() -> usize {
    16
}

fn default_max_input_bytes() -> u64 {
    2 * 1024 * 1024 * 1024
}

fn default_trace_summary_max_traces() -> usize {
    4_000_000
}

fn default_trace_summary_max_bytes() -> usize {
    256 * 1024 * 1024
}

fn default_service_graph_max_spans() -> usize {
    4_000_000
}

fn default_service_graph_max_bytes() -> usize {
    256 * 1024 * 1024
}

fn default_purge_interval() -> u64 {
    60
}

fn default_quarantine_ttl() -> u64 {
    3600
}

fn default_purge_batch() -> i64 {
    100
}

/// Logging.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogConfig {
    /// Filter directive, as `RUST_LOG`.
    #[serde(default = "default_log_level")]
    pub level: String,
    /// Output format.
    #[serde(default)]
    pub format: LogFormat,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: LogFormat::default(),
        }
    }
}

fn default_log_level() -> String {
    "info".to_owned()
}

/// Log output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Human readable lines.
    #[default]
    Text,
    /// One JSON object per line.
    Json,
}

/// Values that must not be printed. `Debug` redacts them.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Secrets {
    /// Catalog URL, may contain a password.
    pub catalog_url: Option<String>,
    /// Bucket access key.
    pub bucket_access_key: Option<String>,
    /// Bucket secret key.
    pub bucket_secret_key: Option<String>,
    /// Query token for `admin query` and `admin action`, never on the
    /// command line.
    pub query_token: Option<String>,
}

impl fmt::Debug for Secrets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Secrets")
            .field("catalog_url", &self.catalog_url.as_deref().map(redact_url))
            .field(
                "bucket_access_key",
                &self.bucket_access_key.as_ref().map(|_| "***"),
            )
            .field(
                "bucket_secret_key",
                &self.bucket_secret_key.as_ref().map(|_| "***"),
            )
            .field("query_token", &self.query_token.as_ref().map(|_| "***"))
            .finish()
    }
}

impl Secrets {
    /// Reads `FS_CATALOG_URL`, `FS_BUCKET_ACCESS_KEY` and `FS_BUCKET_SECRET_KEY`.
    pub fn from_env(get: &dyn Fn(&str) -> Option<String>) -> Self {
        Self {
            catalog_url: get("FS_CATALOG_URL").filter(|s| !s.is_empty()),
            bucket_access_key: get("FS_BUCKET_ACCESS_KEY").filter(|s| !s.is_empty()),
            bucket_secret_key: get("FS_BUCKET_SECRET_KEY").filter(|s| !s.is_empty()),
            query_token: get("FS_QUERY_TOKEN").filter(|s| !s.is_empty()),
        }
    }
}

impl Config {
    /// Parses TOML text.
    ///
    /// # Errors
    /// Syntax errors or unknown fields.
    pub fn parse(text: &str) -> Result<Self, ServerError> {
        Ok(toml::from_str(text)?)
    }

    /// Reads and parses a file.
    ///
    /// # Errors
    /// Unreadable file, syntax errors or unknown fields.
    pub fn load(path: &Path) -> Result<Self, ServerError> {
        let text = std::fs::read_to_string(path).map_err(|source| ServerError::ReadConfig {
            path: path.to_path_buf(),
            source,
        })?;
        Self::parse(&text)
    }

    /// Applies environment overrides: `FS_ROLE`, `FS_NODE_ID`, `FS_FLIGHT_ADDR`,
    /// `FS_BUCKET_ENDPOINT`, `FS_BUCKET_NAME`, `FS_WAL_DIR`, `FS_LOG_LEVEL`.
    ///
    /// # Errors
    /// An unparsable `FS_ROLE`.
    pub fn apply_env(&mut self, get: &dyn Fn(&str) -> Option<String>) -> Result<(), ServerError> {
        if let Some(role) = get("FS_ROLE") {
            self.server.role = role
                .parse()
                .map_err(|e: crate::role::UnknownRole| ServerError::InvalidConfig(e.to_string()))?;
        }
        if let Some(v) = get("FS_NODE_ID") {
            self.server.node_id = Some(v);
        }
        if let Some(v) = get("FS_FLIGHT_ADDR") {
            self.server.flight_addr = v;
        }
        if let Some(v) = get("FS_METRICS_ADDR") {
            self.server.metrics_addr = v;
        }
        if let Some(v) = get("FS_BUCKET_ENDPOINT") {
            self.bucket.endpoint = Some(v);
        }
        if let Some(v) = get("FS_BUCKET_NAME") {
            self.bucket.name = Some(v);
        }
        if let Some(v) = get("FS_WAL_DIR") {
            self.wal.dir = PathBuf::from(v);
        }
        if let Some(v) = get("FS_INGEST_LISTEN") {
            self.ingest.listen = v;
        }
        if let Some(v) = get("FS_INGEST_HOT_ADVERTISE") {
            self.ingest.hot_advertise = v;
        }
        if let Some(v) = get("FS_LOG_LEVEL") {
            self.log.level = v;
        }
        Ok(())
    }

    fn validate_compactor(&self) -> Result<(), ServerError> {
        let invalid = |m: &str| Err(ServerError::InvalidConfig(m.to_owned()));
        let derived_bytes = self
            .compactor
            .trace_summary_max_bytes
            .checked_add(self.compactor.service_graph_max_bytes);
        if self.compactor.purge_batch <= 0 {
            return invalid("compactor.purge_batch must be positive");
        }
        if self.compactor.lease_ttl_secs < 3 {
            return invalid(
                "compactor.lease_ttl_secs must be at least 3, it is renewed every third",
            );
        }
        if self.compactor.max_inputs_per_pass < 2
            || self.compactor.l1_target_bytes < 1024 * 1024
            || self.compactor.max_input_bytes < self.compactor.l1_target_bytes as u64
            || self.compactor.trace_summary_max_traces == 0
            || self.compactor.service_graph_max_spans == 0
            || self.compactor.trace_summary_max_bytes < fs_schema::trace_summary::TRACE_ENTRY_BYTES
            || self.compactor.service_graph_max_bytes < fs_schema::service_graph::SPAN_ENTRY_BYTES
            || derived_bytes
                .is_none_or(|bytes| bytes > fs_compactor::compact::MAX_DERIVED_BUILDER_BYTES)
        {
            return invalid(
                "compactor input limits are invalid or derived-table count and byte caps exceed their bounds",
            );
        }
        if self.compactor.orphan_ttl_secs < 60
            || self.compactor.orphan_interval_secs == 0
            || self.compactor.sweep_objects_per_pass == 0
            || self.compactor.sweep_deletes_per_pass == 0
            || self.compactor.sweep_delete_backoff_secs == 0
        {
            return invalid(
                "compactor.orphan_ttl_secs must be at least 60 and orphan_interval_secs, sweep_objects_per_pass, sweep_deletes_per_pass and sweep_delete_backoff_secs positive",
            );
        }
        if self.compactor.retention_interval_secs == 0
            || self.compactor.retention_files_per_pass <= 0
            || self.compactor.rebuild_max_input_bytes == 0
            || self.compactor.rebuilds_per_tick == 0
            || self.compactor.rebuild_backoff_secs == 0
        {
            return invalid(
                "compactor.retention_interval_secs, retention_files_per_pass, rebuild_max_input_bytes, rebuilds_per_tick and rebuild_backoff_secs must be positive",
            );
        }
        if self.compactor.rebuilds_per_tick > fs_compactor::retention::MAX_REBUILDS_PER_TICK {
            return invalid(
                "compactor.rebuilds_per_tick must be at most 256, so the leader's other passes are never long behind the rebuilds",
            );
        }
        let smallest_rebuild = fs_compactor::retention::rebuild_footprint(
            0,
            0,
            self.compactor.l1_target_bytes as u64,
            0,
            &self
                .compactor
                .to_compactor(self.ingest.row_group_rows)
                .retention,
        );
        if self.compactor.rebuild_max_bytes < smallest_rebuild {
            return invalid(
                "compactor.rebuild_max_bytes must cover one L1 file of l1_target_bytes plus one decoded batch, or no attribute_keys table would ever be rebuilt after retention",
            );
        }

        // A compaction publishes L1 files under their own ids, so it must
        // never cover rows the hot window still serves under the L0's: an
        // hour closes only after the retention, a query and a file cache
        // refresh could all have ended.
        let hot_may_serve = self
            .ingest
            .persisted_retain_secs
            .checked_add(self.query.timeout_secs)
            .and_then(|s| s.checked_add(fs_query::files::STALE_AFTER.as_secs()));
        if hot_may_serve.is_none_or(|floor| self.compactor.hour_close_delay_secs < floor) {
            return invalid(
                "compactor.hour_close_delay_secs must cover ingest.persisted_retain_secs plus query.timeout_secs plus the querier's file cache TTL",
            );
        }
        // Two L0 files must always fit a pass, or an hour could never close;
        // an L0 file is at most one flushed buffer.
        if self.compactor.max_input_bytes < 2 * self.ingest.max_buffer_bytes as u64 {
            return invalid(
                "compactor.max_input_bytes must be at least twice ingest.max_buffer_bytes so two L0 files always fit a pass",
            );
        }
        // A querier may read a file for its cache TTL plus one query after
        // the swap that quarantined it.
        let readers_may_hold = fs_query::files::STALE_AFTER.as_secs() + self.query.timeout_secs;
        if self.compactor.quarantine_ttl_secs < readers_may_hold {
            return invalid(
                "compactor.quarantine_ttl_secs must cover the querier's file cache TTL plus query.timeout_secs",
            );
        }
        Ok(())
    }

    /// The ingest bounds the runtime or the cut enforces as hard ceilings.
    fn validate_access(&self) -> Result<(), ServerError> {
        self.tls.validate()?;
        self.auth.validate()
    }

    fn validate_server(&self) -> Result<(), ServerError> {
        let day = 86_400;
        let within = |secs: u64| (1..=day).contains(&secs);
        if !within(self.server.health_interval_secs) || !within(self.server.drain_timeout_secs) {
            return Err(ServerError::InvalidConfig(
                "server.health_interval_secs and server.drain_timeout_secs must be between 1 and 86400"
                    .into(),
            ));
        }
        Ok(())
    }

    fn validate_ingest_bounds(&self) -> Result<(), ServerError> {
        let invalid = |m: &str| Err(ServerError::InvalidConfig(m.to_owned()));
        if self.ingest.wal_batch_records == 0
            || self.ingest.wal_batch_bytes == 0
            || self.ingest.wal_queue_records == 0
            || self.ingest.wal_queue_bytes == 0
        {
            return invalid("ingest.wal_batch_* and ingest.wal_queue_* must be positive");
        }
        if self.ingest.wal_queue_records > tokio::sync::Semaphore::MAX_PERMITS {
            return invalid("ingest.wal_queue_records is larger than the runtime can track");
        }
        if self.ingest.max_confirms > tokio::sync::Semaphore::MAX_PERMITS {
            return invalid("ingest.max_confirms is larger than the runtime can track");
        }
        if self.ingest.max_hours_per_cut > fs_ingest::config::MAX_HOURS_PER_CUT {
            return invalid("ingest.max_hours_per_cut is above the ceiling of 64");
        }
        if self.ingest.max_hours_per_batch == 0
            || self.ingest.max_live_hours < self.ingest.max_hours_per_batch
        {
            return invalid(
                "ingest.max_hours_per_batch must be positive and ingest.max_live_hours at least that",
            );
        }
        Ok(())
    }

    /// Checks internal consistency. Connectivity is checked by the server.
    ///
    /// # Errors
    /// A description of the first problem found.
    pub fn validate(&self, secrets: &Secrets) -> Result<(), ServerError> {
        let invalid = |m: &str| Err(ServerError::InvalidConfig(m.to_owned()));
        match self.bucket.kind {
            BucketKind::S3 => {
                if self.bucket.name.as_deref().is_none_or(str::is_empty) {
                    return invalid("bucket.name is required for kind = \"s3\"");
                }
                if let Some(endpoint) = &self.bucket.endpoint {
                    if endpoint.starts_with("http://") && !self.bucket.allow_http {
                        return invalid(
                            "bucket.endpoint uses http:// but bucket.allow_http is false",
                        );
                    }
                    if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
                        return invalid("bucket.endpoint must start with http:// or https://");
                    }
                }
                if secrets.bucket_access_key.is_some() != secrets.bucket_secret_key.is_some() {
                    return invalid(
                        "FS_BUCKET_ACCESS_KEY and FS_BUCKET_SECRET_KEY must be set together",
                    );
                }
            }
            BucketKind::Local => {
                if self.bucket.path.is_none() {
                    return invalid("bucket.path is required for kind = \"local\"");
                }
            }
        }
        if self.catalog_url(secrets).is_none() {
            return invalid("catalog url missing: set FS_CATALOG_URL or catalog.url");
        }
        if self.catalog.max_connections == 0 {
            return invalid("catalog.max_connections must be at least 1");
        }
        if self.wal.segment_max_bytes < 1024 * 1024 {
            return invalid("wal.segment_max_bytes must be at least 1 MiB");
        }
        self.validate_compactor()?;
        self.ingest.to_ingest_config()?;
        if self.ingest.flush_rows == 0 || self.ingest.flush_bytes == 0 {
            return invalid("ingest.flush_rows and ingest.flush_bytes must be positive");
        }
        let retention_floor = fs_query::files::STALE_AFTER.as_secs()
            + fs_query::hot::HotWindowConfig::default().timeout.as_secs()
            + 1;
        if self.ingest.persisted_retain_secs < retention_floor {
            return invalid(
                "ingest.persisted_retain_secs must cover the querier's file cache TTL plus a hot scan",
            );
        }
        if self.ingest.coalesce_batches < 2 {
            return invalid("ingest.coalesce_batches must be at least 2");
        }
        if self.ingest.coalesce_bytes == 0
            || self.ingest.max_conversions == 0
            || self.ingest.max_coalesces == 0
            || self.ingest.max_flushes == 0
            || self.ingest.max_confirms == 0
            || self.ingest.confirm_timeout_secs == 0
            || self.ingest.max_hours_per_cut == 0
        {
            return invalid(
                "ingest.coalesce_bytes, ingest.max_conversions, ingest.max_coalesces, ingest.max_flushes, ingest.max_confirms, ingest.confirm_timeout_secs and ingest.max_hours_per_cut must be positive",
            );
        }
        if self.ingest.row_group_rows < 1024
            || self.ingest.row_group_rows > fs_ingest::flush::MAX_ROW_GROUP_ROWS
        {
            return invalid("ingest.row_group_rows must be between 1024 and 1048576");
        }
        if self.ingest.max_buffer_bytes > self.ingest.max_tenant_bytes
            || self.ingest.max_tenant_bytes > self.ingest.max_memory_bytes
        {
            return invalid(
                "ingest.max_buffer_bytes <= ingest.max_tenant_bytes <= ingest.max_memory_bytes must hold",
            );
        }
        self.validate_ingest_bounds()?;
        self.validate_server()?;
        self.validate_access()?;
        if self.query.max_rows == 0
            || self.query.timeout_secs == 0
            || self.query.memory_bytes < 64 * 1024 * 1024
        {
            return invalid(
                "query.max_rows and query.timeout_secs must be positive and query.memory_bytes at least 64 MiB",
            );
        }
        if self.query.prepared_plans == 0
            || self.query.prepared_bytes == 0
            || self.query.prepared_ttl_secs == 0
        {
            return invalid(
                "query.prepared_plans, query.prepared_bytes and query.prepared_ttl_secs must be positive",
            );
        }
        if self.query.prepared_ttl_secs > fs_query::querier::MAX_PREPARED_TTL.as_secs() {
            return invalid("query.prepared_ttl_secs is at most 86400");
        }
        Ok(())
    }

    /// The catalog URL: environment first, then the file.
    pub fn catalog_url<'a>(&'a self, secrets: &'a Secrets) -> Option<&'a str> {
        secrets
            .catalog_url
            .as_deref()
            .or(self.catalog.url.as_deref())
    }

    /// Node id: configured, else `HOSTNAME`, else `node-0`.
    pub fn node_id(&self, get: &dyn Fn(&str) -> Option<String>) -> String {
        self.server
            .node_id
            .clone()
            .or_else(|| get("HOSTNAME").filter(|h| !h.is_empty()))
            .unwrap_or_else(|| "node-0".to_owned())
    }
}

/// Hides the password of a URL for logs: `postgres://u:***@host/db`.
pub fn redact_url(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_owned();
    };
    let Some((userinfo, host)) = rest.split_once('@') else {
        return url.to_owned();
    };
    match userinfo.split_once(':') {
        Some((user, _)) => format!("{scheme}://{user}:***@{host}"),
        None => url.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
        [bucket]
        kind = "local"
        path = "/tmp/bucket"
    "#;

    fn env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |key| {
            pairs
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| (*v).to_owned())
        }
    }

    #[test]
    fn minimal_file_gets_defaults() {
        let config = Config::parse(MINIMAL).unwrap();
        assert_eq!(config.server.role, Role::All);
        assert_eq!(config.catalog.max_connections, 8);
        assert!(config.catalog.migrate);
        assert_eq!(config.wal.dir, PathBuf::from("./data/wal"));
        assert_eq!(config.compactor.quarantine_ttl_secs, 3600);
        assert_eq!(config.log.format, LogFormat::Text);
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let err =
            Config::parse("[bucket]\nkind = \"local\"\npath = \"/x\"\ncolour = 1\n").unwrap_err();
        assert!(matches!(err, ServerError::ParseConfig(_)), "{err}");
    }

    #[test]
    fn env_overrides_and_secrets() {
        let mut config = Config::parse(MINIMAL).unwrap();
        let get = env(&[
            ("FS_ROLE", "querier"),
            ("FS_NODE_ID", "pod-7"),
            ("FS_CATALOG_URL", "postgres://u:hunter2@db/x"),
            ("FS_BUCKET_ACCESS_KEY", "AKIAEXAMPLEKEY"),
            ("FS_BUCKET_SECRET_KEY", "wJalrXUtnFEMI"),
            ("FS_LOG_LEVEL", "debug"),
        ]);
        config.apply_env(&get).unwrap();
        let secrets = Secrets::from_env(&get);
        assert_eq!(config.server.role, Role::Querier);
        assert_eq!(config.node_id(&get), "pod-7");
        assert_eq!(config.log.level, "debug");
        assert_eq!(
            config.catalog_url(&secrets),
            Some("postgres://u:hunter2@db/x")
        );
        let debug = format!("{secrets:?}");
        assert!(!debug.contains("hunter2"), "{debug}");
        assert!(!debug.contains("AKIAEXAMPLEKEY"), "{debug}");
        assert!(!debug.contains("wJalrXUtnFEMI"), "{debug}");
        assert!(debug.contains("postgres://u:***@db/x"));
        assert!(config.validate(&secrets).is_ok());
    }

    #[test]
    fn env_role_must_parse() {
        let mut config = Config::parse(MINIMAL).unwrap();
        let err = config.apply_env(&env(&[("FS_ROLE", "boss")])).unwrap_err();
        assert!(matches!(err, ServerError::InvalidConfig(_)));
    }

    #[test]
    fn validation_catches_inconsistencies() {
        let secrets = Secrets {
            catalog_url: Some("postgres://x".into()),
            ..Secrets::default()
        };
        let short_quarantine = Config::parse(
            "[bucket]\nkind = \"local\"\npath = \"/x\"\n[query]\ntimeout_secs = 60\n[compactor]\nquarantine_ttl_secs = 64\n",
        )
        .unwrap();
        assert!(
            short_quarantine
                .validate(&secrets)
                .unwrap_err()
                .to_string()
                .contains("quarantine_ttl_secs"),
            "a querier may still read a quarantined file for its cache TTL plus one query"
        );
        for (bad, key) in [
            ("sweep_delete_backoff_secs = 0", "sweep_"),
            ("hour_close_delay_secs = 10", "hour_close_delay_secs"),
            (
                "hour_close_delay_secs = 18446744073709551615\n[query]\ntimeout_secs = 18446744073709551615",
                "hour_close_delay_secs",
            ),
        ] {
            let err = Config::parse(&format!(
                "[bucket]\nkind = \"local\"\npath = \"/x\"\n[compactor]\n{bad}\n"
            ))
            .unwrap()
            .validate(&secrets)
            .unwrap_err()
            .to_string();
            assert!(err.contains(key), "{err}");
        }
        let long_enough = Config::parse(
            "[bucket]\nkind = \"local\"\npath = \"/x\"\n[query]\ntimeout_secs = 60\n[compactor]\nquarantine_ttl_secs = 65\n",
        )
        .unwrap();
        assert!(long_enough.validate(&secrets).is_ok());

        let no_name = Config::parse("[bucket]\nkind = \"s3\"\n").unwrap();
        assert!(
            no_name
                .validate(&secrets)
                .unwrap_err()
                .to_string()
                .contains("bucket.name")
        );

        let http =
            Config::parse("[bucket]\nkind = \"s3\"\nname = \"b\"\nendpoint = \"http://s3:9000\"\n")
                .unwrap();
        assert!(
            http.validate(&secrets)
                .unwrap_err()
                .to_string()
                .contains("allow_http")
        );

        let half = Secrets {
            bucket_access_key: Some("ak".into()),
            ..secrets.clone()
        };
        let ok_s3 = Config::parse("[bucket]\nkind = \"s3\"\nname = \"b\"\n").unwrap();
        assert!(
            ok_s3
                .validate(&half)
                .unwrap_err()
                .to_string()
                .contains("together")
        );
        assert!(ok_s3.validate(&secrets).is_ok());

        let no_url = Config::parse(MINIMAL).unwrap();
        assert!(
            no_url
                .validate(&Secrets::default())
                .unwrap_err()
                .to_string()
                .contains("catalog url")
        );
    }

    #[test]
    fn ingest_section_converts_and_validates() {
        let config = Config::parse(
            "[bucket]\nkind = \"local\"\npath = \"/x\"\n[ingest]\nlisten = \"127.0.0.1:4317\"\nflush_rows = 10\nanonymous_tenant = \"00000000-0000-0000-0000-000000000001\"\n",
        )
        .unwrap();
        let ingest = config.ingest.to_ingest_config().unwrap();
        assert_eq!(ingest.listen.port(), 4317);
        assert_eq!(ingest.flush_rows, 10);
        assert!(ingest.anonymous_tenant.is_some());
        assert_eq!(ingest.flush_interval, std::time::Duration::from_secs(30));

        let bad = Config::parse(
            "[bucket]\nkind = \"local\"\npath = \"/x\"\n[ingest]\nlisten = \"nope\"\n",
        )
        .unwrap();
        let secrets = Secrets {
            catalog_url: Some("postgres://x".into()),
            ..Secrets::default()
        };
        assert!(
            bad.validate(&secrets)
                .unwrap_err()
                .to_string()
                .contains("ingest.listen")
        );

        for (secs, accepted) in [(7u64, false), (8, true)] {
            let config = Config::parse(&format!(
                "[bucket]\nkind = \"local\"\npath = \"/x\"\n[ingest]\npersisted_retain_secs = {secs}\n"
            ))
            .unwrap();
            let outcome = config.validate(&secrets);
            assert_eq!(outcome.is_ok(), accepted, "retention {secs}s: {outcome:?}");
            if let Err(e) = outcome {
                assert!(e.to_string().contains("persisted_retain_secs"), "{e}");
            }
        }

        let inverted = Config::parse(
            "[bucket]\nkind = \"local\"\npath = \"/x\"\n[ingest]\nmax_tenant_bytes = 4096\nmax_memory_bytes = 1024\n",
        )
        .unwrap();
        assert!(
            inverted
                .validate(&secrets)
                .unwrap_err()
                .to_string()
                .contains("max_tenant_bytes")
        );
    }

    #[test]
    fn wal_queue_fields_convert_and_validate() {
        let config = Config::parse(
            "[bucket]\nkind = \"local\"\npath = \"/x\"\n[ingest]\nwal_batch_records = 7\nwal_batch_bytes = 8\nwal_linger_ms = 9\nwal_queue_records = 10\nwal_queue_bytes = 11\nwal_queue_wait_ms = 12\n",
        )
        .unwrap();
        let gc = config.ingest.to_ingest_config().unwrap().group_commit;
        assert_eq!(gc.max_records, 7);
        assert_eq!(gc.max_bytes, 8);
        assert_eq!(gc.linger, std::time::Duration::from_millis(9));
        assert_eq!(gc.max_inflight_records, 10);
        assert_eq!(gc.max_inflight_bytes, 11);
        assert_eq!(gc.queue_wait, std::time::Duration::from_millis(12));

        let secrets = Secrets {
            catalog_url: Some("postgres://x".into()),
            ..Secrets::default()
        };
        let huge = Config::parse(&format!(
            "[bucket]\nkind = \"local\"\npath = \"/x\"\n[ingest]\nwal_queue_records = {}\n",
            usize::MAX
        ))
        .unwrap();
        assert!(
            huge.validate(&secrets)
                .unwrap_err()
                .to_string()
                .contains("wal_queue_records")
        );
        let zero = Config::parse(
            "[bucket]\nkind = \"local\"\npath = \"/x\"\n[ingest]\nwal_queue_bytes = 0\n",
        )
        .unwrap();
        assert!(
            zero.validate(&secrets)
                .unwrap_err()
                .to_string()
                .contains("wal_queue")
        );
    }

    #[test]
    fn prepared_plan_limits_are_validated() {
        let secrets = Secrets {
            catalog_url: Some("postgres://x".into()),
            ..Secrets::default()
        };
        for bad in [
            "prepared_plans = 0",
            "prepared_bytes = 0",
            "prepared_ttl_secs = 0",
            "prepared_ttl_secs = 86401",
        ] {
            let err = Config::parse(&format!(
                "[bucket]\nkind = \"local\"\npath = \"/x\"\n[query]\n{bad}\n"
            ))
            .unwrap()
            .validate(&secrets)
            .unwrap_err()
            .to_string();
            assert!(err.contains("prepared"), "{err}");
        }
        let limits = Config::parse(
            "[bucket]\nkind = \"local\"\npath = \"/x\"\n[query]\nprepared_plans = 7\nprepared_bytes = 8\nprepared_ttl_secs = 9\n",
        )
        .unwrap()
        .query
        .to_limits();
        assert_eq!(limits.prepared_plans, 7);
        assert_eq!(limits.prepared_bytes, 8);
        Config::parse(
            "[bucket]\nkind = \"local\"\npath = \"/x\"\n[query]\nprepared_ttl_secs = 86400\n",
        )
        .unwrap()
        .validate(&secrets)
        .unwrap();
        assert_eq!(limits.prepared_ttl, std::time::Duration::from_secs(9));
    }

    #[test]
    fn server_clocks_are_bounded() {
        let secrets = Secrets {
            catalog_url: Some("postgres://x".into()),
            ..Secrets::default()
        };
        for key in ["health_interval_secs", "drain_timeout_secs"] {
            for bad in [0, 86_401] {
                let err = Config::parse(&format!(
                    "[bucket]\nkind = \"local\"\npath = \"/x\"\n[server]\n{key} = {bad}\n"
                ))
                .unwrap()
                .validate(&secrets)
                .unwrap_err()
                .to_string();
                assert!(err.contains("server."), "{key} = {bad}: {err}");
            }
            for good in [1, 86_400] {
                Config::parse(&format!(
                    "[bucket]\nkind = \"local\"\npath = \"/x\"\n[server]\n{key} = {good}\n"
                ))
                .unwrap()
                .validate(&secrets)
                .unwrap_or_else(|e| panic!("{key} = {good}: {e}"));
            }
        }
    }

    #[test]
    fn hour_limits_are_validated() {
        let secrets = Secrets {
            catalog_url: Some("postgres://x".into()),
            ..Secrets::default()
        };
        for (bad, key) in [
            ("max_hours_per_cut = 65", "max_hours_per_cut"),
            ("max_hours_per_cut = 0", "max_hours_per_cut"),
            ("max_hours_per_batch = 0", "max_hours_per_batch"),
            (
                "max_hours_per_batch = 10\nmax_live_hours = 9",
                "max_live_hours",
            ),
        ] {
            let err = Config::parse(&format!(
                "[bucket]\nkind = \"local\"\npath = \"/x\"\n[ingest]\n{bad}\n"
            ))
            .unwrap()
            .validate(&secrets)
            .unwrap_err()
            .to_string();
            assert!(err.contains(key), "{err}");
        }
        let edge = Config::parse(
            "[bucket]\nkind = \"local\"\npath = \"/x\"\n[ingest]\nmax_hours_per_cut = 64\nmax_hours_per_batch = 9\nmax_live_hours = 9\n",
        )
        .unwrap();
        assert!(edge.validate(&secrets).is_ok());
    }

    #[test]
    fn retention_knobs_convert_and_validate() {
        let secrets = Secrets {
            catalog_url: Some("postgres://x".into()),
            ..Secrets::default()
        };
        let config = Config::parse(MINIMAL).unwrap();
        let compactor = config.compactor.to_compactor(config.ingest.row_group_rows);
        assert_eq!(compactor.retention_interval.as_secs(), 3600);
        assert_eq!(compactor.retention.files_per_pass, 1000);
        assert_eq!(compactor.retention.rebuild_max_input_bytes, 256 << 30);
        assert_eq!(compactor.retention.rebuild_max_bytes, 2 << 30);
        assert_eq!(
            compactor.retention.batch_rows,
            fs_compactor::retention::REBUILD_BATCH_ROWS
        );
        assert_eq!(
            compactor.retention.limits,
            fs_schema::limits::Limits::default()
        );
        assert_eq!(compactor.retention.inject_swap_failures, 0);
        assert_eq!(compactor.retention.rebuilds_per_tick, 64);
        assert_eq!(compactor.retention.rebuild_backoff.as_secs(), 3600);
        let floor = fs_compactor::retention::rebuild_footprint(
            0,
            0,
            config.compactor.l1_target_bytes as u64,
            0,
            &compactor.retention,
        );
        assert!(
            floor < 2 << 30,
            "the default budget admits a rebuild: {floor}"
        );
        for bad in [
            "retention_interval_secs = 0",
            "retention_files_per_pass = 0",
            "retention_files_per_pass = -1",
            "rebuild_max_input_bytes = 0",
            "rebuilds_per_tick = 0",
            "rebuild_backoff_secs = 0",
        ] {
            let err = Config::parse(&format!(
                "[bucket]\nkind = \"local\"\npath = \"/x\"\n[compactor]\n{bad}\n"
            ))
            .unwrap()
            .validate(&secrets)
            .unwrap_err()
            .to_string();
            assert!(
                err.contains("compactor.retention_interval_secs"),
                "{bad}: {err}"
            );
        }
        let err = Config::parse(
            "[bucket]\nkind = \"local\"\npath = \"/x\"\n[compactor]\nrebuild_max_bytes = 1048576\n",
        )
        .unwrap()
        .validate(&secrets)
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("compactor.rebuild_max_bytes must cover one L1 file"),
            "{err}"
        );
        let with = |rebuilds: usize| {
            Config::parse(&format!(
                "[bucket]\nkind = \"local\"\npath = \"/x\"\n[compactor]\nrebuilds_per_tick = {rebuilds}\n"
            ))
            .unwrap()
        };
        let most = fs_compactor::retention::MAX_REBUILDS_PER_TICK;
        assert!(with(most).validate(&secrets).is_ok());
        assert!(
            with(most + 1)
                .validate(&secrets)
                .unwrap_err()
                .to_string()
                .contains("compactor.rebuilds_per_tick must be at most 256")
        );
    }

    #[test]
    fn derived_builder_caps_convert_and_validate() {
        let secrets = Secrets {
            catalog_url: Some("postgres://x".into()),
            ..Secrets::default()
        };
        let config = Config::parse(MINIMAL).unwrap();
        let compactor = config.compactor.to_compactor(config.ingest.row_group_rows);
        assert_eq!(compactor.compaction.trace_summary_max_traces, 4_000_000);
        assert_eq!(compactor.compaction.trace_summary_max_bytes, 256 << 20);
        assert_eq!(compactor.compaction.service_graph_max_spans, 4_000_000);
        assert_eq!(compactor.compaction.service_graph_max_bytes, 256 << 20);
        for bad in [
            "trace_summary_max_traces = 0",
            "service_graph_max_spans = 0",
            "trace_summary_max_bytes = 1",
            "service_graph_max_bytes = 1",
            "trace_summary_max_bytes = 1073741824",
        ] {
            let err = Config::parse(&format!(
                "[bucket]\nkind = \"local\"\npath = \"/x\"\n[compactor]\n{bad}\n"
            ))
            .unwrap()
            .validate(&secrets)
            .unwrap_err()
            .to_string();
            assert!(
                err.contains("derived-table count and byte caps"),
                "{bad}: {err}"
            );
        }
    }

    #[test]
    fn max_confirms_is_bounded_by_what_the_runtime_can_track() {
        let secrets = Secrets {
            catalog_url: Some("postgres://x".into()),
            ..Secrets::default()
        };
        let with = |max_confirms: usize| {
            Config::parse(&format!(
                "[bucket]\nkind = \"local\"\npath = \"/x\"\n[ingest]\nmax_confirms = {max_confirms}\n"
            ))
            .unwrap()
        };
        let most = tokio::sync::Semaphore::MAX_PERMITS;
        assert!(with(most).validate(&secrets).is_ok());
        assert!(
            with(most + 1)
                .validate(&secrets)
                .unwrap_err()
                .to_string()
                .contains("max_confirms")
        );
    }

    #[test]
    fn node_id_falls_back_to_hostname() {
        let config = Config::parse(MINIMAL).unwrap();
        assert_eq!(
            config.node_id(&env(&[("HOSTNAME", "ingester-3")])),
            "ingester-3"
        );
        assert_eq!(config.node_id(&env(&[])), "node-0");
    }

    #[test]
    fn redacts_passwords_only() {
        assert_eq!(
            redact_url("postgres://u:p@h:5432/db?x=1"),
            "postgres://u:***@h:5432/db?x=1"
        );
        assert_eq!(redact_url("postgres://h/db"), "postgres://h/db");
        assert_eq!(redact_url("postgres://u@h/db"), "postgres://u@h/db");
        assert_eq!(redact_url("not a url"), "not a url");
    }
}

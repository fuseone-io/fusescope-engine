//! Ingest configuration.

use std::net::SocketAddr;
use std::time::Duration;

use fs_catalog::TenantId;
use fs_schema::limits::Limits;

/// Tunables of the ingest path. Defaults follow DESIGN.md.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestConfig {
    /// OTLP gRPC listen address.
    pub listen: SocketAddr,
    /// Largest accepted gRPC message. Larger requests fail with RESOURCE_EXHAUSTED.
    pub max_message_bytes: usize,
    /// Flush a tenant buffer at this many rows.
    pub flush_rows: usize,
    /// Flush a tenant buffer at this many bytes of Arrow memory.
    pub flush_bytes: usize,
    /// Flush a tenant buffer once its oldest record is this old.
    pub flush_interval: Duration,
    /// Merge a tenant's complete request batches into one chunk once this
    /// many are waiting, so a flush concatenates a few large batches.
    pub coalesce_batches: usize,
    /// Most counted bytes merged into one chunk at a time, Arrow memory plus
    /// the bound of each batch's trace set; bounds the working memory of a
    /// merge, which counts against `max_memory_bytes`.
    pub coalesce_bytes: usize,
    /// Requests converted to Arrow at the same time; bounds the memory of
    /// batches built but not yet admitted, and the CPU the request path uses.
    pub max_conversions: usize,
    /// Chunks built at the same time on the blocking pool.
    pub max_coalesces: usize,
    /// Files written at the same time; a flush is cut inline and encoded,
    /// uploaded and registered on its own task.
    pub max_flushes: usize,
    /// Rows per Parquet row group. A flush gathers, writes and flushes one
    /// row group at a time, so this bounds what the writer holds.
    pub row_group_rows: usize,
    /// Refuse new data for a tenant whose live and flushing rows would exceed
    /// this; flushes are failing or falling behind.
    pub max_buffer_bytes: usize,
    /// Refuse new data for a tenant whose rows in every state, persisted for
    /// the hot window included, would exceed this. One tenant can never hold
    /// the whole `max_memory_bytes` and starve the others.
    pub max_tenant_bytes: usize,
    /// Refuse new data when everything held in memory, live, flushing and
    /// persisted for the hot window, exceeds this. Live and flushing rows
    /// also keep room for the scratch their flush will need, three copies,
    /// so rows once admitted can always be written out. Persisted rows are
    /// never evicted early to make room: queriers may still need them.
    pub max_memory_bytes: usize,
    /// Refuse new data when the WAL on disk exceeds this.
    pub wal_max_bytes: u64,
    /// Per-tenant throughput limit in bytes per second of request payload.
    /// `None` disables it.
    pub tenant_bytes_per_sec: Option<u64>,
    /// Accept unauthenticated requests as this tenant. Development only.
    pub anonymous_tenant: Option<TenantId>,
    /// Attribute limits applied during conversion.
    pub limits: Limits,
    /// How long a resolved token stays cached before the catalog is asked again.
    pub token_cache_ttl: Duration,
    /// Internal Arrow Flight address serving the hot window to queriers.
    /// `None` disables it.
    pub hot_listen: Option<SocketAddr>,
    /// How long a file's rows stay served from memory after it is registered,
    /// so a querier whose file list predates the file still sees them. Must
    /// cover the querier's file cache TTL plus one hot scan: the querier
    /// scans the hot window first and lists files after.
    pub persisted_retain: Duration,
    /// Confirmations retried at the same time, off the flusher.
    pub max_confirms: usize,
    /// Most confirmations one retry pass takes.
    pub confirm_batch: usize,
    /// How often confirmations due for a retry are looked for.
    pub confirm_interval: Duration,
    /// Advance the WAL checkpoint after a successful flush. Always true in
    /// production; tests set it to false to simulate a crash between
    /// registration and checkpoint.
    pub checkpoint_after_flush: bool,
    /// Make this many catalog registrations fail artificially. Zero in
    /// production; tests use it to exercise the retry path.
    pub inject_register_failures: u32,
    /// Make this many registration confirmations fail artificially, after
    /// the file is registered. Zero in production; tests use it to crash or
    /// retry between the registration and its confirmation.
    pub inject_confirm_failures: u32,
    /// Make this many trace index uploads fail artificially, after the
    /// Parquet object is in the bucket. Zero in production; tests use it to
    /// crash between the two uploads.
    pub inject_index_failures: u32,
    /// Hold each injected registration failure this long before failing.
    /// `None` in production; tests use it to act while a flush is pending.
    pub inject_register_delay: Option<Duration>,
    /// Hold the first request this long between its WAL append and its
    /// buffer push, so a later request overtakes it. `None` in production;
    /// tests use it to reorder concurrent requests deterministically.
    pub inject_push_delay: Option<Duration>,
    /// Hold the first retried flush this long before it writes, while it
    /// keeps its flush permit. `None` in production; tests use it to make
    /// other retries wait for a permit.
    pub inject_retry_delay: Option<Duration>,
    /// WAL group commit limits.
    pub group_commit: crate::walwriter::GroupCommitConfig,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            listen: "0.0.0.0:4317"
                .parse()
                .unwrap_or(SocketAddr::from(([0, 0, 0, 0], 4317))),
            max_message_bytes: 16 * 1024 * 1024,
            flush_rows: 1_000_000,
            flush_bytes: 128 * 1024 * 1024,
            flush_interval: Duration::from_secs(30),
            coalesce_batches: 512,
            coalesce_bytes: 64 * 1024 * 1024,
            max_conversions: default_parallelism(),
            max_coalesces: default_coalesces(),
            max_flushes: 2,
            row_group_rows: crate::flush::DEFAULT_ROW_GROUP_ROWS,
            max_buffer_bytes: 512 * 1024 * 1024,
            max_tenant_bytes: 1024 * 1024 * 1024,
            max_memory_bytes: 2 * 1024 * 1024 * 1024,
            wal_max_bytes: 4 * 1024 * 1024 * 1024,
            tenant_bytes_per_sec: None,
            anonymous_tenant: None,
            limits: Limits::default(),
            token_cache_ttl: Duration::from_secs(60),
            hot_listen: None,
            persisted_retain: Duration::from_secs(8),
            max_confirms: 4,
            confirm_batch: 64,
            confirm_interval: Duration::from_secs(1),
            checkpoint_after_flush: true,
            inject_register_failures: 0,
            inject_confirm_failures: 0,
            inject_index_failures: 0,
            inject_register_delay: None,
            inject_push_delay: None,
            inject_retry_delay: None,
            group_commit: crate::walwriter::GroupCommitConfig::default(),
        }
    }
}

/// Available cores, or 4 when the runtime cannot tell.
pub fn default_parallelism() -> usize {
    std::thread::available_parallelism().map_or(4, std::num::NonZero::get)
}

/// Half the cores, at least one: merges share the machine with the request path.
pub fn default_coalesces() -> usize {
    (default_parallelism() / 2).max(1)
}

//! Catalog domain types and their database encodings.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::CatalogError;

/// Tenant identifier.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    sqlx::Type,
    serde::Serialize,
    serde::Deserialize,
)]
#[sqlx(transparent)]
#[serde(transparent)]
pub struct TenantId(pub Uuid);

impl TenantId {
    /// A fresh random id.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for TenantId {
    fn default() -> Self {
        Self::new()
    }
}

/// File identifier. Version 7 UUIDs sort by creation time.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    sqlx::Type,
    serde::Serialize,
    serde::Deserialize,
)]
#[sqlx(transparent)]
#[serde(transparent)]
pub struct FileId(pub Uuid);

impl FileId {
    /// A fresh time-ordered id.
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// An id derived from what a flush contains, so a retried or replayed
    /// flush of the same rows produces the same id and path, and registering
    /// it twice is idempotent. `instance` identifies the write-ahead log
    /// directory (two ingesters start at the same positions), and `segment`
    /// and `offset` locate the first record of the flushed buffer in it.
    pub fn derived(instance: Uuid, tenant: TenantId, segment: u64, offset: u64) -> Self {
        let mut name = Vec::with_capacity(48);
        name.extend_from_slice(instance.as_bytes());
        name.extend_from_slice(tenant.0.as_bytes());
        name.extend_from_slice(&segment.to_le_bytes());
        name.extend_from_slice(&offset.to_le_bytes());
        Self(Uuid::new_v5(&FILE_NAMESPACE, &name))
    }
}

/// Namespace for derived file ids. Fixed forever; changing it changes every id.
const FILE_NAMESPACE: Uuid = Uuid::from_bytes([
    0x5f, 0x73, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x66, 0x69, 0x6c, 0x65, 0x69, 0x64, 0x30, 0x31,
]);

impl Default for FileId {
    fn default() -> Self {
        Self::new()
    }
}

/// What a file holds. Stored as a small integer; codes are part of the on-disk
/// contract and never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Signal {
    /// Span rows.
    Traces,
    /// Log rows.
    Logs,
    /// Metric samples.
    MetricSamples,
    /// Metric series (fingerprint to labels).
    MetricSeries,
    /// Derived: per-trace summary.
    TraceSummary,
    /// Derived: service graph per minute.
    ServiceGraph1m,
    /// Derived: duration top-k per hour.
    DurationSort,
    /// Derived: usage per hour.
    Usage1h,
    /// Derived: attribute keys.
    AttributeKeys,
    /// Derived: 5 minute metric rollup.
    Rollup5m,
    /// Derived: 30 minute metric rollup.
    Rollup30m,
}

impl Signal {
    /// Database code.
    pub fn code(self) -> i16 {
        match self {
            Signal::Traces => 1,
            Signal::Logs => 2,
            Signal::MetricSamples => 3,
            Signal::MetricSeries => 4,
            Signal::TraceSummary => 10,
            Signal::ServiceGraph1m => 11,
            Signal::DurationSort => 12,
            Signal::Usage1h => 13,
            Signal::AttributeKeys => 14,
            Signal::Rollup5m => 15,
            Signal::Rollup30m => 16,
        }
    }

    /// Signal for a database code.
    ///
    /// # Errors
    /// Unknown code.
    pub fn from_code(code: i16) -> Result<Self, CatalogError> {
        Ok(match code {
            1 => Signal::Traces,
            2 => Signal::Logs,
            3 => Signal::MetricSamples,
            4 => Signal::MetricSeries,
            10 => Signal::TraceSummary,
            11 => Signal::ServiceGraph1m,
            12 => Signal::DurationSort,
            13 => Signal::Usage1h,
            14 => Signal::AttributeKeys,
            15 => Signal::Rollup5m,
            16 => Signal::Rollup30m,
            other => {
                return Err(CatalogError::InvalidValue {
                    column: "signal",
                    value: i64::from(other),
                });
            }
        })
    }

    /// Bucket path segment for this signal.
    pub fn path_name(self) -> &'static str {
        match self {
            Signal::Traces => "traces",
            Signal::Logs => "logs",
            Signal::MetricSamples => "metrics/samples",
            Signal::MetricSeries => "metrics/series",
            Signal::TraceSummary => "traces/derived/trace_summary",
            Signal::ServiceGraph1m => "traces/derived/service_graph_1m",
            Signal::DurationSort => "traces/derived/duration_sort",
            Signal::Usage1h => "derived/usage_1h",
            Signal::AttributeKeys => "derived/attribute_keys",
            Signal::Rollup5m => "metrics/derived/rollup_5m",
            Signal::Rollup30m => "metrics/derived/rollup_30m",
        }
    }
}

/// Compaction level of a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Level {
    /// Fresh from an ingester flush.
    L0,
    /// Closed hour, re-sorted.
    L1,
    /// Closed day.
    L2,
}

impl Level {
    /// Database code.
    pub fn code(self) -> i16 {
        match self {
            Level::L0 => 0,
            Level::L1 => 1,
            Level::L2 => 2,
        }
    }

    /// Level for a database code.
    ///
    /// # Errors
    /// Unknown code.
    pub fn from_code(code: i16) -> Result<Self, CatalogError> {
        Ok(match code {
            0 => Level::L0,
            1 => Level::L1,
            2 => Level::L2,
            other => {
                return Err(CatalogError::InvalidValue {
                    column: "level",
                    value: i64::from(other),
                });
            }
        })
    }
}

/// Lifecycle state of a file row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FileState {
    /// Reserved for a two-phase register; not used by the current API.
    Writing,
    /// Readable by queriers.
    Visible,
    /// Replaced or expired; kept until in-flight reads finish, then purged.
    Quarantined,
}

impl FileState {
    /// Database code.
    pub fn code(self) -> i16 {
        match self {
            FileState::Writing => 0,
            FileState::Visible => 1,
            FileState::Quarantined => 2,
        }
    }

    /// State for a database code.
    ///
    /// # Errors
    /// Unknown code.
    pub fn from_code(code: i16) -> Result<Self, CatalogError> {
        Ok(match code {
            0 => FileState::Writing,
            1 => FileState::Visible,
            2 => FileState::Quarantined,
            other => {
                return Err(CatalogError::InvalidValue {
                    column: "state",
                    value: i64::from(other),
                });
            }
        })
    }
}

/// Inclusive time range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeRange {
    /// Start, inclusive.
    pub start: DateTime<Utc>,
    /// End, inclusive.
    pub end: DateTime<Utc>,
}

/// A tenant to create.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTenant {
    /// Unique human readable name.
    pub name: String,
    /// Days of data kept.
    pub retention_days: i32,
    /// New attribute keys allowed per hour.
    pub max_keys_hour: i32,
    /// Bytes one query may scan.
    pub bytes_budget: i64,
    /// Queries per second.
    pub qps_limit: i32,
}

impl NewTenant {
    /// A tenant with the DESIGN.md defaults.
    pub fn with_defaults(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            retention_days: 15,
            max_keys_hour: 5000,
            bytes_budget: 10 * 1024 * 1024 * 1024,
            qps_limit: 50,
        }
    }
}

/// A tenant row.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct Tenant {
    /// Id.
    pub id: TenantId,
    /// Name.
    pub name: String,
    /// Days of data kept.
    pub retention_days: i32,
    /// New attribute keys allowed per hour.
    pub max_keys_hour: i32,
    /// Bytes one query may scan.
    pub bytes_budget: i64,
    /// Queries per second.
    pub qps_limit: i32,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Soft delete time.
    pub deleted_at: Option<DateTime<Utc>>,
}

/// A file to register, as written by an ingester or compactor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewFile {
    /// Id chosen by the writer so the bucket path can embed it.
    pub id: FileId,
    /// Owner.
    pub tenant_id: TenantId,
    /// Content.
    pub signal: Signal,
    /// Compaction level.
    pub level: Level,
    /// Bucket path, unique.
    pub path: String,
    /// Object size.
    pub size_bytes: i64,
    /// Rows in the file.
    pub row_count: i64,
    /// Time span covered.
    pub range: TimeRange,
    /// Distinct service names, for coarse pruning.
    pub services: Vec<String>,
    /// Opaque binary summary for the querier (small filters, promoted column stats).
    pub summary: Option<Vec<u8>>,
}

/// A file row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRecord {
    /// Id.
    pub id: FileId,
    /// Owner.
    pub tenant_id: TenantId,
    /// Content.
    pub signal: Signal,
    /// Compaction level.
    pub level: Level,
    /// Bucket path.
    pub path: String,
    /// Object size.
    pub size_bytes: i64,
    /// Rows in the file.
    pub row_count: i64,
    /// Time span covered.
    pub range: TimeRange,
    /// Distinct service names.
    pub services: Vec<String>,
    /// Opaque binary summary.
    pub summary: Option<Vec<u8>>,
    /// Lifecycle state.
    pub state: FileState,
    /// Registration time.
    pub created_at: DateTime<Utc>,
    /// Quarantine time, when quarantined.
    pub quarantined_at: Option<DateTime<Utc>>,
}

/// Raw row shape; converted into [`FileRecord`] with validation.
#[derive(sqlx::FromRow)]
pub(crate) struct FileRow {
    pub(crate) id: FileId,
    pub(crate) tenant_id: TenantId,
    pub(crate) signal: i16,
    pub(crate) level: i16,
    pub(crate) path: String,
    pub(crate) size_bytes: i64,
    pub(crate) row_count: i64,
    pub(crate) ts_min: DateTime<Utc>,
    pub(crate) ts_max: DateTime<Utc>,
    pub(crate) services: Vec<String>,
    pub(crate) summary: Option<Vec<u8>>,
    pub(crate) state: i16,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) quarantined_at: Option<DateTime<Utc>>,
}

impl TryFrom<FileRow> for FileRecord {
    type Error = CatalogError;

    fn try_from(row: FileRow) -> Result<Self, CatalogError> {
        Ok(FileRecord {
            id: row.id,
            tenant_id: row.tenant_id,
            signal: Signal::from_code(row.signal)?,
            level: Level::from_code(row.level)?,
            path: row.path,
            size_bytes: row.size_bytes,
            row_count: row.row_count,
            range: TimeRange {
                start: row.ts_min,
                end: row.ts_max,
            },
            services: row.services,
            summary: row.summary,
            state: FileState::from_code(row.state)?,
            created_at: row.created_at,
            quarantined_at: row.quarantined_at,
        })
    }
}

/// Record of one compaction swap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Compaction {
    /// Id.
    pub id: Uuid,
    /// Files quarantined.
    pub inputs: Vec<FileId>,
    /// Files made visible.
    pub outputs: Vec<FileId>,
}

/// A quarantined file removed from the catalog; the caller deletes the object.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct PurgedFile {
    /// Id.
    pub id: FileId,
    /// Bucket path to delete.
    pub path: String,
}

/// An ingester known to the catalog.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct Ingester {
    /// Stable id (pod name).
    pub id: String,
    /// Flight address for hot-window scans.
    pub address: String,
    /// Last heartbeat.
    pub last_heartbeat: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_codes_round_trip() {
        for s in [
            Signal::Traces,
            Signal::Logs,
            Signal::MetricSamples,
            Signal::MetricSeries,
            Signal::TraceSummary,
            Signal::ServiceGraph1m,
            Signal::DurationSort,
            Signal::Usage1h,
            Signal::AttributeKeys,
            Signal::Rollup5m,
            Signal::Rollup30m,
        ] {
            assert_eq!(Signal::from_code(s.code()).unwrap(), s);
        }
        assert!(Signal::from_code(99).is_err());
    }

    #[test]
    fn level_and_state_codes_round_trip() {
        for l in [Level::L0, Level::L1, Level::L2] {
            assert_eq!(Level::from_code(l.code()).unwrap(), l);
        }
        for s in [
            FileState::Writing,
            FileState::Visible,
            FileState::Quarantined,
        ] {
            assert_eq!(FileState::from_code(s.code()).unwrap(), s);
        }
        assert!(Level::from_code(3).is_err());
        assert!(FileState::from_code(-1).is_err());
    }

    #[test]
    fn derived_ids_are_stable_and_distinct() {
        let tenant = TenantId::new();
        let wal = Uuid::new_v4();
        let a = FileId::derived(wal, tenant, 3, 4096);
        assert_eq!(a, FileId::derived(wal, tenant, 3, 4096));
        assert_ne!(a, FileId::derived(wal, tenant, 3, 4097));
        assert_ne!(a, FileId::derived(wal, tenant, 4, 4096));
        assert_ne!(a, FileId::derived(wal, TenantId::new(), 3, 4096));
        assert_ne!(
            a,
            FileId::derived(Uuid::new_v4(), tenant, 3, 4096),
            "another ingester, another id"
        );
    }

    #[test]
    fn file_ids_sort_by_creation() {
        let a = FileId::new();
        let b = FileId::new();
        assert!(a <= b);
    }
}

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
    /// Compactor role.
    #[serde(default)]
    pub compactor: CompactorConfig,
    /// Logging.
    #[serde(default)]
    pub log: LogConfig,
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
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            role: default_role(),
            node_id: None,
            flight_addr: default_flight_addr(),
        }
    }
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
    /// Refuse a tenant's data when its buffer exceeds this while flushes fail.
    #[serde(default = "default_max_buffer_bytes")]
    pub max_buffer_bytes: usize,
    /// Refuse data when the WAL on disk exceeds this.
    #[serde(default = "default_wal_max_bytes")]
    pub wal_max_bytes: u64,
    /// Per-tenant throughput limit in bytes per second. Unset disables it.
    pub tenant_bytes_per_sec: Option<u64>,
    /// Accept unauthenticated requests as this tenant id. Development only.
    pub anonymous_tenant: Option<uuid::Uuid>,
}

impl Default for IngestSection {
    fn default() -> Self {
        Self {
            listen: default_ingest_listen(),
            max_message_bytes: default_max_message_bytes(),
            flush_rows: default_flush_rows(),
            flush_bytes: default_flush_bytes(),
            flush_interval_secs: default_flush_interval(),
            max_buffer_bytes: default_max_buffer_bytes(),
            wal_max_bytes: default_wal_max_bytes(),
            tenant_bytes_per_sec: None,
            anonymous_tenant: None,
        }
    }
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
            max_buffer_bytes: self.max_buffer_bytes,
            wal_max_bytes: self.wal_max_bytes,
            tenant_bytes_per_sec: self.tenant_bytes_per_sec,
            anonymous_tenant: self.anonymous_tenant.map(fs_catalog::TenantId),
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

fn default_max_buffer_bytes() -> usize {
    512 * 1024 * 1024
}

fn default_wal_max_bytes() -> u64 {
    4 * 1024 * 1024 * 1024
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
}

impl Default for CompactorConfig {
    fn default() -> Self {
        Self {
            purge_interval_secs: default_purge_interval(),
            quarantine_ttl_secs: default_quarantine_ttl(),
            purge_batch: default_purge_batch(),
        }
    }
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
        if let Some(v) = get("FS_LOG_LEVEL") {
            self.log.level = v;
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
        if self.compactor.purge_batch <= 0 {
            return invalid("compactor.purge_batch must be positive");
        }
        self.ingest.to_ingest_config()?;
        if self.ingest.flush_rows == 0 || self.ingest.flush_bytes == 0 {
            return invalid("ingest.flush_rows and ingest.flush_bytes must be positive");
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

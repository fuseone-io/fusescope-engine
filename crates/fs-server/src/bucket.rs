//! Object store construction and reachability probe.

use std::sync::Arc;

use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use crate::ServerError;
use crate::config::{BucketConfig, BucketKind, Secrets};

/// Builds the store described by `config`.
///
/// For `s3`, explicit credentials come from [`Secrets`]; without them the
/// client falls back to the AWS environment and instance metadata, which is
/// how Kubernetes workload identity is picked up.
///
/// # Errors
/// Invalid configuration or an unreadable local directory.
pub fn open(config: &BucketConfig, secrets: &Secrets) -> Result<Arc<dyn ObjectStore>, ServerError> {
    match config.kind {
        BucketKind::Local => {
            let path = config
                .path
                .as_ref()
                .ok_or_else(|| ServerError::InvalidConfig("bucket.path is required".into()))?;
            std::fs::create_dir_all(path).map_err(|source| ServerError::ReadConfig {
                path: path.clone(),
                source,
            })?;
            Ok(Arc::new(LocalFileSystem::new_with_prefix(path)?))
        }
        BucketKind::S3 => {
            // Conditional writes are required: the ingester never overwrites an
            // object and fails closed on a store that cannot promise that.
            let mut builder = AmazonS3Builder::from_env()
                .with_allow_http(config.allow_http)
                .with_conditional_put(S3ConditionalPut::ETagMatch);
            if let Some(name) = &config.name {
                builder = builder.with_bucket_name(name);
            }
            if let Some(endpoint) = &config.endpoint {
                builder = builder
                    .with_endpoint(endpoint)
                    .with_virtual_hosted_style_request(false);
            }
            if let Some(region) = &config.region {
                builder = builder.with_region(region);
            }
            if let (Some(ak), Some(sk)) = (&secrets.bucket_access_key, &secrets.bucket_secret_key) {
                builder = builder.with_access_key_id(ak).with_secret_access_key(sk);
            }
            Ok(Arc::new(builder.build()?))
        }
    }
}

/// Prefix for engine paths inside the bucket.
pub fn root(config: &BucketConfig) -> Path {
    config
        .prefix
        .as_deref()
        .map_or_else(|| Path::from(""), Path::from)
}

/// Confirms the store answers: lists one page under the engine prefix and
/// writes and deletes a marker under `_probe/`. The marker proves the
/// credentials allow writes, which every role except the querier needs.
///
/// # Errors
/// Any failure talking to the store.
pub async fn probe(store: &dyn ObjectStore, root: &Path, write: bool) -> Result<(), ServerError> {
    store.list_with_delimiter(Some(root)).await?;
    if write {
        let stamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let marker = Path::from(format!("{root}/_probe/{stamp}.txt"));
        store
            .put(&marker, PutPayload::from_static(b"fusescope-engine probe"))
            .await?;
        store.delete(&marker).await?;
    }
    Ok(())
}

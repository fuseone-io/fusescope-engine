//! Object store construction, reachability probe and prefix ownership.
//!
//! A prefix belongs to one catalog: the object `{prefix}/_catalog`, created
//! once and never rewritten, names the catalog's identity as
//! `fusescope-catalog v1\n{uuid}\n`. Every start reads it and refuses a
//! prefix another catalog marked; a prefix with objects and no marker is
//! taken only when the operator says so, since the orphan sweep would
//! otherwise delete what it does not know.

use std::sync::Arc;

use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use uuid::Uuid;

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

/// Name of the ownership marker under the prefix.
pub const OWNER_MARKER: &str = "_catalog";

/// What the start found under the prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ownership {
    /// The marker names this catalog.
    Held,
    /// The prefix was empty and is now marked for this catalog.
    Claimed,
    /// The prefix held objects without a marker; the operator adopted them.
    Adopted,
    /// The prefix is empty and this role may not write; the writer marks it.
    Unclaimed,
}

/// Reads the ownership marker under `root` and makes sure the prefix is
/// `catalog`'s: claims an empty prefix when `may_write`, adopts a prefix
/// with objects and no marker only with `adopt`.
///
/// # Errors
/// A marker naming another catalog, a marker that cannot be read, objects
/// without a marker when `adopt` is off, or a store failure.
pub async fn claim(
    store: &dyn ObjectStore,
    root: &Path,
    catalog: Uuid,
    may_write: bool,
    adopt: bool,
) -> Result<Ownership, ServerError> {
    let marker = under(root, OWNER_MARKER);
    match store.get(&marker).await {
        Ok(found) => {
            let owner = parse_marker(&found.bytes().await?)
                .ok_or_else(|| ServerError::PrefixMarkerUnreadable(marker.to_string()))?;
            if owner == catalog {
                Ok(Ownership::Held)
            } else {
                Err(ServerError::PrefixOwnedByAnother {
                    prefix: root.to_string(),
                    owner,
                })
            }
        }
        Err(object_store::Error::NotFound { .. }) => {
            let occupied = store.list_with_delimiter(Some(&under(root, "t"))).await?;
            let has_objects = !occupied.objects.is_empty() || !occupied.common_prefixes.is_empty();
            if has_objects && !adopt {
                return Err(ServerError::PrefixUnclaimed(root.to_string()));
            }
            if !may_write {
                return Ok(Ownership::Unclaimed);
            }
            write_marker(store, &marker, catalog).await?;
            Ok(if has_objects {
                Ownership::Adopted
            } else {
                Ownership::Claimed
            })
        }
        Err(e) => Err(e.into()),
    }
}

/// Creates the marker, or verifies the one a concurrent start created.
async fn write_marker(
    store: &dyn ObjectStore,
    marker: &Path,
    catalog: Uuid,
) -> Result<(), ServerError> {
    let body = format!("fusescope-catalog v1\n{catalog}\n");
    let create = PutOptions {
        mode: PutMode::Create,
        ..PutOptions::default()
    };
    match store
        .put_opts(marker, PutPayload::from(body.into_bytes()), create)
        .await
    {
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let found = store.get(marker).await?.bytes().await?;
            match parse_marker(&found) {
                Some(owner) if owner == catalog => Ok(()),
                Some(owner) => Err(ServerError::PrefixOwnedByAnother {
                    prefix: marker.to_string(),
                    owner,
                }),
                None => Err(ServerError::PrefixMarkerUnreadable(marker.to_string())),
            }
        }
        Err(e) => Err(e.into()),
    }
}

fn parse_marker(bytes: &[u8]) -> Option<Uuid> {
    let text = std::str::from_utf8(bytes).ok()?;
    let mut lines = text.lines();
    if lines.next()? != "fusescope-catalog v1" {
        return None;
    }
    lines.next()?.trim().parse().ok()
}

fn under(root: &Path, name: &str) -> Path {
    if root.as_ref().is_empty() {
        Path::from(name)
    } else {
        Path::from(format!("{root}/{name}"))
    }
}

/// Prefix for engine paths inside the bucket.
pub fn root(config: &BucketConfig) -> Path {
    config
        .prefix
        .as_deref()
        .map_or_else(|| Path::from(""), Path::from)
}

/// Confirms the store answers by listing one page under the engine
/// prefix. Reads only: it runs before the prefix is known to be ours.
///
/// # Errors
/// Any failure talking to the store.
pub async fn reachable(store: &dyn ObjectStore, root: &Path) -> Result<(), ServerError> {
    store.list_with_delimiter(Some(root)).await?;
    Ok(())
}

/// Writes and deletes a marker under `_probe/`, proving the credentials
/// allow writes, which every role except the querier needs. Only after
/// [`claim`] said the prefix is ours: nothing is written into another
/// catalog's prefix, not even a probe.
///
/// # Errors
/// Any failure talking to the store.
pub async fn probe_write(store: &dyn ObjectStore, root: &Path) -> Result<(), ServerError> {
    let stamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let marker = under(root, &format!("_probe/{stamp}.txt"));
    store
        .put(&marker, PutPayload::from_static(b"fusescope-engine probe"))
        .await?;
    store.delete(&marker).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_marker_names_one_catalog() {
        let id = Uuid::new_v4();
        assert_eq!(
            parse_marker(format!("fusescope-catalog v1\n{id}\n").as_bytes()),
            Some(id)
        );
        for bad in [
            "",
            "fusescope-catalog v2\n{id}\n",
            "{id}\n",
            "fusescope-catalog v1\nnope\n",
        ] {
            assert_eq!(
                parse_marker(bad.replace("{id}", &id.to_string()).as_bytes()),
                None,
                "{bad:?}"
            );
        }
    }
}

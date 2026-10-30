//! Content digests of the objects the ingester writes and meets.

use futures_util::StreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use sha2::{Digest, Sha256};

use crate::IngestError;

/// sha256 of bytes in memory.
pub fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// sha256 of an object, streamed so the whole object is never held.
///
/// # Errors
/// The bucket fails or the object is missing.
pub async fn of_object(store: &dyn ObjectStore, path: &str) -> Result<[u8; 32], IngestError> {
    let mut stream = store.get(&Path::from(path)).await?.into_stream();
    let mut hasher = Sha256::new();
    while let Some(chunk) = stream.next().await {
        hasher.update(chunk?);
    }
    Ok(hasher.finalize().into())
}

//! Objects a pass creates before its swap and what becomes of them when
//! the swap does not commit.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use bytes::Bytes;
use fs_catalog::TenantId;
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use sha2::{Digest, Sha256};
use tracing::warn;

use crate::CompactError;

/// Puts `bytes` at `path` under the create-once rule.
///
/// # Errors
/// An object already at `path`, or a store failure.
pub(crate) async fn create(
    store: &dyn ObjectStore,
    path: &str,
    bytes: Bytes,
) -> Result<(), CompactError> {
    let create = PutOptions {
        mode: PutMode::Create,
        ..PutOptions::default()
    };
    match store
        .put_opts(&Path::from(path), PutPayload::from_bytes(bytes), create)
        .await
    {
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. }) => {
            Err(CompactError::ObjectExists(path.to_owned()))
        }
        Err(e) => Err(e.into()),
    }
}

/// Where the objects a pass did not publish are counted.
pub(crate) trait CountsOrphans {
    fn count_orphans(&self, created: u64);
}

/// Counts the objects a pass created as orphans when it ends without a
/// committed swap, whether by error or by the task being dropped.
pub(crate) struct OrphanGuard<S: CountsOrphans> {
    pub(crate) stats: Arc<S>,
    pub(crate) tenant: TenantId,
    /// The pass, for the log.
    pub(crate) work: String,
    /// Parquet objects created so far; their sidecars may or may not be.
    pub(crate) created: u64,
    pub(crate) armed: bool,
}

impl<S: CountsOrphans> Drop for OrphanGuard<S> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.stats.count_orphans(self.created);
        if self.created > 0 {
            warn!(
                role = "compactor",
                tenant = %self.tenant.0,
                work = %self.work,
                orphans = self.created,
                "not published; the objects it created are orphans"
            );
        }
    }
}

/// Takes one injected failure when any is left.
pub(crate) fn take_injected(counter: &AtomicU32) -> bool {
    let mut remaining = counter.load(Ordering::SeqCst);
    loop {
        if remaining == 0 {
            return false;
        }
        match counter.compare_exchange(remaining, remaining - 1, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => return true,
            Err(actual) => remaining = actual,
        }
    }
}

/// sha256 of an object's bytes, recorded on its catalog row.
pub(crate) fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

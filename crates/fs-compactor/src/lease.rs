//! Who the active compactor is.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;

use fs_catalog::{COMPACTOR_LEASE, Catalog, LeaseFence};
use tracing::{info, warn};

/// This process's claim on the compactor lease. The holder is the node id
/// plus an id of this incarnation, so two processes configured alike never
/// pass for one another.
pub struct Leadership {
    catalog: Catalog,
    holder: String,
    ttl: Duration,
    leader: AtomicBool,
    generation: AtomicI64,
}

impl Leadership {
    /// A claim for this incarnation of `node_id`, not yet made.
    pub fn new(catalog: Catalog, node_id: &str, ttl: Duration) -> Self {
        Self {
            catalog,
            holder: format!("{node_id}/{}", uuid::Uuid::new_v4().simple()),
            ttl,
            leader: AtomicBool::new(false),
            generation: AtomicI64::new(0),
        }
    }

    /// How often [`refresh`](Self::refresh) should run: a third of the TTL,
    /// so two renewals can fail before the lease lapses.
    pub fn renew_every(&self) -> Duration {
        (self.ttl / 3).max(Duration::from_millis(100))
    }

    /// True after the last refresh found this incarnation holding the lease.
    pub fn is_leader(&self) -> bool {
        self.leader.load(Ordering::SeqCst)
    }

    /// Who this incarnation is to the catalog.
    pub fn holder(&self) -> &str {
        &self.holder
    }

    /// The fence for writes under the lease, while it is held.
    pub fn fence(&self) -> Option<LeaseFence> {
        self.is_leader().then(|| LeaseFence {
            name: COMPACTOR_LEASE.to_owned(),
            holder: self.holder.clone(),
            generation: self.generation.load(Ordering::SeqCst),
        })
    }

    /// Takes or renews the lease. A catalog failure, or a renewal that
    /// takes longer than the renewal period, counts as not holding it:
    /// the instance stops working until a refresh succeeds, so two
    /// instances never work at once on a partitioned catalog and a
    /// renewal stuck on the network does not keep the work going past
    /// the TTL.
    pub async fn refresh(&self) -> bool {
        let acquire = self
            .catalog
            .acquire_lease(COMPACTOR_LEASE, &self.holder, self.ttl);
        let held = match tokio::time::timeout(self.renew_every(), acquire).await {
            Ok(Ok(generation)) => generation,
            Ok(Err(e)) => {
                warn!(role = "compactor", holder = %self.holder, error = %e, "lease refresh failed; standing by");
                None
            }
            Err(_) => {
                warn!(role = "compactor", holder = %self.holder, timeout = ?self.renew_every(), "lease refresh timed out; standing by");
                None
            }
        };
        let was = self.leader.swap(held.is_some(), Ordering::SeqCst);
        match held {
            Some(generation) => {
                self.generation.store(generation, Ordering::SeqCst);
                if !was {
                    info!(role = "compactor", holder = %self.holder, generation, "lease taken; this instance compacts");
                }
            }
            None if was => {
                info!(role = "compactor", holder = %self.holder, "lease lost or held elsewhere; standing by");
            }
            None => {}
        }
        held.is_some()
    }

    /// Gives the lease up so another instance takes over at once.
    pub async fn release(&self) {
        if !self.leader.swap(false, Ordering::SeqCst) {
            return;
        }
        match self
            .catalog
            .release_lease(COMPACTOR_LEASE, &self.holder)
            .await
        {
            Ok(true) => info!(role = "compactor", holder = %self.holder, "lease released"),
            Ok(false) => {}
            Err(e) => {
                warn!(role = "compactor", holder = %self.holder, error = %e, "lease release failed; it lapses on its own");
            }
        }
    }
}

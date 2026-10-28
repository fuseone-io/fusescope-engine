//! Who the active compactor is.

use std::time::Duration;

use fs_catalog::{COMPACTOR_LEASE, Catalog};
use tracing::{info, warn};

/// This instance's claim on the compactor lease.
pub struct Leadership {
    catalog: Catalog,
    holder: String,
    ttl: Duration,
    leader: bool,
}

impl Leadership {
    /// A claim for `holder`, not yet made.
    pub fn new(catalog: Catalog, holder: String, ttl: Duration) -> Self {
        Self {
            catalog,
            holder,
            ttl,
            leader: false,
        }
    }

    /// How often [`refresh`](Self::refresh) should run: a third of the TTL,
    /// so two renewals can fail before the lease lapses.
    pub fn renew_every(&self) -> Duration {
        (self.ttl / 3).max(Duration::from_millis(100))
    }

    /// True after the last refresh found this instance holding the lease.
    pub fn is_leader(&self) -> bool {
        self.leader
    }

    /// Who this instance is to the catalog.
    pub fn holder(&self) -> &str {
        &self.holder
    }

    /// Takes or renews the lease. A catalog failure counts as not holding
    /// it: the instance stops working until a refresh succeeds, so two
    /// instances never work at once on a partitioned catalog.
    pub async fn refresh(&mut self) -> bool {
        let held = match self
            .catalog
            .acquire_lease(COMPACTOR_LEASE, &self.holder, self.ttl)
            .await
        {
            Ok(held) => held,
            Err(e) => {
                warn!(role = "compactor", holder = %self.holder, error = %e, "lease refresh failed; standing by");
                false
            }
        };
        if held != self.leader {
            if held {
                info!(role = "compactor", holder = %self.holder, "lease taken; this instance compacts");
            } else {
                info!(role = "compactor", holder = %self.holder, "lease lost or held elsewhere; standing by");
            }
        }
        self.leader = held;
        held
    }

    /// Gives the lease up so another instance takes over at once.
    pub async fn release(&mut self) {
        if !self.leader {
            return;
        }
        self.leader = false;
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

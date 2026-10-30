//! What `/healthz` and `/readyz` answer from.
//!
//! Liveness is the process answering at all. Readiness is every role of
//! the process up, the catalog and the bucket answering the last probe,
//! the ingester's WAL open, and no drain under way: the moment a stop is
//! asked for, readiness drops so a load balancer stops sending work while
//! the roles finish what they hold.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Flags the roles and the prober keep current; cheap to read on every
/// probe request.
#[derive(Debug, Default)]
pub struct Health {
    expected: AtomicUsize,
    up: AtomicUsize,
    draining: AtomicBool,
    catalog_ok: AtomicBool,
    bucket_ok: AtomicBool,
    wal_open: AtomicBool,
    has_ingester: AtomicBool,
    tls: std::sync::OnceLock<std::sync::Arc<fs_auth::TlsStats>>,
}

/// What the last probe of a dependency found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Probe {
    /// It answered.
    Ok,
    /// It did not.
    Down,
}

impl Probe {
    fn of(ok: bool) -> Self {
        if ok { Probe::Ok } else { Probe::Down }
    }

    fn name(self) -> &'static str {
        match self {
            Probe::Ok => "ok",
            Probe::Down => "down",
        }
    }
}

/// The ingester's write-ahead log, as far as readiness is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalState {
    /// Open and taking records.
    Open,
    /// Not open: the ingester has not started or has stopped.
    Closed,
    /// The process runs no ingester.
    None,
}

impl WalState {
    fn name(self) -> &'static str {
        match self {
            WalState::Open => "open",
            WalState::Closed => "closed",
            WalState::None => "none",
        }
    }
}

/// The process's TLS, as far as readiness reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsState {
    /// Off: plain text.
    Off,
    /// On, serving what the files hold.
    Current,
    /// On, but the last reload failed: new connections get the last good
    /// config. Reported, not counted against readiness, since the process
    /// still serves and every replica would read the same broken files.
    Stale,
}

impl TlsState {
    fn name(self) -> &'static str {
        match self {
            TlsState::Off => "off",
            TlsState::Current => "current",
            TlsState::Stale => "stale",
        }
    }
}

/// One readiness verdict with what it was made of.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Readiness {
    /// Everything below is as it should be.
    pub ready: bool,
    /// Roles up over roles expected.
    pub roles: (usize, usize),
    /// The last catalog probe.
    pub catalog: Probe,
    /// The last bucket probe.
    pub bucket: Probe,
    /// The ingester's WAL.
    pub wal: WalState,
    /// A stop was asked for and the roles are finishing.
    pub draining: bool,
    /// The process's TLS.
    pub tls: TlsState,
}

impl Readiness {
    /// One line per fact, the verdict first.
    pub fn render(&self) -> String {
        format!(
            "{}\nroles={}/{}\ncatalog={}\nbucket={}\nwal={}\ndraining={}\ntls={}\n",
            if self.ready { "ready" } else { "not ready" },
            self.roles.0,
            self.roles.1,
            self.catalog.name(),
            self.bucket.name(),
            self.wal.name(),
            self.draining,
            self.tls.name()
        )
    }
}

impl Health {
    /// How many roles must report up before the process is ready, and
    /// whether one of them is an ingester, whose WAL counts.
    pub fn expect(&self, roles: usize, ingester: bool) {
        self.expected.store(roles, Ordering::SeqCst);
        self.has_ingester.store(ingester, Ordering::SeqCst);
    }

    /// Reports the state of the process's TLS from `stats`.
    pub fn follow_tls(&self, stats: std::sync::Arc<fs_auth::TlsStats>) {
        let _ = self.tls.set(stats);
    }

    /// One role is serving.
    pub fn role_up(&self) {
        self.up.fetch_add(1, Ordering::SeqCst);
    }

    /// One role stopped serving; never below none.
    pub fn role_down(&self) {
        let mut up = self.up.load(Ordering::SeqCst);
        while up > 0 {
            match self
                .up
                .compare_exchange(up, up - 1, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return,
                Err(now) => up = now,
            }
        }
    }

    /// A stop was asked for: not ready from here on.
    pub fn draining(&self) {
        self.draining.store(true, Ordering::SeqCst);
    }

    /// The last catalog probe's outcome.
    pub fn catalog(&self, ok: bool) {
        self.catalog_ok.store(ok, Ordering::SeqCst);
    }

    /// The last bucket probe's outcome.
    pub fn bucket(&self, ok: bool) {
        self.bucket_ok.store(ok, Ordering::SeqCst);
    }

    /// The ingester opened or closed its WAL.
    pub fn wal(&self, open: bool) {
        self.wal_open.store(open, Ordering::SeqCst);
    }

    /// The verdict now.
    pub fn readiness(&self) -> Readiness {
        let expected = self.expected.load(Ordering::SeqCst);
        let up = self.up.load(Ordering::SeqCst);
        let draining = self.draining.load(Ordering::SeqCst);
        let catalog_ok = self.catalog_ok.load(Ordering::SeqCst);
        let bucket_ok = self.bucket_ok.load(Ordering::SeqCst);
        let wal = if self.has_ingester.load(Ordering::SeqCst) {
            if self.wal_open.load(Ordering::SeqCst) {
                WalState::Open
            } else {
                WalState::Closed
            }
        } else {
            WalState::None
        };
        Readiness {
            ready: up == expected
                && expected > 0
                && !draining
                && catalog_ok
                && bucket_ok
                && wal != WalState::Closed,
            roles: (up, expected),
            catalog: Probe::of(catalog_ok),
            bucket: Probe::of(bucket_ok),
            wal,
            draining,
            tls: match self.tls.get() {
                None => TlsState::Off,
                Some(stats) if stats.stale.load(Ordering::Relaxed) => TlsState::Stale,
                Some(_) => TlsState::Current,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_only_when_every_fact_holds() {
        let health = Health::default();
        assert!(!health.readiness().ready, "nothing expected yet");
        health.expect(2, true);
        health.catalog(true);
        health.bucket(true);
        health.wal(true);
        health.role_up();
        assert!(!health.readiness().ready, "one of two roles");
        health.role_up();
        assert!(health.readiness().ready);
        assert_eq!(
            health.readiness().render(),
            "ready\nroles=2/2\ncatalog=ok\nbucket=ok\nwal=open\ndraining=false\ntls=off\n"
        );
        let tls = std::sync::Arc::new(fs_auth::TlsStats::default());
        health.follow_tls(std::sync::Arc::clone(&tls));
        assert_eq!(health.readiness().tls, TlsState::Current);
        tls.stale.store(true, Ordering::Relaxed);
        let stale = health.readiness();
        assert_eq!(stale.tls, TlsState::Stale);
        assert!(stale.ready, "a stale config still serves");
        assert!(stale.render().ends_with("tls=stale\n"));
        health.bucket(false);
        assert!(!health.readiness().ready);
        health.bucket(true);
        health.wal(false);
        assert!(!health.readiness().ready);
        health.wal(true);
        health.draining();
        let verdict = health.readiness();
        assert!(!verdict.ready && verdict.draining);
        assert!(verdict.render().starts_with("not ready\n"));
    }

    #[test]
    fn a_role_going_down_without_having_come_up_changes_nothing() {
        let health = Health::default();
        health.expect(1, false);
        health.catalog(true);
        health.bucket(true);
        health.role_down();
        assert_eq!(health.readiness().roles, (0, 1));
        health.role_up();
        assert!(health.readiness().ready);
        health.role_up();
        assert!(
            !health.readiness().ready,
            "more roles than expected is not ready either"
        );
    }

    #[test]
    fn a_process_without_an_ingester_has_no_wal_to_answer_for() {
        let health = Health::default();
        health.expect(1, false);
        health.catalog(true);
        health.bucket(true);
        health.role_up();
        let verdict = health.readiness();
        assert!(verdict.ready);
        assert_eq!(verdict.wal, WalState::None);
        assert!(verdict.render().contains("wal=none\n"));
    }
}

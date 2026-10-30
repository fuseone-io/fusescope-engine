//! mTLS for the engine's Flight listeners and their clients.
//!
//! A process with TLS on has one identity, a certificate and its key, that
//! it serves with and presents as a client, and two disjoint bundles of
//! client CAs: the public Flight SQL listener admits clients of one, the
//! backend's, and the internal hot-window listener clients of the other,
//! the queriers'. Every client CA is a self-signed root and no key is in
//! both bundles, so a certificate that opens one never opens the other.
//! Listeners require a client certificate; clients trust only the operating
//! system's store for the server's. The files are read again every reload
//! interval: a change
//! that loads is published to new connections while open ones keep the
//! config they began with, and a change that does not load keeps the last
//! good config and marks it stale. Tenant JWT validation joins this crate
//! later (roadmap E2).

mod anchors;
mod client;
mod incoming;
mod load;
mod reload;

pub use client::{ClientTls, endpoint, error_chain};
pub use incoming::{ServerTls, serve};
pub use load::{TlsError, TlsFiles};
pub use reload::{MAX_HANDSHAKES, Reload, Tls, TlsLimits, TlsStats};

/// Crate version, exposed for diagnostics and the `--version` flag.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

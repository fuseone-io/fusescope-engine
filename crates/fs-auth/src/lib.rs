//! mTLS for the engine's Flight listeners and their clients.
//!
//! A process with TLS on has one identity, a certificate and its key, that
//! it serves with and presents as a client, and one bundle of client CAs
//! every connecting client must chain to. Listeners require a client
//! certificate; clients trust only the operating system's store for the
//! server's. The three files are read again every reload interval: a change
//! that loads is published to new connections while open ones keep the
//! config they began with, and a change that does not load keeps the last
//! good config and marks it stale. Tenant JWT validation joins this crate
//! later (roadmap E2).

mod client;
mod incoming;
mod load;
mod reload;

pub use client::{ClientTls, endpoint, error_chain};
pub use incoming::{ServerTls, serve};
pub use load::{TlsError, TlsFiles};
pub use reload::{Reload, Tls, TlsLimits, TlsStats};

/// Crate version, exposed for diagnostics and the `--version` flag.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

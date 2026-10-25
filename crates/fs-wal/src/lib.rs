//! Append-only write-ahead log segments with crc32c records and replay.
//!
//! The ingester writes every accepted batch here before acknowledging it, and
//! replays the log on start to rebuild its in-memory buffers. Once a batch has
//! been flushed to the bucket and registered in the catalog, the segments it
//! covers are removed with [`Wal::truncate_before`].
//!
//! # Layout
//!
//! A log is a directory of segment files named `{id:020}.wal`, ids increasing
//! monotonically. Each segment starts with a 16 byte header (magic, version,
//! segment id) followed by records:
//!
//! ```text
//! | len: u32 LE | crc32c: u32 LE | payload: len bytes |
//! ```
//!
//! Payloads are opaque to this crate. The crc covers the payload only; the
//! length is validated against the segment bounds and the configured maximum.
//!
//! # Durability and recovery
//!
//! Appends go through a buffered writer. [`Wal::sync`] flushes and fsyncs the
//! active segment; [`SyncPolicy::Always`] does that on every append. New segment
//! files are fsynced together with their directory so they survive a crash.
//!
//! On open, the last segment is scanned and anything after the last valid
//! record (a torn write from a crash) is cut off so appends continue from a
//! consistent point. Earlier segments are never modified; damage in them is
//! reported during replay and the scan continues with the next segment.

mod format;
mod replay;
mod segment;
mod wal;

pub use format::{HEADER_LEN, RECORD_HEADER_LEN};
pub use replay::{Damage, Entry, Event, Replay};
pub use wal::{Position, SegmentInfo, SyncPolicy, Wal, WalConfig};

use std::path::PathBuf;

use thiserror::Error;

/// Crate version, exposed for diagnostics and the `--version` flag.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Errors from the write-ahead log.
#[derive(Debug, Error)]
pub enum WalError {
    /// Filesystem failure.
    #[error("io at {path}: {source}")]
    Io {
        /// File or directory involved.
        path: PathBuf,
        /// Underlying error.
        #[source]
        source: std::io::Error,
    },
    /// A record larger than the configured maximum.
    #[error("record of {size} bytes exceeds the maximum of {max}")]
    RecordTooLarge {
        /// Offered size.
        size: usize,
        /// Configured maximum.
        max: usize,
    },
    /// Zero-length records carry nothing and are refused.
    #[error("empty record")]
    EmptyRecord,
    /// The segment id space is exhausted. Practically unreachable.
    #[error("segment id overflow")]
    SegmentIdOverflow,
}

impl WalError {
    fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn version_is_set() {
        assert_ne!(super::VERSION, "");
    }
}

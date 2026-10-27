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
//! A frame of length zero carrying the crc of a fixed tag is a commit marker,
//! appended by [`Wal::sync`] after the records of the batch it syncs.
//!
//! # Durability and recovery
//!
//! Appends go through a buffered writer. [`Wal::sync`] appends a commit
//! marker, flushes and fsyncs the active segment; [`SyncPolicy::Always`] does
//! that on every append. New segment files are fsynced together with their
//! directory so they survive a crash.
//!
//! On open, the last segment is scanned. Everything before its last commit
//! marker was acknowledged by a completed sync; damage there is corruption of
//! acknowledged records and the log refuses to open rather than cut them.
//! Damage after the last marker belongs to a batch whose sync never completed,
//! which no one was told about: it is cut off so appends continue from a
//! consistent point. A read failure proves nothing and is never cut. Earlier segments are never modified; damage in them is
//! reported during replay and the scan continues with the next segment.
//!
//! # Checkpoints
//!
//! Once the records before a position are materialised, the owner calls
//! [`Wal::checkpoint`]. The position is stored durably next to the segments,
//! replay starts there after a restart, and whole segments before it are
//! deleted. Without the checkpoint a restart after a flush would replay rows
//! that already live in the bucket.

mod checkpoint;
mod format;
mod instance;
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
    /// Damage in the last segment that recovery cannot prove unacknowledged:
    /// a commit marker follows it, the bytes behind it could not be read, or
    /// the damage is a read failure itself. Cutting could lose acknowledged
    /// records, so the log stays as it is and does not open.
    #[error("damage at segment {segment} offset {offset} that recovery must not cut ({damage:?})")]
    Corrupt {
        /// Segment id.
        segment: u64,
        /// Byte offset where valid data stops.
        offset: u64,
        /// Why.
        damage: Damage,
    },
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

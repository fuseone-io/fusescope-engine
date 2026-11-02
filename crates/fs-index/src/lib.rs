//! Sidecar indexes: xor and bloom filters, ngram bloom, roaring postings, `trace_id` index
//!
//! See DESIGN.md at the repository root for the role of this crate. Every
//! sidecar carries a header with its format version, the body length and an
//! xxh3 checksum of the body; a reader that meets an unknown version fails
//! loudly and one that meets damage reports it so the caller degrades to a
//! scan.

mod frame;
pub mod layout;
pub mod postings;
pub mod text_index;
pub mod trace_index;

pub use layout::EngineObject;
pub use text_index::{TextIndex, TextIndexBuilder, TextIndexLimits};
pub use trace_index::{TraceIndex, owners_of_sidecar, sidecar_path};

/// Every sidecar a file at `parquet` has: the `trace_id` sidecar of a span
/// file, the text sidecar of a log file, the postings of a series file of
/// L1 or L2, none for anything else.
pub fn sidecar_paths(parquet: &str) -> impl Iterator<Item = String> {
    trace_index::sidecar_path(parquet)
        .into_iter()
        .chain(text_index::sidecar_path(parquet))
        .chain(postings::sidecar_path(parquet))
}

/// Crate version, exposed for diagnostics and the `--version` flag.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Why a sidecar could not be built or read.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum IndexError {
    /// The bytes do not start with the sidecar's magic.
    #[error("not a sidecar: bad magic")]
    BadMagic,
    /// A version this engine does not read.
    #[error("sidecar version {0} is not supported by this engine")]
    UnknownVersion(u8),
    /// The header's body length does not match the bytes present.
    #[error("sidecar body length {declared} does not match {present} bytes present")]
    BadLength {
        /// Length the header declares.
        declared: u64,
        /// Bytes after the header.
        present: u64,
    },
    /// The body does not hash to the header's checksum.
    #[error("sidecar checksum mismatch")]
    ChecksumMismatch,
    /// The body is framed wrongly: a count past the end, an unsorted entry,
    /// a file index with no file.
    #[error("sidecar body is malformed: {0}")]
    BadBody(&'static str),
    /// More files or entries than the format can count.
    #[error("sidecar too large: {0}")]
    TooLarge(&'static str),
    /// The sidecar was built for another file, or another version of it.
    #[error("sidecar bound to another file: {0}")]
    NotBound(&'static str),
}

/// What decoding a sidecar allocates at most.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeCost {
    /// Heap the decoded sidecar keeps.
    pub retained: usize,
    /// Heap the decoder uses and frees on the way; the peak is at most
    /// `retained + scratch`.
    pub scratch: usize,
}

#[cfg(test)]
mod tests {
    #[test]
    fn version_is_set() {
        assert_ne!(super::VERSION, "");
    }
}

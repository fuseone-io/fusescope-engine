//! The text sidecar of a log file: per row group, a trigram bloom and a
//! token bloom over the bodies, so a body search reads only the row groups
//! that may match ([ADR 0009]).
//!
//! The sidecar is bound to the exact bytes of its Parquet file by their
//! SHA-256 and lists the rows of each row group, so a reader can check it
//! against the catalog row and the footer before trusting it. A row group
//! without a bloom is always read. Missing or damaged, the file is read.
//!
//! # Format, version 1
//!
//! Little-endian throughout. The blooms come before their descriptors so
//! the builder writes them in place as row groups complete.
//!
//! ```text
//! header   "FSNGB\0" | version u8 | reserved u8 | body_len u32 | xxh3_64(body) u64
//! body     path_len u16 | path utf-8
//!          content_sha256 [32]
//!          normalization u8 (1) | ngram u8 (3)
//!          blooms: for each indexed row group, its trigram bloom bytes then its token bloom bytes
//!          row_group_count × (rows u64 | flags u8
//!                             | ngram_bits u32 | ngram_hashes u8 | ngram_bytes u32
//!                             | token_bits u32 | token_hashes u8 | token_bytes u32)
//!          row_group_count u32
//! flags    bit 0 indexed (the blooms are present) | bit 1 every body is ASCII
//! ```
//!
//! Bits are a multiple of 64 and bytes are bits / 8. A row group without
//! the indexed flag has zero bits, hashes and bytes. An indexed bloom of
//! zero bits holds no term: its row group has no trigram, or no token.
//!
//! [ADR 0009]: ../../../../docs/adr/0009-text-bloom-per-row-group-tantivy-by-measure.md

mod bloom;
mod build;
mod normalize;
mod probe;

pub use bloom::{BITS_PER_TERM, HASHES};
pub use build::{MIN_GROUP_BYTES, TextIndexBuilder, TextIndexLimits};
pub use normalize::{NGRAM, has_token, is_token, tokens, trigram_count, trigrams};
pub use probe::Probe;

use crate::IndexError;
use crate::frame::{self, Cursor, HEADER_LEN};

const MAGIC: [u8; 6] = *b"FSNGB\0";
const VERSION: u8 = 1;
/// Normalization 1: ASCII letters folded, tokens `[A-Za-z0-9]+`.
pub const NORMALIZATION: u8 = 1;
const GROUP_LEN: usize = 8 + 1 + 2 * (4 + 1 + 4);
const INDEXED: u8 = 1;
const ALL_ASCII: u8 = 2;

/// One bloom of a row group: where its bytes are in the body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct Bloom {
    bits: u32,
    hashes: u8,
    offset: usize,
    bytes: usize,
}

/// What the sidecar says about one row group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Group {
    rows: u64,
    flags: u8,
    ngram: Bloom,
    token: Bloom,
}

/// A decoded sidecar over the bytes it was read from.
#[derive(Debug, Clone)]
pub struct TextIndex<B = Vec<u8>> {
    bytes: B,
    path: std::ops::Range<usize>,
    content_sha256: [u8; 32],
    groups: Vec<Group>,
}

impl<B: AsRef<[u8]>> TextIndex<B> {
    /// Parses a sidecar, verifying the header, the normalization, the
    /// descriptors and that the blooms fill the body exactly.
    ///
    /// # Errors
    /// Any header or body check that fails; see [`IndexError`].
    pub fn decode(bytes: B) -> Result<Self, IndexError> {
        let (path, content_sha256, groups) = parse(bytes.as_ref())?;
        Ok(Self {
            bytes,
            path,
            content_sha256,
            groups,
        })
    }

    /// Path of the Parquet file the sidecar was built for.
    pub fn path(&self) -> &str {
        self.bytes
            .as_ref()
            .get(self.path.clone())
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .unwrap_or_default()
    }

    /// SHA-256 of the Parquet file's bytes.
    pub fn content_sha256(&self) -> &[u8; 32] {
        &self.content_sha256
    }

    /// Row groups the sidecar describes.
    pub fn row_groups(&self) -> usize {
        self.groups.len()
    }

    /// Rows of each row group, in file order.
    pub fn rows(&self) -> impl Iterator<Item = u64> + '_ {
        self.groups.iter().map(|g| g.rows)
    }

    /// True when row group `group` carries blooms.
    pub fn indexed(&self, group: usize) -> bool {
        self.groups
            .get(group)
            .is_some_and(|g| g.flags & INDEXED != 0)
    }

    /// Heap the decoded index keeps besides its bytes.
    pub fn heap_bytes(&self) -> usize {
        self.groups.capacity() * std::mem::size_of::<Group>()
    }

    /// What [`decode`](Self::decode) keeps for `bytes` besides them, found
    /// from the trailing count after the header checks.
    ///
    /// # Errors
    /// Any header check that fails, or a count past the body.
    pub fn decode_cost(bytes: &[u8]) -> Result<usize, IndexError> {
        let body = frame::body(bytes, MAGIC, VERSION)?;
        let count = trailing_count(body)?;
        Ok(count * std::mem::size_of::<Group>())
    }

    /// The row groups, in order, that may hold a row `probe` keeps.
    pub fn select(&self, probe: &Probe) -> Vec<usize> {
        let body = self.body();
        (0..self.groups.len())
            .filter(|&g| {
                self.groups
                    .get(g)
                    .is_none_or(|group| probe.may_match(&Lookup { body, group }))
            })
            .collect()
    }

    fn body(&self) -> &[u8] {
        self.bytes.as_ref().get(HEADER_LEN..).unwrap_or_default()
    }
}

/// One row group's blooms, as a probe consults them.
pub(crate) struct Lookup<'a> {
    body: &'a [u8],
    group: &'a Group,
}

impl Lookup<'_> {
    /// `None` when the group has no blooms or, for a `folded` probe, when
    /// a body is not ASCII: the group must be read.
    pub(crate) fn may_have_trigrams(&self, grams: &[[u8; NGRAM]], folded: bool) -> Option<bool> {
        self.usable(folded)?;
        let (bloom, bits, hashes) = self.bloom(&self.group.ngram);
        Some(
            grams
                .iter()
                .all(|g| bloom::may_contain(bloom, bits, hashes, g)),
        )
    }

    /// As [`Self::may_have_trigrams`] for one folded token.
    pub(crate) fn may_have_token(&self, token: &[u8], folded: bool) -> Option<bool> {
        self.usable(folded)?;
        let (bloom, bits, hashes) = self.bloom(&self.group.token);
        Some(bloom::may_contain(bloom, bits, hashes, token))
    }

    fn usable(&self, folded: bool) -> Option<()> {
        let flags = self.group.flags;
        (flags & INDEXED != 0 && (!folded || flags & ALL_ASCII != 0)).then_some(())
    }

    fn bloom(&self, b: &Bloom) -> (&[u8], u64, u8) {
        let bytes = self
            .body
            .get(b.offset..b.offset + b.bytes)
            .unwrap_or_default();
        (bytes, u64::from(b.bits), b.hashes)
    }
}

/// The path range in the bytes, the digest and the groups.
type Parsed = (std::ops::Range<usize>, [u8; 32], Vec<Group>);

/// The parts of a framed sidecar.
fn parse(bytes: &[u8]) -> Result<Parsed, IndexError> {
    let body = frame::body(bytes, MAGIC, VERSION)?;
    let count = trailing_count(body)?;
    let groups_at = body.len() - 4 - count * GROUP_LEN;
    let mut cursor = Cursor::new(body.get(..groups_at).unwrap_or_default());
    let path_len = usize::from(cursor.u16()?);
    let raw = cursor.take(path_len)?;
    std::str::from_utf8(raw).map_err(|_| IndexError::BadBody("path"))?;
    let path = HEADER_LEN + 2..HEADER_LEN + 2 + path_len;
    let mut content_sha256 = [0u8; 32];
    content_sha256.copy_from_slice(cursor.take(32)?);
    if cursor.u8()? != NORMALIZATION {
        return Err(IndexError::BadBody("normalization"));
    }
    if usize::from(cursor.u8()?) != NGRAM {
        return Err(IndexError::BadBody("ngram size"));
    }
    let mut offset = groups_at - cursor.remaining();
    let mut descriptors = Cursor::new(body.get(groups_at..body.len() - 4).unwrap_or_default());
    let mut groups = Vec::with_capacity(count);
    for _ in 0..count {
        let rows = descriptors.u64()?;
        let flags = descriptors.u8()?;
        if flags & !(INDEXED | ALL_ASCII) != 0 {
            return Err(IndexError::BadBody("unknown row group flags"));
        }
        let ngram = bloom_at(&mut descriptors, &mut offset, flags)?;
        let token = bloom_at(&mut descriptors, &mut offset, flags)?;
        groups.push(Group {
            rows,
            flags,
            ngram,
            token,
        });
    }
    if offset != groups_at {
        return Err(IndexError::BadBody("blooms do not fill the body"));
    }
    Ok((path, content_sha256, groups))
}

fn bloom_at(cursor: &mut Cursor<'_>, offset: &mut usize, flags: u8) -> Result<Bloom, IndexError> {
    let bits = cursor.u32()?;
    let hashes = cursor.u8()?;
    let bytes = cursor.u32()? as usize;
    let consistent = if flags & INDEXED == 0 {
        bits == 0 && hashes == 0 && bytes == 0
    } else {
        bits % 64 == 0 && bytes == bits as usize / 8 && (1..=32).contains(&hashes)
    };
    if !consistent {
        return Err(IndexError::BadBody("bloom descriptor"));
    }
    let bloom = Bloom {
        bits,
        hashes,
        offset: *offset,
        bytes,
    };
    *offset = offset
        .checked_add(bytes)
        .ok_or(IndexError::BadBody("count past the end"))?;
    Ok(bloom)
}

/// The row group count at the end of `body`, checked against its length.
fn trailing_count(body: &[u8]) -> Result<usize, IndexError> {
    let at = body
        .len()
        .checked_sub(4)
        .ok_or(IndexError::BadBody("count past the end"))?;
    let count = Cursor::new(body.get(at..).unwrap_or_default()).u32()? as usize;
    if count > at / GROUP_LEN {
        return Err(IndexError::BadBody("count past the end"));
    }
    Ok(count)
}

/// The sidecar of a log file: `.../logs/l{n}/dt=.../hr=.../{id}.parquet`
/// becomes `.../logs/index/ngram/dt=.../hr=.../{id}.ngb`. `None` for a path
/// this engine does not lay out.
pub fn sidecar_path(parquet: &str) -> Option<String> {
    let stem = parquet.strip_suffix(".parquet")?;
    let (prefix, rest) = stem.split_once("/logs/l")?;
    let (level, partition) = rest.split_once('/')?;
    if level.len() != 1 || !level.bytes().all(|b| b.is_ascii_digit()) || partition.is_empty() {
        return None;
    }
    Some(format!("{prefix}/logs/index/ngram/{partition}.ngb"))
}

/// The log files a sidecar at `ngb` could belong to, one per level.
/// Empty for a path this engine does not lay out.
pub fn owners_of_sidecar(ngb: &str) -> Vec<String> {
    let Some(stem) = ngb.strip_suffix(".ngb") else {
        return Vec::new();
    };
    let Some((prefix, partition)) = stem.split_once("/logs/index/ngram/") else {
        return Vec::new();
    };
    (0..3)
        .map(|level| format!("{prefix}/logs/l{level}/{partition}.parquet"))
        .collect()
}

#[cfg(test)]
mod tests;

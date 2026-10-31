//! Building a sidecar row group by row group, in one buffer.
//!
//! Each row group is read twice: once to count its trigrams and tokens
//! with repeats, which needs no memory, then to fill blooms sized from
//! those counts. Repeats oversize a bloom, never undersize it. A group's
//! two blooms share its budget in proportion to what each wants.

use super::bloom::{self, HASHES};
use super::normalize::{self, NGRAM};
use super::{ALL_ASCII, GROUP_LEN, INDEXED, MAGIC, NORMALIZATION, VERSION};
use crate::IndexError;
use crate::frame::{self, HEADER_LEN};

/// Bloom bytes a row group may spend whatever its body bytes, so a group
/// of a few short bodies is still indexed.
pub const MIN_GROUP_BYTES: u64 = 64;

/// Bloom bytes a sidecar may spend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TextIndexLimits {
    /// Both blooms of one row group together; never more than the group's
    /// body bytes either, or [`MIN_GROUP_BYTES`] when that is more.
    pub max_bytes_per_row_group: usize,
    /// Every bloom of the file together.
    pub max_bytes_per_file: usize,
}

impl TextIndexLimits {
    /// 8 MiB per row group, 64 MiB per file.
    pub const DEFAULT: Self = Self {
        max_bytes_per_row_group: 8 * 1024 * 1024,
        max_bytes_per_file: 64 * 1024 * 1024,
    };
    /// Highest `max_bytes_per_row_group` accepted.
    pub const ROW_GROUP_CEILING: usize = 64 * 1024 * 1024;
    /// Highest `max_bytes_per_file` accepted.
    pub const FILE_CEILING: usize = 512 * 1024 * 1024;

    /// Limits checked against their ceilings.
    ///
    /// # Errors
    /// A limit of zero or above its ceiling.
    pub fn new(
        max_bytes_per_row_group: usize,
        max_bytes_per_file: usize,
    ) -> Result<Self, IndexError> {
        if !(1..=Self::ROW_GROUP_CEILING).contains(&max_bytes_per_row_group)
            || !(1..=Self::FILE_CEILING).contains(&max_bytes_per_file)
        {
            return Err(IndexError::TooLarge("text index limit"));
        }
        Ok(Self {
            max_bytes_per_row_group,
            max_bytes_per_file,
        })
    }
}

impl Default for TextIndexLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Writes a sidecar as its file's row groups are encoded.
#[derive(Debug)]
pub struct TextIndexBuilder {
    out: Vec<u8>,
    sha_at: usize,
    groups: Vec<u8>,
    count: u32,
    limits: TextIndexLimits,
    spent: u64,
    fold: Vec<u8>,
}

impl TextIndexBuilder {
    /// Most bytes a sidecar for `path` over `row_groups` row groups and
    /// `body_bytes` bytes of bodies can take: what [`Self::new`] reserves.
    pub fn encoded_bound(
        path: &str,
        body_bytes: usize,
        row_groups: usize,
        limits: TextIndexLimits,
    ) -> usize {
        prefix_len(path)
            .saturating_add(
                body_bytes
                    .saturating_add(
                        row_groups
                            .saturating_mul(usize::try_from(MIN_GROUP_BYTES).unwrap_or(usize::MAX)),
                    )
                    .min(limits.max_bytes_per_file),
            )
            .saturating_add(row_groups.saturating_mul(GROUP_LEN))
            .saturating_add(4)
    }

    /// A sidecar for the file at `path`, its buffer reserved for
    /// `body_bytes` and `row_groups` as [`Self::encoded_bound`] gives.
    ///
    /// # Errors
    /// A path longer than the format counts.
    pub fn new(
        path: &str,
        limits: TextIndexLimits,
        body_bytes: usize,
        row_groups: usize,
    ) -> Result<Self, IndexError> {
        let path_len = u16::try_from(path.len()).map_err(|_| IndexError::TooLarge("file path"))?;
        let mut out = Vec::with_capacity(Self::encoded_bound(path, body_bytes, row_groups, limits));
        out.resize(HEADER_LEN, 0);
        out.extend_from_slice(&path_len.to_le_bytes());
        out.extend_from_slice(path.as_bytes());
        let sha_at = out.len();
        out.resize(sha_at + 32, 0);
        out.push(NORMALIZATION);
        out.push(u8::try_from(NGRAM).unwrap_or(0));
        Ok(Self {
            out,
            sha_at,
            groups: Vec::with_capacity(row_groups.saturating_mul(GROUP_LEN)),
            count: 0,
            limits,
            spent: 0,
            fold: Vec::new(),
        })
    }

    /// Adds the next row group from its bodies, read twice.
    ///
    /// # Errors
    /// More row groups or bytes than the format counts.
    pub fn add_row_group<'a, I>(&mut self, bodies: I) -> Result<(), IndexError>
    where
        I: IntoIterator<Item = &'a [u8]>,
        I::IntoIter: Clone,
    {
        let bodies = bodies.into_iter();
        let mut rows = 0u64;
        let (mut grams, mut tokens, mut body_bytes) = (0u64, 0u64, 0u64);
        let mut ascii = true;
        for body in bodies.clone() {
            rows += 1;
            grams += normalize::trigram_count(body);
            tokens += normalize::tokens(body).count() as u64;
            body_bytes += body.len() as u64;
            ascii &= body.is_ascii();
        }
        let budget = (self.limits.max_bytes_per_row_group as u64)
            .min(body_bytes.max(MIN_GROUP_BYTES))
            .min((self.limits.max_bytes_per_file as u64).saturating_sub(self.spent));
        let (gram_bits, token_bits) = share(
            bloom::wanted_bits(grams),
            bloom::wanted_bits(tokens),
            budget,
        );
        let indexed = (grams == 0 || gram_bits > 0) && (tokens == 0 || token_bits > 0);
        let mut flags = if ascii { ALL_ASCII } else { 0 };
        let (mut gram_desc, mut token_desc) = ((0, 0, 0), (0, 0, 0));
        if indexed {
            flags |= INDEXED;
            gram_desc = descriptor(gram_bits)?;
            token_desc = descriptor(token_bits)?;
            self.fill(bodies, gram_bits, token_bits);
            self.spent += (gram_bits + token_bits) / 8;
        }
        self.groups.extend_from_slice(&rows.to_le_bytes());
        self.groups.push(flags);
        for (bits, hashes, bytes) in [gram_desc, token_desc] {
            self.groups.extend_from_slice(&bits.to_le_bytes());
            self.groups.push(hashes);
            self.groups.extend_from_slice(&bytes.to_le_bytes());
        }
        self.count = self
            .count
            .checked_add(1)
            .ok_or(IndexError::TooLarge("row groups"))?;
        Ok(())
    }

    /// The sidecar, bound to the file's `content_sha256`.
    ///
    /// # Errors
    /// A body longer than the format counts.
    pub fn finish(mut self, content_sha256: [u8; 32]) -> Result<Vec<u8>, IndexError> {
        self.out.extend_from_slice(&self.groups);
        self.out.extend_from_slice(&self.count.to_le_bytes());
        if let Some(sha) = self.out.get_mut(self.sha_at..self.sha_at + 32) {
            sha.copy_from_slice(&content_sha256);
        }
        let (header, body) = self.out.split_at_mut(HEADER_LEN);
        u32::try_from(body.len()).map_err(|_| IndexError::TooLarge("sidecar body"))?;
        header.copy_from_slice(&frame::header(MAGIC, VERSION, body));
        Ok(self.out)
    }

    /// Appends the group's blooms and sets the bits of every term.
    fn fill<'a>(
        &mut self,
        bodies: impl Iterator<Item = &'a [u8]>,
        gram_bits: u64,
        token_bits: u64,
    ) {
        let start = self.out.len();
        let gram_bytes = usize::try_from(gram_bits / 8).unwrap_or(0);
        let token_bytes = usize::try_from(token_bits / 8).unwrap_or(0);
        self.out.resize(start + gram_bytes + token_bytes, 0);
        let (gram_bloom, token_bloom) = self
            .out
            .get_mut(start..)
            .unwrap_or_default()
            .split_at_mut(gram_bytes);
        for body in bodies {
            normalize::fold_into(body, &mut self.fold);
            for gram in self.fold.windows(NGRAM) {
                bloom::insert(gram_bloom, gram_bits, HASHES, gram);
            }
            for token in normalize::tokens(&self.fold) {
                bloom::insert(token_bloom, token_bits, HASHES, token);
            }
        }
    }
}

/// Bits of each bloom within `budget` bytes: what they want when it fits,
/// else the budget split in proportion, each a whole number of words.
fn share(gram_want: u64, token_want: u64, budget: u64) -> (u64, u64) {
    let want = gram_want + token_want;
    if want / 8 <= budget {
        return (gram_want, token_want);
    }
    let bits = u128::from(budget) * 8;
    let grams = bits * u128::from(gram_want) / u128::from(want) / 64 * 64;
    let tokens = (bits - grams) / 64 * 64;
    let grams = u64::try_from(grams).unwrap_or(0).min(gram_want);
    let tokens = u64::try_from(tokens).unwrap_or(0).min(token_want);
    (grams, tokens)
}

fn descriptor(bits: u64) -> Result<(u32, u8, u32), IndexError> {
    let bits = u32::try_from(bits).map_err(|_| IndexError::TooLarge("bloom bits"))?;
    Ok((bits, HASHES, bits / 8))
}

fn prefix_len(path: &str) -> usize {
    HEADER_LEN + 2 + path.len() + 32 + 2
}

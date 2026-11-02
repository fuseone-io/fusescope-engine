//! Label postings of one metric series file, format version 1: for each
//! label key and value of the file's series, the rows holding it.
//!
//! ```text
//! header: magic "FSPST\0" | version u8 | reserved u8 | body_len u32 | xxh3_64(body) u64
//! body:   path_len u32 | path | sha256 [32] | schema_version u32 | row_count u32
//!         keys u32
//!         per key, strictly ascending as bytes:
//!           key_len u32 | key | values u32
//!           per value, strictly ascending as bytes:
//!             value_len u32 | value | bitmap_len u32 | bitmap
//! ```
//!
//! Integers are little-endian. A bitmap is a roaring bitmap in the
//! portable serialization every implementation reads, of row ordinals of
//! the series file, each below `row_count`; none is empty. The binding,
//! the series file's path, the SHA-256 of its bytes, its schema version and
//! its row count, ties the postings to one object: postings read beside
//! any other object are refused, and the reader scans the file instead.
//! A file of more than `u32::MAX` rows has no postings.

use std::collections::BTreeMap;

use roaring::RoaringBitmap;

use crate::frame::{self, Cursor};
use crate::{DecodeCost, IndexError};

mod cost;

/// Format version this engine writes and reads.
pub const VERSION: u8 = 1;

const MAGIC: [u8; 6] = *b"FSPST\0";

/// Bytes one insertion may allocate at most: a container of a bitmap
/// growing, or a new one, of 8 KiB, twice for its vector's growth.
pub const INSERT_BYTES: usize = 16 * 1024;

/// Bytes a key holds besides its text: its node in the map and its map of
/// values.
const KEY_OVERHEAD: usize = 96;

/// Bytes a value holds besides its text and bitmap: its node in the map
/// and the bitmap's own fields.
const VALUE_OVERHEAD: usize = 96;

/// The object postings are of.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    /// The series file's path.
    pub path: String,
    /// The SHA-256 of its bytes.
    pub sha256: [u8; 32],
    /// Its series schema version.
    pub schema_version: u32,
    /// Its rows.
    pub row_count: u32,
}

/// Postings over too many rows or bytes: the file goes without.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Abandoned;

/// Builds the postings of a series file row by row, within a budget.
#[derive(Debug)]
pub struct PostingsBuilder {
    entries: BTreeMap<Box<str>, BTreeMap<Box<str>, RoaringBitmap>>,
    held: usize,
    budget: usize,
}

impl PostingsBuilder {
    /// Postings that may hold `budget` bytes.
    pub fn new(budget: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            held: 0,
            budget,
        }
    }

    /// What the postings hold, by their texts and bitmaps.
    pub fn held(&self) -> usize {
        self.held
    }

    /// Records that row `row` of the file holds `labels`. Every allocation
    /// is admitted before it is made: past the budget, or past a row
    /// ordinal of `u32`, nothing more is built and the caller drops the
    /// postings.
    ///
    /// # Errors
    /// [`Abandoned`] as above.
    pub fn add<'a>(
        &mut self,
        row: u64,
        labels: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<(), Abandoned> {
        let row = u32::try_from(row).map_err(|_| Abandoned)?;
        for (key, value) in labels {
            let new_key = !self.entries.contains_key(key);
            let new_value =
                new_key || !self.entries.get(key).is_some_and(|v| v.contains_key(value));
            let entry = if new_key { KEY_OVERHEAD + key.len() } else { 0 }
                + if new_value {
                    VALUE_OVERHEAD + value.len()
                } else {
                    0
                };
            let admitted = self.held.saturating_add(entry).saturating_add(INSERT_BYTES);
            if admitted > self.budget {
                return Err(Abandoned);
            }
            let values = self.entries.entry(key.into()).or_default();
            let bitmap = values.entry(value.into()).or_default();
            let before = bitmap_bytes(bitmap);
            bitmap.insert(row);
            self.held = self
                .held
                .saturating_add(entry)
                .saturating_add(bitmap_bytes(bitmap).saturating_sub(before));
        }
        Ok(())
    }

    /// Bytes [`PostingsBuilder::finish`] writes for `binding`.
    pub fn encoded_len(&self, binding: &Binding) -> usize {
        let mut len = frame::HEADER_LEN + 4 + binding.path.len() + 32 + 4 + 4 + 4;
        for (key, values) in &self.entries {
            len += 4 + key.len() + 4;
            for (value, bitmap) in values {
                len += 4 + value.len() + 4 + bitmap.serialized_size();
            }
        }
        len
    }

    /// The postings of the file `binding` names.
    ///
    /// # Errors
    /// More keys, values or bytes than the format counts, or a row at or
    /// past the binding's `row_count`.
    pub fn finish(self, binding: &Binding) -> Result<Vec<u8>, IndexError> {
        let count = |n: usize, what| u32::try_from(n).map_err(|_| IndexError::TooLarge(what));
        let mut body = Vec::with_capacity(self.encoded_len(binding) - frame::HEADER_LEN);
        body.extend_from_slice(&count(binding.path.len(), "path")?.to_le_bytes());
        body.extend_from_slice(binding.path.as_bytes());
        body.extend_from_slice(&binding.sha256);
        body.extend_from_slice(&binding.schema_version.to_le_bytes());
        body.extend_from_slice(&binding.row_count.to_le_bytes());
        body.extend_from_slice(&count(self.entries.len(), "keys")?.to_le_bytes());
        for (key, values) in &self.entries {
            body.extend_from_slice(&count(key.len(), "key")?.to_le_bytes());
            body.extend_from_slice(key.as_bytes());
            body.extend_from_slice(&count(values.len(), "values")?.to_le_bytes());
            for (value, bitmap) in values {
                if bitmap.max().is_some_and(|m| m >= binding.row_count) {
                    return Err(IndexError::BadBody("a row past the file's rows"));
                }
                body.extend_from_slice(&count(value.len(), "value")?.to_le_bytes());
                body.extend_from_slice(value.as_bytes());
                body.extend_from_slice(&count(bitmap.serialized_size(), "bitmap")?.to_le_bytes());
                bitmap
                    .serialize_into(&mut body)
                    .map_err(|_| IndexError::BadBody("bitmap"))?;
            }
        }
        count(body.len(), "body")?;
        let mut out = Vec::with_capacity(frame::HEADER_LEN + body.len());
        out.extend_from_slice(&frame::header(MAGIC, VERSION, &body));
        out.extend_from_slice(&body);
        Ok(out)
    }
}

/// What a bitmap holds: its serialized size twice, for the growth of its
/// containers' vectors.
fn bitmap_bytes(bitmap: &RoaringBitmap) -> usize {
    bitmap.serialized_size().saturating_mul(2)
}

/// The values of one key and their rows, in value order.
type Values = Vec<(Box<str>, RoaringBitmap)>;

/// The postings of one series file, read and checked.
#[derive(Debug, Clone, PartialEq)]
pub struct Postings {
    entries: Vec<(Box<str>, Values)>,
}

impl Postings {
    /// The postings in `bytes`, built for the file `expected` names.
    ///
    /// # Errors
    /// Another magic, version, length or checksum; a binding other than
    /// `expected`; a count or length past the bytes present, checked
    /// before anything is allocated for it; keys or values not strictly
    /// ascending; an empty bitmap, one that does not parse or is not as
    /// long as it says, or a row at or past `row_count`. The caller scans
    /// the file instead.
    pub fn decode(bytes: &[u8], expected: &Binding) -> Result<Self, IndexError> {
        let body = frame::body(bytes, MAGIC, VERSION)?;
        let mut cursor = Cursor::new(body);
        let path_len = len(cursor.u32()?)?;
        if cursor.take(path_len)? != expected.path.as_bytes() {
            return Err(IndexError::NotBound("path"));
        }
        if cursor.take(32)? != expected.sha256 {
            return Err(IndexError::NotBound("sha256"));
        }
        let (schema_version, row_count) = (cursor.u32()?, cursor.u32()?);
        if schema_version != expected.schema_version || row_count != expected.row_count {
            return Err(IndexError::NotBound("schema version or rows"));
        }
        let keys = bounded(cursor.u32()?, &cursor, 8)?;
        let mut entries: Vec<(Box<str>, Values)> = Vec::with_capacity(keys);
        for _ in 0..keys {
            let key = text(&mut cursor)?;
            if entries
                .last()
                .is_some_and(|(last, _)| last.as_bytes() >= key.as_bytes())
            {
                return Err(IndexError::BadBody("keys not strictly ascending"));
            }
            let count = bounded(cursor.u32()?, &cursor, 8)?;
            let mut values: Values = Vec::with_capacity(count);
            for _ in 0..count {
                let value = text(&mut cursor)?;
                if values
                    .last()
                    .is_some_and(|(last, _)| last.as_bytes() >= value.as_bytes())
                {
                    return Err(IndexError::BadBody("values not strictly ascending"));
                }
                let declared = len(cursor.u32()?)?;
                let raw = cursor.take(declared)?;
                let bitmap = RoaringBitmap::deserialize_from(raw)
                    .map_err(|_| IndexError::BadBody("bitmap"))?;
                if bitmap.serialized_size() != declared {
                    return Err(IndexError::BadBody("bitmap of another length"));
                }
                match bitmap.max() {
                    None => return Err(IndexError::BadBody("an empty bitmap")),
                    Some(max) if max >= row_count => {
                        return Err(IndexError::BadBody("a row past the file's rows"));
                    }
                    Some(_) => {}
                }
                values.push((value, bitmap));
            }
            entries.push((key, values));
        }
        if cursor.remaining() != 0 {
            return Err(IndexError::BadBody("bytes after the last key"));
        }
        Ok(Self { entries })
    }

    /// What [`decode`](Self::decode) allocates for `bytes` at most, found
    /// without allocating, so a reader can reserve it first.
    ///
    /// # Errors
    /// Whatever `decode` refuses but a binding other than the expected one.
    pub fn decode_cost(bytes: &[u8]) -> Result<DecodeCost, IndexError> {
        cost::decode_cost(bytes)
    }

    /// The rows holding `key` with `value`, `None` for none.
    pub fn rows(&self, key: &str, value: &str) -> Option<&RoaringBitmap> {
        let at = self
            .entries
            .binary_search_by(|(k, _)| k.as_bytes().cmp(key.as_bytes()))
            .ok()?;
        let values = &self.entries.get(at)?.1;
        let at = values
            .binary_search_by(|(v, _)| v.as_bytes().cmp(value.as_bytes()))
            .ok()?;
        values.get(at).map(|(_, bitmap)| bitmap)
    }

    /// Every key, in order, with its values in order.
    pub fn keys(&self) -> impl Iterator<Item = (&str, impl Iterator<Item = &str>)> {
        self.entries
            .iter()
            .map(|(k, values)| (&**k, values.iter().map(|(v, _)| &**v)))
    }
}

fn len(n: u32) -> Result<usize, IndexError> {
    usize::try_from(n).map_err(|_| IndexError::TooLarge("length"))
}

/// `n` entries of at least `least` bytes each, refused when the bytes left
/// cannot hold them, before a vector is allocated for them.
fn bounded(n: u32, cursor: &Cursor<'_>, least: usize) -> Result<usize, IndexError> {
    let n = len(n)?;
    if n.saturating_mul(least) > cursor.remaining() {
        return Err(IndexError::BadBody("count past the end"));
    }
    Ok(n)
}

fn text(cursor: &mut Cursor<'_>) -> Result<Box<str>, IndexError> {
    let n = len(cursor.u32()?)?;
    std::str::from_utf8(cursor.take(n)?)
        .map(Into::into)
        .map_err(|_| IndexError::BadBody("text not UTF-8"))
}

/// The postings of the series file at `parquet`, of L1 or L2:
/// `…/metrics/index/postings/{partition}/{id}.pst`. `None` for any other
/// path.
pub fn sidecar_path(parquet: &str) -> Option<String> {
    let stem = parquet.strip_suffix(".parquet")?;
    let (prefix, rest) = stem.split_once("/metrics/series/l")?;
    let (level, partition) = rest.split_once('/')?;
    if !matches!(level, "1" | "2") || partition.is_empty() {
        return None;
    }
    Some(format!("{prefix}/metrics/index/postings/{partition}.pst"))
}

/// The series files the postings at `pst` may belong to, one a level.
pub fn owners_of_sidecar(pst: &str) -> Vec<String> {
    let Some(stem) = pst.strip_suffix(".pst") else {
        return Vec::new();
    };
    let Some((prefix, partition)) = stem.split_once("/metrics/index/postings/") else {
        return Vec::new();
    };
    (1..3)
        .map(|level| format!("{prefix}/metrics/series/l{level}/{partition}.parquet"))
        .collect()
}

#[cfg(test)]
mod tests;

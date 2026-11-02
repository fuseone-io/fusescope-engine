//! What [`Postings::decode`](super::Postings::decode) allocates, found by
//! walking the frame and each bitmap's portable serialization without
//! allocating. The walk checks everything `decode` checks but the binding.
//!
//! A bitmap decodes as a vector of containers, one per 65536 rows: an
//! array of `u16` up to 4096 rows, a bitmap of 8 KiB past it, or a vector
//! of runs. The run marks, descriptions and offsets are read into scratch
//! first, and each run container's runs once more before they are kept.

use std::mem::size_of;

use roaring::RoaringBitmap;

use super::{MAGIC, VERSION, Values, bounded, len};
use crate::frame::{self, Cursor};
use crate::{DecodeCost, IndexError};

const NO_RUN_COOKIE: u32 = 12346;
const RUN_COOKIE: u16 = 12347;
/// Containers from which a serialization with runs carries offsets.
const NO_OFFSET_THRESHOLD: usize = 4;
const MAX_CONTAINERS: usize = 1 << 16;
const ARRAY_LIMIT: usize = 4096;
const BITMAP_BYTES: usize = 8192;
/// Bytes of one container in a bitmap's vector: its key and its store.
const CONTAINER_BYTES: usize = 40;

pub(super) fn decode_cost(bytes: &[u8]) -> Result<DecodeCost, IndexError> {
    let body = frame::body(bytes, MAGIC, VERSION)?;
    let mut cursor = Cursor::new(body);
    let path_len = len(cursor.u32()?)?;
    cursor.take(path_len)?;
    cursor.take(32)?;
    let (_schema_version, row_count) = (cursor.u32()?, cursor.u32()?);
    let keys = bounded(cursor.u32()?, &cursor, 8)?;
    let mut cost = DecodeCost {
        retained: keys * size_of::<(Box<str>, Values)>(),
        scratch: 0,
    };
    let mut last_key: Option<&[u8]> = None;
    for _ in 0..keys {
        let key = text(&mut cursor)?;
        if last_key.is_some_and(|last| last >= key) {
            return Err(IndexError::BadBody("keys not strictly ascending"));
        }
        last_key = Some(key);
        let count = bounded(cursor.u32()?, &cursor, 8)?;
        cost.retained += key.len() + count * size_of::<(Box<str>, RoaringBitmap)>();
        let mut last_value: Option<&[u8]> = None;
        for _ in 0..count {
            let value = text(&mut cursor)?;
            if last_value.is_some_and(|last| last >= value) {
                return Err(IndexError::BadBody("values not strictly ascending"));
            }
            last_value = Some(value);
            let declared = len(cursor.u32()?)?;
            let bitmap = walk(cursor.take(declared)?)?;
            match bitmap.max {
                None => return Err(IndexError::BadBody("an empty bitmap")),
                Some(max) if max >= row_count => {
                    return Err(IndexError::BadBody("a row past the file's rows"));
                }
                Some(_) => {}
            }
            cost.retained += value.len() + bitmap.cost.retained;
            cost.scratch = cost.scratch.max(bitmap.cost.scratch);
        }
    }
    if cursor.remaining() != 0 {
        return Err(IndexError::BadBody("bytes after the last key"));
    }
    Ok(cost)
}

struct Walked {
    cost: DecodeCost,
    max: Option<u32>,
}

/// One bitmap's cost and largest row, refusing what `deserialize_from`
/// refuses and a serialization other than the one the bitmap writes back.
fn walk(raw: &[u8]) -> Result<Walked, IndexError> {
    let bad = || IndexError::BadBody("bitmap");
    let mut cursor = Cursor::new(raw);
    let cookie = cursor.u32()?;
    let (size, runs) = if cookie == NO_RUN_COOKIE {
        (len(cursor.u32()?)?, None)
    } else if cookie & 0xffff == u32::from(RUN_COOKIE) {
        let size = len(cookie >> 16)? + 1;
        (size, Some(cursor.take(size.div_ceil(8))?))
    } else {
        return Err(bad());
    };
    if size > MAX_CONTAINERS {
        return Err(bad());
    }
    let has_offsets = runs.is_none() || size >= NO_OFFSET_THRESHOLD;
    let descriptions = cursor.take(size * 4)?;
    if has_offsets {
        cursor.take(size * 4)?;
    }
    let header = runs.map_or(0, <[u8]>::len) + size * 4 * (1 + usize::from(has_offsets));
    let mut cost = DecodeCost {
        retained: size * CONTAINER_BYTES,
        scratch: header,
    };
    let (mut max, mut last_key, mut any_run) = (None, None, false);
    for (i, description) in descriptions.chunks_exact(4).enumerate() {
        let (key, cardinality) = pair(description).ok_or_else(bad)?;
        if last_key.is_some_and(|last| key <= last) {
            return Err(bad());
        }
        last_key = Some(key);
        let cardinality = usize::from(cardinality) + 1;
        let is_run =
            runs.is_some_and(|marks| marks.get(i / 8).is_some_and(|m| m & (1 << (i % 8)) != 0));
        let top = if is_run {
            any_run = true;
            let count = usize::from(cursor.u16()?);
            if count == 0 {
                return Err(bad());
            }
            let bytes = count * 4;
            cost.retained += bytes;
            cost.scratch = cost.scratch.max(header + bytes);
            last_run_end(cursor.take(bytes)?).ok_or_else(bad)?
        } else if cardinality <= ARRAY_LIMIT {
            cost.retained += cardinality * 2;
            last_ascending(cursor.take(cardinality * 2)?).ok_or_else(bad)?
        } else {
            cost.retained += BITMAP_BYTES;
            last_bit(cursor.take(BITMAP_BYTES)?, cardinality).ok_or_else(bad)?
        };
        max = Some(u32::from(key) << 16 | u32::from(top));
    }
    if cursor.remaining() != 0 || (runs.is_some() && !any_run) {
        return Err(IndexError::BadBody("bitmap of another length"));
    }
    Ok(Walked { cost, max })
}

/// The end of the last of `runs`, `(start, length)` pairs, when none
/// overflows, overlaps or touches the one before.
fn last_run_end(runs: &[u8]) -> Option<u16> {
    let mut last: Option<u16> = None;
    for run in runs.chunks_exact(4) {
        let (start, length) = pair(run)?;
        let end = start.checked_add(length)?;
        if last.is_some_and(|last| start <= last.saturating_add(1)) {
            return None;
        }
        last = Some(end);
    }
    last
}

/// The last of `values` when they are strictly ascending.
fn last_ascending(values: &[u8]) -> Option<u16> {
    let mut last: Option<u16> = None;
    for value in values.chunks_exact(2) {
        let &[low, high] = value else {
            return None;
        };
        let value = u16::from_le_bytes([low, high]);
        if last.is_some_and(|last| value <= last) {
            return None;
        }
        last = Some(value);
    }
    last
}

/// The highest set bit of `words` when `cardinality` bits are set.
fn last_bit(words: &[u8], cardinality: usize) -> Option<u16> {
    let mut set = 0usize;
    let mut top = None;
    for (i, word) in words.chunks_exact(8).enumerate() {
        let word = u64::from_le_bytes(word.try_into().ok()?);
        set += word.count_ones() as usize;
        if word != 0 {
            top = Some(i * 64 + 63 - word.leading_zeros() as usize);
        }
    }
    (set == cardinality).then_some(())?;
    top.and_then(|top| u16::try_from(top).ok())
}

/// Two little-endian `u16` of a four-byte chunk.
fn pair(chunk: &[u8]) -> Option<(u16, u16)> {
    let &[a, b, c, d] = chunk else {
        return None;
    };
    Some((u16::from_le_bytes([a, b]), u16::from_le_bytes([c, d])))
}

fn text<'a>(cursor: &mut Cursor<'a>) -> Result<&'a [u8], IndexError> {
    let n = len(cursor.u32()?)?;
    let bytes = cursor.take(n)?;
    std::str::from_utf8(bytes).map_err(|_| IndexError::BadBody("text not UTF-8"))?;
    Ok(bytes)
}

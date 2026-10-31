//! The header every sidecar starts with.
//!
//! ```text
//! magic [6] | version u8 | reserved u8 | body_len u32 | xxh3_64(body) u64
//! ```

use crate::IndexError;

/// Bytes of the header.
pub(crate) const HEADER_LEN: usize = 6 + 1 + 1 + 4 + 8;

/// The header for `body`.
pub(crate) fn header(magic: [u8; 6], version: u8, body: &[u8]) -> [u8; HEADER_LEN] {
    let mut out = [0u8; HEADER_LEN];
    out[..6].copy_from_slice(&magic);
    out[6] = version;
    out[8..12].copy_from_slice(&u32::try_from(body.len()).unwrap_or(u32::MAX).to_le_bytes());
    out[12..].copy_from_slice(&checksum(body).to_le_bytes());
    out
}

/// The body of a framed sidecar, after every header check.
///
/// # Errors
/// Another magic, another version, a length that does not match the bytes
/// present or a checksum that does not match the body.
pub(crate) fn body(bytes: &[u8], magic: [u8; 6], version: u8) -> Result<&[u8], IndexError> {
    let (header, body) = bytes
        .split_at_checked(HEADER_LEN)
        .ok_or(IndexError::BadMagic)?;
    if header.get(..6) != Some(magic.as_slice()) {
        return Err(IndexError::BadMagic);
    }
    let found = *header.get(6).ok_or(IndexError::BadMagic)?;
    if found != version {
        return Err(IndexError::UnknownVersion(found));
    }
    let declared = u64::from(read_u32(header, 8).ok_or(IndexError::BadMagic)?);
    let present = body.len() as u64;
    if declared != present {
        return Err(IndexError::BadLength { declared, present });
    }
    let expected = read_u64(header, 12).ok_or(IndexError::BadMagic)?;
    if checksum(body) != expected {
        return Err(IndexError::ChecksumMismatch);
    }
    Ok(body)
}

pub(crate) fn checksum(body: &[u8]) -> u64 {
    twox_hash::XxHash3_64::oneshot(body)
}

fn read_u32(bytes: &[u8], at: usize) -> Option<u32> {
    bytes
        .get(at..at + 4)
        .and_then(|b| b.try_into().ok())
        .map(u32::from_le_bytes)
}

fn read_u64(bytes: &[u8], at: usize) -> Option<u64> {
    bytes
        .get(at..at + 8)
        .and_then(|b| b.try_into().ok())
        .map(u64::from_le_bytes)
}

/// Reads little-endian fields off a body, failing past its end.
pub(crate) struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.at)
    }

    pub(crate) fn take(&mut self, len: usize) -> Result<&[u8], IndexError> {
        let end = self
            .at
            .checked_add(len)
            .ok_or(IndexError::BadBody("count past the end"))?;
        let out = self
            .bytes
            .get(self.at..end)
            .ok_or(IndexError::BadBody("count past the end"))?;
        self.at = end;
        Ok(out)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, IndexError> {
        self.take(1)?
            .first()
            .copied()
            .ok_or(IndexError::BadBody("u8"))
    }

    pub(crate) fn u16(&mut self) -> Result<u16, IndexError> {
        let raw = self.take(2)?;
        raw.try_into()
            .map(u16::from_le_bytes)
            .map_err(|_| IndexError::BadBody("u16"))
    }

    pub(crate) fn u32(&mut self) -> Result<u32, IndexError> {
        let raw = self.take(4)?;
        raw.try_into()
            .map(u32::from_le_bytes)
            .map_err(|_| IndexError::BadBody("u32"))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, IndexError> {
        let raw = self.take(8)?;
        raw.try_into()
            .map(u64::from_le_bytes)
            .map_err(|_| IndexError::BadBody("u64"))
    }
}

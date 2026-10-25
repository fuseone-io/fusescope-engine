//! Durable checkpoint: the position before which every record has been
//! materialised and must not be replayed.
//!
//! Stored in `checkpoint` next to the segments as magic, segment id, offset
//! and crc32c, written to a temporary file, fsynced and renamed into place so
//! a crash leaves either the old or the new checkpoint, never a torn one.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

use crate::WalError;
use crate::segment::sync_dir;
use crate::wal::Position;

const MAGIC: [u8; 8] = *b"FSWALCK\x01";
const FILE: &str = "checkpoint";
const TMP: &str = "checkpoint.tmp";
const LEN: usize = 8 + 8 + 8 + 4;

/// Reads the checkpoint, or `None` when there is none or it is unreadable.
/// An unreadable checkpoint means replaying from the start, which is safe for
/// correctness (duplicates are the failure mode this file prevents, so the
/// caller should log it).
pub(crate) fn read_checkpoint(dir: &Path) -> Result<Option<Position>, WalError> {
    let path = dir.join(FILE);
    let mut file = match File::open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(WalError::io(&path, e)),
    };
    let mut buf = [0u8; LEN];
    if file.read_exact(&mut buf).is_err() {
        return Ok(None);
    }
    let (magic, rest) = buf.split_at(8);
    if magic != MAGIC {
        return Ok(None);
    }
    let (body, crc) = rest.split_at(16);
    let crc = crc.try_into().map_or(0, u32::from_le_bytes);
    if crc32c::crc32c(body) != crc {
        return Ok(None);
    }
    let (segment, offset) = body.split_at(8);
    let segment = segment.try_into().map_or(0, u64::from_le_bytes);
    let offset = offset.try_into().map_or(0, u64::from_le_bytes);
    Ok(Some(Position { segment, offset }))
}

/// Writes the checkpoint atomically.
pub(crate) fn write_checkpoint(dir: &Path, position: Position) -> Result<(), WalError> {
    let mut body = Vec::with_capacity(16);
    body.extend_from_slice(&position.segment.to_le_bytes());
    body.extend_from_slice(&position.offset.to_le_bytes());
    let crc = crc32c::crc32c(&body);
    let mut buf = Vec::with_capacity(LEN);
    buf.extend_from_slice(&MAGIC);
    buf.extend_from_slice(&body);
    buf.extend_from_slice(&crc.to_le_bytes());

    let tmp = dir.join(TMP);
    let final_path = dir.join(FILE);
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp)
        .map_err(|e| WalError::io(&tmp, e))?;
    file.write_all(&buf).map_err(|e| WalError::io(&tmp, e))?;
    file.sync_all().map_err(|e| WalError::io(&tmp, e))?;
    drop(file);
    fs::rename(&tmp, &final_path).map_err(|e| WalError::io(&final_path, e))?;
    sync_dir(dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_corruption() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read_checkpoint(dir.path()).unwrap(), None);
        write_checkpoint(
            dir.path(),
            Position {
                segment: 3,
                offset: 4096,
            },
        )
        .unwrap();
        assert_eq!(
            read_checkpoint(dir.path()).unwrap(),
            Some(Position {
                segment: 3,
                offset: 4096
            })
        );
        write_checkpoint(
            dir.path(),
            Position {
                segment: 4,
                offset: 16,
            },
        )
        .unwrap();
        assert_eq!(
            read_checkpoint(dir.path()).unwrap(),
            Some(Position {
                segment: 4,
                offset: 16
            })
        );
        assert!(!dir.path().join(TMP).exists());

        fs::write(dir.path().join(FILE), b"garbage").unwrap();
        assert_eq!(
            read_checkpoint(dir.path()).unwrap(),
            None,
            "unreadable means replay everything"
        );
    }
}

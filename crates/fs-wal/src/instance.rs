//! Persistent identity of one log directory.
//!
//! Two ingesters start at the same segment and offset, and a StatefulSet pod
//! that loses its volume comes back with the same name and a fresh log. File
//! ids derived from log positions therefore also include this id, created once
//! with the directory and never changed.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

use uuid::Uuid;

use crate::WalError;
use crate::segment::sync_dir;

const MAGIC: [u8; 8] = *b"FSWALID\x01";
const FILE: &str = "instance";
const TMP: &str = "instance.tmp";

/// Reads the instance id, creating it when the directory has none.
pub(crate) fn load_or_create(dir: &Path) -> Result<Uuid, WalError> {
    let path = dir.join(FILE);
    match File::open(&path) {
        Ok(mut file) => {
            let mut buf = [0u8; 24];
            file.read_exact(&mut buf)
                .map_err(|e| WalError::io(&path, e))?;
            let (magic, id) = buf.split_at(8);
            if magic != MAGIC {
                return Err(WalError::io(
                    &path,
                    std::io::Error::other("bad instance file magic"),
                ));
            }
            Uuid::from_slice(id)
                .map_err(|e| WalError::io(&path, std::io::Error::other(e.to_string())))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let id = Uuid::new_v4();
            let tmp = dir.join(TMP);
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)
                .map_err(|e| WalError::io(&tmp, e))?;
            file.write_all(&MAGIC).map_err(|e| WalError::io(&tmp, e))?;
            file.write_all(id.as_bytes())
                .map_err(|e| WalError::io(&tmp, e))?;
            file.sync_all().map_err(|e| WalError::io(&tmp, e))?;
            drop(file);
            fs::rename(&tmp, &path).map_err(|e| WalError::io(&path, e))?;
            sync_dir(dir)?;
            Ok(id)
        }
        Err(e) => Err(WalError::io(&path, e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn created_once_and_stable() {
        let dir = tempfile::tempdir().unwrap();
        let first = load_or_create(dir.path()).unwrap();
        assert_eq!(load_or_create(dir.path()).unwrap(), first);
        let other = tempfile::tempdir().unwrap();
        assert_ne!(load_or_create(other.path()).unwrap(), first);
    }
}

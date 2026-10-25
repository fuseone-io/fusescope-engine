//! The log: a directory of segments with one active writer.

use std::fs;
use std::path::{Path, PathBuf};

use crate::WalError;
use crate::format::{parse_segment_file_name, segment_file_name};
use crate::replay::{Damage, Replay, SegmentReader};
use crate::segment::{SegmentWriter, sync_dir};

/// When appended data reaches the disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    /// Flush and fsync after every append. Safest, slowest.
    Always,
    /// Flush and fsync only on [`Wal::sync`]. The ingester calls it once per
    /// accepted request, before acknowledging.
    Batch,
}

/// Configuration of a log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalConfig {
    /// Directory holding the segments. Created if missing.
    pub dir: PathBuf,
    /// A new segment starts once the active one reaches this size.
    pub segment_max_bytes: u64,
    /// Largest accepted record. Also bounds memory during replay.
    pub max_record_bytes: usize,
    /// Sync policy.
    pub sync: SyncPolicy,
}

impl WalConfig {
    /// Defaults from DESIGN.md: 64 MiB segments, 16 MiB records, sync per batch.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            segment_max_bytes: 64 * 1024 * 1024,
            max_record_bytes: 16 * 1024 * 1024,
            sync: SyncPolicy::Batch,
        }
    }
}

/// A location in the log: segment id and byte offset. Orders by write order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position {
    /// Segment id.
    pub segment: u64,
    /// Byte offset of the record within the segment.
    pub offset: u64,
}

/// Size information about one segment on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentInfo {
    /// Segment id.
    pub id: u64,
    /// File size in bytes.
    pub bytes: u64,
}

/// What recovery found when the log was opened.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Recovery {
    /// Bytes cut from the end of the last segment because they were not a valid record.
    pub truncated_bytes: u64,
    /// Damage found at the cut point, if any.
    pub damage: Option<Damage>,
}

/// An open write-ahead log.
pub struct Wal {
    config: WalConfig,
    active: SegmentWriter,
    recovery: Recovery,
}

impl Wal {
    /// Opens or creates the log at `config.dir` and repairs the tail of the last segment.
    ///
    /// # Errors
    /// Filesystem errors.
    pub fn open(config: WalConfig) -> Result<Self, WalError> {
        fs::create_dir_all(&config.dir).map_err(|e| WalError::io(&config.dir, e))?;
        let segments = list_segments(&config.dir)?;
        let (active, recovery) = match segments.last() {
            None => (SegmentWriter::create(&config.dir, 0)?, Recovery::default()),
            Some(&(id, ref path)) => recover_last(&config, id, path)?,
        };
        Ok(Self {
            config,
            active,
            recovery,
        })
    }

    /// What recovery did on open.
    pub fn recovery(&self) -> &Recovery {
        &self.recovery
    }

    /// Where the next append will land.
    pub fn position(&self) -> Position {
        Position {
            segment: self.active.id(),
            offset: self.active.len(),
        }
    }

    /// Appends one record, rotating the segment first if the active one is full.
    ///
    /// # Errors
    /// Empty or oversized payloads, and filesystem errors.
    pub fn append(&mut self, payload: &[u8]) -> Result<Position, WalError> {
        if payload.is_empty() {
            return Err(WalError::EmptyRecord);
        }
        if payload.len() > self.config.max_record_bytes {
            return Err(WalError::RecordTooLarge {
                size: payload.len(),
                max: self.config.max_record_bytes,
            });
        }
        if self.active.len() >= self.config.segment_max_bytes {
            self.rotate()?;
        }
        let offset = self.active.append(payload)?;
        if self.config.sync == SyncPolicy::Always {
            self.active.sync()?;
        }
        Ok(Position {
            segment: self.active.id(),
            offset,
        })
    }

    /// Makes every append so far durable.
    ///
    /// # Errors
    /// Filesystem errors.
    pub fn sync(&mut self) -> Result<(), WalError> {
        self.active.sync()
    }

    /// Closes the active segment and starts the next one.
    fn rotate(&mut self) -> Result<(), WalError> {
        self.active.sync()?;
        let next = self
            .active
            .id()
            .checked_add(1)
            .ok_or(WalError::SegmentIdOverflow)?;
        self.active = SegmentWriter::create(&self.config.dir, next)?;
        Ok(())
    }

    /// Iterates every record on disk, oldest first. Call before appending; the
    /// active segment is read up to the last [`Wal::sync`].
    ///
    /// # Errors
    /// Filesystem errors while listing segments.
    pub fn replay(&mut self) -> Result<Replay, WalError> {
        self.active.sync()?;
        Ok(Replay::new(
            list_segments(&self.config.dir)?,
            self.config.max_record_bytes,
        ))
    }

    /// Deletes every segment that ends before `position`, keeping the one that
    /// contains it. Granularity is the segment, so the ingester calls this with
    /// the position of the first record not yet flushed.
    ///
    /// # Errors
    /// Filesystem errors.
    pub fn truncate_before(&mut self, position: Position) -> Result<usize, WalError> {
        let mut removed = 0;
        for (id, path) in list_segments(&self.config.dir)? {
            if id < position.segment && id != self.active.id() {
                fs::remove_file(&path).map_err(|e| WalError::io(&path, e))?;
                removed += 1;
            }
        }
        if removed > 0 {
            sync_dir(&self.config.dir)?;
        }
        Ok(removed)
    }

    /// Segments on disk with their sizes, oldest first.
    ///
    /// # Errors
    /// Filesystem errors.
    pub fn segments(&self) -> Result<Vec<SegmentInfo>, WalError> {
        list_segments(&self.config.dir)?
            .into_iter()
            .map(|(id, path)| {
                let bytes = fs::metadata(&path)
                    .map_err(|e| WalError::io(&path, e))?
                    .len();
                Ok(SegmentInfo { id, bytes })
            })
            .collect()
    }
}

/// Validates the last segment and cuts a torn tail so appends can continue.
fn recover_last(
    config: &WalConfig,
    id: u64,
    path: &Path,
) -> Result<(SegmentWriter, Recovery), WalError> {
    let file_len = fs::metadata(path).map_err(|e| WalError::io(path, e))?.len();
    let scan = match SegmentReader::open(id, path) {
        Ok(reader) => reader.scan(config.max_record_bytes),
        Err(damage) => {
            // Unreadable header: leave the file for forensics and start a new segment.
            let next = id.checked_add(1).ok_or(WalError::SegmentIdOverflow)?;
            let recovery = Recovery {
                truncated_bytes: 0,
                damage: Some(damage),
            };
            return Ok((SegmentWriter::create(&config.dir, next)?, recovery));
        }
    };
    let mut recovery = Recovery {
        truncated_bytes: file_len - scan.valid_len,
        damage: scan.damage,
    };
    if recovery.truncated_bytes > 0 {
        let file = fs::OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(|e| WalError::io(path, e))?;
        file.set_len(scan.valid_len)
            .map_err(|e| WalError::io(path, e))?;
        file.sync_all().map_err(|e| WalError::io(path, e))?;
    } else {
        recovery.damage = None;
    }
    if scan.valid_len >= config.segment_max_bytes {
        let next = id.checked_add(1).ok_or(WalError::SegmentIdOverflow)?;
        return Ok((SegmentWriter::create(&config.dir, next)?, recovery));
    }
    Ok((
        SegmentWriter::open_at(path.to_path_buf(), id, scan.valid_len)?,
        recovery,
    ))
}

/// Segment files in `dir`, sorted by id. Other files are ignored.
fn list_segments(dir: &Path) -> Result<Vec<(u64, PathBuf)>, WalError> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir).map_err(|e| WalError::io(dir, e))? {
        let entry = entry.map_err(|e| WalError::io(dir, e))?;
        let name = entry.file_name();
        if let Some(id) = name.to_str().and_then(parse_segment_file_name) {
            out.push((id, dir.join(segment_file_name(id))));
        }
    }
    out.sort_unstable();
    Ok(out)
}

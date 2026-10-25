//! A single segment file open for appending.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::WalError;
use crate::format::{HEADER_LEN, encode_header, encode_record_header};

/// Buffer in front of the segment file. Records are usually a few KiB to a
/// few MiB; the buffer absorbs small appends between syncs.
const WRITE_BUFFER: usize = 256 * 1024;

pub(crate) struct SegmentWriter {
    id: u64,
    path: PathBuf,
    file: BufWriter<File>,
    /// Bytes written so far, including the header and anything still buffered.
    len: u64,
}

impl SegmentWriter {
    /// Creates a new segment with its header and makes the directory entry durable.
    pub(crate) fn create(dir: &Path, id: u64) -> Result<Self, WalError> {
        let path = dir.join(crate::format::segment_file_name(id));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .map_err(|e| WalError::io(&path, e))?;
        file.write_all(&encode_header(id))
            .map_err(|e| WalError::io(&path, e))?;
        file.sync_all().map_err(|e| WalError::io(&path, e))?;
        sync_dir(dir)?;
        Ok(Self {
            id,
            path,
            file: BufWriter::with_capacity(WRITE_BUFFER, file),
            len: HEADER_LEN,
        })
    }

    /// Reopens an existing, already validated segment for appending at `len`.
    pub(crate) fn open_at(path: PathBuf, id: u64, len: u64) -> Result<Self, WalError> {
        let file = OpenOptions::new()
            .append(true)
            .open(&path)
            .map_err(|e| WalError::io(&path, e))?;
        Ok(Self {
            id,
            path,
            file: BufWriter::with_capacity(WRITE_BUFFER, file),
            len,
        })
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn len(&self) -> u64 {
        self.len
    }

    /// Appends one record and returns its byte offset within the segment.
    pub(crate) fn append(&mut self, payload: &[u8]) -> Result<u64, WalError> {
        let header = encode_record_header(payload).ok_or(WalError::RecordTooLarge {
            size: payload.len(),
            max: u32::MAX as usize,
        })?;
        let offset = self.len;
        self.file
            .write_all(&header)
            .map_err(|e| WalError::io(&self.path, e))?;
        self.file
            .write_all(payload)
            .map_err(|e| WalError::io(&self.path, e))?;
        self.len += (header.len() + payload.len()) as u64;
        Ok(offset)
    }

    /// Flushes the buffer and fsyncs the file.
    pub(crate) fn sync(&mut self) -> Result<(), WalError> {
        self.file.flush().map_err(|e| WalError::io(&self.path, e))?;
        self.file
            .get_ref()
            .sync_data()
            .map_err(|e| WalError::io(&self.path, e))
    }
}

/// Fsyncs a directory so new or removed entries survive a crash.
pub(crate) fn sync_dir(dir: &Path) -> Result<(), WalError> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| WalError::io(dir, e))
}

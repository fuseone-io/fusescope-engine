//! Sequential reading of segments with damage reporting.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use crate::format::{HEADER_LEN, RECORD_HEADER_LEN, decode_header, decode_record_header, verify};
use crate::wal::Position;

/// One record read back from the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Where the record starts. Compare with [`Position`]s returned by append.
    pub position: Position,
    /// The payload exactly as appended.
    pub payload: Vec<u8>,
}

/// Why a segment could not be read to its end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Damage {
    /// The header is missing or carries the wrong magic or id. The file is skipped.
    BadHeader,
    /// The file ends inside a record. Expected after a crash on the last segment.
    TruncatedTail,
    /// A record's checksum does not match. Nothing after it can be trusted.
    ChecksumMismatch,
    /// A record claims a length beyond the configured maximum or the file size.
    BadLength,
    /// Reading failed.
    Io(String),
}

/// What a replay yields: a record, or the point where a segment stopped being readable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// A valid record.
    Record(Entry),
    /// A segment could not be read past `position`. Replay continues with the next segment.
    Damaged {
        /// Segment id and byte offset where reading stopped.
        position: Position,
        /// Why.
        damage: Damage,
    },
}

/// Iterator over every record of every segment, in order.
pub struct Replay {
    segments: std::vec::IntoIter<(u64, PathBuf)>,
    current: Option<SegmentReader>,
    max_record_bytes: usize,
    skip_before: Option<Position>,
}

impl Replay {
    pub(crate) fn new(segments: Vec<(u64, PathBuf)>, max_record_bytes: usize) -> Self {
        Self {
            segments: segments.into_iter(),
            current: None,
            max_record_bytes,
            skip_before: None,
        }
    }

    /// Yields only records at or after `position`. Segments entirely before it
    /// are not even opened.
    pub(crate) fn skipping_before(mut self, position: Option<Position>) -> Self {
        self.skip_before = position;
        self
    }
}

impl Iterator for Replay {
    type Item = Event;

    fn next(&mut self) -> Option<Event> {
        loop {
            if let Some(reader) = self.current.as_mut() {
                match reader.next_record(self.max_record_bytes) {
                    Step::Record(entry)
                        if self.skip_before.is_some_and(|cut| entry.position < cut) => {}
                    Step::Record(entry) => return Some(Event::Record(entry)),
                    Step::End => self.current = None,
                    Step::Damaged(position, damage) => {
                        self.current = None;
                        return Some(Event::Damaged { position, damage });
                    }
                }
                continue;
            }
            let (id, path) = self.segments.next()?;
            if self.skip_before.is_some_and(|cut| id < cut.segment) {
                continue;
            }
            match SegmentReader::open(id, &path) {
                Ok(reader) => self.current = Some(reader),
                Err(damage) => {
                    return Some(Event::Damaged {
                        position: Position {
                            segment: id,
                            offset: 0,
                        },
                        damage,
                    });
                }
            }
        }
    }
}

enum Step {
    Record(Entry),
    End,
    Damaged(Position, Damage),
}

/// Result of scanning a whole segment for recovery.
pub(crate) struct Scan {
    /// Offset just after the last valid record.
    pub(crate) valid_len: u64,
    /// Damage found at `valid_len`, if the scan did not reach a clean end.
    pub(crate) damage: Option<Damage>,
}

pub(crate) struct SegmentReader {
    id: u64,
    reader: BufReader<File>,
    file_len: u64,
    offset: u64,
}

impl SegmentReader {
    pub(crate) fn open(id: u64, path: &Path) -> Result<Self, Damage> {
        let file = File::open(path).map_err(|e| Damage::Io(e.to_string()))?;
        let file_len = file
            .metadata()
            .map_err(|e| Damage::Io(e.to_string()))?
            .len();
        let mut reader = BufReader::with_capacity(256 * 1024, file);
        let mut header = [0u8; 16];
        reader
            .read_exact(&mut header)
            .map_err(|_| Damage::BadHeader)?;
        if decode_header(&header) != Some(id) {
            return Err(Damage::BadHeader);
        }
        Ok(Self {
            id,
            reader,
            file_len,
            offset: HEADER_LEN,
        })
    }

    /// Scans to the end, returning where valid data stops.
    pub(crate) fn scan(mut self, max_record_bytes: usize) -> Scan {
        loop {
            match self.next_record(max_record_bytes) {
                Step::Record(_) => {}
                Step::End => {
                    return Scan {
                        valid_len: self.offset,
                        damage: None,
                    };
                }
                Step::Damaged(position, damage) => {
                    return Scan {
                        valid_len: position.offset,
                        damage: Some(damage),
                    };
                }
            }
        }
    }

    fn next_record(&mut self, max_record_bytes: usize) -> Step {
        let start = Position {
            segment: self.id,
            offset: self.offset,
        };
        if self.offset == self.file_len {
            return Step::End;
        }
        let remaining = self.file_len - self.offset;
        if remaining < RECORD_HEADER_LEN as u64 {
            return Step::Damaged(start, Damage::TruncatedTail);
        }
        let mut header = [0u8; RECORD_HEADER_LEN];
        if let Err(e) = self.reader.read_exact(&mut header) {
            return Step::Damaged(start, Damage::Io(e.to_string()));
        }
        let (len, crc) = decode_record_header(header);
        let len_usize = len as usize;
        if len == 0 || len_usize > max_record_bytes {
            return Step::Damaged(start, Damage::BadLength);
        }
        if u64::from(len) > remaining - RECORD_HEADER_LEN as u64 {
            return Step::Damaged(start, Damage::TruncatedTail);
        }
        let mut payload = vec![0u8; len_usize];
        if let Err(e) = self.reader.read_exact(&mut payload) {
            return Step::Damaged(start, Damage::Io(e.to_string()));
        }
        if !verify(&payload, crc) {
            return Step::Damaged(start, Damage::ChecksumMismatch);
        }
        self.offset += (RECORD_HEADER_LEN + len_usize) as u64;
        Step::Record(Entry {
            position: start,
            payload,
        })
    }
}

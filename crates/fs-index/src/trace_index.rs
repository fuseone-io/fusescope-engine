//! The `trace_id` sidecar: which files of an hour hold a trace.
//!
//! One sidecar per L0 file lists the traces the file holds; the compactor
//! merges them into one per hour mapping each trace to its files. The
//! querier reads the sidecars of the files a time range keeps and opens only
//! those listing a requested trace. A sidecar is derived from its files and
//! never the source of truth: missing or damaged, the querier scans.
//!
//! # Format, version 1
//!
//! Little-endian throughout.
//!
//! ```text
//! header   "FSTIX\0" | version u8 | reserved u8 | body_len u32 | xxh3_64(body) u64
//! body     file_count u32
//!          file_count × (path_len u16 | path utf-8)
//!          entry_count u32
//!          entry_count × (trace_id [16] | file u32)      sorted by (trace_id, file), unique
//! ```

use std::ops::Range;

use crate::IndexError;

/// Bytes of a trace id.
pub const TRACE_ID_BYTES: usize = 16;

const MAGIC: &[u8; 6] = b"FSTIX\0";
const VERSION: u8 = 1;
const HEADER_LEN: usize = 6 + 1 + 1 + 4 + 8;
const ENTRY_LEN: usize = TRACE_ID_BYTES + 4;

/// One trace in one file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Entry {
    trace: [u8; TRACE_ID_BYTES],
    file: u32,
}

/// The decoded sidecar: file paths and the sorted (trace, file) pairs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceIndex {
    files: Vec<String>,
    entries: Vec<Entry>,
}

impl TraceIndex {
    /// Builds an index over `files` from `(trace, file index)` pairs, in any
    /// order and with repeats.
    ///
    /// # Errors
    /// A file index without a file, or more files or entries than the format
    /// counts.
    pub fn new(
        files: Vec<String>,
        pairs: impl IntoIterator<Item = ([u8; TRACE_ID_BYTES], u32)>,
    ) -> Result<Self, IndexError> {
        let file_count = u32::try_from(files.len()).map_err(|_| IndexError::TooLarge("files"))?;
        if files.iter().any(|f| u16::try_from(f.len()).is_err()) {
            return Err(IndexError::TooLarge("file path"));
        }
        let mut entries: Vec<Entry> = pairs
            .into_iter()
            .map(|(trace, file)| {
                if file < file_count {
                    Ok(Entry { trace, file })
                } else {
                    Err(IndexError::BadBody("file index without a file"))
                }
            })
            .collect::<Result<_, _>>()?;
        entries.sort_unstable();
        entries.dedup();
        u32::try_from(entries.len()).map_err(|_| IndexError::TooLarge("entries"))?;
        Ok(Self { files, entries })
    }

    /// The files the index covers, in the order entries refer to them.
    pub fn files(&self) -> &[String] {
        &self.files
    }

    /// Number of (trace, file) pairs.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True when no trace is listed.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Heap bytes the decoded index holds, for a cache budget.
    pub fn heap_bytes(&self) -> usize {
        self.entries.len() * ENTRY_LEN + self.files.iter().map(|f| f.len() + 24).sum::<usize>()
    }

    /// Files holding `trace`, in index order.
    pub fn files_for(&self, trace: &[u8; TRACE_ID_BYTES]) -> impl Iterator<Item = &str> {
        let range = self.range_of(trace);
        self.entries
            .get(range)
            .unwrap_or(&[])
            .iter()
            .filter_map(|e| self.files.get(e.file as usize).map(String::as_str))
    }

    /// True when any of `traces` is listed.
    pub fn contains_any(&self, traces: &[[u8; TRACE_ID_BYTES]]) -> bool {
        traces.iter().any(|t| !self.range_of(t).is_empty())
    }

    /// True when the index covers the file at `path`.
    pub fn covers(&self, path: &str) -> bool {
        self.files.iter().any(|f| f == path)
    }

    /// True when the index lists any of `traces` in the file at `path`.
    /// False for a path the index does not cover.
    pub fn file_holds(&self, path: &str, traces: &[[u8; TRACE_ID_BYTES]]) -> bool {
        let Some(file) = self.files.iter().position(|f| f == path) else {
            return false;
        };
        let Ok(file) = u32::try_from(file) else {
            return false;
        };
        traces.iter().any(|trace| {
            self.entries
                .binary_search(&Entry {
                    trace: *trace,
                    file,
                })
                .is_ok()
        })
    }

    fn range_of(&self, trace: &[u8; TRACE_ID_BYTES]) -> Range<usize> {
        let start = self.entries.partition_point(|e| e.trace < *trace);
        let end = start
            + self
                .entries
                .get(start..)
                .unwrap_or(&[])
                .partition_point(|e| e.trace == *trace);
        start..end
    }

    /// Serialises the sidecar with its header.
    pub fn encode(&self) -> Vec<u8> {
        let mut body = Vec::with_capacity(4 + self.entries.len() * ENTRY_LEN);
        body.extend_from_slice(&u32::try_from(self.files.len()).unwrap_or(0).to_le_bytes());
        for file in &self.files {
            body.extend_from_slice(&u16::try_from(file.len()).unwrap_or(0).to_le_bytes());
            body.extend_from_slice(file.as_bytes());
        }
        body.extend_from_slice(&u32::try_from(self.entries.len()).unwrap_or(0).to_le_bytes());
        for e in &self.entries {
            body.extend_from_slice(&e.trace);
            body.extend_from_slice(&e.file.to_le_bytes());
        }
        let mut out = Vec::with_capacity(HEADER_LEN + body.len());
        out.extend_from_slice(MAGIC);
        out.push(VERSION);
        out.push(0);
        out.extend_from_slice(&u32::try_from(body.len()).unwrap_or(u32::MAX).to_le_bytes());
        out.extend_from_slice(&checksum(&body).to_le_bytes());
        out.extend_from_slice(&body);
        out
    }

    /// Parses a sidecar, verifying version, length and checksum.
    ///
    /// # Errors
    /// Any header or body check that fails; see [`IndexError`].
    pub fn decode(bytes: &[u8]) -> Result<Self, IndexError> {
        let (header, body) = bytes
            .split_at_checked(HEADER_LEN)
            .ok_or(IndexError::BadMagic)?;
        if header.get(..6) != Some(MAGIC.as_slice()) {
            return Err(IndexError::BadMagic);
        }
        let version = *header.get(6).ok_or(IndexError::BadMagic)?;
        if version != VERSION {
            return Err(IndexError::UnknownVersion(version));
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
        let mut cursor = Cursor { bytes: body, at: 0 };
        let file_count = cursor.u32()? as usize;
        let mut files = Vec::with_capacity(file_count.min(1 << 16));
        for _ in 0..file_count {
            let len = cursor.u16()? as usize;
            let raw = cursor.take(len)?;
            let path = std::str::from_utf8(raw).map_err(|_| IndexError::BadBody("path"))?;
            files.push(path.to_owned());
        }
        let entry_count = cursor.u32()? as usize;
        let mut entries = Vec::with_capacity(entry_count.min(1 << 20));
        let mut last: Option<Entry> = None;
        for _ in 0..entry_count {
            let mut trace = [0u8; TRACE_ID_BYTES];
            trace.copy_from_slice(cursor.take(TRACE_ID_BYTES)?);
            let file = cursor.u32()?;
            let entry = Entry { trace, file };
            if file as usize >= files.len() {
                return Err(IndexError::BadBody("file index without a file"));
            }
            if last.is_some_and(|l| l >= entry) {
                return Err(IndexError::BadBody("entries out of order"));
            }
            last = Some(entry);
            entries.push(entry);
        }
        if cursor.at != body.len() {
            return Err(IndexError::BadBody("trailing bytes"));
        }
        Ok(Self { files, entries })
    }
}

/// The sidecar of a span file: `.../traces/l{n}/dt=.../hr=.../{id}.parquet`
/// becomes `.../traces/index/trace_id/dt=.../hr=.../{id}.tix`. `None` for a
/// path this engine does not lay out.
pub fn sidecar_path(parquet: &str) -> Option<String> {
    let stem = parquet.strip_suffix(".parquet")?;
    let (prefix, rest) = stem.split_once("/traces/l")?;
    let (level, partition) = rest.split_once('/')?;
    if level.len() != 1 || !level.bytes().all(|b| b.is_ascii_digit()) || partition.is_empty() {
        return None;
    }
    Some(format!("{prefix}/traces/index/trace_id/{partition}.tix"))
}

fn checksum(body: &[u8]) -> u64 {
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

struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], IndexError> {
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

    fn u16(&mut self) -> Result<u16, IndexError> {
        let raw = self.take(2)?;
        raw.try_into()
            .map(u16::from_le_bytes)
            .map_err(|_| IndexError::BadBody("u16"))
    }

    fn u32(&mut self) -> Result<u32, IndexError> {
        let raw = self.take(4)?;
        raw.try_into()
            .map(u32::from_le_bytes)
            .map_err(|_| IndexError::BadBody("u32"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trace(n: u8) -> [u8; TRACE_ID_BYTES] {
        [n; TRACE_ID_BYTES]
    }

    fn sample() -> TraceIndex {
        TraceIndex::new(
            vec!["a.parquet".into(), "b.parquet".into()],
            [
                (trace(3), 1),
                (trace(1), 0),
                (trace(3), 0),
                (trace(1), 0),
                (trace(2), 1),
            ],
        )
        .unwrap()
    }

    #[test]
    fn round_trip_keeps_files_and_sorted_unique_entries() {
        let index = sample();
        assert_eq!(index.len(), 4, "the repeated pair collapses");
        let bytes = index.encode();
        let back = TraceIndex::decode(&bytes).unwrap();
        assert_eq!(back, index);
        assert_eq!(
            back.files_for(&trace(3)).collect::<Vec<_>>(),
            ["a.parquet", "b.parquet"]
        );
        assert_eq!(back.files_for(&trace(2)).collect::<Vec<_>>(), ["b.parquet"]);
        assert_eq!(back.files_for(&trace(9)).count(), 0);
        assert!(back.contains_any(&[trace(9), trace(1)]));
        assert!(!back.contains_any(&[trace(9), trace(0)]));
        assert!(!back.contains_any(&[]));
    }

    #[test]
    fn encoding_is_deterministic_whatever_the_input_order() {
        let a = TraceIndex::new(vec!["f".into()], [(trace(1), 0), (trace(2), 0)]).unwrap();
        let b = TraceIndex::new(
            vec!["f".into()],
            [(trace(2), 0), (trace(1), 0), (trace(2), 0)],
        )
        .unwrap();
        assert_eq!(a.encode(), b.encode());
        assert_eq!(a.encode().len(), HEADER_LEN + 4 + 2 + 1 + 4 + 2 * ENTRY_LEN);
    }

    #[test]
    fn a_file_index_without_a_file_is_refused() {
        let err = TraceIndex::new(vec!["f".into()], [(trace(1), 1)]).unwrap_err();
        assert_eq!(err, IndexError::BadBody("file index without a file"));
    }

    #[test]
    fn every_header_check_fails_loudly() {
        let good = sample().encode();

        let mut magic = good.clone();
        magic[0] = b'X';
        assert_eq!(
            TraceIndex::decode(&magic).unwrap_err(),
            IndexError::BadMagic
        );
        assert_eq!(
            TraceIndex::decode(&good[..10]).unwrap_err(),
            IndexError::BadMagic
        );

        let mut version = good.clone();
        version[6] = 7;
        assert_eq!(
            TraceIndex::decode(&version).unwrap_err(),
            IndexError::UnknownVersion(7)
        );

        let short = &good[..good.len() - 1];
        assert!(matches!(
            TraceIndex::decode(short).unwrap_err(),
            IndexError::BadLength { .. }
        ));

        let mut flipped = good.clone();
        let last = flipped.len() - 1;
        flipped[last] ^= 0xff;
        assert_eq!(
            TraceIndex::decode(&flipped).unwrap_err(),
            IndexError::ChecksumMismatch
        );
    }

    #[test]
    fn a_body_that_hashes_but_is_malformed_is_refused() {
        // Entries out of order, re-signed so only the body check can catch it.
        let index = sample();
        let mut bytes = index.encode();
        let body_start = HEADER_LEN;
        let entries_start = body_start + 4 + (2 + 9) * 2 + 4;
        let (first, second) = (entries_start, entries_start + ENTRY_LEN);
        let mut a = [0u8; ENTRY_LEN];
        a.copy_from_slice(&bytes[first..first + ENTRY_LEN]);
        let mut b = [0u8; ENTRY_LEN];
        b.copy_from_slice(&bytes[second..second + ENTRY_LEN]);
        bytes[first..first + ENTRY_LEN].copy_from_slice(&b);
        bytes[second..second + ENTRY_LEN].copy_from_slice(&a);
        let sum = checksum(&bytes[body_start..]).to_le_bytes();
        bytes[12..20].copy_from_slice(&sum);
        assert_eq!(
            TraceIndex::decode(&bytes).unwrap_err(),
            IndexError::BadBody("entries out of order")
        );
    }

    #[test]
    fn a_file_holds_a_trace_only_when_the_pair_is_listed() {
        let index = sample();
        assert!(index.file_holds("a.parquet", &[trace(1), trace(9)]));
        assert!(index.file_holds("b.parquet", &[trace(2)]));
        assert!(
            !index.file_holds("b.parquet", &[trace(1)]),
            "trace 1 is in a only"
        );
        assert!(!index.file_holds("c.parquet", &[trace(1)]), "not covered");
        assert!(index.covers("a.parquet") && !index.covers("c.parquet"));
    }

    /// The bytes of version 1 for a canonical input. Changing the encoder
    /// or the decoder without bumping `VERSION` fails here.
    #[test]
    fn canonical_input_gives_the_pinned_bytes_for_version_1() {
        let index = TraceIndex::new(
            vec!["t/a/traces/l0/dt=2026-10-24/hr=14/f.parquet".into()],
            [(trace(2), 0), (trace(1), 0)],
        )
        .unwrap();
        let pinned = "46535449580001005d000000fbbda878620547bc010000002b00742f612f7472616365732f6c302f64743d323032362d31302d32342f68723d31342f662e706172717565740200000001010101010101010101010101010101000000000202020202020202020202020202020200000000";
        let bytes = index.encode();
        assert_eq!(hex(&bytes), pinned);
        assert_eq!(TraceIndex::decode(&unhex(pinned)).unwrap(), index);
    }

    fn hex(bytes: &[u8]) -> String {
        use std::fmt::Write;
        bytes.iter().fold(String::new(), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
    }

    fn unhex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn sidecar_paths_follow_the_file_layout() {
        assert_eq!(
            sidecar_path("t/abc/traces/l0/dt=2026-10-24/hr=14/id.parquet").as_deref(),
            Some("t/abc/traces/index/trace_id/dt=2026-10-24/hr=14/id.tix")
        );
        assert_eq!(
            sidecar_path("engine/t/abc/traces/l1/dt=2026-10-24/hr=14/id.parquet").as_deref(),
            Some("engine/t/abc/traces/index/trace_id/dt=2026-10-24/hr=14/id.tix")
        );
        assert_eq!(
            sidecar_path("t/abc/logs/l0/dt=2026-10-24/hr=14/id.parquet"),
            None
        );
        assert_eq!(
            sidecar_path("t/abc/traces/l0/dt=2026-10-24/hr=14/id.tix"),
            None
        );
        assert_eq!(
            sidecar_path("t/abc/traces/lx/dt=2026-10-24/hr=14/id.parquet"),
            None
        );
    }
}

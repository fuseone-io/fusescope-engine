//! Behaviour of the write-ahead log across appends, rotation, crashes and truncation.
#![allow(
    clippy::too_many_lines,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use fs_wal::{Damage, Event, HEADER_LEN, Position, SyncPolicy, Wal, WalConfig, WalError};

fn config(dir: &Path) -> WalConfig {
    WalConfig {
        segment_max_bytes: 4096,
        max_record_bytes: 1024,
        ..WalConfig::new(dir)
    }
}

fn payload(i: usize) -> Vec<u8> {
    format!("record-{i:04}-").repeat(10).into_bytes() // 120 bytes
}

fn records(wal: &mut Wal) -> (Vec<Vec<u8>>, Vec<Event>) {
    let mut payloads = Vec::new();
    let mut damaged = Vec::new();
    for event in wal.replay().unwrap() {
        match event {
            Event::Record(entry) => payloads.push(entry.payload),
            other @ Event::Damaged { .. } => damaged.push(other),
        }
    }
    (payloads, damaged)
}

#[test]
fn append_replay_round_trip_across_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::open(config(dir.path())).unwrap();
    let mut positions = Vec::new();
    for i in 0..100 {
        positions.push(wal.append(&payload(i)).unwrap());
    }
    wal.sync().unwrap();
    assert!(
        positions.windows(2).all(|w| w[0] < w[1]),
        "positions are monotonic"
    );
    let segments = wal.segments().unwrap();
    assert!(
        segments.len() > 2,
        "4 KiB segments must have rotated: {segments:?}"
    );
    assert!(segments.iter().all(|s| s.bytes >= HEADER_LEN));

    let (payloads, damaged) = records(&mut wal);
    assert_eq!(damaged, vec![]);
    assert_eq!(payloads.len(), 100);
    assert_eq!(payloads[0], payload(0));
    assert_eq!(payloads[99], payload(99));

    // Positions reported by append are the ones replay sees.
    let replayed: Vec<Position> = wal
        .replay()
        .unwrap()
        .filter_map(|e| match e {
            Event::Record(entry) => Some(entry.position),
            Event::Damaged { .. } => None,
        })
        .collect();
    assert_eq!(replayed, positions);
}

#[test]
fn reopen_continues_after_last_record() {
    let dir = tempfile::tempdir().unwrap();
    let before = {
        let mut wal = Wal::open(config(dir.path())).unwrap();
        for i in 0..10 {
            wal.append(&payload(i)).unwrap();
        }
        wal.sync().unwrap();
        wal.position()
    };
    let mut wal = Wal::open(config(dir.path())).unwrap();
    assert_eq!(wal.recovery().truncated_bytes, 0);
    assert_eq!(
        wal.position(),
        before,
        "appends resume where the last run stopped"
    );
    wal.append(&payload(10)).unwrap();
    wal.sync().unwrap();
    let (payloads, damaged) = records(&mut wal);
    assert_eq!(damaged, vec![]);
    assert_eq!(payloads.len(), 11);
}

#[test]
fn torn_tail_is_cut_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let last_segment = {
        let mut wal = Wal::open(config(dir.path())).unwrap();
        for i in 0..5 {
            wal.append(&payload(i)).unwrap();
        }
        wal.sync().unwrap();
        wal.position().segment
    };
    // Simulate a crash mid-write: append half a record by hand.
    let path = dir.path().join(format!("{last_segment:020}.wal"));
    let len = fs::metadata(&path).unwrap().len();
    let mut f = OpenOptions::new().append(true).open(&path).unwrap();
    f.write_all(&[200u8, 0, 0, 0, 1, 2, 3, 4, 9, 9, 9]).unwrap(); // header says 200 bytes, only 3 follow
    drop(f);

    let mut wal = Wal::open(config(dir.path())).unwrap();
    assert_eq!(wal.recovery().truncated_bytes, 11);
    assert_eq!(wal.recovery().damage, Some(Damage::TruncatedTail));
    assert_eq!(fs::metadata(&path).unwrap().len(), len);

    wal.append(&payload(5)).unwrap();
    wal.sync().unwrap();
    let (payloads, damaged) = records(&mut wal);
    assert_eq!(damaged, vec![]);
    assert_eq!(payloads.len(), 6);
    assert_eq!(payloads[5], payload(5));
}

#[test]
fn corrupted_middle_segment_is_reported_and_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::open(config(dir.path())).unwrap();
    let mut positions = Vec::new();
    for i in 0..100 {
        positions.push(wal.append(&payload(i)).unwrap());
    }
    wal.sync().unwrap();
    let segments = wal.segments().unwrap();
    assert!(segments.len() >= 3);
    let victim = segments[1].id;

    // Flip one payload byte of the second record in the middle segment.
    let second = positions
        .iter()
        .filter(|p| p.segment == victim)
        .nth(1)
        .unwrap();
    let path = dir.path().join(format!("{victim:020}.wal"));
    let mut f = OpenOptions::new().write(true).open(&path).unwrap();
    f.seek(SeekFrom::Start(second.offset + 8 + 3)).unwrap();
    f.write_all(b"!").unwrap();
    drop(f);

    let (payloads, damaged) = records(&mut wal);
    assert_eq!(
        damaged,
        vec![Event::Damaged {
            position: *second,
            damage: Damage::ChecksumMismatch
        }]
    );
    let in_victim_before = positions.iter().filter(|p| p.segment == victim).count();
    let expected = 100 - (in_victim_before - 1);
    assert_eq!(
        payloads.len(),
        expected,
        "records after the bad one in that segment are lost, later segments survive"
    );
    assert_eq!(payloads.last().unwrap(), &payload(99));
}

#[test]
fn truncate_before_removes_only_covered_segments() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::open(config(dir.path())).unwrap();
    let mut positions = Vec::new();
    for i in 0..60 {
        positions.push(wal.append(&payload(i)).unwrap());
    }
    wal.sync().unwrap();
    let before = wal.segments().unwrap();
    let cut = positions[30];
    let removed = wal.truncate_before(cut).unwrap();
    let after = wal.segments().unwrap();
    assert_eq!(
        removed,
        before.iter().filter(|s| s.id < cut.segment).count()
    );
    assert!(after.iter().all(|s| s.id >= cut.segment));
    let (payloads, damaged) = records(&mut wal);
    assert_eq!(damaged, vec![]);
    assert!(payloads.contains(&payload(30)));
    assert!(payloads.contains(&payload(59)));
    // Truncating at the live position never removes the active segment.
    wal.truncate_before(wal.position()).unwrap();
    assert_ne!(wal.segments().unwrap().len(), 0);
}

#[test]
fn rejects_empty_and_oversized_records() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::open(config(dir.path())).unwrap();
    assert!(matches!(wal.append(&[]), Err(WalError::EmptyRecord)));
    assert!(matches!(
        wal.append(&[0u8; 1025]),
        Err(WalError::RecordTooLarge {
            size: 1025,
            max: 1024
        })
    ));
    wal.append(&[0u8; 1024]).unwrap();
}

#[test]
fn ignores_foreign_files_and_bad_headers() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("notes.txt"), b"hello").unwrap();
    fs::write(
        dir.path().join("00000000000000000000.wal"),
        b"garbage header here!",
    )
    .unwrap();
    let mut wal = Wal::open(config(dir.path())).unwrap();
    assert_eq!(wal.recovery().damage, Some(Damage::BadHeader));
    assert_eq!(
        wal.position().segment,
        1,
        "a fresh segment follows the unreadable one"
    );
    wal.append(&payload(1)).unwrap();
    wal.sync().unwrap();
    let (payloads, damaged) = records(&mut wal);
    assert_eq!(payloads.len(), 1);
    assert_eq!(
        damaged,
        vec![Event::Damaged {
            position: Position {
                segment: 0,
                offset: 0
            },
            damage: Damage::BadHeader
        }]
    );
}

#[test]
fn sync_always_is_durable_without_explicit_sync() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut wal = Wal::open(WalConfig {
            sync: SyncPolicy::Always,
            ..config(dir.path())
        })
        .unwrap();
        wal.append(&payload(1)).unwrap();
        // no sync(), drop without flushing explicitly
    }
    let mut wal = Wal::open(config(dir.path())).unwrap();
    let (payloads, _) = records(&mut wal);
    assert_eq!(payloads, vec![payload(1)]);
}

#[test]
fn checkpoint_skips_materialised_records_across_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::open(config(dir.path())).unwrap();
    let positions: Vec<Position> = (0..100).map(|i| wal.append(&payload(i)).unwrap()).collect();
    wal.sync().unwrap();
    let on_disk: u64 = wal.segments().unwrap().iter().map(|s| s.bytes).sum();
    assert_eq!(wal.bytes(), on_disk, "size counter matches the files");

    // Everything before record 60 is materialised.
    wal.checkpoint(positions[60]).unwrap();
    assert_eq!(wal.checkpoint_position(), Some(positions[60]));
    let (payloads, damaged) = records(&mut wal);
    assert_eq!(damaged, vec![]);
    assert_eq!(payloads.len(), 40);
    assert_eq!(payloads[0], payload(60));
    assert!(
        wal.segments()
            .unwrap()
            .iter()
            .all(|s| s.id >= positions[60].segment),
        "older segments are gone"
    );
    assert_eq!(
        wal.bytes(),
        wal.segments().unwrap().iter().map(|s| s.bytes).sum::<u64>()
    );

    // Moving backwards is ignored.
    wal.checkpoint(positions[10]).unwrap();
    assert_eq!(wal.checkpoint_position(), Some(positions[60]));

    // The checkpoint survives a restart and new records land after it.
    drop(wal);
    let mut wal = Wal::open(config(dir.path())).unwrap();
    assert_eq!(wal.checkpoint_position(), Some(positions[60]));
    wal.append(&payload(100)).unwrap();
    wal.sync().unwrap();
    let (payloads, _) = records(&mut wal);
    assert_eq!(payloads.len(), 41);
    assert_eq!(payloads.last().unwrap(), &payload(100));

    // Checkpointing at the live position leaves nothing to replay.
    wal.checkpoint(wal.position()).unwrap();
    let (payloads, _) = records(&mut wal);
    assert_eq!(payloads.len(), 0);
}

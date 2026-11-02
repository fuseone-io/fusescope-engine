#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::too_many_lines,
    unsafe_code
)]
//! `Postings::decode_cost` against what `Postings::decode` allocates,
//! counted by this binary's allocator on the decoding thread.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use fs_index::DecodeCost;
use fs_index::postings::{Binding, Postings, PostingsBuilder};
use roaring::RoaringBitmap;

struct Counting;

thread_local! {
    static LIVE: Cell<isize> = const { Cell::new(0) };
    static PEAK: Cell<isize> = const { Cell::new(0) };
}

fn grow(bytes: usize) {
    let _ = LIVE.try_with(|live| {
        live.set(live.get() + isize::try_from(bytes).unwrap_or(isize::MAX));
        let _ = PEAK.try_with(|peak| peak.set(peak.get().max(live.get())));
    });
}

fn shrink(bytes: usize) {
    let _ = LIVE.try_with(|live| live.set(live.get() - isize::try_from(bytes).unwrap_or(0)));
}

// SAFETY: every call is forwarded unchanged to the system allocator; the
// counters are thread-local cells that never allocate.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `layout` is passed on as is.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            grow(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: as for `alloc`.
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            grow(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from `System` with `layout`.
        unsafe { System.dealloc(ptr, layout) };
        shrink(layout.size());
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: as for `dealloc`, with the caller's `new_size`.
        let out = unsafe { System.realloc(ptr, layout, new_size) };
        if !out.is_null() {
            shrink(layout.size());
            grow(new_size);
        }
        out
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Peak and live heap of `decode` on this thread, from zero.
fn measured(bytes: &[u8], binding: &Binding) -> (usize, usize, Postings) {
    LIVE.with(|live| live.set(0));
    PEAK.with(|peak| peak.set(0));
    let postings = Postings::decode(bytes, binding).unwrap();
    let peak = PEAK.with(Cell::get);
    let live = LIVE.with(Cell::get);
    (
        usize::try_from(peak).unwrap(),
        usize::try_from(live.max(0)).unwrap(),
        postings,
    )
}

fn binding(rows: u32) -> Binding {
    Binding {
        path: "t/x/metrics/series/l2/dt=2026-10-24/hr=14/b.parquet".into(),
        sha256: [3; 32],
        schema_version: 2,
        row_count: rows,
    }
}

/// Postings framed by hand, so a bitmap may carry runs.
fn framed(binding: &Binding, entries: &[(&str, Vec<(&str, RoaringBitmap)>)]) -> Vec<u8> {
    let mut body = Vec::new();
    let put = |body: &mut Vec<u8>, bytes: &[u8]| {
        body.extend_from_slice(&u32::try_from(bytes.len()).unwrap().to_le_bytes());
        body.extend_from_slice(bytes);
    };
    put(&mut body, binding.path.as_bytes());
    body.extend_from_slice(&binding.sha256);
    body.extend_from_slice(&binding.schema_version.to_le_bytes());
    body.extend_from_slice(&binding.row_count.to_le_bytes());
    body.extend_from_slice(&u32::try_from(entries.len()).unwrap().to_le_bytes());
    for (key, values) in entries {
        put(&mut body, key.as_bytes());
        body.extend_from_slice(&u32::try_from(values.len()).unwrap().to_le_bytes());
        for (value, bitmap) in values {
            put(&mut body, value.as_bytes());
            let mut raw = Vec::new();
            bitmap.serialize_into(&mut raw).unwrap();
            put(&mut body, &raw);
        }
    }
    let mut out = b"FSPST\0".to_vec();
    out.extend_from_slice(&[1, 0]);
    out.extend_from_slice(&u32::try_from(body.len()).unwrap().to_le_bytes());
    out.extend_from_slice(&twox_hash::XxHash3_64::oneshot(&body).to_le_bytes());
    out.extend_from_slice(&body);
    out
}

fn reframed(bytes: &[u8]) -> Vec<u8> {
    let mut out = bytes.to_vec();
    let checksum = twox_hash::XxHash3_64::oneshot(&out[20..]);
    out[12..20].copy_from_slice(&checksum.to_le_bytes());
    out
}

/// The cost is what the decode keeps, exactly, and covers its peak.
fn covers(bytes: &[u8], binding: &Binding) -> DecodeCost {
    let cost = Postings::decode_cost(bytes).unwrap();
    let (peak, live, postings) = measured(bytes, binding);
    assert_eq!(live, cost.retained, "kept against charged");
    assert!(
        peak <= cost.retained + cost.scratch,
        "peak {peak} past {cost:?}"
    );
    drop(postings);
    cost
}

#[test]
fn the_cost_of_small_postings_covers_their_decode() {
    let mut builder = PostingsBuilder::new(usize::MAX);
    for (row, host) in ["a", "b", "a"].into_iter().enumerate() {
        builder
            .add(row as u64, [("__name__", "cpu"), ("host", host)])
            .unwrap();
    }
    let bytes = builder.finish(&binding(3)).unwrap();
    let cost = covers(&bytes, &binding(3));
    assert!(cost.retained > 0);
}

#[test]
fn the_cost_of_large_postings_covers_their_decode() {
    let rows = 300_000u32;
    let mut builder = PostingsBuilder::new(usize::MAX);
    for row in 0..rows {
        let host = format!("h{}", row % 997);
        let zone = format!("z{}", row % 3);
        builder
            .add(
                u64::from(row),
                [
                    ("__name__", "cpu"),
                    ("host", host.as_str()),
                    ("zone", zone.as_str()),
                ],
            )
            .unwrap();
    }
    let bytes = builder.finish(&binding(rows)).unwrap();
    covers(&bytes, &binding(rows));
}

#[test]
fn the_cost_of_run_containers_covers_their_decode() {
    let rows = 1 << 20;
    let mut dense = RoaringBitmap::new();
    dense.insert_range(0..rows);
    dense.optimize();
    let mut striped = RoaringBitmap::new();
    for start in (0..rows).step_by(1000) {
        striped.insert_range(start..start + 10);
    }
    striped.optimize();
    let mut sparse = RoaringBitmap::new();
    sparse.extend([5, 70_000, 900_000]);
    let few: Vec<RoaringBitmap> = (0..3).map(|n| RoaringBitmap::from([n])).collect();
    let binding = binding(rows);
    let entries = vec![
        ("__name__", vec![("cpu", dense)]),
        (
            "a",
            few.iter()
                .cloned()
                .zip(["x", "y", "z"])
                .map(|(b, v)| (v, b))
                .collect(),
        ),
        ("host", vec![("s", sparse), ("t", striped)]),
    ];
    for (_, values) in &entries {
        for (_, bitmap) in values {
            let mut raw = Vec::new();
            bitmap.serialize_into(&mut raw).unwrap();
            assert_eq!(
                u16::from_le_bytes([raw[0], raw[1]]) == 12347,
                bitmap.len() > 3,
                "runs in exactly the dense and striped bitmaps"
            );
        }
    }
    let bytes = framed(&binding, &entries);
    covers(&bytes, &binding);
}

#[test]
fn a_frame_the_cost_accepts_decodes_and_one_it_refuses_does_not() {
    let mut striped = RoaringBitmap::new();
    striped.insert_range(10..20);
    striped.insert_range(40..50);
    striped.optimize();
    let mut wide = RoaringBitmap::new();
    wide.insert_range(0..5000);
    let binding = binding(70_000);
    let entries = vec![
        (
            "a",
            vec![("p", RoaringBitmap::from([1, 3, 65_600])), ("q", striped)],
        ),
        ("b", vec![("r", wide)]),
    ];
    let good = framed(&binding, &entries);
    let (mut accepted, mut refused) = (0, 0);
    for at in 20..good.len() {
        for flip in [0x01u8, 0x80, 0xff] {
            let mut bytes = good.clone();
            bytes[at] ^= flip;
            let bytes = reframed(&bytes);
            let cost = Postings::decode_cost(&bytes);
            let decoded = Postings::decode(&bytes, &binding);
            match (&cost, &decoded) {
                (Ok(_), Ok(_)) => {
                    accepted += 1;
                    covers(&bytes, &binding);
                }
                (Ok(_), Err(fs_index::IndexError::NotBound(_))) | (Err(_), Err(_)) => refused += 1,
                _ => panic!("at {at} ^ {flip:#x}: cost {cost:?}, decode {decoded:?}"),
            }
        }
    }
    assert!(
        accepted > 0 && refused > 0,
        "{accepted} accepted, {refused} refused"
    );
}

#[test]
fn a_damaged_frame_has_no_cost() {
    let bytes = framed(&binding(3), &[("a", vec![("b", RoaringBitmap::from([1]))])]);
    let mut flipped = bytes.clone();
    *flipped.last_mut().unwrap() ^= 1;
    assert_eq!(
        Postings::decode_cost(&flipped).unwrap_err(),
        fs_index::IndexError::ChecksumMismatch
    );
    assert_eq!(
        Postings::decode_cost(&bytes[..bytes.len() - 1]).unwrap_err(),
        fs_index::IndexError::BadLength {
            declared: (bytes.len() - 20) as u64,
            present: (bytes.len() - 21) as u64
        }
    );
    let empty = framed(&binding(3), &[("a", vec![("b", RoaringBitmap::new())])]);
    assert_eq!(
        Postings::decode_cost(&empty).unwrap_err(),
        fs_index::IndexError::BadBody("an empty bitmap")
    );
    let past = framed(&binding(3), &[("a", vec![("b", RoaringBitmap::from([3]))])]);
    assert_eq!(
        Postings::decode_cost(&past).unwrap_err(),
        fs_index::IndexError::BadBody("a row past the file's rows")
    );
}

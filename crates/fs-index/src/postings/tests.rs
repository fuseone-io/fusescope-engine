use super::*;

fn binding(rows: u32) -> Binding {
    Binding {
        path: "t/x/metrics/series/l1/dt=2026-10-24/hr=14/a.parquet".into(),
        sha256: [7; 32],
        schema_version: 2,
        row_count: rows,
    }
}

/// Three rows of `host` and `region`, and `__name__`.
fn built() -> Vec<u8> {
    let mut builder = PostingsBuilder::new(usize::MAX);
    for (row, (host, region)) in [("a", "eu"), ("b", "eu"), ("a", "us")]
        .into_iter()
        .enumerate()
    {
        builder
            .add(
                row as u64,
                [("__name__", "cpu"), ("host", host), ("region", region)],
            )
            .unwrap();
    }
    builder.finish(&binding(3)).unwrap()
}

#[test]
fn postings_list_the_rows_of_each_label_value() {
    let bytes = built();
    let postings = Postings::decode(&bytes, &binding(3)).unwrap();
    let rows = |k, v| postings.rows(k, v).map(|b| b.iter().collect::<Vec<_>>());
    assert_eq!(rows("host", "a"), Some(vec![0, 2]));
    assert_eq!(rows("region", "eu"), Some(vec![0, 1]));
    assert_eq!(rows("__name__", "cpu"), Some(vec![0, 1, 2]));
    assert_eq!(rows("host", "c"), None);
    assert_eq!(rows("zone", "a"), None);
    let keys: Vec<(&str, Vec<&str>)> = postings.keys().map(|(k, v)| (k, v.collect())).collect();
    assert_eq!(
        keys,
        [
            ("__name__", vec!["cpu"]),
            ("host", vec!["a", "b"]),
            ("region", vec!["eu", "us"])
        ]
    );
}

/// The committed fixture is what this encoder writes, byte for byte, its
/// bitmaps in the portable serialization.
#[test]
fn the_v1_fixture_is_written_byte_for_byte() {
    let fixture = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/postings_v1.pst"
    ));
    assert_eq!(built(), fixture.to_vec());
    let mut portable = RoaringBitmap::new();
    portable.insert(0);
    portable.insert(2);
    let mut bytes = Vec::new();
    portable.serialize_into(&mut bytes).unwrap();
    assert_eq!(
        bytes,
        [
            58, 48, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 16, 0, 0, 0, 0, 0, 2, 0
        ],
        "the portable format"
    );
}

/// Postings beside another object, or of another version of it, are
/// refused.
#[test]
fn postings_bound_to_another_file_are_refused() {
    let bytes = built();
    let mut other = binding(3);
    other.path.push('x');
    assert_eq!(
        Postings::decode(&bytes, &other),
        Err(IndexError::NotBound("path"))
    );
    let mut other = binding(3);
    other.sha256[0] = 8;
    assert_eq!(
        Postings::decode(&bytes, &other),
        Err(IndexError::NotBound("sha256"))
    );
    let mut other = binding(4);
    assert!(Postings::decode(&bytes, &other).is_err(), "rows");
    other = binding(3);
    other.schema_version = 1;
    assert!(Postings::decode(&bytes, &other).is_err(), "schema version");
}

/// A body framed right but wrong in any way is refused: the reader scans.
#[test]
fn damaged_or_malformed_postings_are_refused() {
    let bytes = built();
    let mut flipped = bytes.clone();
    *flipped.last_mut().unwrap() ^= 1;
    assert_eq!(
        Postings::decode(&flipped, &binding(3)),
        Err(IndexError::ChecksumMismatch)
    );
    let mut version = bytes.clone();
    version[6] = 2;
    assert_eq!(
        Postings::decode(&version, &binding(3)),
        Err(IndexError::UnknownVersion(2))
    );
    assert!(Postings::decode(&bytes[..bytes.len() - 1], &binding(3)).is_err());

    // A body rebuilt with one field wrong, its frame recomputed.
    let reframe = |body: &[u8]| {
        let mut out = frame::header(MAGIC, VERSION, body).to_vec();
        out.extend_from_slice(body);
        out
    };
    let body = &bytes[frame::HEADER_LEN..];
    let head = 4 + binding(3).path.len() + 32 + 4 + 4;
    let mut huge = body.to_vec();
    huge[head..head + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(
        Postings::decode(&reframe(&huge), &binding(3)),
        Err(IndexError::BadBody("count past the end")),
        "a count is checked before anything is allocated"
    );
    let mut swapped = PostingsBuilder::new(usize::MAX);
    swapped.add(0, [("b", "1")]).unwrap();
    swapped.add(1, [("a", "1")]).unwrap();
    let sorted = swapped.finish(&binding(2)).unwrap();
    // Swap the two keys' one-byte texts: still framed, now descending.
    let mut descending = sorted[frame::HEADER_LEN..].to_vec();
    let first = 4 + binding(2).path.len() + 32 + 4 + 4 + 4 + 4;
    let entry = 4 + 1 + 4 + 4 + 1 + 4 + RoaringBitmap::from_iter([0u32]).serialized_size();
    descending.swap(first, first + entry);
    assert_eq!(
        Postings::decode(&reframe(&descending), &binding(2)),
        Err(IndexError::BadBody("keys not strictly ascending"))
    );

    let mut past = PostingsBuilder::new(usize::MAX);
    past.add(5, [("k", "v")]).unwrap();
    assert!(past.finish(&binding(5)).is_err(), "row 5 of 5");
}

/// Past the budget, or past a row ordinal of u32, the postings are
/// abandoned; what they hold never passes the budget.
#[test]
fn postings_are_abandoned_at_their_budget_or_past_u32_rows() {
    let budget = 64 * 1024;
    let mut builder = PostingsBuilder::new(budget);
    let mut abandoned = false;
    for row in 0..100_000u64 {
        let value = format!("v{row}");
        if builder.add(row, [("id", value.as_str())]).is_err() {
            abandoned = true;
            break;
        }
        assert!(builder.held() + INSERT_BYTES <= budget + INSERT_BYTES);
    }
    assert!(abandoned);
    assert!(builder.held() <= budget);
    let mut wide = PostingsBuilder::new(usize::MAX);
    assert_eq!(
        wide.add(u64::from(u32::MAX) + 1, [("k", "v")]),
        Err(Abandoned)
    );
}

#[test]
fn only_series_files_of_l1_and_l2_have_postings() {
    let l1 = "t/x/metrics/series/l1/dt=2026-10-24/hr=14/a.parquet";
    let pst = "t/x/metrics/index/postings/dt=2026-10-24/hr=14/a.pst";
    assert_eq!(sidecar_path(l1).as_deref(), Some(pst));
    assert_eq!(
        sidecar_path("t/x/metrics/series/l2/dt=2026-10-24/a.parquet").as_deref(),
        Some("t/x/metrics/index/postings/dt=2026-10-24/a.pst")
    );
    assert_eq!(
        sidecar_path("t/x/metrics/series/l0/dt=2026-10-24/hr=14/a.parquet"),
        None
    );
    assert_eq!(
        sidecar_path("t/x/metrics/samples/l1/dt=2026-10-24/hr=14/a.parquet"),
        None
    );
    assert!(owners_of_sidecar(pst).contains(&l1.to_owned()));
}

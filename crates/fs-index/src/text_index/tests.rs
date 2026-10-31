use super::*;

const PATH: &str = "t/00000000-0000-0000-0000-000000000001/logs/l0/dt=2026-10-31/hr=10/00000000-0000-0000-0000-0000000000aa.parquet";

fn build(groups: &[&[&str]], limits: TextIndexLimits) -> Vec<u8> {
    let body_bytes = groups.iter().flat_map(|g| g.iter()).map(|b| b.len()).sum();
    let mut builder = TextIndexBuilder::new(PATH, limits, body_bytes, groups.len()).unwrap();
    for group in groups {
        builder
            .add_row_group(group.iter().map(|b| b.as_bytes()))
            .unwrap();
    }
    builder.finish([7u8; 32]).unwrap()
}

fn sample() -> Vec<u8> {
    build(
        &[
            &["GET /api/users 200", "connection reset by peer"],
            &["payment declined: card expired", "retry scheduled"],
            &["usuário não encontrado", "Timeout after 30s"],
        ],
        TextIndexLimits::DEFAULT,
    )
}

fn fragment(s: &str) -> Probe {
    Probe::fragment(s.as_bytes(), false)
}

fn folded(s: &str) -> Probe {
    Probe::fragment(s.as_bytes(), true)
}

fn token(s: &str) -> Probe {
    Probe::token(s.as_bytes(), false)
}

/// Rewrites the checksum after a change to the body, so only the body
/// check can refuse it.
fn reseal(bytes: &mut [u8]) {
    let sum = crate::frame::checksum(&bytes[HEADER_LEN..]).to_le_bytes();
    bytes[12..20].copy_from_slice(&sum);
}

#[test]
fn a_sidecar_round_trips_its_file_digest_and_geometry() {
    let bytes = sample();
    let index = TextIndex::decode(bytes.as_slice()).unwrap();
    assert_eq!(index.path(), PATH);
    assert_eq!(index.content_sha256(), &[7u8; 32]);
    assert_eq!(index.row_groups(), 3);
    assert_eq!(index.rows().collect::<Vec<_>>(), vec![2, 2, 2]);
    assert!((0..3).all(|g| index.indexed(g)));
    assert_eq!(
        TextIndex::<&[u8]>::decode_cost(&bytes).unwrap(),
        index.heap_bytes()
    );
    let bound = TextIndexBuilder::encoded_bound(PATH, 140, 3, TextIndexLimits::DEFAULT);
    assert!(
        bytes.len() <= bound,
        "{} over the bound {bound}",
        bytes.len()
    );
}

#[test]
fn fragments_and_tokens_select_the_row_groups_that_hold_them() {
    let bytes = sample();
    let index = TextIndex::decode(bytes).unwrap();
    assert_eq!(index.select(&fragment("declined")), vec![1]);
    assert_eq!(
        index.select(&fragment("DECLINED")),
        vec![1],
        "probes are folded"
    );
    assert_eq!(index.select(&fragment("encontrado")), vec![2]);
    assert_eq!(
        index.select(&fragment("não")),
        vec![2],
        "non-ASCII bytes are trigrams too"
    );
    assert_eq!(index.select(&token("200")), vec![0]);
    assert_eq!(index.select(&token("30s")), vec![2]);
    assert_eq!(index.select(&token("30")), Vec::<usize>::new());
    assert_eq!(index.select(&fragment("zzzqqq")), Vec::<usize>::new());
    assert_eq!(index.select(&Probe::Unknown), vec![0, 1, 2]);
    assert_eq!(
        index.select(&fragment("ab")),
        vec![0, 1, 2],
        "too short to probe"
    );
}

#[test]
fn a_case_insensitive_probe_tests_only_ascii_row_groups() {
    let bytes = sample();
    let index = TextIndex::decode(bytes).unwrap();
    // Row group 2 has a non-ASCII body: a folded probe must read it.
    assert_eq!(index.select(&folded("declined")), vec![1, 2]);
    assert_eq!(index.select(&folded("zzzqqq")), vec![2]);
    assert_eq!(
        folded("não"),
        Probe::Unknown,
        "a non-ASCII literal is not folded"
    );
    assert_eq!(
        index.select(&Probe::token(b"Timeout", true)),
        vec![2],
        "row group 2 is kept as non-ASCII, the others lack the token"
    );
}

#[test]
fn and_prunes_on_any_known_branch_or_needs_every_branch_known() {
    let bytes = sample();
    let index = TextIndex::decode(bytes).unwrap();
    let and = Probe::all([fragment("payment"), Probe::Unknown, token("retry")]);
    assert_eq!(index.select(&and), vec![1]);
    let and_none = Probe::all([fragment("payment"), token("200")]);
    assert_eq!(index.select(&and_none), Vec::<usize>::new());
    let or = Probe::any([fragment("payment"), token("200")]);
    assert_eq!(index.select(&or), vec![0, 1]);
    let or_unknown = Probe::any([fragment("payment"), Probe::Unknown]);
    assert_eq!(or_unknown, Probe::Unknown);
    assert_eq!(index.select(&or_unknown), vec![0, 1, 2]);
    assert!(!Probe::all([Probe::Unknown, fragment("ab")]).prunes());
}

#[test]
fn the_row_group_budget_bounds_both_blooms_together() {
    let bodies: Vec<String> = (0..500)
        .map(|i| format!("request {i} served in {i}ms"))
        .collect();
    let group: Vec<&str> = bodies.iter().map(String::as_str).collect();
    let limits = TextIndexLimits::new(256, 1024 * 1024).unwrap();
    let bytes = build(&[&group], limits);
    let index = TextIndex::decode(bytes).unwrap();
    let g = index.groups[0];
    assert!(index.indexed(0));
    assert!(g.ngram.bytes + g.token.bytes <= 256, "{g:?}");
    assert!(
        g.ngram.bytes > g.token.bytes,
        "trigrams want more and get more"
    );
    assert_eq!(index.select(&fragment("served")), vec![0]);
}

#[test]
fn a_group_spends_at_most_its_body_bytes_or_the_floor() {
    let long = "abcdefghij".repeat(20);
    let bytes = build(&[&[long.as_str()], &["error"]], TextIndexLimits::DEFAULT);
    let index = TextIndex::decode(bytes).unwrap();
    let (long, short) = (index.groups[0], index.groups[1]);
    assert!(long.ngram.bytes + long.token.bytes <= 200, "{long:?}");
    assert!(index.indexed(1), "a short body is indexed within the floor");
    assert!(short.ngram.bytes + short.token.bytes <= 64, "{short:?}");
    assert_eq!(index.select(&token("error")), vec![1]);
}

#[test]
fn row_groups_past_the_file_budget_carry_no_bloom_and_are_always_read() {
    let body = "x".repeat(4000);
    let groups: Vec<[&str; 1]> = (0..4).map(|_| [body.as_str()]).collect();
    let refs: Vec<&[&str]> = groups.iter().map(<[&str; 1]>::as_slice).collect();
    let limits = TextIndexLimits::new(1024, 2048).unwrap();
    let bytes = build(&refs, limits);
    let index = TextIndex::decode(bytes).unwrap();
    assert_eq!(
        (0..4).map(|g| index.indexed(g)).collect::<Vec<_>>(),
        vec![true, true, false, false]
    );
    assert_eq!(index.select(&fragment("yyy")), vec![2, 3]);
    let spent: usize = index
        .groups
        .iter()
        .map(|g| g.ngram.bytes + g.token.bytes)
        .sum();
    assert!(spent <= 2048);
}

#[test]
fn a_group_without_terms_is_indexed_and_holds_none() {
    let bytes = build(
        &[&["", "ab", "."], &["hello world"]],
        TextIndexLimits::DEFAULT,
    );
    let index = TextIndex::decode(bytes).unwrap();
    assert!(index.indexed(0));
    let g = index.groups[0];
    assert_eq!(g.ngram.bits, 0);
    assert_eq!(index.select(&fragment("hello")), vec![1]);
    // "ab" is a token of group 0: the token bloom is not empty.
    assert_eq!(index.select(&token("ab")), vec![0]);
}

#[test]
fn limits_are_refused_outside_their_range() {
    assert!(TextIndexLimits::new(0, 1).is_err());
    assert!(TextIndexLimits::new(1, 0).is_err());
    assert!(TextIndexLimits::new(TextIndexLimits::ROW_GROUP_CEILING + 1, 1).is_err());
    assert!(TextIndexLimits::new(1, TextIndexLimits::FILE_CEILING + 1).is_err());
    assert!(
        TextIndexLimits::new(
            TextIndexLimits::ROW_GROUP_CEILING,
            TextIndexLimits::FILE_CEILING
        )
        .is_ok()
    );
}

#[test]
fn damage_is_refused() {
    let good = sample();
    let mut magic = good.clone();
    magic[0] = b'X';
    assert_eq!(TextIndex::decode(magic).unwrap_err(), IndexError::BadMagic);
    let mut version = good.clone();
    version[6] = 2;
    assert_eq!(
        TextIndex::decode(version).unwrap_err(),
        IndexError::UnknownVersion(2)
    );
    let mut short = good.clone();
    short.pop();
    assert!(matches!(
        TextIndex::decode(short).unwrap_err(),
        IndexError::BadLength { .. }
    ));
    let mut flipped = good.clone();
    let mid = good.len() / 2;
    flipped[mid] ^= 1;
    assert_eq!(
        TextIndex::decode(flipped).unwrap_err(),
        IndexError::ChecksumMismatch
    );

    let normalization_at = HEADER_LEN + 2 + PATH.len() + 32;
    let mut normalization = good.clone();
    normalization[normalization_at] = 2;
    reseal(&mut normalization);
    assert_eq!(
        TextIndex::decode(normalization).unwrap_err(),
        IndexError::BadBody("normalization")
    );

    let groups_at = good.len() - 4 - 3 * GROUP_LEN;
    let mut flags = good.clone();
    flags[groups_at + 8] |= 4;
    reseal(&mut flags);
    assert_eq!(
        TextIndex::decode(flags).unwrap_err(),
        IndexError::BadBody("unknown row group flags")
    );

    let mut bits = good.clone();
    bits[groups_at + 9] ^= 1;
    reseal(&mut bits);
    assert_eq!(
        TextIndex::decode(bits).unwrap_err(),
        IndexError::BadBody("bloom descriptor")
    );

    let mut count = good.clone();
    let at = count.len() - 4;
    count[at..].copy_from_slice(&2u32.to_le_bytes());
    reseal(&mut count);
    assert!(TextIndex::decode(count).is_err());
}

#[test]
fn the_v1_fixture_is_what_the_encoder_still_writes() {
    let fixture: &[u8] = include_bytes!("../../tests/fixtures/text_index_v1.ngb");
    assert_eq!(
        sample(),
        fixture,
        "the encoder of version 1 changed its bytes"
    );
    let index = TextIndex::decode(fixture).unwrap();
    assert_eq!(index.select(&fragment("declined")), vec![1]);
}

#[test]
fn sidecar_paths_belong_to_log_files_of_any_level() {
    let path = sidecar_path(PATH).unwrap();
    assert_eq!(
        path,
        "t/00000000-0000-0000-0000-000000000001/logs/index/ngram/dt=2026-10-31/hr=10/00000000-0000-0000-0000-0000000000aa.ngb"
    );
    assert!(owners_of_sidecar(&path).contains(&PATH.to_owned()));
    assert_eq!(owners_of_sidecar(&path).len(), 3);
    assert_eq!(sidecar_path(&PATH.replace("/logs/", "/traces/")), None);
    assert_eq!(sidecar_path(&PATH.replace("/l0/", "/lx/")), None);
    assert_eq!(
        owners_of_sidecar("t/x/traces/index/trace_id/a.tix"),
        Vec::<String>::new()
    );
}

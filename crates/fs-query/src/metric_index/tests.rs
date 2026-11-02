use chrono::Utc;
use fs_catalog::{FileId, FileState, Level, Signal, TenantId, TimeRange};
use fs_index::postings::PostingsBuilder;
use object_store::memory::InMemory;

use super::*;

fn series_file(n: u128, rows: i64) -> FileRecord {
    let now = Utc::now();
    FileRecord {
        id: FileId(uuid::Uuid::from_u128(n)),
        origin: FileId(uuid::Uuid::from_u128(n)),
        tenant_id: TenantId(uuid::Uuid::nil()),
        signal: Signal::MetricSeries,
        level: Level::L1,
        path: format!("t/x/metrics/series/l1/dt=2026-10-24/hr=14/{n}.parquet"),
        size_bytes: 1,
        row_count: rows,
        range: TimeRange {
            start: now,
            end: now,
        },
        services: vec![],
        summary: None,
        state: FileState::Visible,
        created_at: now,
        content_sha256: Some([u8::try_from(n % 256).unwrap(); 32]),
        usage_accounted: false,
        quarantined_at: None,
    }
}

fn binding_of(file: &FileRecord, schema_version: u32) -> Binding {
    Binding {
        path: file.path.clone(),
        sha256: file.content_sha256.unwrap(),
        schema_version,
        row_count: u32::try_from(file.row_count).unwrap(),
    }
}

/// Writes postings of `hosts`, one row each, for `binding` beside `at`;
/// returns their decoded heap and the object's size.
async fn put_postings(
    store: &dyn ObjectStore,
    at: &FileRecord,
    binding: &Binding,
    hosts: &[&str],
) -> (usize, usize) {
    let mut builder = PostingsBuilder::new(usize::MAX);
    for (row, host) in hosts.iter().enumerate() {
        builder
            .add(row as u64, [("__name__", "cpu"), ("host", *host)])
            .unwrap();
    }
    let encoded = builder.finish(binding).unwrap();
    let cost = Postings::decode_cost(&encoded).unwrap();
    store
        .put(
            &Path::from(sidecar_path(&at.path).unwrap()),
            encoded.clone().into(),
        )
        .await
        .unwrap();
    (cost.retained, encoded.len())
}

fn cache(store: Arc<dyn ObjectStore>, max_bytes: usize) -> MetricIndexCache {
    MetricIndexCache::new(store, max_bytes).with_stats(Arc::new(QueryStats::default()))
}

fn stats(cache: &MetricIndexCache) -> (u64, u64) {
    let s = cache.stats.as_ref().unwrap();
    (
        s.metric_index_fetches.load(Relaxed),
        s.metric_index_misses.load(Relaxed),
    )
}

#[tokio::test]
async fn postings_are_fetched_once_and_charge_their_decoded_heap() {
    let store = Arc::new(InMemory::new());
    let file = series_file(1, 2);
    let (heap, _) = put_postings(store.as_ref(), &file, &binding_of(&file, 2), &["a", "b"]).await;
    let cache = cache(store, 1 << 20);
    let postings = cache.postings(&file, 2).await.unwrap();
    assert_eq!(
        postings
            .rows("host", "b")
            .map(|r| r.iter().collect::<Vec<_>>()),
        Some(vec![1])
    );
    assert_eq!(cache.bytes(), heap);
    assert_eq!(cache.committed_bytes(), heap, "nothing left in flight");
    cache.postings(&file, 2).await.unwrap();
    assert_eq!(stats(&cache), (1, 0), "the second lookup is a hit");
}

#[tokio::test]
async fn postings_bound_to_anything_else_are_a_remembered_miss() {
    let store = Arc::new(InMemory::new());
    let cases = [
        ("another schema version", series_file(1, 2), 1),
        ("another file", series_file(2, 2), 2),
        ("other rows", series_file(3, 3), 2),
    ];
    for (n, (what, file, version)) in cases.into_iter().enumerate() {
        let bound = match n {
            0 => binding_of(&file, 2),
            1 => binding_of(&series_file(9, 2), 2),
            _ => binding_of(&series_file(3, 2), 2),
        };
        put_postings(store.as_ref(), &file, &bound, &["a", "b"]).await;
        let cache = cache(Arc::clone(&store) as Arc<dyn ObjectStore>, 1 << 20);
        assert!(cache.postings(&file, version).await.is_none(), "{what}");
        assert!(cache.postings(&file, version).await.is_none(), "{what}");
        assert_eq!(
            stats(&cache),
            (1, 1),
            "{what}: fetched once, then remembered"
        );
        assert_eq!(cache.bytes(), MISS_BYTES, "{what}");
    }
}

#[tokio::test]
async fn damaged_or_missing_postings_are_a_miss() {
    let store = Arc::new(InMemory::new());
    let file = series_file(1, 2);
    let cache = cache(Arc::clone(&store) as Arc<dyn ObjectStore>, 1 << 20);
    assert!(cache.postings(&file, 2).await.is_none(), "missing");
    assert_eq!(stats(&cache), (1, 1));

    let damaged = series_file(2, 2);
    put_postings(
        store.as_ref(),
        &damaged,
        &binding_of(&damaged, 2),
        &["a", "b"],
    )
    .await;
    let path = Path::from(sidecar_path(&damaged.path).unwrap());
    let mut bytes = store
        .get(&path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
        .to_vec();
    *bytes.last_mut().unwrap() ^= 1;
    store.put(&path, bytes.into()).await.unwrap();
    assert!(cache.postings(&damaged, 2).await.is_none(), "damaged");
    assert_eq!(stats(&cache), (2, 2));

    let mut no_sha = series_file(3, 2);
    no_sha.content_sha256 = None;
    assert!(
        cache.postings(&no_sha, 2).await.is_none(),
        "a file without a digest has none"
    );
    let mut l0 = series_file(4, 2);
    l0.path = "t/x/metrics/series/l0/dt=2026-10-24/hr=14/4.parquet".into();
    assert!(
        cache.postings(&l0, 2).await.is_none(),
        "an L0 file has none"
    );
    assert_eq!(stats(&cache), (2, 4), "neither was fetched");
}

#[tokio::test]
async fn postings_that_decode_beyond_the_budget_are_never_decoded() {
    let store = Arc::new(InMemory::new());
    let file = series_file(1, 400);
    let hosts: Vec<String> = (0..400).map(|n| format!("host-{n}")).collect();
    let hosts: Vec<&str> = hosts.iter().map(String::as_str).collect();
    let (heap, object) = put_postings(store.as_ref(), &file, &binding_of(&file, 2), &hosts).await;
    // The download fits; its decode does not.
    let cache = cache(store, object + heap / 2);
    assert!(cache.postings(&file, 2).await.is_none());
    assert_eq!(stats(&cache), (1, 1));
    assert_eq!(
        cache.committed_bytes(),
        MISS_BYTES,
        "remembered, nothing in flight"
    );
}

#[tokio::test]
async fn a_busy_budget_scans_the_file_and_is_not_remembered() {
    let store = Arc::new(InMemory::new());
    let file = series_file(1, 2);
    let (heap, object) =
        put_postings(store.as_ref(), &file, &binding_of(&file, 2), &["a", "b"]).await;
    let budget = (object + heap) * 4;
    let cache = cache(store, budget);
    // Room for the download but not for its decode.
    let mut other = Lease::begin(&cache.shared, FileId(uuid::Uuid::from_u128(9))).unwrap();
    assert_eq!(other.reserve(budget - object), Reserve::Fits);
    assert!(
        cache.postings(&file, 2).await.is_none(),
        "no room while another download holds it"
    );
    drop(other);
    assert!(
        cache.postings(&file, 2).await.is_some(),
        "tried again once there is room"
    );
    assert_eq!(stats(&cache), (2, 1));
}

#[tokio::test]
async fn zero_bytes_disables_the_postings_and_the_size_is_clamped() {
    let store = Arc::new(InMemory::new());
    let file = series_file(1, 2);
    put_postings(store.as_ref(), &file, &binding_of(&file, 2), &["a", "b"]).await;
    let disabled = cache(Arc::clone(&store) as Arc<dyn ObjectStore>, 0);
    assert!(!disabled.enabled());
    assert!(disabled.postings(&file, 2).await.is_none());
    assert_eq!(stats(&disabled), (0, 0), "nothing consulted");
    let huge = MetricIndexCache::new(store, usize::MAX);
    assert_eq!(huge.shared.max_bytes, MetricIndexCache::MAX_BYTES);
}

#![allow(
    clippy::indexing_slicing,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array};

use super::*;

const QUANTILES: [f64; 10] = [0.05, 0.10, 0.20, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.999];
const ROWS_PER_BATCH: usize = 8192;

/// SplitMix64, so the corpus is the same on every run.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn below(&mut self, lo: usize, hi: usize) -> usize {
        lo + usize::try_from(self.next() % (hi - lo) as u64).unwrap()
    }

    /// Log-normal around `median` nanoseconds, truncated to whole microseconds.
    fn duration(&mut self, median: f64, sigma: f64) -> f64 {
        let normal =
            (-2.0 * (1.0 - self.unit()).ln()).sqrt() * (std::f64::consts::TAU * self.unit()).cos();
        ((median * (sigma * normal).exp()) / 1_000.0)
            .floor()
            .max(1.0)
            * 1_000.0
    }

    fn values(&mut self, n: usize, shape: usize) -> Vec<f64> {
        (0..n)
            .map(|_| match shape {
                0 => self.duration(20e6, 0.5),
                1 => self.duration(5e6, 1.6),
                2 if self.unit() < 0.3 => self.duration(200e6, 0.2),
                2 => self.duration(2e6, 0.3),
                _ => (self.duration(10e6, 0.8) / 1e6).round() * 1e6,
            })
            .collect()
    }
}

/// The parity's exact quantile: rank `q·(n − 1)` over every value, linear
/// interpolation.
fn parity_exact_quantile(values: &[f64], q: f64) -> f64 {
    let mut s = values.to_vec();
    s.sort_by(f64::total_cmp);
    let h = q * (s.len() - 1) as f64;
    let (lo, hi) = (h.floor(), h.ceil());
    s[lo as usize] + (s[hi as usize] - s[lo as usize]) * (h - lo)
}

/// The parity's `JudgeQuantile` against the exact distribution, `sorted`
/// ascending.
fn parity_accepts(sorted: &[f64], q: f64, v: f64) -> bool {
    let n = sorted.len() as f64;
    if 0.1 * q * (1.0 - q) <= 1.0 / n {
        let h = q * (n - 1.0) + 1.0;
        let low = sorted[h.floor() as usize - 1];
        let high = sorted[(h.ceil() as usize).min(sorted.len()) - 1];
        return low <= v && v <= high;
    }
    let below = sorted.partition_point(|x| *x < v) as f64 / n;
    let at_or_below = sorted.partition_point(|x| *x <= v) as f64 / n;
    let error = if q < below {
        below - q
    } else if q > at_or_below {
        q - at_or_below
    } else {
        0.0
    };
    error <= 0.1 * q * (1.0 - q)
}

fn update(accumulator: &mut CompatAccumulator, values: &[f64], q: f64) {
    for chunk in values.chunks(ROWS_PER_BATCH) {
        let values: ArrayRef = Arc::new(Float64Array::from(chunk.to_vec()));
        let quantiles: ArrayRef = Arc::new(Float64Array::from(vec![q; chunk.len()]));
        accumulator.update_batch(&[values, quantiles]).unwrap();
    }
}

fn state_arrays(accumulator: &mut CompatAccumulator) -> Vec<ArrayRef> {
    accumulator
        .state()
        .unwrap()
        .iter()
        .map(|s| s.to_array().unwrap())
        .collect()
}

/// One partial state per partition, merged in order into a final one.
fn merged(partitions: &[&[f64]], q: f64) -> CompatAccumulator {
    let mut last = CompatAccumulator::default();
    for partition in partitions {
        let mut partial = CompatAccumulator::default();
        update(&mut partial, partition, q);
        last.merge_batch(&state_arrays(&mut partial)).unwrap();
    }
    last
}

fn value(accumulator: &mut CompatAccumulator) -> Option<f64> {
    match accumulator.evaluate().unwrap() {
        ScalarValue::Float64(v) => v,
        other => panic!("{other:?}"),
    }
}

fn is_exact(accumulator: &CompatAccumulator) -> bool {
    matches!(accumulator.values, Values::Exact(_))
}

/// `values` dealt round-robin to `k` partitions.
fn deal(values: &[f64], k: usize) -> Vec<Vec<f64>> {
    let mut out = vec![Vec::new(); k];
    for (i, v) in values.iter().enumerate() {
        out[i % k].push(*v);
    }
    out
}

#[test]
fn up_to_the_limit_it_answers_the_parity_exact_interpolation() {
    let mut rng = Rng(1);
    for n in [1, 2, 3, 37, 211, 1_013, 9_973, 10_010] {
        let values = rng.values(n, 0);
        for q in QUANTILES {
            let mut accumulator = CompatAccumulator::default();
            update(&mut accumulator, &values, q);
            assert!(is_exact(&accumulator), "n={n}");
            let got = value(&mut accumulator).unwrap();
            let want = parity_exact_quantile(&values, q);
            assert!(
                (got - want).abs() <= want.abs() * 1e-12,
                "n={n} q={q}: {got} vs {want}"
            );
        }
    }
}

#[test]
fn the_limit_is_where_the_parity_stops_judging_the_p999_by_order_statistics() {
    let tolerance = 0.1 * 0.999 * (1.0 - 0.999);
    assert!(tolerance <= 1.0 / 10_010.0 && tolerance > 1.0 / 10_011.0);
    assert_eq!(EXACT_LIMIT, 10_010);
}

#[test]
fn the_limit_is_the_last_group_kept_exact() {
    let mut rng = Rng(2);
    let values = rng.values(10_011, 0);
    let mut at = CompatAccumulator::default();
    update(&mut at, &values[..10_010], 0.999);
    let state = state_arrays(&mut at);
    assert!(
        state[0].is_valid(0) && state[1].is_null(0),
        "10 010 values travel as values"
    );
    let mut past = CompatAccumulator::default();
    update(&mut past, &values, 0.999);
    let state = state_arrays(&mut past);
    assert!(
        state[0].is_null(0) && state[1].is_valid(0),
        "10 011 values travel as a digest"
    );
    let mut sorted = values.clone();
    sorted.sort_by(f64::total_cmp);
    for q in QUANTILES {
        let mut accumulator = CompatAccumulator::default();
        update(&mut accumulator, &values, q);
        let got = value(&mut accumulator).unwrap();
        assert!(parity_accepts(&sorted, q, got), "n=10 011 q={q}: {got}");
    }
}

#[test]
fn every_split_of_a_group_up_to_the_limit_answers_the_same() {
    let mut rng = Rng(3);
    let values = rng.values(EXACT_LIMIT, 1);
    for q in [0.5, 0.95, 0.99, 0.999] {
        let want = parity_exact_quantile(&values, q);
        let (a, b) = values.split_at(7_919);
        let (b, c) = b.split_at(1);
        for partitions in [vec![&values[..]], vec![a, b, c], vec![c, &[], b, a]] {
            let mut accumulator = merged(&partitions, q);
            assert!(is_exact(&accumulator));
            let got = value(&mut accumulator).unwrap();
            assert!(
                (got - want).abs() <= want.abs() * 1e-12,
                "q={q}: {got} vs {want}"
            );
        }
        let dealt = deal(&values, 7);
        let parts: Vec<&[f64]> = dealt.iter().map(Vec::as_slice).collect();
        let got = value(&mut merged(&parts, q)).unwrap();
        assert!(
            (got - want).abs() <= want.abs() * 1e-12,
            "q={q} dealt: {got} vs {want}"
        );
    }
}

#[test]
fn exact_and_digest_partials_merge_in_any_order() {
    let mut rng = Rng(4);
    let values = rng.values(21_023, 0);
    let mut sorted = values.clone();
    sorted.sort_by(f64::total_cmp);
    let (big, rest) = values.split_at(EXACT_LIMIT + 1);
    let (other_big, small) = rest.split_at(EXACT_LIMIT + 1);
    let (small, tiny) = small.split_at(small.len() - 1);
    let cases: [(&str, Vec<&[f64]>); 5] = [
        (
            "exact+exact past the limit",
            vec![
                &values[..EXACT_LIMIT],
                &values[EXACT_LIMIT..2 * EXACT_LIMIT],
                &values[2 * EXACT_LIMIT..],
            ],
        ),
        ("exact+digest", vec![small, big, other_big, tiny]),
        ("digest+exact", vec![big, small, tiny, other_big]),
        ("digest+digest", vec![big, other_big, small, tiny]),
        ("dealt to 4", Vec::new()),
    ];
    let dealt = deal(&values, 4);
    for (name, mut partitions) in cases {
        if partitions.is_empty() {
            partitions = dealt.iter().map(Vec::as_slice).collect();
        }
        for q in QUANTILES {
            let mut accumulator = merged(&partitions, q);
            let Values::Digest(digest) = &accumulator.values else {
                panic!("{name}: a digest")
            };
            assert_eq!(digest.count(), 21_023.0, "{name}: every value counted");
            let got = value(&mut accumulator).unwrap();
            assert!(parity_accepts(&sorted, q, got), "{name} q={q}: {got}");
        }
    }
}

#[test]
fn a_random_corpus_of_odd_sizes_passes_the_parity_rule() {
    let mut rng = Rng(5);
    let bands = [
        (2, 50),
        (50, 211),
        (211, 1_000),
        (1_000, 3_000),
        (3_000, EXACT_LIMIT + 1),
        (EXACT_LIMIT + 1, 12_000),
        (12_000, 40_000),
    ];
    let mut misses = Vec::new();
    let mut judged = 0;
    for (lo, hi) in bands {
        for group in 0..12 {
            let n = rng.below(lo, hi);
            let values = rng.values(n, group % 4);
            let mut sorted = values.clone();
            sorted.sort_by(f64::total_cmp);
            let dealt = deal(&values, 1 + group % 4);
            let parts: Vec<&[f64]> = dealt.iter().map(Vec::as_slice).collect();
            for q in QUANTILES {
                let got = value(&mut merged(&parts, q)).unwrap();
                judged += 1;
                if !parity_accepts(&sorted, q, got) {
                    misses.push(format!("n={n} shape={} q={q}: {got}", group % 4));
                }
            }
        }
    }
    assert!(
        misses.is_empty(),
        "{} of {judged} outside the rule:\n{}",
        misses.len(),
        misses.join("\n")
    );
}

#[test]
fn size_counts_the_values_held_and_the_digest() {
    let mut rng = Rng(6);
    let values = rng.values(EXACT_LIMIT + 1, 0);
    let own = std::mem::size_of::<CompatAccumulator>();
    let mut accumulator = CompatAccumulator::default();
    update(&mut accumulator, &values[..1_013], 0.99);
    let Values::Exact(held) = &accumulator.values else {
        panic!("exact")
    };
    assert_eq!(accumulator.size(), own + held.capacity() * 8);
    assert!(held.capacity() >= 1_013);
    update(&mut accumulator, &values[1_013..EXACT_LIMIT], 0.99);
    assert_eq!(
        accumulator.size(),
        own + EXACT_LIMIT * 8,
        "the vector never grows past the limit"
    );
    update(&mut accumulator, &values[EXACT_LIMIT..], 0.99);
    let Values::Digest(digest) = &accumulator.values else {
        panic!("digest")
    };
    let centroids = digest.centroids().len();
    assert!(centroids > 0 && centroids <= DIGEST_CENTROIDS);
    assert!(accumulator.size() >= own + std::mem::size_of_val(digest.centroids()));
    assert!(
        accumulator.size() < own + EXACT_LIMIT * 8,
        "the values are released"
    );
}

#[test]
fn nulls_and_nan_are_skipped_and_an_empty_group_is_null() {
    let mut accumulator = CompatAccumulator::default();
    assert_eq!(value(&mut accumulator), None);
    let values: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(3.0),
        None,
        Some(f64::NAN),
        Some(1.0),
    ]));
    let quantiles: ArrayRef = Arc::new(Float64Array::from(vec![0.5; 4]));
    accumulator.update_batch(&[values, quantiles]).unwrap();
    assert_eq!(value(&mut accumulator), Some(2.0));
    let mut empty = CompatAccumulator::default();
    let mut last = CompatAccumulator::default();
    last.merge_batch(&state_arrays(&mut empty)).unwrap();
    assert_eq!(value(&mut last), None);
}

#[test]
fn a_bad_quantile_or_a_damaged_digest_is_refused() {
    let mut accumulator = CompatAccumulator::default();
    assert!(accumulator.set_quantile(1.5).is_err());
    assert!(accumulator.set_quantile(f64::NAN).is_err());
    accumulator.set_quantile(0.5).unwrap();
    assert!(accumulator.set_quantile(0.9).is_err());
    assert!(decode_digest(&[1.0, 2.0, 3.0]).is_err());
    assert!(decode_digest(&[1.0, 2.0, 3.0, 1.0, 5.0]).is_err());
    assert!(decode_digest(&[3.0, 2.0, 2.0, 1.0, 2.0, 1.0, 1.0, 0.0]).is_err());
    let mut digest = TDigest::new(DIGEST_CENTROIDS).merge_unsorted_f64(vec![1.0, 2.0, 4.0]);
    digest = decode_digest(&encode_digest(&digest)).unwrap();
    assert_eq!(digest.count(), 3.0);
}

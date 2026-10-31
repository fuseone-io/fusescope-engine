//! The bloom filters of version 1: bit `i` is bit `i % 8` of byte `i / 8`.
//! A term sets [`HASHES`] bits: its xxh3, plus `j` times the golden ratio
//! for the `j`-th bit, through the splitmix64 finalizer, modulo the bits.
//! Plain double hashing would make small blooms collide far above their
//! rate.

/// Hashes per term.
pub const HASHES: u8 = 7;

/// Bits wanted per term counted, about 1 % false positives at
/// [`HASHES`] hashes when the terms are distinct.
pub const BITS_PER_TERM: u64 = 10;

const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

/// The bits a bloom wants for `terms`: [`BITS_PER_TERM`] each, rounded up
/// to a whole 64-bit word.
pub fn wanted_bits(terms: u64) -> u64 {
    terms
        .saturating_mul(BITS_PER_TERM)
        .div_ceil(64)
        .saturating_mul(64)
}

/// Sets the bits of `term` in `bloom`, `bits` long.
pub fn insert(bloom: &mut [u8], bits: u64, hashes: u8, term: &[u8]) {
    for bit in positions(term, bits, hashes) {
        if let Some(byte) = usize::try_from(bit / 8).ok().and_then(|i| bloom.get_mut(i)) {
            *byte |= 1 << (bit % 8);
        }
    }
}

/// True when every bit of `term` is set: the term may have been inserted.
/// An empty bloom holds nothing.
pub fn may_contain(bloom: &[u8], bits: u64, hashes: u8, term: &[u8]) -> bool {
    bits > 0
        && positions(term, bits, hashes).all(|bit| {
            usize::try_from(bit / 8)
                .ok()
                .and_then(|i| bloom.get(i))
                .is_some_and(|byte| byte & (1 << (bit % 8)) != 0)
        })
}

fn positions(term: &[u8], bits: u64, hashes: u8) -> impl Iterator<Item = u64> {
    let h = twox_hash::XxHash3_64::oneshot(term);
    (1..=u64::from(hashes)).map(move |j| mix(h.wrapping_add(j.wrapping_mul(GOLDEN))) % bits.max(1))
}

fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_terms_at_the_wanted_size_give_about_one_percent_false_positives() {
        let terms = 20_000u64;
        let bits = wanted_bits(terms);
        let mut bloom = vec![0u8; usize::try_from(bits / 8).unwrap()];
        for t in 0..terms {
            insert(&mut bloom, bits, HASHES, format!("in-{t}").as_bytes());
        }
        for t in 0..terms {
            assert!(may_contain(
                &bloom,
                bits,
                HASHES,
                format!("in-{t}").as_bytes()
            ));
        }
        let probes = 200_000u32;
        let hits = (0..probes)
            .filter(|t| may_contain(&bloom, bits, HASHES, format!("out-{t}").as_bytes()))
            .count();
        let rate = f64::from(u32::try_from(hits).unwrap()) / f64::from(probes);
        assert!(rate < 0.015, "false positive rate {rate}");
        assert!(
            rate > 0.003,
            "false positive rate {rate} is suspiciously low"
        );
    }

    #[test]
    fn a_small_bloom_stays_near_its_rate() {
        let mut hits = 0usize;
        for n in 0..2000u32 {
            let mut bloom = [0u8; 8];
            insert(&mut bloom, 64, HASHES, format!("term{n}").as_bytes());
            hits += (0..200u32)
                .filter(|p| may_contain(&bloom, 64, HASHES, format!("probe{p}").as_bytes()))
                .count();
        }
        // One term in 64 bits: (7/64)^7 is about 2e-7; double hashing gave 1e-3.
        assert!(hits < 10, "{hits} false positives in 400000 probes");
    }

    #[test]
    fn an_empty_bloom_holds_nothing() {
        assert!(!may_contain(&[], 0, HASHES, b"abc"));
        assert_eq!(wanted_bits(0), 0);
        assert_eq!(wanted_bits(1), 64);
        assert_eq!(wanted_bits(7), 128);
    }
}

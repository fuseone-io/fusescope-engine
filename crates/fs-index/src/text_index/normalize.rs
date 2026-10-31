//! Normalization 1: what a body's trigrams and tokens are.
//!
//! ASCII letters are lowercased and every other byte is kept, so a
//! trigram is any window of three bytes of the folded body, multi-byte
//! characters included. A token is a maximal run of `[A-Za-z0-9]`; any
//! other byte, non-ASCII ones included, separates tokens.

/// Bytes of a trigram.
pub const NGRAM: usize = 3;

/// The folded trigrams of `bytes`, in order and with repeats.
pub fn trigrams(bytes: &[u8]) -> impl Iterator<Item = [u8; NGRAM]> + '_ {
    bytes
        .windows(NGRAM)
        .map(|w| std::array::from_fn(|i| w.get(i).copied().unwrap_or(0).to_ascii_lowercase()))
}

/// Number of trigrams of `bytes`, repeats included.
pub fn trigram_count(bytes: &[u8]) -> u64 {
    bytes.len().saturating_sub(NGRAM - 1) as u64
}

/// The tokens of `bytes` as written, in order and with repeats.
pub fn tokens(bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
    bytes
        .split(|b| !b.is_ascii_alphanumeric())
        .filter(|t| !t.is_empty())
}

/// True when `needle` is exactly one token: non-empty, all `[A-Za-z0-9]`.
pub fn is_token(needle: &[u8]) -> bool {
    !needle.is_empty() && needle.iter().all(u8::is_ascii_alphanumeric)
}

/// `has_token(body, needle)`: `needle` is exactly one token and one of
/// the tokens of `body`, compared case-sensitively.
pub fn has_token(body: &[u8], needle: &[u8]) -> bool {
    is_token(needle) && tokens(body).any(|t| t == needle)
}

/// `bytes` with ASCII letters lowercased into `into`, which is cleared
/// first.
pub(crate) fn fold_into(bytes: &[u8], into: &mut Vec<u8>) {
    into.clear();
    into.extend(bytes.iter().map(u8::to_ascii_lowercase));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigrams_fold_ascii_and_keep_other_bytes() {
        let got: Vec<[u8; 3]> = trigrams("AbÇd".as_bytes()).collect();
        // "Ç" is two bytes, 0xC3 0x87, kept as they are.
        assert_eq!(
            got,
            vec![[b'a', b'b', 0xC3], [b'b', 0xC3, 0x87], [0xC3, 0x87, b'd']]
        );
        assert_eq!(trigram_count("AbÇd".as_bytes()), 3);
        assert_eq!(trigram_count(b"ab"), 0);
    }

    #[test]
    fn tokens_are_alphanumeric_runs_split_by_any_other_byte() {
        let got: Vec<&[u8]> = tokens("GET /api/v1?id=42 naïve-x".as_bytes()).collect();
        let want: Vec<&[u8]> = vec![b"GET", b"api", b"v1", b"id", b"42", b"na", b"ve", b"x"];
        assert_eq!(got, want);
    }

    #[test]
    fn has_token_is_case_sensitive_and_wants_one_whole_token() {
        assert!(has_token(b"connection timeout: retry", b"timeout"));
        assert!(!has_token(b"connection timeout: retry", b"Timeout"));
        assert!(!has_token(b"connection timeouts", b"timeout"));
        assert!(!has_token(b"a b", b""));
        assert!(!has_token(b"a-b", b"a-b"));
        assert!(has_token(b"x=1", b"1"));
        assert!(!has_token("ça va".as_bytes(), "ça".as_bytes()));
        assert!(has_token("ça va".as_bytes(), b"a"));
    }
}

//! What a body predicate asks of a row group's blooms.

use super::Lookup;
use super::normalize::{self, NGRAM};

/// A body predicate as the blooms can test it. A row group may match
/// unless a recognised condition proves otherwise; anything not
/// recognised is [`Probe::Unknown`] and keeps every group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Probe {
    /// Proves nothing.
    Unknown,
    /// The body holds these folded trigrams. `folded` probes come from a
    /// case-insensitive predicate and test only all-ASCII row groups.
    Trigrams {
        /// Folded trigrams, at least one.
        grams: Vec<[u8; NGRAM]>,
        /// Case-insensitive predicate.
        folded: bool,
    },
    /// The body holds this folded token.
    Token {
        /// Folded token.
        token: Vec<u8>,
        /// Case-insensitive predicate.
        folded: bool,
    },
    /// Every probe holds.
    And(Vec<Probe>),
    /// One probe holds.
    Or(Vec<Probe>),
}

impl Probe {
    /// The body contains `fragment`. Unknown below [`NGRAM`] bytes, or
    /// for a case-insensitive fragment that is not ASCII.
    pub fn fragment(fragment: &[u8], folded: bool) -> Self {
        if fragment.len() < NGRAM || (folded && !fragment.is_ascii()) {
            return Self::Unknown;
        }
        let mut grams: Vec<[u8; NGRAM]> = normalize::trigrams(fragment).collect();
        grams.sort_unstable();
        grams.dedup();
        Self::Trigrams { grams, folded }
    }

    /// The body has `needle` as a token. Unknown when `needle` is not
    /// exactly one token.
    pub fn token(needle: &[u8], folded: bool) -> Self {
        if !normalize::is_token(needle) {
            return Self::Unknown;
        }
        let mut token = Vec::with_capacity(needle.len());
        normalize::fold_into(needle, &mut token);
        Self::Token { token, folded }
    }

    /// Every one of `probes`; unknown ones drop out.
    pub fn all(probes: impl IntoIterator<Item = Self>) -> Self {
        let mut known: Vec<Self> = probes.into_iter().filter(Self::prunes).collect();
        match known.len() {
            0 => Self::Unknown,
            1 => known.pop().unwrap_or(Self::Unknown),
            _ => Self::And(known),
        }
    }

    /// One of `probes`; one unknown makes the whole unknown.
    pub fn any(probes: impl IntoIterator<Item = Self>) -> Self {
        let mut all: Vec<Self> = probes.into_iter().collect();
        if all.is_empty() || !all.iter().all(Self::prunes) {
            return Self::Unknown;
        }
        if all.len() == 1 {
            return all.pop().unwrap_or(Self::Unknown);
        }
        Self::Or(all)
    }

    /// True when the probe can rule a row group out.
    pub fn prunes(&self) -> bool {
        match self {
            Self::Unknown => false,
            Self::Trigrams { .. } | Self::Token { .. } => true,
            Self::And(ps) => ps.iter().any(Self::prunes),
            Self::Or(ps) => !ps.is_empty() && ps.iter().all(Self::prunes),
        }
    }

    pub(crate) fn may_match(&self, group: &Lookup<'_>) -> bool {
        match self {
            Self::Unknown => true,
            Self::Trigrams { grams, folded } => {
                group.may_have_trigrams(grams, *folded).unwrap_or(true)
            }
            Self::Token { token, folded } => group.may_have_token(token, *folded).unwrap_or(true),
            Self::And(ps) => ps.iter().all(|p| p.may_match(group)),
            Self::Or(ps) => ps.iter().any(|p| p.may_match(group)),
        }
    }
}

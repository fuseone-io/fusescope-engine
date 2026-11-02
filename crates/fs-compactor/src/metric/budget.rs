//! The one budget of a metric pass, `compactor.metric_pass_bytes`, split
//! into shares that together are the whole: every allocation of a part is
//! admitted against its share before it is made, so the parts never hold
//! more than the budget together.

use crate::CompactError;

/// What a pass holds memory for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Part {
    /// The inputs: their metadata, two chunks each and a decoded row.
    Inputs = 0,
    /// The samples file being written: its buffers, row group and footer.
    Samples = 1,
    /// The series file being written.
    Series = 2,
    /// The postings of the series file being written.
    Postings = 3,
    /// The metadata contribution being counted, and its encoding.
    Metadata = 4,
    /// The usage counts and their encoding.
    Usage = 5,
    /// The row groups being gathered and the group's series rows.
    Gather = 6,
}

/// Each part's share of the budget, in sixteenths; they sum to sixteen.
const SIXTEENTHS: [usize; 7] = [4, 4, 2, 2, 2, 1, 1];

/// Least budget a pass takes.
pub const MIN_METRIC_PASS_BYTES: usize = 16 << 20;

/// Most budget a pass takes.
pub const MAX_METRIC_PASS_BYTES: usize = 8 << 30;

/// Budget of a pass by default.
pub const DEFAULT_METRIC_PASS_BYTES: usize = 512 << 20;

/// What each part holds, against its share.
#[derive(Debug)]
pub(crate) struct Budget {
    cap: usize,
    held: [usize; 7],
    peak: usize,
}

impl Budget {
    /// A budget of `bytes`, clamped to its bounds whatever the caller set.
    pub(crate) fn new(bytes: usize) -> Self {
        Self {
            cap: bytes.clamp(MIN_METRIC_PASS_BYTES, MAX_METRIC_PASS_BYTES),
            held: [0; 7],
            peak: 0,
        }
    }

    /// The share of `part`.
    pub(crate) fn share(&self, part: Part) -> usize {
        self.cap / 16 * SIXTEENTHS.get(part as usize).copied().unwrap_or(0)
    }

    /// What `part` holds now.
    pub(crate) fn held(&self, part: Part) -> usize {
        self.held.get(part as usize).copied().unwrap_or(0)
    }

    /// Whether `part` may hold `bytes` more.
    pub(crate) fn fits(&self, part: Part, bytes: usize) -> bool {
        self.held(part).saturating_add(bytes) <= self.share(part)
    }

    /// Admits `bytes` more for `part`, before they are allocated.
    ///
    /// # Errors
    /// Past the part's share.
    pub(crate) fn take(&mut self, part: Part, bytes: usize) -> Result<(), CompactError> {
        if !self.fits(part, bytes) {
            return Err(CompactError::Merge("a metric pass past its budget"));
        }
        self.set(part, self.held(part) + bytes);
        Ok(())
    }

    /// Records what `part` holds, as measured after an allocation it
    /// admitted, or once it freed some.
    pub(crate) fn set(&mut self, part: Part, bytes: usize) {
        if let Some(held) = self.held.get_mut(part as usize) {
            *held = bytes;
        }
        self.peak = self
            .peak
            .max(self.held.iter().fold(0, |a, b| a.saturating_add(*b)));
    }

    /// The most the parts held together.
    pub(crate) fn peak(&self) -> usize {
        self.peak
    }

    /// The budget.
    pub(crate) fn cap(&self) -> usize {
        self.cap
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_shares_are_the_budget_and_it_is_clamped() {
        let budget = Budget::new(DEFAULT_METRIC_PASS_BYTES);
        let parts = [
            Part::Inputs,
            Part::Samples,
            Part::Series,
            Part::Postings,
            Part::Metadata,
            Part::Usage,
            Part::Gather,
        ];
        let total: usize = parts.iter().map(|p| budget.share(*p)).sum();
        assert_eq!(total, budget.cap());
        assert_eq!(Budget::new(0).cap(), MIN_METRIC_PASS_BYTES);
        assert_eq!(Budget::new(usize::MAX).cap(), MAX_METRIC_PASS_BYTES);
        let mut small = Budget::new(MIN_METRIC_PASS_BYTES);
        let share = small.share(Part::Postings);
        small.take(Part::Postings, share).unwrap();
        assert!(small.take(Part::Postings, 1).is_err());
        assert_eq!(small.peak(), share);
    }
}

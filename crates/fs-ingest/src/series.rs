//! The series of a metric buffer: one row a fingerprint and hour, merged
//! as requests arrive, in the order [`SeriesRow::order`] keeps.
//!
//! Rows sit in two sorted runs: `main`, large, and `tail`, small. A
//! request's rows that are already known only refine a description; the
//! others are inserted into `tail`, and once `tail` outgrows a share of
//! `main` the two are merged into a new `main`. That merge holds the old
//! runs and the new one at once, [`SeriesSet::merge_peak`], which the
//! caller reserves before adding.

use std::cmp::Ordering;

use arrow::record_batch::RecordBatch;
use fs_schema::metrics::{SeriesRow, series_batch};

use crate::IngestError;

/// `tail` merges into `main` past this many rows or an eighth of `main`.
const TAIL_ROWS: usize = 1024;

/// Bytes of one row in a run, its texts apart.
const ROW: usize = std::mem::size_of::<SeriesRow>();

/// The series of a buffer.
#[derive(Debug, Default)]
pub struct SeriesSet {
    main: Vec<SeriesRow>,
    tail: Vec<SeriesRow>,
    /// Bytes of the rows' texts and labels.
    heap: usize,
}

impl SeriesSet {
    /// Rows held.
    pub fn len(&self) -> usize {
        self.main.len() + self.tail.len()
    }

    /// True without rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bytes held: both runs as allocated and every row's texts.
    pub fn bytes(&self) -> usize {
        (self.main.capacity() + self.tail.capacity()) * ROW + self.heap
    }

    /// Most bytes adding `incoming` rows holds beyond what the set and
    /// the rows already hold: a merge of the runs into a new one, and the
    /// growth of `tail` before it.
    pub fn merge_peak(&self, incoming: usize) -> usize {
        (self.len() + incoming) * ROW + incoming * ROW
    }

    /// Adds `rows`, sorted as [`fs_schema::metrics::dedupe_series`] leaves
    /// them, keeping one row a series and hour.
    pub fn add(&mut self, rows: Vec<SeriesRow>) {
        for row in rows {
            self.add_one(row);
        }
        if self.tail.len() > TAIL_ROWS.max(self.main.len() / 8) {
            self.compact();
        }
    }

    fn add_one(&mut self, row: SeriesRow) {
        if let Ok(at) = self.main.binary_search_by(|r| r.order(&row)) {
            if let Some(kept) = self.main.get_mut(at) {
                self.heap = self.heap.saturating_add_signed(kept.absorb(row));
            }
            return;
        }
        match self.tail.binary_search_by(|r| r.order(&row)) {
            Ok(at) => {
                if let Some(kept) = self.tail.get_mut(at) {
                    self.heap = self.heap.saturating_add_signed(kept.absorb(row));
                }
            }
            Err(at) => {
                self.heap += row.heap_bytes();
                if self.tail.len() == self.tail.capacity() {
                    self.tail.reserve_exact(self.tail.len().max(16));
                }
                self.tail.insert(at, row);
            }
        }
    }

    /// Merges `tail` into a new `main`.
    fn compact(&mut self) {
        let main = std::mem::take(&mut self.main);
        let tail = std::mem::take(&mut self.tail);
        let mut merged = Vec::with_capacity(main.len() + tail.len());
        let mut a = main.into_iter().peekable();
        let mut b = tail.into_iter().peekable();
        loop {
            let next = match (a.peek(), b.peek()) {
                (Some(x), Some(y)) => match x.order(y) {
                    Ordering::Greater => b.next(),
                    _ => a.next(),
                },
                (Some(_), None) => a.next(),
                (None, Some(_)) => b.next(),
                (None, None) => break,
            };
            merged.extend(next);
        }
        self.main = merged;
    }

    /// Every row in order.
    pub fn rows(&self) -> impl Iterator<Item = &SeriesRow> {
        let mut a = self.main.iter().peekable();
        let mut b = self.tail.iter().peekable();
        std::iter::from_fn(move || match (a.peek(), b.peek()) {
            (Some(x), Some(y)) => match x.order(y) {
                Ordering::Greater => b.next(),
                _ => a.next(),
            },
            (Some(_), None) => a.next(),
            (None, Some(_)) => b.next(),
            (None, None) => None,
        })
    }

    /// The rows as a batch of the series schema, in order.
    ///
    /// # Errors
    /// An Arrow failure.
    pub fn batch(&self) -> Result<RecordBatch, IngestError> {
        Ok(series_batch(self.rows(), self.len())?)
    }

    /// Takes the rows of each of `hours`, starts in Unix milliseconds, as
    /// sets of their own, in the same order; the rest stay.
    pub fn take_hours(&mut self, hours: &[i64]) -> Vec<SeriesSet> {
        self.compact();
        let main = std::mem::take(&mut self.main);
        let mut parts: Vec<SeriesSet> = hours.iter().map(|_| SeriesSet::default()).collect();
        let mut counts = vec![0usize; hours.len()];
        for row in &main {
            if let Some(i) = hours.iter().position(|h| *h == row.unix_milli) {
                if let Some(c) = counts.get_mut(i) {
                    *c += 1;
                }
            }
        }
        for (part, count) in parts.iter_mut().zip(&counts) {
            part.main.reserve_exact(*count);
        }
        let kept = main.len() - counts.iter().sum::<usize>();
        let mut rest = Vec::with_capacity(kept);
        for row in main {
            match hours.iter().position(|h| *h == row.unix_milli) {
                Some(i) => {
                    if let Some(part) = parts.get_mut(i) {
                        part.heap += row.heap_bytes();
                        part.main.push(row);
                    }
                }
                None => rest.push(row),
            }
        }
        self.heap = rest.iter().map(SeriesRow::heap_bytes).sum();
        self.main = rest;
        parts
    }

    /// Takes every row of `other` back, as after a cut whose intent
    /// failed.
    pub fn absorb(&mut self, mut other: SeriesSet) {
        other.compact();
        self.add(std::mem::take(&mut other.main));
    }
}

#[cfg(test)]
mod tests {
    use fs_schema::metrics::{MetricType, Temporality};

    use super::*;

    const HOUR: i64 = 3_600_000;

    fn row(fingerprint: u64, hour: i64, description: &str) -> SeriesRow {
        SeriesRow {
            fingerprint,
            unix_milli: hour * HOUR,
            name: format!("m{}", fingerprint % 3).into(),
            kind: MetricType::Gauge,
            temporality: Temporality::Unspecified,
            monotonic: false,
            unit: "".into(),
            description: description.into(),
            labels: vec![("k".into(), fingerprint.to_string().into())].into(),
        }
    }

    fn keys(set: &SeriesSet) -> Vec<(u64, i64, String)> {
        set.rows()
            .map(|r| {
                (
                    r.fingerprint,
                    r.unix_milli / HOUR,
                    r.description.to_string(),
                )
            })
            .collect()
    }

    #[test]
    fn rows_merge_as_requests_arrive_one_a_series_and_hour_in_order() {
        let mut set = SeriesSet::default();
        for request in 0..3000u64 {
            let mut rows = vec![row(request % 2000, 1, "d"), row(request % 7, 2, "")];
            fs_schema::metrics::dedupe_series(&mut rows);
            set.add(rows);
        }
        set.add(vec![row(5, 1, "longer")]);
        let all = keys(&set);
        assert_eq!(all.len(), 2000 + 7);
        let mut sorted: Vec<&SeriesRow> = set.rows().collect();
        sorted.sort_by(|a, b| a.order(b));
        assert!(
            set.rows().zip(sorted).all(|(a, b)| std::ptr::eq(a, b)),
            "in order"
        );
        assert!(all.contains(&(5, 1, "longer".into())));
        let heap: usize = set.rows().map(SeriesRow::heap_bytes).sum();
        assert_eq!(
            set.bytes(),
            (set.main.capacity() + set.tail.capacity()) * ROW + heap
        );
        assert!(set.merge_peak(10) >= (set.len() + 10) * ROW);
    }

    #[test]
    fn hours_are_taken_whole_and_given_back_whole() {
        let mut set = SeriesSet::default();
        set.add(
            (0..50u64)
                .map(|f| row(f, i64::try_from(f % 3).unwrap(), "x"))
                .collect(),
        );
        let before = keys(&set);
        let parts = set.take_hours(&[0, 2 * HOUR]);
        assert_eq!(parts.len(), 2);
        let (first, last) = (parts.first().unwrap(), parts.get(1).unwrap());
        assert!(first.rows().all(|r| r.unix_milli == 0));
        assert!(last.rows().all(|r| r.unix_milli == 2 * HOUR));
        assert!(set.rows().all(|r| r.unix_milli == HOUR));
        assert_eq!(first.len() + last.len() + set.len(), 50);
        for part in parts {
            set.absorb(part);
        }
        assert_eq!(keys(&set), before);
        let batch = set.batch().unwrap();
        assert_eq!(batch.num_rows(), 50);
    }
}

//! The series of a metric buffer: one row a fingerprint and hour, merged
//! as requests arrive, in the order [`SeriesRow::order`] keeps.
//!
//! Rows sit in two sorted runs: `main`, large, and `tail`, small. A
//! request's rows that are already known only refine a description; the
//! others are merged with `tail` into a new `tail`, or, once that would
//! outgrow a share of `main`, with both into a new `main`. Either run is
//! allocated once, of exactly its rows, while the old runs and the
//! request's rows are still held: [`SeriesSet::merge_peak`], which the
//! caller reserves before adding.

use std::cmp::Ordering;

use arrow::record_batch::RecordBatch;
use fs_schema::metrics::{SeriesRow, dedupe_series, series_batch};

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
    /// Bytes of the run the last add allocated.
    #[cfg(test)]
    peak: usize,
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

    /// Most bytes adding `incoming` rows allocates while the set and the
    /// rows are still held: one run of every row.
    pub fn merge_peak(&self, incoming: usize) -> usize {
        self.len().saturating_add(incoming).saturating_mul(ROW)
    }

    /// Adds `rows`, keeping one row a series and hour, merged as
    /// [`SeriesRow::absorb`] does; returns how many keys, name,
    /// fingerprint and hour, came to hold more than one identity.
    pub fn add(&mut self, mut rows: Vec<SeriesRow>) -> usize {
        let mut collisions = dedupe_series(&mut rows);
        rows.retain_mut(|row| match self.refine(row) {
            Refined::Merged => false,
            Refined::New => true,
            Refined::Collided => {
                collisions += 1;
                true
            }
        });
        if rows.is_empty() {
            #[cfg(test)]
            {
                self.peak = 0;
            }
            return collisions;
        }
        self.heap += rows.iter().map(SeriesRow::heap_bytes).sum::<usize>();
        let tail = std::mem::take(&mut self.tail);
        let run = if tail.len() + rows.len() > TAIL_ROWS.max(self.main.len() / 8) {
            let main = std::mem::take(&mut self.main);
            let len = main.len() + tail.len() + rows.len();
            self.main = merge(merge_sorted(main, tail), rows, len);
            &self.main
        } else {
            let len = tail.len() + rows.len();
            self.tail = merge(tail.into_iter(), rows, len);
            &self.tail
        };
        #[cfg(test)]
        {
            self.peak = run.capacity() * ROW;
        }
        #[cfg(not(test))]
        let _ = run;
        collisions
    }

    /// Merges `row` into the row of its series and hour already held. A
    /// key held under one identity only and now another is a collision.
    fn refine(&mut self, row: &mut SeriesRow) -> Refined {
        let mut held = 0;
        for run in [&mut self.main, &mut self.tail] {
            let start = run.partition_point(|r| key(r, row) == Ordering::Less);
            let end = start
                + run.get(start..).map_or(0, |rest| {
                    rest.partition_point(|r| key(r, row) == Ordering::Equal)
                });
            for kept in run.get_mut(start..end).into_iter().flatten() {
                if let Some(grown) = kept.absorb(row) {
                    self.heap = self.heap.saturating_add_signed(grown);
                    return Refined::Merged;
                }
            }
            held += end - start;
        }
        if held == 1 {
            Refined::Collided
        } else {
            Refined::New
        }
    }

    /// Merges `tail` into a new `main`.
    fn compact(&mut self) {
        let main = std::mem::take(&mut self.main);
        let tail = std::mem::take(&mut self.tail);
        let len = main.len() + tail.len();
        self.main = merge(main.into_iter(), tail, len);
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

/// What became of a row added to a set.
enum Refined {
    /// Merged into the row of its series and hour.
    Merged,
    /// New, of a key not held.
    New,
    /// New, of a key held under one other identity.
    Collided,
}

/// The order of the keys rows are kept by: name, fingerprint and hour.
fn key(a: &SeriesRow, b: &SeriesRow) -> Ordering {
    a.name
        .as_bytes()
        .cmp(b.name.as_bytes())
        .then(a.fingerprint.cmp(&b.fingerprint))
        .then(a.unix_milli.cmp(&b.unix_milli))
}

/// The rows of `a` and `b`, in order, sorted runs both, `len` of them,
/// in one allocation of exactly `len`.
fn merge(a: impl Iterator<Item = SeriesRow>, b: Vec<SeriesRow>, len: usize) -> Vec<SeriesRow> {
    let mut merged = Vec::with_capacity(len);
    merged.extend(merge_sorted(a, b));
    merged
}

/// The rows of `a` and `b`, sorted runs both, in order, `a`'s first of
/// equal ones.
fn merge_sorted(
    a: impl IntoIterator<Item = SeriesRow>,
    b: Vec<SeriesRow>,
) -> impl Iterator<Item = SeriesRow> {
    let mut a = a.into_iter().peekable();
    let mut b = b.into_iter().peekable();
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

#[cfg(test)]
mod tests {
    use fs_schema::metrics::{Datatype, LabelLevel, LabelMeta, MetricType, Temporality};

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
            label_meta: vec![LabelMeta::native(LabelLevel::Point, Datatype::Str)].into(),
        }
    }

    fn with_level(mut row: SeriesRow, level: Option<LabelLevel>, datatype: Datatype) -> SeriesRow {
        row.label_meta =
            vec![level.map_or(LabelMeta::LEGACY, |l| LabelMeta::native(l, datatype))].into();
        row
    }

    /// A row of a legacy origin and the same series of a known one are one
    /// row, the known origin and every type seen kept; another level of
    /// the same key is a second identity, counted once however often it
    /// comes back.
    #[test]
    fn a_buffer_merges_legacy_and_known_origins_and_counts_a_collision_once() {
        let mut set = SeriesSet::default();
        assert_eq!(
            set.add(vec![with_level(row(1, 1, ""), None, Datatype::Str)]),
            0
        );
        assert_eq!(
            set.add(vec![with_level(
                row(1, 1, ""),
                Some(LabelLevel::Point),
                Datatype::Int
            )]),
            0
        );
        assert_eq!(set.add(vec![row(1, 1, "")]), 0);
        assert_eq!(set.len(), 1);
        let meta = *set.rows().next().unwrap().label_meta.first().unwrap();
        assert_eq!(meta.level(), Some(LabelLevel::Point));
        assert_eq!(
            meta.datatypes().collect::<Vec<_>>(),
            [Datatype::Str, Datatype::Int]
        );
        let resource = || with_level(row(1, 1, ""), Some(LabelLevel::Resource), Datatype::Str);
        assert_eq!(set.add(vec![resource()]), 1);
        assert_eq!(set.add(vec![resource()]), 0, "the same identity again");
        assert_eq!(set.len(), 2);
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

    /// Whatever the runs hold, adding allocates one run, of exactly the
    /// rows it keeps, within what [`SeriesSet::merge_peak`] said before:
    /// a full `tail` meeting a new series does not grow it and then merge.
    #[test]
    fn adding_allocates_one_exact_run_within_the_merge_peak() {
        let mut set = SeriesSet::default();
        // A `main` small enough that `tail` merges at 1024 rows, its
        // capacity then.
        set.add((0..4000u64).map(|f| row(f * 2, 1, "")).collect());
        let mut next = 1u64;
        let mut merged = 0;
        for request in 0..400u64 {
            let rows: Vec<SeriesRow> = (0..u64::from(request % 7 == 0) * 40 + 3)
                .map(|_| {
                    next += 2;
                    row(next, 1, "")
                })
                .chain([row(request * 2, 1, "known")])
                .collect();
            let incoming = rows.len();
            let peak = set.merge_peak(incoming);
            let main_before = set.main.capacity();
            set.add(rows);
            assert!(set.peak <= peak, "{request}: {} > {peak}", set.peak);
            assert_eq!(set.main.capacity(), set.main.len());
            assert_eq!(set.tail.capacity(), set.tail.len());
            merged += usize::from(set.main.capacity() != main_before);
        }
        assert!(merged > 0, "the tail merged into main");
        assert_eq!(set.len(), 4000 + usize::try_from(next / 2).unwrap());
        let mut sorted: Vec<&SeriesRow> = set.rows().collect();
        sorted.sort_by(|a, b| a.order(b));
        assert!(set.rows().zip(sorted).all(|(a, b)| std::ptr::eq(a, b)));
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

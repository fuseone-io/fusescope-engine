//! The metric actions: a selector's series and samples, and the label
//! names and values of its series, read as the Go backend's Prometheus
//! querier reads them (`DESIGN.md` §8).
//!
//! A select resolves its series in four steps before reading a sample:
//!
//! 1. **Candidates.** The keys, name and fingerprint, of the rows of the
//!    window that pass every matcher. A series file with usable postings
//!    reads only the rows its label-requiring matchers allow.
//! 2. **Expansion.** Every row of those keys in the window, in every file
//!    and the hot window, whether it passes or not; files with postings
//!    read the rows of the candidates' names.
//! 3. **Validation.** A key holds one identity per hour, and the same
//!    across hours; otherwise the select fails as ambiguous.
//! 4. **Matchers** again, on each key's identity.
//!
//! Its samples are then deduplicated by time, the greatest source order
//! winning, optionally reduced to the last of each step bucket, and series
//! of equal labelsets merged. Everything held is reserved in the session's
//! memory pool before it is held.

mod matchers;
mod output;
mod request;
mod samples;
mod series;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::execution::memory_pool::MemoryReservation;
use fs_catalog::{Signal, TenantId};
use fs_ingest::hot::MetricTable;
use fs_ingest::record::RowSignal;
use fs_schema::metrics::{SeriesRow, Temporality, dedupe_series};
use object_store::ObjectStore;

pub use matchers::{MatchType, MatcherLimits, REGEX_SIZE_LIMIT, RawMatcher};
pub use output::select_schema;
pub use request::{
    Buckets, LabelLookup, LabelQuery, LastPerStep, SelectLookup, SelectQuery, Window,
};
pub use samples::STALE_NAN_BITS;

use crate::QueryError;
use crate::files::FileCache;
use crate::hot::HotWindow;
use crate::metric_index::MetricIndexCache;
use crate::prune::Pruning;
use crate::querier::{QueryStats, QueryWarnings};
use matchers::{Matcher, all_match, value_of};
use output::Series;
use series::{Narrow, Sources};

/// Most series a select may resolve, whatever it is configured with.
pub const MAX_SERIES: usize = 1_000_000;
/// Most matchers a request may carry, whatever it is configured with.
pub const MAX_MATCHERS: usize = 1024;
/// Most bytes of regex a request may carry, whatever it is configured with.
pub const MAX_REGEX_BYTES: usize = 1 << 20;

/// Bytes a candidate key holds besides its name: its slot in the maps.
const KEY_BYTES: usize = 64;

/// Bytes a listed value holds besides its text: its node in the set.
const VALUE_BYTES: usize = 64;

/// The bounds of a metric action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricLimits {
    /// Most series a select resolves.
    pub max_series: usize,
    /// Most samples a select answers.
    pub max_rows: usize,
    /// Bounds on the matchers.
    pub matchers: MatcherLimits,
}

impl MetricLimits {
    /// The limits clamped to [`MAX_SERIES`], [`MAX_MATCHERS`] and
    /// [`MAX_REGEX_BYTES`].
    #[must_use]
    pub fn clamped(self) -> Self {
        Self {
            max_series: self.max_series.min(MAX_SERIES),
            max_rows: self.max_rows,
            matchers: MatcherLimits {
                max_matchers: self.matchers.max_matchers.min(MAX_MATCHERS),
                max_regex_bytes: self.matchers.max_regex_bytes.min(MAX_REGEX_BYTES),
            },
        }
    }
}

/// What one metric action reads through.
pub(crate) struct Context<'a> {
    pub(crate) tenant: TenantId,
    pub(crate) cache: &'a Arc<FileCache>,
    pub(crate) hot: &'a Arc<HotWindow>,
    pub(crate) store: &'a Arc<dyn ObjectStore>,
    pub(crate) postings: &'a MetricIndexCache,
    pub(crate) stats: &'a QueryStats,
    pub(crate) limits: MetricLimits,
    pub(crate) budget: u64,
}

/// Name to fingerprints.
type Keys = HashMap<Box<str>, HashSet<u64>>;

/// A key whose rows agree on one identity.
#[derive(Debug)]
struct Resolved {
    name: Box<str>,
    fingerprint: u64,
    temporality: Temporality,
    labels: Box<[(Box<str>, Box<str>)]>,
}

impl Context<'_> {
    /// The answer of `query`: one batch of [`select_schema`].
    ///
    /// # Errors
    /// Invalid matchers, an ingester that does not answer, an ambiguous
    /// key, more series or samples than the limits, the byte budget, the
    /// memory pool, or a read failure.
    pub(crate) async fn select(
        &self,
        samples_frame: datafusion::dataframe::DataFrame,
        query: &SelectQuery,
        reservation: &mut MemoryReservation,
    ) -> Result<RecordBatch, QueryError> {
        let matchers: Arc<[Matcher]> =
            matchers::compile(&query.matchers, self.limits.matchers, reservation)?.into();
        let sources = self.sources(query.window, None, reservation).await?;
        let candidates = self
            .candidates(&sources, &matchers, &query.temporalities, reservation)
            .await?;
        if candidates.is_empty() {
            return output::batch(&[], reservation);
        }
        let rows = self.expand(&sources, &candidates, reservation).await?;
        drop(candidates);
        let resolved: Vec<Resolved> = self
            .resolve(rows)?
            .into_iter()
            .filter(|r| {
                query.temporalities.contains(&r.temporality) && all_match(&matchers, &r.labels)
            })
            .collect();
        if resolved.len() > self.limits.max_series {
            return Err(QueryError::TooManySeries(self.limits.max_series));
        }
        reservation.try_grow(
            resolved
                .iter()
                .map(|r| r.name.len() + KEY_BYTES + std::mem::size_of::<Series>())
                .sum(),
        )?;
        let mut positions = samples::Positions::new();
        for (at, r) in resolved.iter().enumerate() {
            positions
                .entry(r.name.clone())
                .or_default()
                .insert(r.fingerprint, at);
        }
        let mut read = vec![Vec::new(); resolved.len()];
        samples::read(
            samples_frame,
            &positions,
            query.window,
            query.buckets,
            self.limits.max_rows,
            &mut read,
            reservation,
        )
        .await?;
        let series = resolved
            .into_iter()
            .zip(read)
            .map(|(r, samples)| Series {
                labels: r
                    .labels
                    .into_vec()
                    .into_iter()
                    .filter(|(_, v)| !v.is_empty())
                    .collect(),
                samples,
            })
            .collect();
        let fused = output::fuse(series, self.limits.max_rows, reservation)?;
        output::batch(&fused, reservation)
    }

    /// The label values, or with no label the label names, of the rows of
    /// the window that pass `query`'s matchers, sorted, the first
    /// `limit` of them. Rows the hot window could not give are noted in
    /// `warnings`.
    ///
    /// # Errors
    /// Invalid matchers, the byte budget, the memory pool, or a read
    /// failure.
    pub(crate) async fn labels(
        &self,
        query: &LabelQuery,
        warnings: &QueryWarnings,
        reservation: &mut MemoryReservation,
    ) -> Result<RecordBatch, QueryError> {
        let matchers: Arc<[Matcher]> =
            matchers::compile(&query.matchers, self.limits.matchers, reservation)?.into();
        let sources = self
            .sources(query.window, Some(warnings), reservation)
            .await?;
        let temporalities = [Temporality::Cumulative, Temporality::Unspecified];
        let limit = query.limit.min(self.limits.max_rows);
        let mut listed: BTreeSet<Box<str>> = BTreeSet::new();
        let mut list =
            |value: &str, reservation: &mut MemoryReservation| -> Result<(), QueryError> {
                if value.is_empty() || listed.contains(value) {
                    return Ok(());
                }
                reservation.try_grow(value.len() + VALUE_BYTES)?;
                listed.insert(value.into());
                if listed.len() > limit {
                    if let Some(last) = listed.pop_last() {
                        reservation.shrink(last.len() + VALUE_BYTES);
                    }
                }
                Ok(())
            };
        let mut visit = |row: SeriesRow, reservation: &mut MemoryReservation| {
            if !temporalities.contains(&row.temporality) || !all_match(&matchers, &row.labels) {
                return Ok(());
            }
            match &query.label {
                Some(label) => list(value_of(&row.labels, label), reservation),
                None => row
                    .labels
                    .iter()
                    .filter(|(_, v)| !v.is_empty())
                    .try_for_each(|(k, _)| list(k, reservation)),
            }
        };
        let narrow = Narrow::Matchers(Arc::clone(&matchers));
        self.count_pruned(sources.visit(&narrow, reservation, &mut visit).await?);
        let values: Vec<Box<str>> = listed.into_iter().collect();
        output::text_batch(
            if query.label.is_some() {
                "value"
            } else {
                "name"
            },
            &values,
            reservation,
        )
    }

    /// The series files and hot rows of `window`. With `warnings`, an
    /// ingester that does not answer is noted there; without, it fails
    /// the action.
    async fn sources(
        &self,
        window: Window,
        warnings: Option<&QueryWarnings>,
        reservation: &mut MemoryReservation,
    ) -> Result<Sources<'_>, QueryError> {
        let nanos = |ms: i64| ms.saturating_mul(1_000_000);
        let pruning = Pruning {
            ts_min: Some(nanos(window.first_hour)),
            ts_max: Some(nanos(window.end)),
            ..Pruning::default()
        };
        let mut hot = crate::hot::HotResult::default();
        let held = Arc::new(reservation.new_empty());
        if self.hot.touches_recent(&pruning) {
            hot = self
                .hot
                .scan_charged(
                    self.tenant,
                    (RowSignal::Metrics, Some(MetricTable::Series)),
                    &pruning,
                    (None, None),
                    &hot_charge(&held),
                )
                .await?;
            if !hot.exhausted.is_empty() {
                return Err(hot_exhausted(&hot.exhausted));
            }
            match warnings {
                _ if hot.unavailable.is_empty() => {}
                Some(warnings) => {
                    for ingester in &hot.unavailable {
                        warnings.push(format!(
                            "ingester {ingester} did not answer; recent series may be missing"
                        ));
                    }
                }
                None => return Err(QueryError::HotUnavailable(hot.unavailable)),
            }
        }
        let files: Vec<_> = self
            .cache
            .files(self.tenant, Signal::MetricSeries)
            .await?
            .iter()
            .filter(|f| {
                pruning.keeps(f)
                    && !hot.excluded_files.contains(&f.id)
                    && !hot.excluded_files.contains(&f.origin)
            })
            .cloned()
            .collect();
        let bytes = files.iter().fold(0u64, |sum, f| {
            sum.saturating_add(u64::try_from(f.size_bytes).unwrap_or(0))
        });
        if bytes > self.budget {
            return Err(QueryError::OverBudget {
                bytes,
                budget: self.budget,
            });
        }
        Ok(Sources {
            store: Arc::clone(self.store),
            files,
            hot: hot.batches,
            _hot_held: held,
            postings: self.postings,
            window,
        })
    }

    /// Step 1: the keys of the rows that pass, at most `max_series`.
    async fn candidates(
        &self,
        sources: &Sources<'_>,
        matchers: &Arc<[Matcher]>,
        temporalities: &[Temporality],
        reservation: &mut MemoryReservation,
    ) -> Result<Keys, QueryError> {
        let mut keys = Keys::new();
        let mut count = 0usize;
        let max_series = self.limits.max_series;
        let mut visit = |row: SeriesRow, reservation: &mut MemoryReservation| {
            if !temporalities.contains(&row.temporality) || !all_match(matchers, &row.labels) {
                return Ok(());
            }
            if keys
                .get(&row.name)
                .is_some_and(|f| f.contains(&row.fingerprint))
            {
                return Ok(());
            }
            count += 1;
            if count > max_series {
                return Err(QueryError::TooManySeries(max_series));
            }
            reservation.try_grow(row.name.len() + KEY_BYTES)?;
            keys.entry(row.name).or_default().insert(row.fingerprint);
            Ok(())
        };
        let narrow = Narrow::Matchers(Arc::clone(matchers));
        self.count_pruned(sources.visit(&narrow, reservation, &mut visit).await?);
        Ok(keys)
    }

    /// Step 2: every row of `keys` in the window.
    async fn expand(
        &self,
        sources: &Sources<'_>,
        keys: &Keys,
        reservation: &mut MemoryReservation,
    ) -> Result<Vec<SeriesRow>, QueryError> {
        let names: HashSet<Box<str>> = keys.keys().cloned().collect();
        reservation.try_grow(names.iter().map(|n| n.len() + KEY_BYTES).sum())?;
        let mut rows = Vec::new();
        let mut visit = |row: SeriesRow, reservation: &mut MemoryReservation| {
            if keys
                .get(&row.name)
                .is_some_and(|f| f.contains(&row.fingerprint))
            {
                reservation.try_grow(row.bytes() + std::mem::size_of::<SeriesRow>())?;
                rows.push(row);
            }
            Ok(())
        };
        let narrow = Narrow::Names(Arc::new(names));
        self.count_pruned(sources.visit(&narrow, reservation, &mut visit).await?);
        Ok(rows)
    }

    /// Step 3: one identity per key, in name and fingerprint order.
    fn resolve(&self, rows: Vec<SeriesRow>) -> Result<Vec<Resolved>, QueryError> {
        resolve(rows).inspect_err(|e| {
            if matches!(e, QueryError::Ambiguous { .. }) {
                self.stats
                    .metric_ambiguous
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        })
    }

    fn count_pruned(&self, visited: series::Visited) {
        self.stats
            .metric_index_pruned
            .fetch_add(visited.pruned, std::sync::atomic::Ordering::Relaxed);
    }
}

/// A charge of the hot window's answers to `held`, refused past the pool.
pub(crate) fn hot_charge(held: &Arc<MemoryReservation>) -> crate::hot::Charge {
    let held = Arc::clone(held);
    Arc::new(move |bytes| held.try_grow(bytes).is_ok())
}

/// What a hot window answer past the pool fails with.
pub(crate) fn hot_exhausted(ingesters: &[String]) -> QueryError {
    QueryError::DataFusion(datafusion::error::DataFusionError::ResourcesExhausted(
        format!(
            "hot window answers of {} past the memory pool",
            ingesters.join(", ")
        ),
    ))
}

/// The identity of each key of `rows`: the rows of one hour merged as
/// [`dedupe_series`] merges them, each hour then holding one identity and
/// every hour the same, a legacy level agreeing with any.
fn resolve(mut rows: Vec<SeriesRow>) -> Result<Vec<Resolved>, QueryError> {
    dedupe_series(&mut rows);
    let mut out = Vec::new();
    let mut rows = rows.into_iter().peekable();
    while let Some(mut first) = rows.next() {
        while let Some(next) =
            rows.next_if(|n| n.name == first.name && n.fingerprint == first.fingerprint)
        {
            if next.unix_milli == first.unix_milli || !first.same_identity(&next) {
                return Err(QueryError::Ambiguous {
                    name: first.name.into(),
                    fingerprint: first.fingerprint,
                });
            }
            for (mine, theirs) in first.label_meta.iter_mut().zip(next.label_meta.iter()) {
                *mine = mine.merge(*theirs).unwrap_or(*mine);
            }
            first.unix_milli = next.unix_milli;
        }
        out.push(Resolved {
            name: first.name,
            fingerprint: first.fingerprint,
            temporality: first.temporality,
            labels: first.labels,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests;

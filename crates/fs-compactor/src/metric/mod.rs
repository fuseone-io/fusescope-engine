//! Compaction of metric file sets.
//!
//! A pass takes whole sets, samples and series together: the L0 sets of a
//! cold hour into L1, the L1 sets of a closed day into L2, or, with
//! `compactor.compact_metrics` off, the L0 sets of a cold hour not
//! accounted yet, read and left in place. Their files are merged in step
//! by `(metric_name, fingerprint, hour)` ([`merge`]) into sets of the next
//! level ([`writer`]), each closing when a file nears its share of the
//! pass's budget or its target size, so the samples of one key may span
//! sets, each with the key's series row. A pass of L0 sets not accounted
//! counts their usage and metadata and publishes them as contributions in
//! the same fenced transaction that publishes its sets, or marks the sets
//! accounted. Everything it holds is admitted against
//! `compactor.metric_pass_bytes` before it is allocated ([`budget`]).
//!
//! Objects go up create-once, samples, series, postings, then the
//! contributions; a pass that does not commit leaves them to the orphan
//! sweep and its inputs visible.

pub(crate) mod budget;
mod merge;
mod writer;

use std::sync::atomic::Ordering;

use chrono::{DateTime, TimeZone, Utc};
use fs_catalog::{
    FileId, FileSetId, LeaseFence, Level, MetricInputs, MetricPass, MetricSet, NewFile, Signal,
    TenantId, TimeRange,
};
use fs_schema::metric_metadata::MetadataCounts;
use fs_schema::metrics::{
    SeriesRow, check_samples_schema, samples_schema_v1, series_schema_v2, series_schema_version,
};
use fs_schema::usage::{UsageCounts, UsageSignal, sample_bytes};
use object_store::PutPayload;
use tracing::{info, warn};

use self::budget::{Budget, Part};
use self::merge::{Key, Merge};
use self::writer::{Closed, SetWriter};
use crate::compact::Compactor;
use crate::merge::partitioned_path;
use crate::objects::{OrphanGuard, take_injected};
use crate::{CompactError, read};

pub use budget::{DEFAULT_METRIC_PASS_BYTES, MAX_METRIC_PASS_BYTES, MIN_METRIC_PASS_BYTES};

/// What a metric pass did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricPassed {
    /// Owner.
    pub tenant: TenantId,
    /// The hour, or the day, it took.
    pub start: DateTime<Utc>,
    /// Sets it took.
    pub inputs: usize,
    /// Sets it published; none for an accounting pass.
    pub outputs: Vec<FileSetId>,
    /// Contributions it published, metadata and usage.
    pub derived: usize,
}

/// A set opened: its samples file, and its series file with its version.
type Opened = (MetricSet, read::Input, (read::Input, u32));

/// The service a series row names, by its `service.name` label.
fn service_of(row: &SeriesRow) -> Option<&str> {
    row.labels
        .binary_search_by(|(k, _)| k.as_ref().cmp("service.name"))
        .ok()
        .and_then(|at| row.labels.get(at))
        .map(|(_, v)| v.as_ref())
}

impl Compactor {
    /// One L0 to L1 pass over the oldest cold hour of metric sets, with
    /// `compactor.compact_metrics` on; `None` when there is none.
    ///
    /// # Errors
    /// Catalog, store or Parquet failure, a set in part, a swap refused for
    /// a conflict or a lost lease.
    pub async fn compact_metrics_once(
        &self,
        fence: &LeaseFence,
    ) -> Result<Option<MetricPassed>, CompactError> {
        if !self.config.compact_metrics {
            return Ok(None);
        }
        self.metric_once(MetricPass::Compact, fence).await
    }

    /// One L1 to L2 pass over the oldest closed day of metric sets, with
    /// `compactor.compact_metrics` and `compactor.compact_l2` on.
    ///
    /// # Errors
    /// As [`Compactor::compact_metrics_once`].
    pub async fn merge_metrics_once(
        &self,
        fence: &LeaseFence,
    ) -> Result<Option<MetricPassed>, CompactError> {
        if !(self.config.compact_metrics && self.config.compact_l2) {
            return Ok(None);
        }
        self.metric_once(MetricPass::Merge, fence).await
    }

    /// One accounting pass over the oldest cold hour of metric sets not
    /// accounted, with `compactor.compact_metrics` off: their usage and
    /// metadata published and the sets marked accounted, left in place.
    ///
    /// # Errors
    /// As [`Compactor::compact_metrics_once`].
    pub async fn account_metrics_once(
        &self,
        fence: &LeaseFence,
    ) -> Result<Option<MetricPassed>, CompactError> {
        if self.config.compact_metrics {
            return Ok(None);
        }
        self.metric_once(MetricPass::Account, fence).await
    }

    async fn metric_once(
        &self,
        pass: MetricPass,
        fence: &LeaseFence,
    ) -> Result<Option<MetricPassed>, CompactError> {
        let mut skip = 0;
        loop {
            let Some(candidate) = self
                .catalog
                .metric_inputs(pass, self.config.hour_close_delay, skip)
                .await?
            else {
                return Ok(None);
            };
            if candidate.sets.is_empty() {
                skip += 1;
                continue;
            }
            let mut budget = Budget::new(self.config.metric_pass_bytes);
            let Some(inputs) = self.open_sets(&candidate, &mut budget).await? else {
                self.stats
                    .metric_over_budget
                    .fetch_add(1, Ordering::Relaxed);
                warn!(
                    role = "compactor",
                    tenant = %candidate.tenant_id.0,
                    start = %candidate.start,
                    budget = budget.cap(),
                    "metric sets passed over: the oldest cannot be read within the pass's budget"
                );
                skip += 1;
                continue;
            };
            return self
                .metric_pass(pass, &candidate, inputs, budget, fence)
                .await
                .map(Some);
        }
    }

    /// The oldest sets of `candidate` that fit the pass's limits and its
    /// share of inputs, opened, one at least; `None` when the oldest does
    /// not fit.
    async fn open_sets(
        &self,
        candidate: &MetricInputs,
        budget: &mut Budget,
    ) -> Result<Option<Vec<Opened>>, CompactError> {
        let share = budget.share(Part::Inputs);
        let mut opened = Vec::new();
        let (mut metadata, mut floors, mut bytes) = (0usize, 0usize, 0u64);
        for set in &candidate.sets {
            if opened.len() >= self.config.max_inputs_per_pass.max(1) {
                break;
            }
            let size =
                u64::try_from(set.samples.size_bytes + set.series.size_bytes).unwrap_or(u64::MAX);
            if !opened.is_empty() && bytes.saturating_add(size) > self.config.max_input_bytes {
                break;
            }
            let (samples_schema, series_schema) = (samples_schema_v1(), series_schema_v2());
            let (samples, samples_layout) = read::Input::bounded(
                std::sync::Arc::clone(&self.store),
                (&set.samples.path, set.samples.size_bytes),
                &samples_schema,
                &[],
            )
            .await?;
            let (series, series_layout) = read::Input::bounded(
                std::sync::Arc::clone(&self.store),
                (&set.series.path, set.series.size_bytes),
                &series_schema,
                &[],
            )
            .await?;
            // A file of a version this engine does not read fails the pass
            // before any chunk is planned or anything written.
            let footer = |input: &read::Input| {
                input
                    .file_schema()
                    .ok_or(CompactError::Merge("a metric file read unbounded"))
            };
            check_samples_schema(footer(&samples)?.as_ref())?;
            let version = series_schema_version(footer(&series)?.as_ref())?;
            let more = samples_layout.metadata_bytes + series_layout.metadata_bytes;
            let floor = 2 * (samples_layout.floor + series_layout.floor);
            if metadata + floors + more + floor > share / 3 {
                break;
            }
            metadata += more;
            floors += floor;
            bytes = bytes.saturating_add(size);
            opened.push((
                set.clone(),
                (samples, samples_layout),
                (series, series_layout, version),
            ));
        }
        if opened.is_empty() {
            return Ok(None);
        }
        // A third of the share is the files' metadata and their costliest
        // rows, admitted above; a third their chunks, each read as two at
        // most, so no file's chunk passes what is held for it; a third the
        // rows the inputs decode and the key's, admitted as they are.
        let files = 2 * opened.len();
        let chunk = (share / 3) / (2 * files);
        let held: usize = opened
            .iter()
            .map(|(_, (_, sl), (_, rl, _))| 2 * (chunk.max(sl.floor) + chunk.max(rl.floor)))
            .sum();
        budget.take(Part::Inputs, metadata + held)?;
        let rows = self.config.row_group_rows.max(1);
        Ok(Some(
            opened
                .into_iter()
                .map(|(set, (samples, sl), (series, rl, version))| {
                    let samples = samples.with_chunks(sl.plan(chunk.max(sl.floor), rows));
                    let series = series.with_chunks(rl.plan(chunk.max(rl.floor), rows));
                    (set, samples, (series, version))
                })
                .collect(),
        ))
    }

    async fn metric_pass(
        &self,
        pass: MetricPass,
        candidate: &MetricInputs,
        inputs: Vec<Opened>,
        mut budget: Budget,
        fence: &LeaseFence,
    ) -> Result<MetricPassed, CompactError> {
        let tenant = candidate.tenant_id;
        let sets: Vec<FileSetId> = inputs.iter().map(|(s, ..)| s.set).collect();
        let counts = pass != MetricPass::Merge;
        let mut merge = Merge::open(
            inputs
                .into_iter()
                .map(|(set, samples, series)| (samples, series, !set.samples.usage_accounted))
                .collect(),
            &mut budget,
        )
        .await?;
        let mut run = Run {
            compactor: self,
            tenant,
            start: candidate.start,
            level: if pass == MetricPass::Merge {
                Level::L2
            } else {
                Level::L1
            },
            writes: pass != MetricPass::Account,
            puts: 0,
            closed: Vec::new(),
            derived: Vec::new(),
            metadata: MetadataCounts::default(),
            usage: UsageCounts::new(UsageSignal::Samples),
            guard: OrphanGuard {
                stats: std::sync::Arc::clone(&self.stats),
                tenant,
                work: format!("metric {pass:?} pass of {}", candidate.start),
                created: 0,
                armed: true,
            },
        };
        let mut writer = run.new_writer(&budget)?;
        while let Some(key) = merge.next_key()? {
            let (series, collided) = merge.take_series(&key, &mut budget).await?;
            merge.start_group();
            run.group(
                &mut merge,
                &mut writer,
                (&key, &series, collided),
                &mut budget,
            )
            .await?;
            if counts && merge.uncounted > 0 {
                run.count(
                    &key,
                    &series,
                    (merge.uncounted, merge.uncounted_times),
                    &mut budget,
                )
                .await?;
            }
            if let Some(w) = writer.as_mut() {
                w.end_group(&mut budget);
            }
            drop(series);
            merge.end_group(&mut budget);
        }
        if let Some(w) = writer.take().filter(|w| !w.is_empty()) {
            run.close(w, &mut budget).await?;
        }
        if counts {
            run.publish_metadata(&mut budget).await?;
            run.publish_usage().await?;
        }
        self.stats
            .metric_peak_bytes
            .fetch_max(budget.peak() as u64, Ordering::Relaxed);
        run.commit(pass, &sets, fence).await
    }
}

/// The state of one pass while it runs.
struct Run<'a> {
    compactor: &'a Compactor,
    tenant: TenantId,
    start: DateTime<Utc>,
    level: Level,
    writes: bool,
    puts: usize,
    closed: Vec<Closed>,
    derived: Vec<NewFile>,
    metadata: MetadataCounts,
    usage: UsageCounts,
    guard: OrphanGuard<crate::compact::CompactorStats>,
}

impl Run<'_> {
    fn new_writer(&self, budget: &Budget) -> Result<Option<SetWriter>, CompactError> {
        if !self.writes {
            return Ok(None);
        }
        let config = &self.compactor.config;
        SetWriter::new(
            &self.compactor.root,
            (self.tenant, self.level, self.start),
            config.row_group_rows,
            budget,
        )
        .map(Some)
    }

    /// Whether `writer` must close before more rows: a file near its share
    /// or past its target size.
    fn full(&self, writer: &SetWriter, budget: &Budget) -> bool {
        let target = if self.level == Level::L2 {
            self.compactor.config.l2_target_bytes
        } else {
            self.compactor.config.l1_target_bytes
        };
        !writer.has_room(budget) || writer.bytes_written() >= target
    }

    /// Writes one group: its series rows, then its samples, closing the set
    /// when it fills and writing the series rows again into the next.
    async fn group(
        &mut self,
        merge: &mut Merge,
        writer: &mut Option<SetWriter>,
        (key, series, collided): (&Key, &[SeriesRow], bool),
        budget: &mut Budget,
    ) -> Result<(), CompactError> {
        if let Some(w) = writer.as_mut() {
            if !w.is_empty() && self.full(w, budget) {
                let full = writer.take().ok_or(CompactError::Merge("a writer lost"))?;
                self.close(full, budget).await?;
                *writer = self.new_writer(budget)?;
            }
        }
        if let Some(w) = writer.as_mut() {
            w.add_series(series, collided, budget)?;
        }
        while let Some(sample) = merge.next_sample(key).await? {
            let Some(w) = writer.as_mut() else {
                continue;
            };
            if !w.is_empty() && self.full(w, budget) {
                let full = writer.take().ok_or(CompactError::Merge("a writer lost"))?;
                self.close(full, budget).await?;
                let mut next = self
                    .new_writer(budget)?
                    .ok_or(CompactError::Merge("no writer"))?;
                next.add_series(series, collided, budget)?;
                *writer = Some(next);
            }
            if let Some(w) = writer.as_mut() {
                w.add_sample(key, &sample, budget)?;
            }
        }
        Ok(())
    }

    /// Counts a group's samples of inputs not accounted: their usage, by
    /// the service of the group's series, and the metadata of its series
    /// over their times.
    async fn count(
        &mut self,
        key: &Key,
        series: &[SeriesRow],
        (rows, times): (u64, (i64, i64)),
        budget: &mut Budget,
    ) -> Result<(), CompactError> {
        let metadata_budget = budget.share(Part::Metadata) / 3;
        for row in series {
            let mut from = 0;
            while let Some(next) = self
                .metadata
                .add_series(row, times, from, metadata_budget)?
            {
                self.publish_metadata(budget).await?;
                from = next;
            }
        }
        budget.set(Part::Metadata, self.metadata.estimated_bytes());
        let service = series.first().and_then(service_of);
        let usage_budget = self
            .compactor
            .config
            .usage_max_bytes
            .min(budget.share(Part::Usage) / 3);
        let bytes = rows.saturating_mul(sample_bytes(&key.name));
        let hour = key.hour.saturating_mul(1_000_000);
        if let Some(full) = self
            .usage
            .add_samples((hour, service), rows, bytes, usage_budget)?
        {
            self.publish_usage_counts(&full).await?;
        }
        budget.set(Part::Usage, self.usage.estimated_bytes());
        Ok(())
    }

    /// Uploads a set: samples, series, then postings.
    async fn close(&mut self, writer: SetWriter, budget: &mut Budget) -> Result<(), CompactError> {
        let collisions = writer.collisions;
        let (closed, files, postings) = writer.finish(budget)?;
        for (path, bytes) in files {
            self.put(path.as_ref(), bytes).await?;
        }
        match postings {
            Some((path, bytes)) => self.put(&path, PutPayload::from(bytes)).await?,
            None => {
                self.compactor
                    .stats
                    .postings_abandoned
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        budget.set(Part::Postings, 0);
        self.compactor
            .stats
            .metric_collisions
            .fetch_add(collisions, Ordering::Relaxed);
        self.closed.push(closed);
        Ok(())
    }

    /// Uploads the metadata counted so far as one contribution.
    async fn publish_metadata(&mut self, budget: &mut Budget) -> Result<(), CompactError> {
        let counts = self.metadata.take();
        budget.set(Part::Metadata, 0);
        if counts.is_empty() {
            return Ok(());
        }
        let (first, last) = counts.times().unwrap_or((0, 0));
        let bytes = fs_schema::metric_metadata::encode(&counts)?;
        let id = FileId::new();
        let path = partitioned_path(
            &self.compactor.root,
            self.tenant,
            Signal::MetricMetadata.path_name(),
            self.start,
            id,
        );
        let at = |ms: i64| Utc.timestamp_millis_opt(ms).single().unwrap_or(self.start);
        self.derived.push(NewFile {
            id,
            origin: id,
            tenant_id: self.tenant,
            signal: Signal::MetricMetadata,
            level: Level::L1,
            path: path.to_string(),
            size_bytes: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
            row_count: i64::try_from(counts.len()).unwrap_or(i64::MAX),
            range: TimeRange {
                start: at(first),
                end: at(last),
            },
            services: Vec::new(),
            summary: None,
            content_sha256: Some(crate::objects::sha256(&bytes)),
            usage_accounted: true,
        });
        self.put(path.as_ref(), PutPayload::from(bytes)).await
    }

    async fn publish_usage(&mut self) -> Result<(), CompactError> {
        let counts = std::mem::replace(&mut self.usage, UsageCounts::new(UsageSignal::Samples));
        self.publish_usage_counts(&counts).await
    }

    async fn publish_usage_counts(&mut self, counts: &UsageCounts) -> Result<(), CompactError> {
        for (file, bytes) in crate::usage::usage_files(&self.compactor.root, self.tenant, counts)? {
            self.put(&file.path, PutPayload::from(bytes)).await?;
            self.derived.push(file);
        }
        Ok(())
    }

    async fn put(&mut self, path: &str, bytes: PutPayload) -> Result<(), CompactError> {
        let result = self.compactor.put_once(path, bytes, &mut self.puts).await;
        self.guard.created += 1;
        result
    }

    /// Publishes what the pass made under `fence`: its sets swapped for
    /// `sets` with the contributions, or, accounting, the contributions
    /// with `sets` marked accounted.
    async fn commit(
        mut self,
        pass: MetricPass,
        sets: &[FileSetId],
        fence: &LeaseFence,
    ) -> Result<MetricPassed, CompactError> {
        let compactor = self.compactor;
        if take_injected(&compactor.inject_swap_failures) {
            return Err(CompactError::Injected("swap"));
        }
        let outputs: Vec<(FileSetId, [NewFile; 2])> = self
            .closed
            .iter()
            .map(|c| (c.set, c.files.clone()))
            .collect();
        if pass == MetricPass::Account {
            compactor
                .catalog
                .account_file_sets_fenced(self.tenant, sets, &self.derived, fence)
                .await?;
            compactor
                .stats
                .metric_accounted
                .fetch_add(1, Ordering::Relaxed);
        } else {
            compactor
                .catalog
                .swap_file_sets_fenced(self.tenant, sets, &outputs, &self.derived, fence)
                .await?;
            let counter = if pass == MetricPass::Merge {
                &compactor.stats.metric_l2_passes
            } else {
                &compactor.stats.metric_passes
            };
            counter.fetch_add(1, Ordering::Relaxed);
        }
        self.guard.armed = false;
        info!(
            role = "compactor",
            tenant = %self.tenant.0,
            start = %self.start,
            pass = ?pass,
            inputs = sets.len(),
            outputs = outputs.len(),
            derived = self.derived.len(),
            bytes = self.closed.iter().map(|c| c.bytes).sum::<u64>(),
            "metric pass"
        );
        Ok(MetricPassed {
            tenant: self.tenant,
            start: self.start,
            inputs: sets.len(),
            outputs: outputs.iter().map(|(set, _)| *set).collect(),
            derived: self.derived.len(),
        })
    }
}

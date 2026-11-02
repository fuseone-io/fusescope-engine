//! The metric paths of the ingester: a request is counted before it is
//! built, and a flush writes its samples and series as one set.
//!
//! A request's preflight bounds what its conversion holds, which is
//! admitted, with the merge of its series into the buffer's, before any
//! row is built. The conversion runs before the WAL append, so its source
//! orders take the record's position once it is logged, under the gate,
//! before the push. A flush encodes and uploads the samples file, drops
//! its bytes, encodes and uploads the series file, and registers both in
//! one transaction; a doomed or differing object gives the pair two fresh
//! ids and a fresh set under a new intent.

use std::sync::atomic::Ordering;
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use chrono::{DateTime, TimeZone, Utc};
use fs_catalog::{CatalogError, FileId, FileSetId, MetricRole, NewFile, TenantId};
use fs_schema::limits::Limits;
use fs_schema::metrics::{
    MetricLimits, Rejections, Source, check_points, convert_mapping, preflight, walk_bound,
    with_source,
};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use tracing::{info, warn};

use crate::IngestError;
use crate::buffer::{HotBatch, MetricPair, Scratch, Stream};
use crate::flush::{self, Prepared};
use crate::ingester::{
    Converted, Shared, StageTiming, metric_source, nanos, take_injected, too_large,
};
use crate::metric_flush::{metric_path, prepare_samples, prepare_series};
use crate::metric_record::MetricConversion;
use crate::record::{FileLayout, FlushIntent, MetricFile, MetricIntent, RowSignal};

/// What a metric flush wrote: the samples file, the samples and series as
/// written, and the writer's peak.
type Written = (NewFile, Vec<HotBatch>, Vec<HotBatch>, usize);

/// The source of a request converted before its WAL position is known.
const UNKNOWN_SOURCE: Source = Source {
    instance: [0; 16],
    segment: 0,
    offset: 0,
};

impl Shared {
    /// Holds the traversals' scratch, bounded from `message`'s sizes,
    /// before the preflight walks it; kept until conversion ends.
    async fn hold_walk(
        &self,
        message: &ExportMetricsServiceRequest,
        (limits, metrics): (&Limits, &MetricLimits),
    ) -> Result<Scratch, IngestError> {
        let bound = walk_bound(message, limits, metrics);
        self.buffers
            .lock()
            .await
            .try_hold_scratch(bound, self.config.max_memory_bytes)
            .ok_or(IngestError::Capacity(
                "ingester memory full, hot-window retention pending",
            ))
    }

    /// Holds as scratch under the cap what converting `checked` holds
    /// only while it runs, beyond the walk's own, and the merge of its
    /// series into the buffer's; the rows that stay are admitted apart,
    /// with the margin of their flush.
    async fn hold_conversion(
        &self,
        stream: Stream,
        checked: &fs_schema::metrics::Preflight,
    ) -> Result<(Scratch, crate::buffer::SeriesMerge), IngestError> {
        let buffers = self.buffers.lock().await;
        let full = || IngestError::Capacity("ingester memory full, hot-window retention pending");
        let held = buffers
            .try_hold_scratch(
                checked.transient_bound().saturating_sub(checked.scratch),
                self.config.max_memory_bytes,
            )
            .ok_or_else(full)?;
        let merge = buffers
            .hold_series_merge(stream, checked.series, self.config.max_memory_bytes)
            .ok_or_else(full)?;
        Ok((held, merge))
    }

    /// Counts `message`, admits what converting it holds, converts it under
    /// `conversion`, the one `record` carries, logs `record` and adds the
    /// samples and series to the tenant's metric buffer. A request with no
    /// sample is answered without the log.
    ///
    /// # Errors
    /// A request over a request-wide bound or spanning too many hours, a
    /// cap it would cross, or a WAL or Arrow failure.
    pub(crate) async fn ingest_metrics(
        &self,
        tenant: TenantId,
        message: &ExportMetricsServiceRequest,
        conversion: MetricConversion,
        record: Vec<u8>,
        now: Instant,
    ) -> Result<Converted, IngestError> {
        let slot = self
            .conversions
            .acquire()
            .await
            .map_err(|_| IngestError::Backpressure("conversions closed"))?;
        let (limits, metrics) = conversion.apply(&self.config.limits, &self.config.metrics);
        let (limits, metrics) = (&limits, &metrics);
        check_points(message, metrics).map_err(|e| too_large(e, metrics))?;
        let walk = self.hold_walk(message, (limits, metrics)).await?;
        let checked = preflight(message, limits, metrics).map_err(|e| too_large(e, metrics))?;
        let rejected = checked.rejected;
        let accepted = checked
            .points
            .saturating_sub(usize::try_from(rejected.total()).unwrap_or(usize::MAX));
        if checked.samples == 0 {
            self.count_metrics(accepted, 0, &rejected);
            self.stats.requests.fetch_add(1, Ordering::Relaxed);
            return Ok(Converted {
                accepted,
                rejected: usize::try_from(rejected.total()).unwrap_or(usize::MAX),
                detail: Some(rejected.summary()),
            });
        }
        if checked.hours_over || checked.hours.len() > self.config.max_hours_per_batch {
            return Err(IngestError::TooManyHours {
                hours: checked.hours.len(),
                limit: self.config.max_hours_per_batch,
            });
        }
        let hours: Vec<DateTime<Utc>> = checked
            .hours
            .iter()
            .filter_map(|ms| Utc.timestamp_millis_opt(*ms).single())
            .collect();
        let stream = Stream::metrics(tenant);
        let (transient, incoming) = self.hold_conversion(stream, &checked).await?;
        let reservation = self
            .admit(stream, checked.rows_bound(), checked.samples, &hours)
            .await?;
        let rows = convert_mapping(
            conversion.mapping,
            message,
            limits,
            metrics,
            &checked,
            UNKNOWN_SOURCE,
        )?;
        drop(walk);
        drop(slot);
        let samples_rows = rows.samples.num_rows();
        let _shared = self.gate.read().await;
        let tracked = self.append_wal(tenant, record).await?;
        let samples = with_source(
            &rows.samples,
            metric_source(self.instance, tracked.position),
        )?;
        drop(rows.samples);
        let (due, coalesce, collided) = {
            let mut buffers = self.buffers.lock().await;
            let was_due = self.flush_is_due(buffers.get_mut(stream));
            let collided = buffers.get_mut(stream).push_metric(
                samples,
                rows.series,
                hours,
                tracked.position,
                now,
            );
            drop(reservation);
            drop(transient);
            drop(incoming);
            drop(tracked);
            let live = buffers.get_mut(stream);
            (
                self.flush_is_due(live) && !was_due,
                live.request_batches() == self.config.coalesce_batches,
                rows.collisions + collided,
            )
        };
        if due {
            self.flush_now.notify_one();
        }
        if coalesce {
            self.coalesce_now.notify_one();
        }
        self.count_metrics(accepted, samples_rows, &rejected);
        self.stats
            .metric_series_collisions
            .fetch_add(collided as u64, Ordering::Relaxed);
        self.stats.requests.fetch_add(1, Ordering::Relaxed);
        Ok(Converted {
            accepted,
            rejected: usize::try_from(rejected.total()).unwrap_or(usize::MAX),
            detail: Some(rejected.summary()),
        })
    }

    fn count_metrics(&self, points: usize, samples: usize, rejected: &Rejections) {
        self.stats
            .metric_points
            .fetch_add(points as u64, Ordering::Relaxed);
        self.stats
            .metric_samples
            .fetch_add(samples as u64, Ordering::Relaxed);
        for (counter, reason) in self
            .stats
            .metric_points_rejected
            .iter()
            .zip(fs_schema::metrics::Rejection::ALL)
        {
            counter.fetch_add(rejected.of(reason), Ordering::Relaxed);
        }
    }

    /// Writes and registers one metric flush, as `materialize` does other
    /// rows: on failure the rows stay bound and are retried, under fresh ids
    /// when an object was doomed or differed.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn materialize_metric(
        &self,
        stream: Stream,
        file: FileId,
        origin: FileId,
        batches: Vec<RecordBatch>,
        held: Scratch,
        layout: Option<FileLayout>,
        (pair, series, hour): (MetricPair, RecordBatch, DateTime<Utc>),
    ) -> Result<(), IngestError> {
        let fs = &self.flush_stats;
        fs.attempts.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let mut timing = StageTiming::default();
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let result = self
            .write_metric_set(
                stream,
                (file, origin, hour),
                pair,
                batches,
                series,
                layout,
                &mut timing,
            )
            .await;
        match result {
            Ok((samples, written, series_written, writer_peak)) => {
                self.stats.files_flushed.fetch_add(2, Ordering::Relaxed);
                info!(
                    tenant = %stream.tenant.0,
                    path = %samples.path,
                    rows = samples.row_count,
                    bytes = samples.size_bytes,
                    "flushed metric l0"
                );
                {
                    let mut buffers = self.buffers.lock().await;
                    buffers.finish_metric_flush(
                        stream,
                        file,
                        written,
                        series_written,
                        Instant::now(),
                    );
                    self.publish_unconfirmed(&buffers);
                    drop(held);
                }
                self.confirm_now.notify_one();
                if self.config.checkpoint_after_flush {
                    self.checkpoint_wal().await;
                }
                fs.encode_nanos.fetch_add(timing.encode, Ordering::Relaxed);
                fs.upload_nanos.fetch_add(timing.upload, Ordering::Relaxed);
                fs.register_nanos
                    .fetch_add(timing.register, Ordering::Relaxed);
                fs.flushes.fetch_add(1, Ordering::Relaxed);
                let peak = u64::try_from(writer_peak).unwrap_or(u64::MAX);
                fs.writer_peak_bytes.fetch_max(peak, Ordering::Relaxed);
                fs.rows.fetch_add(
                    u64::try_from(samples.row_count).unwrap_or(0),
                    Ordering::Relaxed,
                );
                fs.total_nanos
                    .fetch_add(nanos(started.elapsed()), Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.stats.flush_failures.fetch_add(1, Ordering::Relaxed);
                warn!(tenant = %stream.tenant.0, file = %file.0, error = %e, rows, "metric flush failed; will retry");
                let file = match &e {
                    IngestError::Catalog(CatalogError::ObjectDoomed { .. }) => {
                        self.supersede_metric(stream, file).await
                    }
                    IngestError::ObjectMismatch { .. } => {
                        self.flush_stats
                            .replay_mismatch
                            .fetch_add(1, Ordering::Relaxed);
                        self.supersede_metric(stream, file).await
                    }
                    _ => file,
                };
                {
                    let mut buffers = self.buffers.lock().await;
                    buffers.flush_failed(stream, file, Instant::now());
                    drop(held);
                }
                Err(e)
            }
        }
    }

    /// Encodes and uploads the samples file, drops its bytes, encodes and
    /// uploads the series file, and registers both as `pair.set`.
    #[allow(clippy::too_many_arguments)]
    async fn write_metric_set(
        &self,
        stream: Stream,
        (file, origin, hour): (FileId, FileId, DateTime<Utc>),
        pair: MetricPair,
        batches: Vec<RecordBatch>,
        series: RecordBatch,
        layout: Option<FileLayout>,
        timing: &mut StageTiming,
    ) -> Result<Written, IngestError> {
        let tenant = stream.tenant;
        let layout =
            layout.ok_or_else(|| IngestError::Encode("a metric intent without a layout".into()))?;
        flush::check_layout(&layout, RowSignal::Metrics)?;
        flush::check_layout(&pair.series_layout, RowSignal::Metrics)?;
        for (role, id) in [
            (MetricRole::Samples, file),
            (MetricRole::Series, pair.series),
        ] {
            let path = metric_path(&self.root, tenant, role, hour, id);
            if let Some(path) = self.catalog.object_refused(path.as_ref()).await? {
                return Err(IngestError::Catalog(CatalogError::ObjectDoomed { path }));
            }
        }
        let root = self.root.clone();
        let encode_started = Instant::now();
        let samples = tokio::task::spawn_blocking(move || {
            prepare_samples(tenant, &root, file, hour, &batches, layout)
        })
        .await??;
        timing.encode = nanos(encode_started.elapsed());
        let upload_started = Instant::now();
        self.put_verified(&samples).await?;
        let Prepared {
            file: mut samples_file,
            bytes,
            rows,
            writer_peak,
            ..
        } = samples;
        samples_file.origin = origin;
        drop(bytes);
        let root = self.root.clone();
        let series_layout = pair.series_layout;
        let series_id = pair.series;
        let prepared = tokio::task::spawn_blocking(move || {
            prepare_series(tenant, &root, series_id, hour, &series, series_layout)
        })
        .await??;
        self.put_verified(&prepared).await?;
        timing.upload = nanos(upload_started.elapsed());
        let Prepared {
            file: mut series_file,
            bytes,
            rows: series_rows,
            ..
        } = prepared;
        series_file.origin = pair.series_origin;
        drop(bytes);
        let register_started = Instant::now();
        self.register_set(pair.set, [samples_file.clone(), series_file])
            .await?;
        timing.register = nanos(register_started.elapsed());
        Ok((samples_file, rows, series_rows, writer_peak))
    }

    /// Uploads `prepared` under the create-once rule; an object already
    /// there must be these bytes, its size and digest alike.
    async fn put_verified(&self, prepared: &Prepared) -> Result<(), IngestError> {
        match flush::upload(self.store.as_ref(), prepared).await? {
            flush::Upload::Created => Ok(()),
            flush::Upload::Existing { size } => {
                let path = &prepared.file.path;
                if u64::try_from(prepared.bytes.len()).ok() != Some(size) {
                    return Err(IngestError::ObjectMismatch { path: path.clone() });
                }
                let digest = prepared.file.content_sha256.unwrap_or([0; 32]);
                flush::verify_existing(self.store.as_ref(), path, digest, false).await?;
                Ok(())
            }
        }
    }

    /// Registers the set, with the test-only failure injection in front.
    async fn register_set(&self, set: FileSetId, files: [NewFile; 2]) -> Result<(), IngestError> {
        if take_injected(&self.inject_register_failures) {
            if let Some(delay) = self.config.inject_register_delay {
                tokio::time::sleep(delay).await;
            }
            return Err(IngestError::Injected("catalog registration"));
        }
        self.catalog.register_file_set(set, &files).await?;
        Ok(())
    }

    /// Gives a metric flush two fresh ids and a fresh set under a new
    /// intent for the same records, logged before the rows take them; a
    /// log failure keeps the old ones. The origins stay those of the first
    /// intent. Returns the samples id the rows have afterwards.
    pub(crate) async fn supersede_metric(&self, stream: Stream, old: FileId) -> FileId {
        let tenant = stream.tenant;
        let bound = self.buffers.lock().await.flushing_range(stream, old);
        let Some((bound, pair)) = bound.and_then(|b| b.metric.map(|p| (b, p))) else {
            return old;
        };
        let (Some(layout), Some(hour)) = (bound.layout, bound.hour) else {
            return old;
        };
        let new = FileId::new();
        let fresh = MetricPair {
            set: FileSetId::new(),
            series: FileId::new(),
            ..pair
        };
        let intent = metric_intent(tenant, (new, layout), bound.range, hour, fresh);
        let logged = if take_injected(&self.inject_intent_failures) {
            Err(IngestError::Injected("superseding intent"))
        } else {
            self.append_wal(tenant, intent.to_record().encode())
                .await
                .map(drop)
        };
        if let Err(e) = logged {
            warn!(tenant = %tenant.0, file = %old.0, error = %e, "superseding metric intent could not be logged; keeping the ids");
            return old;
        }
        if self
            .buffers
            .lock()
            .await
            .rename_metric(stream, old, new, fresh)
        {
            self.flush_stats.superseded.fetch_add(1, Ordering::Relaxed);
            info!(tenant = %tenant.0, old = %old.0, new = %new.0, "metric flush takes fresh ids");
            new
        } else {
            old
        }
    }
}

/// The intent of a metric flush of `hour` over `range`: the samples file
/// and its layout, and the series file and set of `pair`.
pub(crate) fn metric_intent(
    tenant: TenantId,
    (file, layout): (FileId, FileLayout),
    range: (fs_wal::Position, fs_wal::Position),
    hour: DateTime<Utc>,
    pair: MetricPair,
) -> FlushIntent {
    FlushIntent {
        tenant,
        file,
        first: range.0,
        end: range.1,
        layout: Some(layout),
        hour: Some(hour.timestamp()),
        signal: RowSignal::Metrics,
        metric: Some(MetricIntent {
            set: pair.set,
            files: vec![
                MetricFile {
                    role: MetricRole::Samples,
                    file,
                    layout,
                },
                MetricFile {
                    role: MetricRole::Series,
                    file: pair.series,
                    layout: pair.series_layout,
                },
            ],
        }),
    }
}

//! In-memory buffers of converted batches, one per tenant.
//!
//! Rows move through three states and are visible to the hot window in all of
//! them: `live` (not yet flushed), `flushing` (being written under a known file
//! id) and `persisted` (registered, kept briefly so queriers whose file list
//! predates the file still see the rows).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use fs_catalog::{FileId, TenantId};
use fs_wal::Position;

/// Pending rows of one tenant.
#[derive(Debug, Default)]
pub struct TenantBuffer {
    /// Batches in arrival order.
    pub batches: Vec<RecordBatch>,
    /// Rows across `batches`.
    pub rows: usize,
    /// Arrow memory across `batches`.
    pub bytes: usize,
    /// WAL position of the first record in this buffer.
    pub first_wal: Option<Position>,
    /// When the first record arrived.
    pub oldest: Option<Instant>,
}

impl TenantBuffer {
    /// Adds a batch that was logged at `wal`.
    pub fn push(&mut self, batch: RecordBatch, wal: Position, now: Instant) {
        if batch.num_rows() == 0 {
            return;
        }
        self.rows += batch.num_rows();
        self.bytes += batch.get_array_memory_size();
        self.first_wal.get_or_insert(wal);
        self.oldest.get_or_insert(now);
        self.batches.push(batch);
    }

    /// True when nothing is pending.
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Puts batches back in front after a failed flush.
    pub fn restore(&mut self, mut earlier: TenantBuffer) {
        if earlier.is_empty() {
            return;
        }
        earlier.batches.append(&mut self.batches);
        self.batches = earlier.batches;
        self.rows += earlier.rows;
        self.bytes += earlier.bytes;
        self.first_wal = earlier.first_wal.or(self.first_wal);
        self.oldest = earlier.oldest.or(self.oldest);
    }
}

/// Rows bound to a file by a durable intent. They never return to the live
/// buffer: every failure is retried with the same id and the same rows, so the
/// object and the catalog row can only ever have one content.
#[derive(Debug)]
pub struct Flushing {
    /// Id the file will have.
    pub file: FileId,
    /// The rows.
    pub buffer: TenantBuffer,
    /// Failed attempts so far.
    pub attempts: u32,
    /// Earliest time of the next attempt.
    pub next_attempt: Instant,
}

/// Longest wait between retries of a failed flush.
pub const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(60);

/// Rows already written to a registered file but still served from memory.
#[derive(Debug, Clone)]
pub struct Persisted {
    /// Catalog id of the file holding these rows.
    pub file: FileId,
    /// The rows, as they were buffered.
    pub batches: Vec<RecordBatch>,
    /// Arrow memory of `batches`, carried over from the buffer so accounting
    /// never walks the batches again.
    pub bytes: usize,
    /// When the file was registered.
    pub at: Instant,
}

/// What a hot-window scan sees for one tenant.
#[derive(Debug, Default)]
pub struct Snapshot {
    /// Rows not yet flushed.
    pub live: Vec<RecordBatch>,
    /// WAL position of the first live record; the next flush derives its id from it.
    pub live_first_wal: Option<Position>,
    /// Rows being flushed, with the id their file will have.
    pub flushing: Vec<(FileId, Vec<RecordBatch>)>,
    /// Rows flushed recently and still served; the querier skips these files.
    pub persisted: Vec<Persisted>,
}

/// All tenant buffers.
#[derive(Debug, Default)]
pub struct Buffers {
    tenants: HashMap<TenantId, TenantBuffer>,
    flushing: HashMap<TenantId, Vec<Flushing>>,
    persisted: HashMap<TenantId, Vec<Persisted>>,
}

impl Buffers {
    /// Live buffer of `tenant`, created on first use.
    pub fn get_mut(&mut self, tenant: TenantId) -> &mut TenantBuffer {
        self.tenants.entry(tenant).or_default()
    }

    /// Bytes held for `tenant`, live and flushing.
    pub fn bytes_of(&self, tenant: TenantId) -> usize {
        self.tenants.get(&tenant).map_or(0, |b| b.bytes)
            + self
                .flushing
                .get(&tenant)
                .map_or(0, |f| f.iter().map(|x| x.buffer.bytes).sum())
    }

    /// WAL position of the first live record of `tenant`, if any rows are live.
    pub fn live_first_wal(&self, tenant: TenantId) -> Option<Position> {
        self.tenants
            .get(&tenant)
            .filter(|b| !b.is_empty())
            .and_then(|b| b.first_wal)
    }

    /// Moves the live rows of `tenant` into the flushing state under `file`
    /// and returns them for writing. `None` when nothing is live.
    pub fn begin_flush(&mut self, tenant: TenantId, file: FileId) -> Option<Vec<RecordBatch>> {
        let buffer = self.tenants.remove(&tenant).filter(|b| !b.is_empty())?;
        let batches = buffer.batches.clone();
        self.flushing.entry(tenant).or_default().push(Flushing {
            file,
            buffer,
            attempts: 0,
            next_attempt: Instant::now(),
        });
        Some(batches)
    }

    /// Registers rows recovered from the WAL that an intent had already
    /// assigned to `file`.
    pub fn insert_flushing(&mut self, tenant: TenantId, file: FileId, buffer: TenantBuffer) {
        if !buffer.is_empty() {
            self.flushing.entry(tenant).or_default().push(Flushing {
                file,
                buffer,
                attempts: 0,
                next_attempt: Instant::now(),
            });
        }
    }

    /// Flushes whose next attempt is due at `now`, or every flush when `force`.
    pub fn flushing_due(
        &self,
        now: Instant,
        force: bool,
    ) -> Vec<(TenantId, FileId, Vec<RecordBatch>)> {
        self.flushing
            .iter()
            .flat_map(|(t, list)| {
                list.iter()
                    .filter(move |f| force || f.next_attempt <= now)
                    .map(move |f| (*t, f.file, f.buffer.batches.clone()))
            })
            .collect()
    }

    /// Records a failed attempt and schedules the next one with exponential backoff.
    pub fn flush_failed(&mut self, tenant: TenantId, file: FileId, now: Instant) {
        if let Some(entry) = self
            .flushing
            .get_mut(&tenant)
            .and_then(|l| l.iter_mut().find(|f| f.file == file))
        {
            entry.attempts = entry.attempts.saturating_add(1);
            let backoff = Duration::from_secs(1u64 << entry.attempts.min(6)).min(MAX_RETRY_BACKOFF);
            entry.next_attempt = now + backoff;
        }
    }

    /// The file is registered: its rows become persisted until swept.
    pub fn finish_flush(&mut self, tenant: TenantId, file: FileId, now: Instant) {
        if let Some(entry) = self.remove_flushing(tenant, file) {
            self.persisted.entry(tenant).or_default().push(Persisted {
                file,
                bytes: entry.buffer.bytes,
                batches: entry.buffer.batches,
                at: now,
            });
        }
    }

    /// The intent could not be logged, so the rows are not bound to the file
    /// yet: they go back in front of the live buffer. Never called once the
    /// intent is durable.
    pub fn abort_flush(&mut self, tenant: TenantId, file: FileId) {
        if let Some(entry) = self.remove_flushing(tenant, file) {
            self.get_mut(tenant).restore(entry.buffer);
        }
    }

    fn remove_flushing(&mut self, tenant: TenantId, file: FileId) -> Option<Flushing> {
        let list = self.flushing.get_mut(&tenant)?;
        let index = list.iter().position(|f| f.file == file)?;
        let entry = list.swap_remove(index);
        if list.is_empty() {
            self.flushing.remove(&tenant);
        }
        Some(entry)
    }

    /// Drops persisted rows older than `retain`.
    pub fn sweep_persisted(&mut self, retain: Duration, now: Instant) {
        self.persisted.retain(|_, entries| {
            entries.retain(|p| now.saturating_duration_since(p.at) < retain);
            !entries.is_empty()
        });
    }

    /// Live, flushing and recently persisted rows of `tenant`. Batches are
    /// shared, not copied.
    pub fn snapshot(&self, tenant: TenantId) -> Snapshot {
        let live = self.tenants.get(&tenant);
        Snapshot {
            live: live.map(|b| b.batches.clone()).unwrap_or_default(),
            live_first_wal: live.filter(|b| !b.is_empty()).and_then(|b| b.first_wal),
            flushing: self
                .flushing
                .get(&tenant)
                .map(|list| {
                    list.iter()
                        .map(|f| (f.file, f.buffer.batches.clone()))
                        .collect()
                })
                .unwrap_or_default(),
            persisted: self.persisted.get(&tenant).cloned().unwrap_or_default(),
        }
    }

    /// Tenants whose live buffer crossed a threshold or aged past `max_age`.
    pub fn due(&self, rows: usize, bytes: usize, max_age: Duration, now: Instant) -> Vec<TenantId> {
        self.tenants
            .iter()
            .filter(|(_, b)| {
                !b.is_empty()
                    && (b.rows >= rows
                        || b.bytes >= bytes
                        || b.oldest
                            .is_some_and(|o| now.saturating_duration_since(o) >= max_age))
            })
            .map(|(t, _)| *t)
            .collect()
    }

    /// Every tenant with live rows.
    pub fn all(&self) -> Vec<TenantId> {
        self.tenants
            .iter()
            .filter(|(_, b)| !b.is_empty())
            .map(|(t, _)| *t)
            .collect()
    }

    /// Earliest WAL position still live or flushing for any tenant. Everything
    /// before it is materialised and safe to checkpoint.
    pub fn earliest_wal(&self) -> Option<Position> {
        let live = self.tenants.values().filter_map(|b| b.first_wal);
        let flushing = self
            .flushing
            .values()
            .flatten()
            .filter_map(|f| f.buffer.first_wal);
        live.chain(flushing).min()
    }

    /// Bytes held in each state across tenants: `(live, flushing, persisted)`.
    /// Sums per-entry counters; it never walks batches.
    pub fn memory(&self) -> (usize, usize, usize) {
        let live = self.tenants.values().map(|b| b.bytes).sum();
        let flushing = self
            .flushing
            .values()
            .flatten()
            .map(|f| f.buffer.bytes)
            .sum();
        let persisted = self.persisted.values().flatten().map(|p| p.bytes).sum();
        (live, flushing, persisted)
    }

    /// Everything the buffers hold, in every state.
    pub fn total_bytes(&self) -> usize {
        let (live, flushing, persisted) = self.memory();
        live + flushing + persisted
    }

    /// Rows pending across tenants, live and flushing.
    pub fn total_rows(&self) -> usize {
        self.tenants.values().map(|b| b.rows).sum::<usize>()
            + self
                .flushing
                .values()
                .flatten()
                .map(|f| f.buffer.rows)
                .sum::<usize>()
    }
}

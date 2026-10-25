//! In-memory buffers of converted batches, one per tenant.
//!
//! Rows move through three states and are visible to the hot window in all of
//! them: `live` (not yet flushed), `flushing` (being written under a known file
//! id) and `persisted` (registered, kept briefly so queriers whose file list
//! predates the file still see the rows).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
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
    /// Smallest WAL position among the records in this buffer. Concurrent
    /// requests reach the buffer in any order, so this is a minimum, never
    /// the position of whichever record arrived first.
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
        self.first_wal = Some(self.first_wal.map_or(wal, |f| f.min(wal)));
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
        self.first_wal = match (earlier.first_wal, self.first_wal) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
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
    /// An attempt is running right now; the flusher tick must not start another.
    pub in_progress: bool,
    /// The tenant's file seed before this cut, put back if the intent cannot
    /// be logged.
    seed_before: Option<Position>,
}

/// Longest wait between retries of a failed flush.
pub const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(60);

/// Bytes admitted against the caps but not yet in a buffer. Dropping it
/// releases them, so a request cancelled between admission and its push
/// cannot pin the caps.
#[derive(Debug)]
pub struct Reservation {
    bytes: usize,
    tenant: Arc<AtomicUsize>,
    total: Arc<AtomicUsize>,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.tenant.fetch_sub(self.bytes, Ordering::Relaxed);
        self.total.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

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
    /// Seed of the id the live rows' file will get, when any are live.
    pub next_file_seed: Option<Position>,
    /// Rows being flushed, with the id their file will have.
    pub flushing: Vec<(FileId, Vec<RecordBatch>)>,
    /// Rows flushed recently and still served; the querier skips these files.
    pub persisted: Vec<Persisted>,
}

/// All tenant buffers.
///
/// File ids derive from a per-tenant seed: the WAL position that ended the
/// tenant's previous intent, or the position at startup before any intent.
/// The seed is fixed before the rows arrive, so the id the hot window
/// announces for live rows is the id their file gets, whatever order
/// concurrent requests reached the buffer in.
#[derive(Debug)]
pub struct Buffers {
    tenants: HashMap<TenantId, TenantBuffer>,
    flushing: HashMap<TenantId, Vec<Flushing>>,
    persisted: HashMap<TenantId, Vec<Persisted>>,
    seeds: HashMap<TenantId, Position>,
    default_seed: Position,
    reserved: HashMap<TenantId, Arc<AtomicUsize>>,
    reserved_total: Arc<AtomicUsize>,
}

impl Buffers {
    /// Empty buffers; tenants without an intent yet seed their first file
    /// from `default_seed`.
    pub fn new(default_seed: Position) -> Self {
        Self {
            tenants: HashMap::new(),
            flushing: HashMap::new(),
            persisted: HashMap::new(),
            seeds: HashMap::new(),
            default_seed,
            reserved: HashMap::new(),
            reserved_total: Arc::new(AtomicUsize::new(0)),
        }
    }

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

    /// Bytes held for `tenant` in every state, persisted included.
    pub fn tenant_total(&self, tenant: TenantId) -> usize {
        self.bytes_of(tenant)
            + self
                .persisted
                .get(&tenant)
                .map_or(0, |p| p.iter().map(|x| x.bytes).sum())
    }

    /// Bytes admitted for `tenant` whose rows are not in a buffer yet.
    pub fn reserved_of(&self, tenant: TenantId) -> usize {
        self.reserved
            .get(&tenant)
            .map_or(0, |r| r.load(Ordering::Relaxed))
    }

    /// Bytes admitted for any tenant whose rows are not in a buffer yet.
    pub fn reserved_total(&self) -> usize {
        self.reserved_total.load(Ordering::Relaxed)
    }

    /// Counts `bytes` against `tenant` and the total until the reservation is
    /// dropped. Callers check the caps and reserve under the same lock, and
    /// drop the reservation under the lock that pushed the rows, so no
    /// admission ever sees the bytes missing from both sides.
    pub fn reserve(&mut self, tenant: TenantId, bytes: usize) -> Reservation {
        let counter = self.reserved.entry(tenant).or_default();
        counter.fetch_add(bytes, Ordering::Relaxed);
        self.reserved_total.fetch_add(bytes, Ordering::Relaxed);
        Reservation {
            bytes,
            tenant: Arc::clone(counter),
            total: Arc::clone(&self.reserved_total),
        }
    }

    /// Seed of the next file id of `tenant`.
    pub fn file_seed(&self, tenant: TenantId) -> Position {
        self.seeds
            .get(&tenant)
            .copied()
            .unwrap_or(self.default_seed)
    }

    /// Records that an intent of `tenant` ended at `end`, so the next file
    /// seeds from the latest end seen. Replay calls it for every intent.
    pub fn note_intent_end(&mut self, tenant: TenantId, end: Position) {
        let seed = self.seeds.entry(tenant).or_insert(end);
        *seed = (*seed).max(end);
    }

    /// WAL position of the first live record of `tenant`, if any rows are live.
    pub fn live_first_wal(&self, tenant: TenantId) -> Option<Position> {
        self.tenants
            .get(&tenant)
            .filter(|b| !b.is_empty())
            .and_then(|b| b.first_wal)
    }

    /// Moves the live rows of `tenant` into the flushing state under `file`,
    /// whose intent ends at `end`, and returns them for writing. The next
    /// file of the tenant seeds from `end`. `None` when nothing is live.
    pub fn begin_flush(
        &mut self,
        tenant: TenantId,
        file: FileId,
        end: Position,
    ) -> Option<Vec<RecordBatch>> {
        let buffer = self.tenants.remove(&tenant).filter(|b| !b.is_empty())?;
        let batches = buffer.batches.clone();
        let seed_before = self.seeds.insert(tenant, end);
        self.flushing.entry(tenant).or_default().push(Flushing {
            file,
            buffer,
            attempts: 0,
            next_attempt: Instant::now(),
            in_progress: true,
            seed_before,
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
                in_progress: false,
                seed_before: None,
            });
        }
    }

    /// Takes the flushes whose next attempt is due at `now`, or every idle
    /// flush when `force`, marking each as in progress so a concurrent caller
    /// cannot take it again until it fails or finishes.
    pub fn take_due(
        &mut self,
        now: Instant,
        force: bool,
    ) -> Vec<(TenantId, FileId, Vec<RecordBatch>)> {
        let mut out = Vec::new();
        for (tenant, list) in &mut self.flushing {
            for f in list
                .iter_mut()
                .filter(|f| !f.in_progress && (force || f.next_attempt <= now))
            {
                f.in_progress = true;
                out.push((*tenant, f.file, f.buffer.batches.clone()));
            }
        }
        out
    }

    /// Flushes bound by an intent and not being worked on right now.
    pub fn idle_flushing(&self) -> usize {
        self.flushing
            .values()
            .flatten()
            .filter(|f| !f.in_progress)
            .count()
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
            entry.in_progress = false;
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
            match entry.seed_before {
                Some(seed) => self.seeds.insert(tenant, seed),
                None => self.seeds.remove(&tenant),
            };
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
            next_file_seed: live
                .filter(|b| !b.is_empty())
                .map(|_| self.file_seed(tenant)),
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

#[cfg(test)]
mod tests {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let values = Int64Array::from_iter_values(0..i64::try_from(rows).unwrap());
        RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap()
    }

    fn at(offset: u64) -> Position {
        Position { segment: 1, offset }
    }

    #[test]
    fn first_wal_is_the_minimum_whatever_the_arrival_order() {
        let now = Instant::now();
        let mut buffer = TenantBuffer::default();
        buffer.push(batch(2), at(300), now);
        buffer.push(batch(2), at(100), now);
        buffer.push(batch(2), at(200), now);
        assert_eq!(buffer.first_wal, Some(at(100)));

        let mut later = TenantBuffer::default();
        later.push(batch(1), at(50), now);
        later.restore(buffer);
        assert_eq!(later.first_wal, Some(at(50)));
        assert_eq!(later.rows, 7);
    }

    #[test]
    fn file_seed_follows_intent_ends_and_survives_aborts() {
        let tenant = TenantId(uuid::Uuid::from_u128(7));
        let file = FileId(uuid::Uuid::from_u128(1));
        let mut buffers = Buffers::new(at(10));
        assert_eq!(buffers.file_seed(tenant), at(10));
        buffers
            .get_mut(tenant)
            .push(batch(1), at(20), Instant::now());

        assert!(buffers.begin_flush(tenant, file, at(40)).is_some());
        assert_eq!(buffers.file_seed(tenant), at(40));
        buffers.abort_flush(tenant, file);
        assert_eq!(
            buffers.file_seed(tenant),
            at(10),
            "no intent, seed unchanged"
        );
        assert_eq!(buffers.live_first_wal(tenant), Some(at(20)));

        buffers.note_intent_end(tenant, at(90));
        buffers.note_intent_end(tenant, at(60));
        assert_eq!(buffers.file_seed(tenant), at(90), "latest end wins");
    }

    #[test]
    fn reservations_count_until_dropped() {
        let tenant = TenantId(uuid::Uuid::from_u128(7));
        let mut buffers = Buffers::new(at(0));
        let first = buffers.reserve(tenant, 100);
        let second = buffers.reserve(tenant, 50);
        assert_eq!(buffers.reserved_of(tenant), 150);
        assert_eq!(buffers.reserved_total(), 150);
        drop(first);
        assert_eq!(buffers.reserved_of(tenant), 50);
        drop(second);
        assert_eq!(buffers.reserved_total(), 0);
    }
}

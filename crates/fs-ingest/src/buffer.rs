//! In-memory buffers of converted batches, one per tenant.
//!
//! Rows move through three states and are visible to the hot window in all of
//! them: `live` (not yet flushed), `flushing` (being written under a known file
//! id) and `persisted` (registered, kept briefly so queriers whose file list
//! predates the file still see the rows).
//!
//! Batches are kept in the order of their WAL positions, the order replay
//! produces, so a file holds the same rows in the same order whether it was
//! built from live batches or from replayed records. Request batches are
//! small; the coalescer merges complete runs of them into chunks that keep
//! that order, so a flush concatenates a few large batches and live rows
//! take the memory of a packed batch.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use fs_catalog::{FileId, TenantId};
use fs_wal::Position;

/// Why an entry is in the buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// One request's batch.
    Request,
    /// A request's batch taken by the coalesce with this id; still served
    /// and counted until the chunk replaces it.
    Coalescing(u64),
    /// Several requests' batches merged, in log order.
    Chunk,
}

/// One batch and the position of the request it came from; a chunk carries
/// the position of its earliest request.
#[derive(Debug, Clone)]
struct Entry {
    position: Position,
    bytes: usize,
    kind: Kind,
    batch: RecordBatch,
}

/// Pending rows of one tenant.
#[derive(Debug, Default)]
pub struct TenantBuffer {
    /// Sorted by position.
    entries: Vec<Entry>,
    /// Coalesce in progress over this buffer's entries.
    coalesce: Option<u64>,
    /// Rows across the entries.
    pub rows: usize,
    /// Arrow memory across the entries.
    pub bytes: usize,
    /// Smallest WAL position among the records in this buffer. Concurrent
    /// requests reach the buffer in any order, so this is a minimum, never
    /// the position of whichever record arrived first.
    pub first_wal: Option<Position>,
    /// When the first record arrived.
    pub oldest: Option<Instant>,
}

impl TenantBuffer {
    /// Adds a batch that was logged at `wal`, keeping position order.
    pub fn push(&mut self, batch: RecordBatch, wal: Position, now: Instant) {
        if batch.num_rows() == 0 {
            return;
        }
        self.rows += batch.num_rows();
        let bytes = batch.get_array_memory_size();
        self.bytes += bytes;
        self.first_wal = Some(self.first_wal.map_or(wal, |f| f.min(wal)));
        self.oldest.get_or_insert(now);
        let entry = Entry {
            position: wal,
            bytes,
            kind: Kind::Request,
            batch,
        };
        // Arrivals are almost always in order: search from the back.
        let at = self
            .entries
            .iter()
            .rposition(|e| e.position <= wal)
            .map_or(0, |i| i + 1);
        self.entries.insert(at, entry);
    }

    /// True when nothing is pending.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The batches in position order. Shared, not copied.
    pub fn batches(&self) -> Vec<RecordBatch> {
        self.entries.iter().map(|e| e.batch.clone()).collect()
    }

    /// The batches in position order, consuming the buffer.
    pub fn into_batches(self) -> Vec<RecordBatch> {
        self.entries.into_iter().map(|e| e.batch).collect()
    }

    /// Request batches not yet merged into a chunk.
    pub fn request_batches(&self) -> usize {
        self.entries
            .iter()
            .filter(|e| e.kind == Kind::Request)
            .count()
    }

    /// Puts batches back in front after a failed flush. Every position in
    /// `earlier` is below every position here.
    pub fn restore(&mut self, mut earlier: TenantBuffer) {
        if earlier.is_empty() {
            return;
        }
        // The coalesce over the earlier rows, if any, is discarded when it
        // reports: its entries are plain requests again.
        for entry in &mut earlier.entries {
            if matches!(entry.kind, Kind::Coalescing(_)) {
                entry.kind = Kind::Request;
            }
        }
        earlier.entries.append(&mut self.entries);
        self.entries = earlier.entries;
        self.rows += earlier.rows;
        self.bytes += earlier.bytes;
        self.first_wal = match (earlier.first_wal, self.first_wal) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        self.oldest = earlier.oldest.or(self.oldest);
    }

    /// Request batches below `bound` (all of them when `None`), if at least
    /// `min`: marked as taken by coalesce `id` and returned in position order
    /// with their bytes. The run stops before the batch that would take it
    /// past `max_bytes`, so it stays a prefix. `None` while another coalesce
    /// is in progress.
    fn take_for_coalesce(
        &mut self,
        bound: Option<Position>,
        min: usize,
        max_bytes: usize,
        id: u64,
    ) -> Option<(Vec<RecordBatch>, usize)> {
        if self.coalesce.is_some() {
            return None;
        }
        let complete = |e: &Entry| e.kind == Kind::Request && bound.is_none_or(|b| e.position < b);
        if self.entries.iter().filter(|e| complete(e)).count() < min {
            return None;
        }
        self.coalesce = Some(id);
        let mut taken = Vec::new();
        let mut bytes = 0;
        for entry in self.entries.iter_mut().filter(|e| complete(e)) {
            if !taken.is_empty() && bytes + entry.bytes > max_bytes {
                break;
            }
            entry.kind = Kind::Coalescing(id);
            bytes += entry.bytes;
            taken.push(entry.batch.clone());
        }
        Some((taken, bytes))
    }

    /// Replaces the entries taken by coalesce `id` with `chunk`, at the
    /// position of the earliest. False, with the entries back as requests,
    /// when `id` is not the coalesce in progress or the taken entries are no
    /// longer contiguous, which no push can cause but is checked anyway: a
    /// chunk out of order would change the file content.
    fn finish_coalesce(&mut self, id: u64, chunk: RecordBatch) -> bool {
        if self.coalesce != Some(id) {
            self.cancel_coalesce(id);
            return false;
        }
        let taken = |e: &Entry| e.kind == Kind::Coalescing(id);
        let Some(first) = self.entries.iter().position(taken) else {
            self.coalesce = None;
            return false;
        };
        let last = self.entries.iter().rposition(taken).unwrap_or(first);
        let Some(run) = self.entries.get(first..=last) else {
            self.cancel_coalesce(id);
            return false;
        };
        let (Some(position), true) = (
            run.first().map(|e| e.position),
            run.iter().all(taken)
                && run.iter().map(|e| e.batch.num_rows()).sum::<usize>() == chunk.num_rows(),
        ) else {
            self.cancel_coalesce(id);
            return false;
        };
        let removed: usize = run.iter().map(|e| e.bytes).sum();
        let bytes = chunk.get_array_memory_size();
        self.entries.drain(first..=last);
        self.entries.insert(
            first,
            Entry {
                position,
                bytes,
                kind: Kind::Chunk,
                batch: chunk,
            },
        );
        self.bytes = self.bytes - removed + bytes;
        self.coalesce = None;
        true
    }

    /// Returns the entries taken by coalesce `id` to plain requests.
    fn cancel_coalesce(&mut self, id: u64) {
        for entry in &mut self.entries {
            if entry.kind == Kind::Coalescing(id) {
                entry.kind = Kind::Request;
            }
        }
        if self.coalesce == Some(id) {
            self.coalesce = None;
        }
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

/// Bytes of working memory counted against the global cap while a chunk is
/// being built: the inputs of the merge, an upper bound of its output.
#[derive(Debug)]
pub struct Scratch {
    bytes: usize,
    total: Arc<AtomicUsize>,
}

impl Drop for Scratch {
    fn drop(&mut self) {
        self.total.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

/// Batches taken for one coalesce.
#[derive(Debug)]
pub struct Taken {
    /// Coalesce id, to report the chunk under.
    pub id: u64,
    /// The batches, in position order.
    pub batches: Vec<RecordBatch>,
    /// Working memory held while the chunk is built.
    pub scratch: Scratch,
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
    /// Smallest WAL position of the live rows, which the next flush derives
    /// its file id from; final when the snapshot was taken with no request
    /// between its append and its push.
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
    reserved: HashMap<TenantId, Arc<AtomicUsize>>,
    reserved_total: Arc<AtomicUsize>,
    scratch_total: Arc<AtomicUsize>,
    next_coalesce: u64,
}

impl Buffers {
    /// A tenant with at least `min` complete request batches below its bound
    /// and no coalesce in progress. `bound_of` gives each tenant's bound.
    pub fn coalesce_candidate(
        &self,
        min: usize,
        bound_of: impl Fn(TenantId) -> Option<Position>,
    ) -> Option<TenantId> {
        self.tenants
            .iter()
            .find(|(tenant, b)| {
                let bound = bound_of(**tenant);
                b.coalesce.is_none()
                    && b.entries
                        .iter()
                        .filter(|e| e.kind == Kind::Request && bound.is_none_or(|x| e.position < x))
                        .count()
                        >= min
            })
            .map(|(t, _)| *t)
    }

    /// Takes complete request batches of `tenant` for coalescing, at least
    /// `min` of them and at most `max_bytes` of Arrow memory; see
    /// [`TenantBuffer::take_for_coalesce`]. Their bytes are held as scratch
    /// against the global cap until the result is dropped.
    pub fn take_for_coalesce(
        &mut self,
        tenant: TenantId,
        bound: Option<Position>,
        min: usize,
        max_bytes: usize,
    ) -> Option<Taken> {
        let id = self.next_coalesce;
        let (batches, bytes) = self
            .tenants
            .get_mut(&tenant)?
            .take_for_coalesce(bound, min, max_bytes, id)?;
        self.next_coalesce += 1;
        Some(Taken {
            id,
            batches,
            scratch: self.hold_scratch(bytes),
        })
    }

    /// Counts `bytes` of working memory against the global cap until dropped.
    pub fn hold_scratch(&self, bytes: usize) -> Scratch {
        self.scratch_total.fetch_add(bytes, Ordering::Relaxed);
        Scratch {
            bytes,
            total: Arc::clone(&self.scratch_total),
        }
    }

    /// Working memory currently held by chunks being built.
    pub fn scratch_total(&self) -> usize {
        self.scratch_total.load(Ordering::Relaxed)
    }

    /// Puts the chunk of coalesce `id` in place of the batches it merged.
    /// False when the rows were cut for a flush meanwhile: the flush took
    /// the original batches and the chunk is dropped.
    pub fn finish_coalesce(&mut self, tenant: TenantId, id: u64, chunk: RecordBatch) -> bool {
        self.tenants
            .get_mut(&tenant)
            .is_some_and(|b| b.finish_coalesce(id, chunk))
    }

    /// Gives up coalesce `id`: its batches stay as they are.
    pub fn cancel_coalesce(&mut self, tenant: TenantId, id: u64) {
        if let Some(b) = self.tenants.get_mut(&tenant) {
            b.cancel_coalesce(id);
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

    /// Smallest WAL position among the live rows of `tenant`, if any.
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
        let batches = buffer.batches();
        self.flushing.entry(tenant).or_default().push(Flushing {
            file,
            buffer,
            attempts: 0,
            next_attempt: Instant::now(),
            in_progress: true,
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
                out.push((*tenant, f.file, f.buffer.batches()));
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
                batches: entry.buffer.into_batches(),
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
            live: live.map(TenantBuffer::batches).unwrap_or_default(),
            live_first_wal: live.filter(|b| !b.is_empty()).and_then(|b| b.first_wal),
            flushing: self
                .flushing
                .get(&tenant)
                .map(|list| list.iter().map(|f| (f.file, f.buffer.batches())).collect())
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
    use arrow::array::{AsArray, Int64Array};
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
    fn live_first_wal_survives_an_aborted_flush() {
        let tenant = TenantId(uuid::Uuid::from_u128(7));
        let file = FileId(uuid::Uuid::from_u128(1));
        let mut buffers = Buffers::default();
        buffers
            .get_mut(tenant)
            .push(batch(1), at(20), Instant::now());
        buffers
            .get_mut(tenant)
            .push(batch(1), at(15), Instant::now());
        assert_eq!(buffers.live_first_wal(tenant), Some(at(15)));
        assert!(buffers.begin_flush(tenant, file).is_some());
        assert_eq!(buffers.live_first_wal(tenant), None);
        buffers.abort_flush(tenant, file);
        assert_eq!(buffers.live_first_wal(tenant), Some(at(15)));
        assert_eq!(buffers.earliest_wal(), Some(at(15)));
    }

    #[test]
    fn scratch_counts_until_dropped() {
        let buffers = Buffers::default();
        let held = buffers.hold_scratch(40);
        assert_eq!(buffers.scratch_total(), 40);
        drop(held);
        assert_eq!(buffers.scratch_total(), 0);
    }

    fn values(batches: &[RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .values()
                    .iter()
                    .copied()
            })
            .collect()
    }

    fn tagged(rows: usize, tag: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let values =
            Int64Array::from_iter_values((0..rows).map(|i| tag * 100 + i64::try_from(i).unwrap()));
        RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap()
    }

    #[test]
    fn coalesce_merges_complete_batches_in_log_order() {
        let tenant = TenantId(uuid::Uuid::from_u128(7));
        let now = Instant::now();
        let mut buffers = Buffers::default();
        for (offset, tag) in [(30, 3), (10, 1), (40, 4), (20, 2)] {
            buffers
                .get_mut(tenant)
                .push(tagged(2, tag), at(offset), now);
        }
        assert_eq!(
            buffers.coalesce_candidate(4, |_| Some(at(35))),
            None,
            "only three are complete"
        );
        assert_eq!(
            buffers.coalesce_candidate(3, |_| Some(at(35))),
            Some(tenant)
        );
        let Taken {
            id,
            batches: taken,
            scratch,
        } = buffers
            .take_for_coalesce(tenant, Some(at(35)), 3, usize::MAX)
            .unwrap();
        assert_eq!(values(&taken), vec![100, 101, 200, 201, 300, 301]);
        assert!(buffers.scratch_total() > 0, "inputs are held as scratch");
        assert!(
            buffers
                .take_for_coalesce(tenant, None, 1, usize::MAX)
                .is_none(),
            "one at a time"
        );
        drop(scratch);
        assert_eq!(buffers.scratch_total(), 0);
        // Rows arriving meanwhile sit above the bound and after the chunk.
        buffers.get_mut(tenant).push(tagged(1, 5), at(50), now);
        let before = buffers.get_mut(tenant).bytes;
        let chunk =
            arrow::compute::concat_batches(&taken.first().unwrap().schema(), &taken).unwrap();
        assert!(buffers.finish_coalesce(tenant, id, chunk));
        let live = buffers.get_mut(tenant);
        assert_eq!(
            values(&live.batches()),
            vec![100, 101, 200, 201, 300, 301, 400, 401, 500]
        );
        assert_eq!(live.request_batches(), 2);
        assert_eq!(live.rows, 9);
        assert!(
            live.bytes < before,
            "a chunk is smaller than the batches it merged"
        );
        assert_eq!(live.first_wal, Some(at(10)));
    }

    #[test]
    fn coalesce_result_after_a_cut_is_dropped() {
        let tenant = TenantId(uuid::Uuid::from_u128(7));
        let file = FileId(uuid::Uuid::from_u128(1));
        let now = Instant::now();
        let mut buffers = Buffers::default();
        buffers.get_mut(tenant).push(tagged(1, 1), at(10), now);
        buffers.get_mut(tenant).push(tagged(1, 2), at(20), now);
        let Taken {
            id, batches: taken, ..
        } = buffers
            .take_for_coalesce(tenant, None, 2, usize::MAX)
            .unwrap();
        let flushed = buffers.begin_flush(tenant, file).unwrap();
        assert_eq!(
            values(&flushed),
            vec![100, 200],
            "the flush took the originals"
        );
        let chunk =
            arrow::compute::concat_batches(&taken.first().unwrap().schema(), &taken).unwrap();
        assert!(!buffers.finish_coalesce(tenant, id, chunk));
        buffers.get_mut(tenant).push(tagged(1, 3), at(40), now);
        assert_eq!(
            buffers
                .take_for_coalesce(tenant, None, 1, usize::MAX)
                .map(|t| values(&t.batches)),
            Some(vec![300])
        );
    }

    #[test]
    fn coalesce_groups_stop_at_the_byte_limit() {
        let tenant = TenantId(uuid::Uuid::from_u128(7));
        let now = Instant::now();
        let mut buffers = Buffers::default();
        for (offset, tag) in [(10, 1), (20, 2), (30, 3), (40, 4)] {
            buffers
                .get_mut(tenant)
                .push(tagged(2, tag), at(offset), now);
        }
        let one = buffers.get_mut(tenant).entries.first().unwrap().bytes;
        let taken = buffers.take_for_coalesce(tenant, None, 2, one * 2).unwrap();
        assert_eq!(
            values(&taken.batches),
            vec![100, 101, 200, 201],
            "a prefix within the limit"
        );
        let chunk = arrow::compute::concat_batches(
            &taken.batches.first().unwrap().schema(),
            &taken.batches,
        )
        .unwrap();
        assert!(buffers.finish_coalesce(tenant, taken.id, chunk));
        assert_eq!(
            values(&buffers.get_mut(tenant).batches()),
            vec![100, 101, 200, 201, 300, 301, 400, 401]
        );
        assert_eq!(buffers.get_mut(tenant).request_batches(), 2);
        let tiny = buffers.take_for_coalesce(tenant, None, 1, 1).unwrap();
        assert_eq!(
            values(&tiny.batches),
            vec![300, 301],
            "at least one batch is always taken"
        );
    }

    #[test]
    fn reservations_count_until_dropped() {
        let tenant = TenantId(uuid::Uuid::from_u128(7));
        let mut buffers = Buffers::default();
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

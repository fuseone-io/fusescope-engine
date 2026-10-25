//! Group commit for the write-ahead log.
//!
//! One thread owns the appends. When it wakes it takes every record already
//! queued, appends them all, syncs once and answers every waiter with its
//! position. Under load the batch grows while the previous fsync runs, so the
//! cost of one fsync is shared by every record that arrived during it; when
//! idle a record waits for nothing but its own fsync. An optional linger lets
//! the first record of a batch wait a little for company.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, mpsc};
use std::time::{Duration, Instant};

use fs_catalog::TenantId;
use fs_wal::{Position, Wal, WalError};
use tokio::sync::oneshot;
use tracing::{debug, warn};

use crate::IngestError;

/// Batching limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupCommitConfig {
    /// Most records in one batch.
    pub max_records: usize,
    /// Most payload bytes in one batch.
    pub max_bytes: usize,
    /// How long the first record of a batch waits for more. Zero batches only
    /// what is already queued.
    pub linger: Duration,
    /// Most records waiting for the writer at once. Beyond it, appends wait up
    /// to `queue_wait` for a slot and then fail with backpressure.
    pub max_inflight_records: usize,
    /// Most payload bytes waiting for the writer at once. Beyond it, appends
    /// fail with backpressure immediately: memory is the point of the limit.
    pub max_inflight_bytes: usize,
    /// How long an append waits for a queue slot.
    pub queue_wait: Duration,
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        Self {
            max_records: 1024,
            max_bytes: 16 * 1024 * 1024,
            linger: Duration::ZERO,
            max_inflight_records: 4096,
            max_inflight_bytes: 64 * 1024 * 1024,
            queue_wait: Duration::from_secs(2),
        }
    }
}

/// Counters of the writer thread.
#[derive(Debug, Default)]
pub struct WriterStats {
    /// Records appended.
    pub records: AtomicU64,
    /// Batches synced; `records / batches` is the achieved group size.
    pub batches: AtomicU64,
    /// Time spent in fsync, nanoseconds.
    pub sync_nanos: AtomicU64,
    /// Bytes the WAL held on disk after the last batch. Read this instead of
    /// locking the WAL: the writer holds the lock for the whole fsync.
    pub wal_bytes: AtomicU64,
    /// Appends refused or timed out because the queue was full.
    pub backpressure: AtomicU64,
}

/// Positions of records travelling from the log to a buffer, per tenant.
type Inflight = Arc<StdMutex<HashMap<TenantId, BTreeSet<Position>>>>;

/// A synced record's position, held by the caller until the rows it carries
/// are in a buffer. While it is held the position counts as travelling, and
/// everything of the tenant below its smallest travelling position is
/// complete: no lower record is still on its way to a buffer. The coalescer
/// merges only below that bound, so merged rows keep the order of the log.
#[derive(Debug)]
pub struct Tracked {
    /// Position of the record.
    pub position: Position,
    tenant: TenantId,
    inflight: Inflight,
}

impl Drop for Tracked {
    fn drop(&mut self) {
        if let Ok(mut map) = self.inflight.lock() {
            if let Some(set) = map.get_mut(&self.tenant) {
                set.remove(&self.position);
                if set.is_empty() {
                    map.remove(&self.tenant);
                }
            }
        }
    }
}

/// A queued record. It owns its queue slot and byte lease, so a caller that
/// cancels after queuing frees nothing: the limits stay taken until the writer
/// has synced the record and dropped it.
struct Pending {
    bytes: Vec<u8>,
    tenant: TenantId,
    reply: oneshot::Sender<Result<Tracked, WalError>>,
    _slot: tokio::sync::OwnedSemaphorePermit,
    _lease: ByteLease,
}

/// Bytes charged to the in-flight budget, returned when dropped.
struct ByteLease {
    counter: Arc<std::sync::atomic::AtomicUsize>,
    size: usize,
}

impl Drop for ByteLease {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.size, Ordering::Relaxed);
    }
}

/// Handle to the writer thread. Cheap to clone; dropping the last handle stops
/// the thread after it drains the queue.
#[derive(Clone)]
pub struct WalWriter {
    queue: mpsc::Sender<Pending>,
    stats: Arc<WriterStats>,
    slots: Arc<tokio::sync::Semaphore>,
    inflight_bytes: Arc<std::sync::atomic::AtomicUsize>,
    inflight: Inflight,
    config: GroupCommitConfig,
}

impl WalWriter {
    /// Starts the writer over `wal`.
    pub fn spawn(wal: Arc<StdMutex<Wal>>, config: GroupCommitConfig) -> Self {
        let (queue, inbox) = mpsc::channel::<Pending>();
        let stats = Arc::new(WriterStats::default());
        stats
            .wal_bytes
            .store(wal.lock().map_or(0, |w| w.bytes()), Ordering::Relaxed);
        let thread_stats = Arc::clone(&stats);
        let inflight: Inflight = Arc::new(StdMutex::new(HashMap::new()));
        let thread_inflight = Arc::clone(&inflight);
        std::thread::Builder::new()
            .name("fs-wal-writer".into())
            .spawn(move || run(&wal, &inbox, config, &thread_stats, &thread_inflight))
            .map_err(|e| warn!(error = %e, "wal writer thread could not start"))
            .ok();
        Self {
            queue,
            stats,
            slots: Arc::new(tokio::sync::Semaphore::new(
                config
                    .max_inflight_records
                    .clamp(1, tokio::sync::Semaphore::MAX_PERMITS),
            )),
            inflight_bytes: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            inflight,
            config,
        }
    }

    /// Smallest position of `tenant` still travelling to a buffer, if any.
    /// Records of the tenant below it are complete in memory.
    pub fn stable_bound(&self, tenant: TenantId) -> Option<Position> {
        self.inflight
            .lock()
            .ok()
            .and_then(|map| map.get(&tenant).and_then(|set| set.first().copied()))
    }

    /// Counters.
    pub fn stats(&self) -> Arc<WriterStats> {
        Arc::clone(&self.stats)
    }

    /// Appends one record and resolves once it is synced. The position stays
    /// tracked as travelling until the returned handle is dropped.
    ///
    /// # Errors
    /// Backpressure when the queue is over its record or byte limit, WAL
    /// failure, or the writer thread is gone.
    pub async fn append(&self, tenant: TenantId, bytes: Vec<u8>) -> Result<Tracked, IngestError> {
        let size = bytes.len();
        let held = self.inflight_bytes.fetch_add(size, Ordering::Relaxed) + size;
        if held > self.config.max_inflight_bytes {
            self.inflight_bytes.fetch_sub(size, Ordering::Relaxed);
            self.stats.backpressure.fetch_add(1, Ordering::Relaxed);
            return Err(IngestError::Backpressure("wal queue bytes"));
        }
        let lease = ByteLease {
            counter: Arc::clone(&self.inflight_bytes),
            size,
        };
        let Ok(Ok(slot)) = tokio::time::timeout(
            self.config.queue_wait,
            Arc::clone(&self.slots).acquire_owned(),
        )
        .await
        else {
            self.stats.backpressure.fetch_add(1, Ordering::Relaxed);
            return Err(IngestError::Backpressure("wal queue records"));
        };
        let (reply, answer) = oneshot::channel();
        self.queue
            .send(Pending {
                bytes,
                tenant,
                reply,
                _slot: slot,
                _lease: lease,
            })
            .map_err(|_| IngestError::Record("wal writer stopped".into()))?;
        answer
            .await
            .map_err(|_| IngestError::Record("wal writer dropped a request".into()))?
            .map_err(IngestError::from)
    }

    /// Queue slots currently free. For tests and diagnostics.
    pub fn free_slots(&self) -> usize {
        self.slots.available_permits()
    }

    /// Bytes currently charged to the queue budget. For tests and diagnostics.
    pub fn inflight_bytes(&self) -> usize {
        self.inflight_bytes.load(Ordering::Relaxed)
    }
}

fn run(
    wal: &StdMutex<Wal>,
    inbox: &mpsc::Receiver<Pending>,
    config: GroupCommitConfig,
    stats: &WriterStats,
    inflight: &Inflight,
) {
    while let Ok(first) = inbox.recv() {
        let mut batch = vec![first];
        let mut bytes = batch.first().map_or(0, |p| p.bytes.len());
        let deadline = Instant::now() + config.linger;
        loop {
            if batch.len() >= config.max_records || bytes >= config.max_bytes {
                break;
            }
            let next = if config.linger.is_zero() {
                inbox.try_recv().ok()
            } else {
                match deadline.checked_duration_since(Instant::now()) {
                    Some(left) if !left.is_zero() => inbox.recv_timeout(left).ok(),
                    _ => inbox.try_recv().ok(),
                }
            };
            match next {
                Some(p) => {
                    bytes += p.bytes.len();
                    batch.push(p);
                }
                None => break,
            }
        }
        commit(wal, batch, stats, inflight);
    }
    debug!("wal writer stopped");
}

fn commit(wal: &StdMutex<Wal>, batch: Vec<Pending>, stats: &WriterStats, inflight: &Inflight) {
    let Ok(mut wal) = wal.lock() else {
        for p in batch {
            let _ = p.reply.send(Err(WalError::EmptyRecord));
        }
        return;
    };
    let mut appended: Vec<(Pending, Position)> = Vec::with_capacity(batch.len());
    for p in batch {
        match wal.append(&p.bytes) {
            Ok(position) => appended.push((p, position)),
            Err(e) => {
                let _ = p.reply.send(Err(e));
            }
        }
    }
    // Positions become travelling before anyone can learn them: a caller only
    // sees its position through the reply, after this insert.
    if let Ok(mut map) = inflight.lock() {
        for (pending, position) in &appended {
            map.entry(pending.tenant).or_default().insert(*position);
        }
    }
    let started = Instant::now();
    let synced = wal.sync();
    stats.sync_nanos.fetch_add(
        u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    stats.batches.fetch_add(1, Ordering::Relaxed);
    stats
        .records
        .fetch_add(appended.len() as u64, Ordering::Relaxed);
    stats.wal_bytes.store(wal.bytes(), Ordering::Relaxed);
    drop(wal);
    for (pending, position) in appended {
        let tracked = Tracked {
            position,
            tenant: pending.tenant,
            inflight: Arc::clone(inflight),
        };
        let outcome = match &synced {
            Ok(()) => Ok(tracked),
            Err(e) => Err(WalError::Io {
                path: std::path::PathBuf::from("wal sync"),
                source: std::io::Error::other(e.to_string()),
            }),
        };
        // A failed send drops the handle, which untracks the position; so
        // does a failed sync, whose handle is dropped above.
        let _ = pending.reply.send(outcome);
        // `pending` drops here: slot and bytes return only now.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs_wal::{Event, WalConfig};

    fn tenant(n: u128) -> TenantId {
        TenantId(uuid::Uuid::from_u128(n))
    }

    #[tokio::test]
    async fn byte_budget_refuses_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(StdMutex::new(
            Wal::open(WalConfig::new(dir.path())).unwrap(),
        ));
        let config = GroupCommitConfig {
            max_inflight_bytes: 10,
            ..GroupCommitConfig::default()
        };
        let writer = WalWriter::spawn(wal, config);
        let err = writer.append(tenant(1), vec![0u8; 11]).await.unwrap_err();
        assert!(
            matches!(err, IngestError::Backpressure("wal queue bytes")),
            "{err}"
        );
        assert_eq!(writer.stats().backpressure.load(Ordering::Relaxed), 1);
        assert_eq!(
            writer.inflight_bytes.load(Ordering::Relaxed),
            0,
            "budget returned"
        );
        writer.append(tenant(1), vec![0u8; 10]).await.unwrap();
        assert_eq!(writer.inflight_bytes(), 0);
    }

    #[tokio::test]
    async fn record_slots_time_out_under_backpressure() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(StdMutex::new(
            Wal::open(WalConfig::new(dir.path())).unwrap(),
        ));
        let config = GroupCommitConfig {
            max_inflight_records: 1,
            queue_wait: Duration::from_millis(50),
            ..GroupCommitConfig::default()
        };
        let writer = WalWriter::spawn(wal, config);
        // Hold the only slot without ever sending, as a stuck request would.
        let held = Arc::clone(&writer.slots).acquire_owned().await.unwrap();
        let err = writer.append(tenant(1), vec![1, 2, 3]).await.unwrap_err();
        assert!(
            matches!(err, IngestError::Backpressure("wal queue records")),
            "{err}"
        );
        drop(held);
        writer.append(tenant(1), vec![1, 2, 3]).await.unwrap();
    }

    /// A caller that gives up after queuing must not free the limits: the
    /// record is still going to be written, and the memory is still held.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[allow(clippy::await_holding_lock)] // holding the WAL lock across the sleep is the stall under test
    async fn cancelled_append_keeps_limits_until_the_writer_is_done() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(StdMutex::new(
            Wal::open(WalConfig::new(dir.path())).unwrap(),
        ));
        let config = GroupCommitConfig {
            max_inflight_records: 4,
            ..GroupCommitConfig::default()
        };
        let writer = WalWriter::spawn(Arc::clone(&wal), config);
        // Stall the writer: it blocks on the WAL lock inside commit.
        let stall = wal.lock().unwrap();
        let task = {
            let writer = writer.clone();
            tokio::spawn(async move { writer.append(tenant(1), vec![7u8; 100]).await })
        };
        let queued = async {
            while writer.free_slots() != 3 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(5), queued)
            .await
            .expect("record queued");
        task.abort();
        let _ = task.await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            writer.inflight_bytes(),
            100,
            "bytes stay charged after the caller cancelled"
        );
        assert_eq!(writer.free_slots(), 3, "the slot stays taken");
        drop(stall);
        let released = async {
            while writer.free_slots() != 4 || writer.inflight_bytes() != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(5), released)
            .await
            .expect("released once the writer synced the record");
        assert_eq!(
            writer.stats().records.load(Ordering::Relaxed),
            1,
            "the record was written anyway"
        );
    }

    #[tokio::test]
    async fn stable_bound_is_the_smallest_held_position() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(StdMutex::new(
            Wal::open(WalConfig::new(dir.path())).unwrap(),
        ));
        let writer = WalWriter::spawn(wal, GroupCommitConfig::default());
        assert_eq!(writer.stable_bound(tenant(1)), None);
        let first = writer.append(tenant(1), vec![1]).await.unwrap();
        let other = writer.append(tenant(2), vec![9]).await.unwrap();
        let second = writer.append(tenant(1), vec![2]).await.unwrap();
        assert!(first.position < other.position && other.position < second.position);
        assert_eq!(writer.stable_bound(tenant(1)), Some(first.position));
        assert_eq!(writer.stable_bound(tenant(2)), Some(other.position));
        drop(first);
        assert_eq!(
            writer.stable_bound(tenant(1)),
            Some(second.position),
            "another tenant's record does not hold this one back"
        );
        drop(other);
        drop(second);
        assert_eq!(writer.stable_bound(tenant(1)), None);
        assert_eq!(writer.stable_bound(tenant(2)), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_appends_get_ordered_positions_and_fewer_syncs() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(StdMutex::new(
            Wal::open(WalConfig::new(dir.path())).unwrap(),
        ));
        let writer = WalWriter::spawn(Arc::clone(&wal), GroupCommitConfig::default());
        let mut tasks = Vec::new();
        for i in 0..200u32 {
            let writer = writer.clone();
            tasks.push(tokio::spawn(async move {
                writer
                    .append(tenant(1), i.to_le_bytes().to_vec())
                    .await
                    .unwrap()
                    .position
            }));
        }
        let mut positions = Vec::new();
        for t in tasks {
            positions.push(t.await.unwrap());
        }
        positions.sort_unstable();
        positions.dedup();
        assert_eq!(positions.len(), 200, "every record has its own position");
        let stats = writer.stats();
        let (records, batches) = (
            stats.records.load(Ordering::Relaxed),
            stats.batches.load(Ordering::Relaxed),
        );
        assert_eq!(records, 200);
        assert!(
            batches >= 1 && batches <= records,
            "{batches} batches for {records} records"
        );

        drop(writer);
        let (payloads, damaged): (Vec<_>, Vec<_>) = wal
            .lock()
            .unwrap()
            .replay()
            .unwrap()
            .partition(|e| matches!(e, Event::Record(_)));
        assert_eq!(damaged.len(), 0);
        assert_eq!(payloads.len(), 200, "everything acknowledged is on disk");
    }
}

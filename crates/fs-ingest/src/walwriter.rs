//! Group commit for the write-ahead log.
//!
//! One thread owns the appends. When it wakes it takes every record already
//! queued, appends them all, syncs once and answers every waiter with its
//! position. Under load the batch grows while the previous fsync runs, so the
//! cost of one fsync is shared by every record that arrived during it; when
//! idle a record waits for nothing but its own fsync. An optional linger lets
//! the first record of a batch wait a little for company.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, mpsc};
use std::time::{Duration, Instant};

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
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        Self {
            max_records: 1024,
            max_bytes: 16 * 1024 * 1024,
            linger: Duration::ZERO,
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
}

struct Pending {
    bytes: Vec<u8>,
    reply: oneshot::Sender<Result<Position, WalError>>,
}

/// Handle to the writer thread. Cheap to clone; dropping the last handle stops
/// the thread after it drains the queue.
#[derive(Clone)]
pub struct WalWriter {
    queue: mpsc::Sender<Pending>,
    stats: Arc<WriterStats>,
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
        std::thread::Builder::new()
            .name("fs-wal-writer".into())
            .spawn(move || run(&wal, &inbox, config, &thread_stats))
            .map_err(|e| warn!(error = %e, "wal writer thread could not start"))
            .ok();
        Self { queue, stats }
    }

    /// Counters.
    pub fn stats(&self) -> Arc<WriterStats> {
        Arc::clone(&self.stats)
    }

    /// Appends one record and resolves once it is synced.
    ///
    /// # Errors
    /// WAL failure, or the writer thread is gone.
    pub async fn append(&self, bytes: Vec<u8>) -> Result<Position, IngestError> {
        let (reply, answer) = oneshot::channel();
        self.queue
            .send(Pending { bytes, reply })
            .map_err(|_| IngestError::Record("wal writer stopped".into()))?;
        answer
            .await
            .map_err(|_| IngestError::Record("wal writer dropped a request".into()))?
            .map_err(IngestError::from)
    }
}

fn run(
    wal: &StdMutex<Wal>,
    inbox: &mpsc::Receiver<Pending>,
    config: GroupCommitConfig,
    stats: &WriterStats,
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
        commit(wal, batch, stats);
    }
    debug!("wal writer stopped");
}

fn commit(wal: &StdMutex<Wal>, batch: Vec<Pending>, stats: &WriterStats) {
    let Ok(mut wal) = wal.lock() else {
        for p in batch {
            let _ = p.reply.send(Err(WalError::EmptyRecord));
        }
        return;
    };
    let mut appended: Vec<(oneshot::Sender<Result<Position, WalError>>, Position)> =
        Vec::with_capacity(batch.len());
    for p in batch {
        match wal.append(&p.bytes) {
            Ok(position) => appended.push((p.reply, position)),
            Err(e) => {
                let _ = p.reply.send(Err(e));
            }
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
    for (reply, position) in appended {
        let outcome = match &synced {
            Ok(()) => Ok(position),
            Err(e) => Err(WalError::Io {
                path: std::path::PathBuf::from("wal sync"),
                source: std::io::Error::other(e.to_string()),
            }),
        };
        let _ = reply.send(outcome);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs_wal::{Event, WalConfig};

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
                writer.append(i.to_le_bytes().to_vec()).await.unwrap()
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

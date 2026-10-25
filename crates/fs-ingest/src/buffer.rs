//! In-memory buffers of converted batches, one per tenant.

use std::collections::HashMap;
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use fs_catalog::TenantId;
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

/// All tenant buffers.
#[derive(Debug, Default)]
pub struct Buffers {
    tenants: HashMap<TenantId, TenantBuffer>,
}

impl Buffers {
    /// Buffer of `tenant`, created on first use.
    pub fn get_mut(&mut self, tenant: TenantId) -> &mut TenantBuffer {
        self.tenants.entry(tenant).or_default()
    }

    /// Bytes pending for `tenant`.
    pub fn bytes_of(&self, tenant: TenantId) -> usize {
        self.tenants.get(&tenant).map_or(0, |b| b.bytes)
    }

    /// Removes and returns the buffer of `tenant`, if it has rows.
    pub fn take(&mut self, tenant: TenantId) -> Option<TenantBuffer> {
        self.tenants.remove(&tenant).filter(|b| !b.is_empty())
    }

    /// Tenants whose buffer crossed a threshold or aged past `max_age`.
    pub fn due(
        &self,
        rows: usize,
        bytes: usize,
        max_age: std::time::Duration,
        now: Instant,
    ) -> Vec<TenantId> {
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

    /// Every tenant with pending rows.
    pub fn all(&self) -> Vec<TenantId> {
        self.tenants
            .iter()
            .filter(|(_, b)| !b.is_empty())
            .map(|(t, _)| *t)
            .collect()
    }

    /// Earliest WAL position still buffered by any tenant. Everything before it
    /// is safe to drop from the log.
    pub fn earliest_wal(&self) -> Option<Position> {
        self.tenants.values().filter_map(|b| b.first_wal).min()
    }

    /// Rows pending across tenants.
    pub fn total_rows(&self) -> usize {
        self.tenants.values().map(|b| b.rows).sum()
    }
}

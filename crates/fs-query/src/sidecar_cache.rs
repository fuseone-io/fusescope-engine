//! The byte budget sidecar caches share: a ceiling on what a cache makes
//! the process hold, decoded sidecars alive anywhere, including ones
//! evicted from the map but still used by a query, plus the encoded bytes
//! and the heap of downloads in flight. A download holds a [`Lease`]: it is
//! marked per file so one is in flight at a time, reserves what it reads
//! and decodes before it does, evicting idle entries to make room, and
//! returns everything it reserved when dropped, on a timeout or a
//! cancelled query.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use fs_catalog::FileId;

/// Bytes charged for remembering that a file has no usable sidecar.
pub(crate) const MISS_BYTES: usize = 64;

/// The budget and the entries of one cache of sidecars of type `T`.
pub(crate) struct Shared<T> {
    pub(crate) max_bytes: usize,
    inner: Mutex<Inner<T>>,
}

struct Inner<T> {
    entries: HashMap<FileId, Slot<T>>,
    /// Bytes of decoded sidecars alive and of misses remembered.
    bytes: usize,
    /// Bytes reserved by downloads in flight.
    inflight: usize,
    fetching: HashSet<FileId>,
    clock: u64,
}

struct Slot<T> {
    index: Option<Arc<Held<T>>>,
    /// Bytes charged to the slot itself: a remembered miss. A decoded
    /// index charges its own bytes for as long as it is alive.
    charged: usize,
    used: u64,
}

/// A decoded sidecar that holds its bytes against the budget until the
/// last query using it lets go.
pub struct Held<T> {
    index: T,
    bytes: usize,
    /// Weak, so entries never keep the cache alive.
    shared: Weak<Shared<T>>,
}

impl<T> std::ops::Deref for Held<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.index
    }
}

impl<T> Drop for Held<T> {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.upgrade() {
            shared.lock().bytes -= self.bytes;
        }
    }
}

impl<T> Shared<T> {
    /// An empty cache of at most `max_bytes`.
    pub(crate) fn new(max_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            max_bytes,
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                bytes: 0,
                inflight: 0,
                fetching: HashSet::new(),
                clock: 0,
            }),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Inner<T>> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Bytes of decoded sidecars alive and misses remembered.
    pub(crate) fn bytes(&self) -> usize {
        self.lock().bytes
    }

    /// Bytes alive plus bytes reserved by downloads in flight.
    pub(crate) fn committed_bytes(&self) -> usize {
        let inner = self.lock();
        inner.bytes + inner.inflight
    }

    /// Entries in the map, misses included.
    #[cfg(test)]
    pub(crate) fn entries(&self) -> usize {
        self.lock().entries.len()
    }

    /// True when no download is in flight.
    #[cfg(test)]
    pub(crate) fn idle(&self) -> bool {
        self.lock().fetching.is_empty()
    }

    /// A cache hit, or `None` when the file was never looked up.
    pub(crate) fn cached(&self, id: FileId) -> Option<Entry<T>> {
        let mut inner = self.lock();
        inner.clock += 1;
        let clock = inner.clock;
        let slot = inner.entries.get_mut(&id)?;
        slot.used = clock;
        Some(slot.index.clone().map_or(Entry::Miss, Entry::Index))
    }
}

/// Evicts the least recently used entry from the map: `None` when there
/// is none, else the decoded index it held, if any. That index keeps
/// charging its bytes until every query using it lets go, so the caller
/// drops it outside the lock.
fn evict_one<T>(inner: &mut Inner<T>) -> Option<Entry<T>> {
    let (&victim, _) = inner.entries.iter().min_by_key(|(_, s)| s.used)?;
    let slot = inner.entries.remove(&victim)?;
    inner.bytes -= slot.charged;
    Some(slot.index.map_or(Entry::Miss, Entry::Index))
}

/// What a slot holds.
pub(crate) enum Entry<T> {
    Index(Arc<Held<T>>),
    /// The file has no sidecar this engine can use.
    Miss,
}

/// Evicts until `extra` more bytes fit or nothing is left to evict. An
/// evicted index charges its bytes until dropped, which happens outside
/// the lock, so the bytes an eviction frees are counted here: those of
/// indexes no query still uses.
fn make_room<T>(inner: &mut Inner<T>, max_bytes: usize, extra: usize) -> Evicted<T> {
    let mut evicted = Evicted {
        held: Vec::new(),
        freed: 0,
    };
    while inner.bytes - evicted.freed + inner.inflight + extra > max_bytes {
        match evict_one(inner) {
            Some(Entry::Index(held)) => {
                if Arc::strong_count(&held) == 1 {
                    evicted.freed += held.bytes;
                }
                evicted.held.push(held);
            }
            Some(Entry::Miss) => {}
            None => break,
        }
    }
    evicted
}

/// Indexes evicted under the lock, dropped after it.
struct Evicted<T> {
    held: Vec<Arc<Held<T>>>,
    /// Bytes that drop will return.
    freed: usize,
}

impl<T> Evicted<T> {
    fn fits(&self, inner: &Inner<T>, max_bytes: usize, extra: usize) -> bool {
        inner.bytes - self.freed + inner.inflight + extra <= max_bytes
    }
}

/// One download's claim on the cache: the per-file mark and the bytes it
/// reserved. Dropped, it returns both; committed, it becomes an entry.
pub(crate) struct Lease<T> {
    shared: Arc<Shared<T>>,
    id: FileId,
    reserved: usize,
}

impl<T> Lease<T> {
    /// Marks a download of `id` as in flight. `None` when one already is.
    pub(crate) fn begin(shared: &Arc<Shared<T>>, id: FileId) -> Option<Self> {
        if !shared.lock().fetching.insert(id) {
            return None;
        }
        Some(Self {
            shared: Arc::clone(shared),
            id,
            reserved: 0,
        })
    }

    /// Reserves `size` more bytes of the budget, evicting idle entries to
    /// make room.
    pub(crate) fn reserve(&mut self, size: usize) -> Reserve {
        let evicted;
        {
            let mut inner = self.shared.lock();
            if size > self.shared.max_bytes {
                return Reserve::TooLarge;
            }
            evicted = make_room(&mut inner, self.shared.max_bytes, size);
            if !evicted.fits(&inner, self.shared.max_bytes, size) {
                drop(inner);
                drop(evicted);
                return Reserve::Busy;
            }
            inner.inflight += size;
            self.reserved += size;
        }
        drop(evicted);
        Reserve::Fits
    }

    /// Turns the lease into an entry charging `charged` bytes: a decoded
    /// index that charges them while alive, or a remembered miss.
    pub(crate) fn commit(mut self, index: Option<T>, charged: usize) -> Option<Arc<Held<T>>> {
        let held = index.map(|index| {
            Arc::new(Held {
                index,
                bytes: charged,
                shared: Arc::downgrade(&self.shared),
            })
        });
        let slot_charge = if held.is_some() { 0 } else { charged };
        let evicted;
        {
            let mut inner = self.shared.lock();
            inner.inflight -= self.reserved;
            inner.fetching.remove(&self.id);
            self.reserved = 0;
            inner.clock += 1;
            let used = inner.clock;
            let mut old = Vec::new();
            if let Some(previous) = inner.entries.insert(
                self.id,
                Slot {
                    index: held.clone(),
                    charged: slot_charge,
                    used,
                },
            ) {
                inner.bytes -= previous.charged;
                old.extend(previous.index);
            }
            inner.bytes += charged;
            evicted = make_room(&mut inner, self.shared.max_bytes, 0);
            drop(inner);
            drop(old);
        }
        drop(evicted);
        held
    }
}

impl<T> Drop for Lease<T> {
    fn drop(&mut self) {
        let mut inner = self.shared.lock();
        inner.inflight -= self.reserved;
        inner.fetching.remove(&self.id);
    }
}

/// Whether a download's bytes fit the budget.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Reserve {
    Fits,
    /// Larger than the whole budget: never fits, remembered as a miss.
    TooLarge,
    /// Held by entries or downloads in use right now: tried again next time.
    Busy,
}

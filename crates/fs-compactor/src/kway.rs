//! The k-way merge of sorted inputs. The heads of the inputs are kept in a
//! heap keyed by `(service, timestamp, input, row)`; a row group is picked
//! from the heads in order, then gathered. Each input holds its loaded
//! batch, and the one before while rows of it are picked: a group that
//! would make an input load a third ends early, and the input loads its
//! next batch once the group is gathered and the older one freed.

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, SchemaRef, TimestampNanosecondType};
use arrow::record_batch::RecordBatch;
use fs_catalog::FileRecord;
use fs_schema::parquet::gather;
use fs_schema::spans::col;
use fs_schema::usage::{UsageCounts, UsageRows, UsageSignal};
use object_store::ObjectStore;

use crate::CompactError;
use crate::read;
use crate::sizes::row_sizes;

/// How a merge reads its inputs.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Reading {
    /// Batches of a number of rows, unbounded.
    Rows(usize),
    /// Chunks of at most `rows` rows planned from each input's page index.
    /// Inputs are taken in order while each holds two chunks and its
    /// metadata within `share` between them.
    Bounded { share: usize, rows: usize },
}

/// Rows picked for the next row group, `(slot, row)` in order, and what
/// gathering them allocates at most.
pub(crate) struct Picks {
    picked: Vec<(usize, usize)>,
    pub(crate) bytes: usize,
    /// The input of the first row.
    first: usize,
}

/// A service name as a sort key: `None` is a null, before every name.
type Service = Option<Arc<str>>;

/// The head of one input in the heap: its sort key and where the row is.
type Head = Reverse<(Service, i64, usize, usize, usize)>;

struct Input {
    path: String,
    read: read::Input,
    /// Slot of the batch being consumed, if any.
    slot: Option<usize>,
    /// What its metadata and layout hold while it is read.
    metadata: usize,
    /// What it may hold at once: its metadata and two chunks.
    reserved: usize,
    /// Loaded a batch while the current group was picked.
    loaded_in_group: bool,
}

pub(crate) struct Merge {
    inputs: Vec<Input>,
    slots: Vec<Option<Slot>>,
    heap: BinaryHeap<Head>,
    /// Usage of the inputs flagged in `uncounted`, within its budget.
    pub(crate) usage: Option<UsageCounts>,
    uncounted: Vec<bool>,
    usage_budget: usize,
    pub(crate) usage_peak: usize,
    bounded: bool,
    /// Inputs whose next batch loads before the next group, at their row.
    deferred: Vec<(usize, usize)>,
    /// Most bytes held while a batch was read, since last taken.
    decode_peak: usize,
}

impl Merge {
    /// Opens `files` read as `reading`, taking all of them, or for a
    /// bounded read those that fit in order, one at least.
    ///
    /// # Errors
    /// The first file cannot be read within a bounded budget, or a store
    /// or Parquet failure.
    pub(crate) async fn open(
        store: Arc<dyn ObjectStore>,
        files: &[FileRecord],
        schema: &SchemaRef,
        reading: Reading,
        usage: Option<(UsageSignal, usize)>,
    ) -> Result<Self, CompactError> {
        let inputs = match reading {
            Reading::Rows(rows) => {
                let mut inputs = Vec::with_capacity(files.len());
                for file in files {
                    let read = read::Input::rows(Arc::clone(&store), file, rows).await?;
                    inputs.push(input(&file.path, read, 0, 0));
                }
                inputs
            }
            Reading::Bounded { share, rows } => {
                bounded_inputs(store, files, schema, (share, rows)).await?
            }
        };
        let uncounted: Vec<bool> = files
            .iter()
            .take(inputs.len())
            .map(|f| !f.usage_accounted)
            .collect();
        let counts = usage
            .filter(|_| uncounted.contains(&true))
            .map(|(signal, _)| UsageCounts::new(signal));
        let mut merge = Self {
            inputs,
            slots: Vec::new(),
            heap: BinaryHeap::new(),
            usage: counts,
            uncounted,
            usage_budget: usage.map_or(0, |(_, budget)| budget),
            usage_peak: 0,
            bounded: matches!(reading, Reading::Bounded { .. }),
            deferred: Vec::new(),
            decode_peak: 0,
        };
        for input in 0..merge.inputs.len() {
            merge.advance(input, 0, schema).await?;
        }
        Ok(merge)
    }

    /// How many of the files given were taken, the oldest.
    pub(crate) fn taken(&self) -> usize {
        self.inputs.len()
    }

    /// What the inputs may hold at once, 0 when unbounded.
    pub(crate) fn reserved(&self) -> usize {
        self.inputs.iter().map(|i| i.reserved).sum()
    }

    /// The input the first row of `picks` comes from.
    pub(crate) fn path_of(&self, picks: &Picks) -> String {
        self.inputs
            .get(picks.first)
            .map(|i| i.path.clone())
            .unwrap_or_default()
    }

    /// What the loaded batches and the inputs' metadata hold.
    pub(crate) fn held_bytes(&self) -> usize {
        let slots: usize = self.slots.iter().flatten().map(|s| s.bytes).sum();
        slots + self.inputs.iter().map(|i| i.metadata).sum::<usize>()
    }

    /// Most bytes held while a batch was read since the last call.
    pub(crate) fn take_decode_peak(&mut self) -> usize {
        std::mem::take(&mut self.decode_peak)
    }

    /// Makes `row` of `input`'s current slot the head, loading the next
    /// batch when the slot is spent.
    async fn advance(
        &mut self,
        input: usize,
        row: usize,
        schema: &SchemaRef,
    ) -> Result<(), CompactError> {
        let mut row = row;
        loop {
            if let Some(slot) = self.inputs.get(input).and_then(|i| i.slot) {
                if let Some(loaded) = self.slots.get(slot).and_then(Option::as_ref) {
                    if let (Some(service), Some(ts)) =
                        (loaded.services.get(row), loaded.timestamps.get(row))
                    {
                        self.heap
                            .push(Reverse((service.clone(), *ts, input, row, slot)));
                        return Ok(());
                    }
                }
            }
            let held = self.held_bytes();
            let bounded = self.bounded;
            let Some(next) = self.inputs.get_mut(input) else {
                return Ok(());
            };
            let Some((batch, bound)) = next.read.next().await? else {
                next.slot = None;
                return Ok(());
            };
            self.decode_peak = self.decode_peak.max(held.saturating_add(bound));
            if batch.num_rows() == 0 {
                continue;
            }
            next.loaded_in_group = true;
            let loaded = Slot::load(input, batch, schema, bounded)?;
            let slot = self.slots.len();
            next.slot = Some(slot);
            self.slots.push(Some(loaded));
            row = 0;
        }
    }

    /// `group` as usage counts it, when it has rows of inputs whose usage
    /// was not counted.
    ///
    /// # Errors
    /// As [`UsageRows::new`].
    pub(crate) fn usage_rows(
        &self,
        group: &RecordBatch,
        take: &[bool],
    ) -> Result<Option<UsageRows>, CompactError> {
        match &self.usage {
            Some(usage) if take.contains(&true) => Ok(Some(UsageRows::new(group, usage.signal())?)),
            _ => Ok(None),
        }
    }

    /// Counts the rows `take` flags of `rows`, from `from`, into the
    /// usage; the counts set aside full and the row to resume at when the
    /// budget stops it.
    pub(crate) fn count_usage(
        &mut self,
        rows: &UsageRows,
        from: usize,
        take: &[bool],
    ) -> Result<Option<(UsageCounts, usize)>, CompactError> {
        let Some(usage) = self.usage.as_mut() else {
            return Ok(None);
        };
        let full = usage.absorb_from(rows, from, self.usage_budget, Some(take))?;
        let held =
            usage.estimated_bytes() + full.as_ref().map_or(0, |(full, _)| full.estimated_bytes());
        self.usage_peak = self.usage_peak.max(held);
        Ok(full)
    }

    /// The rows of the next row group in sort order, at most `rows` and,
    /// for a bounded read, as many as gathering within `max_bytes` allows,
    /// one at least; `None` when every input is spent.
    pub(crate) async fn pick(
        &mut self,
        schema: &SchemaRef,
        rows: usize,
        max_bytes: usize,
    ) -> Result<Option<Picks>, CompactError> {
        for (input, row) in std::mem::take(&mut self.deferred) {
            self.advance(input, row, schema).await?;
        }
        for input in &mut self.inputs {
            input.loaded_in_group = false;
        }
        let mut picked: Vec<(usize, usize)> = Vec::new();
        let mut bytes = 0usize;
        let mut first = 0;
        while picked.len() < rows {
            let Some(head) = self.heap.pop() else {
                break;
            };
            let Reverse((_, _, input, row, slot)) = head;
            let (size, last) =
                self.slots
                    .get(slot)
                    .and_then(Option::as_ref)
                    .map_or((0, true), |s| {
                        // A gathered copy may grow its buffers to twice it.
                        let size = s.sizes.get(row).copied().unwrap_or(0).saturating_mul(2);
                        (size, row + 1 >= s.timestamps.len())
                    });
            if !picked.is_empty() && bytes.saturating_add(size) > max_bytes {
                self.heap.push(head);
                break;
            }
            bytes = bytes.saturating_add(size);
            if picked.is_empty() {
                first = input;
            }
            picked.push((slot, row));
            let third =
                self.bounded && last && self.inputs.get(input).is_some_and(|i| i.loaded_in_group);
            if third {
                self.deferred.push((input, row + 1));
                break;
            }
            self.advance(input, row + 1, schema).await?;
        }
        Ok((!picked.is_empty()).then_some(Picks {
            picked,
            bytes,
            first,
        }))
    }

    /// Gathers `picks` into one batch, with which of its rows come from
    /// inputs whose usage was not counted, and frees the batches no input
    /// reads any more.
    ///
    /// # Errors
    /// A pick of a freed batch, or an Arrow failure.
    pub(crate) fn gather(
        &mut self,
        schema: &SchemaRef,
        picks: &Picks,
    ) -> Result<(RecordBatch, Vec<bool>), CompactError> {
        let take = picks
            .picked
            .iter()
            .map(|(slot, _)| {
                self.slots
                    .get(*slot)
                    .and_then(Option::as_ref)
                    .and_then(|s| self.uncounted.get(s.input))
                    .copied()
                    .unwrap_or(false)
            })
            .collect();
        let used: BTreeSet<usize> = picks.picked.iter().map(|(slot, _)| *slot).collect();
        let order: Vec<usize> = used.into_iter().collect();
        let batches = order
            .iter()
            .map(|slot| {
                self.slots
                    .get(*slot)
                    .and_then(Option::as_ref)
                    .map(|s| s.batch.clone())
                    .ok_or(CompactError::Merge("a pick names a dropped batch"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let indices = picks
            .picked
            .iter()
            .map(|(slot, row)| {
                order
                    .binary_search(slot)
                    .map(|position| (position, *row))
                    .map_err(|_| CompactError::Merge("a pick names an unknown batch"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let group = gather(schema, &batches, &indices)?;
        drop(batches);
        self.drop_spent();
        Ok((group, take))
    }

    /// Frees batches no input is reading any more.
    fn drop_spent(&mut self) {
        let current: BTreeSet<usize> = self.inputs.iter().filter_map(|i| i.slot).collect();
        for (index, slot) in self.slots.iter_mut().enumerate() {
            if !current.contains(&index) {
                *slot = None;
            }
        }
    }
}

fn input(path: &str, read: read::Input, metadata: usize, reserved: usize) -> Input {
    Input {
        path: path.to_owned(),
        read,
        slot: None,
        metadata,
        reserved,
        loaded_in_group: false,
    }
}

/// The oldest of `files` whose two cheapest chunks and metadata fit
/// `share` together, each with its chunks planned from what is left of
/// the share.
async fn bounded_inputs(
    store: Arc<dyn ObjectStore>,
    files: &[FileRecord],
    schema: &SchemaRef,
    (share, rows): (usize, usize),
) -> Result<Vec<Input>, CompactError> {
    let mut opened = Vec::new();
    let mut used = 0usize;
    for file in files {
        let (read, layout) = read::Input::bounded(
            Arc::clone(&store),
            (&file.path, file.size_bytes),
            schema,
            &[col::SERVICE_NAME],
        )
        .await?;
        let need = layout
            .metadata_bytes
            .saturating_add(layout.floor.saturating_mul(2));
        if used.saturating_add(need) > share {
            if opened.is_empty() {
                return Err(CompactError::OverBudget {
                    path: file.path.clone(),
                    floor: layout.floor,
                });
            }
            break;
        }
        used += need;
        opened.push((file.path.as_str(), read, layout));
    }
    let left = (share - used) / (2 * opened.len().max(1));
    Ok(opened
        .into_iter()
        .map(|(path, read, layout)| {
            let chunk = layout.floor + left;
            let read = read.with_chunks(layout.plan(chunk, rows));
            input(
                path,
                read,
                layout.metadata_bytes,
                layout.metadata_bytes + 2 * chunk,
            )
        })
        .collect())
}

/// A loaded batch of one input with its sort keys and, for a bounded
/// read, the size of each row.
struct Slot {
    input: usize,
    batch: RecordBatch,
    services: Vec<Service>,
    timestamps: Vec<i64>,
    sizes: Vec<usize>,
    /// What the slot holds: the batch, its keys and sizes.
    bytes: usize,
}

impl Slot {
    fn load(
        input: usize,
        batch: RecordBatch,
        schema: &SchemaRef,
        with_sizes: bool,
    ) -> Result<Self, CompactError> {
        let batch = coerce(batch, schema)?;
        let service = arrow::compute::cast(
            batch.column(schema.index_of(col::SERVICE_NAME)?),
            &DataType::Utf8,
        )?;
        let mut services = Vec::with_capacity(batch.num_rows());
        let mut shared_bytes = 0usize;
        let mut last: Option<Arc<str>> = None;
        for value in service.as_string::<i32>() {
            let shared = value.map(|value| match &last {
                Some(l) if l.as_ref() == value => Arc::clone(l),
                _ => {
                    let s: Arc<str> = Arc::from(value);
                    shared_bytes += value.len() + 2 * std::mem::size_of::<usize>();
                    last = Some(Arc::clone(&s));
                    s
                }
            });
            services.push(shared);
        }
        drop(service);
        let timestamps = batch
            .column(schema.index_of(col::TIMESTAMP)?)
            .as_primitive::<TimestampNanosecondType>()
            .values()
            .to_vec();
        let sizes = if with_sizes {
            row_sizes(&batch)?
        } else {
            Vec::new()
        };
        let bytes = batch.get_array_memory_size()
            + services.capacity() * std::mem::size_of::<Service>()
            + shared_bytes
            + timestamps.capacity() * std::mem::size_of::<i64>()
            + sizes.capacity() * std::mem::size_of::<usize>();
        Ok(Self {
            input,
            batch,
            services,
            timestamps,
            sizes,
            bytes,
        })
    }
}

/// Casts columns whose type differs from the declared schema, as a file
/// read back without its Arrow metadata would need.
fn coerce(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, CompactError> {
    if batch.schema().fields() == schema.fields() {
        return Ok(batch);
    }
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let index = batch.schema().index_of(field.name())?;
            let column = batch.column(index);
            if column.data_type() == field.data_type() {
                Ok(Arc::clone(column))
            } else {
                Ok(arrow::compute::cast(column, field.data_type())?)
            }
        })
        .collect::<Result<Vec<_>, CompactError>>()?;
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

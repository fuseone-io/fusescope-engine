//! Charges held for as long as the arrays they pay for: the reservation is
//! handed to the arrays' buffers, which release it when the last of them
//! is dropped, wherever the arrays went.

use std::sync::Mutex;

use arrow::array::{Array, ArrayRef};
use datafusion::execution::memory_pool::MemoryReservation;

/// Hands `prepaid`, already granted by the pool, to the buffers of
/// `arrays`; each buffer takes its share, and what is left is released.
pub fn claim(arrays: &[ArrayRef], prepaid: MemoryReservation) {
    let prepaid = Prepaid(Mutex::new(prepaid));
    for array in arrays {
        array.claim(&prepaid);
    }
}

/// The arrow side of a charge already granted: each buffer that claims takes
/// its share of it, and a buffer past it grows it, which exact sizing rules out.
#[derive(Debug)]
struct Prepaid(Mutex<MemoryReservation>);

#[derive(Debug)]
struct Held(MemoryReservation);

impl arrow_buffer::MemoryReservation for Held {
    fn size(&self) -> usize {
        self.0.size()
    }

    fn resize(&mut self, new_size: usize) {
        self.0.resize(new_size);
    }
}

impl arrow_buffer::MemoryPool for Prepaid {
    fn reserve(&self, size: usize) -> Box<dyn arrow_buffer::MemoryReservation> {
        let mut prepaid = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let held = if prepaid.size() >= size {
            prepaid.split(size)
        } else {
            let held = prepaid.take();
            held.grow(size - held.size());
            held
        };
        Box::new(Held(held))
    }

    fn available(&self) -> isize {
        let prepaid = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        isize::try_from(prepaid.size()).unwrap_or(isize::MAX)
    }

    fn used(&self) -> usize {
        0
    }

    fn capacity(&self) -> usize {
        self.available().unsigned_abs()
    }
}

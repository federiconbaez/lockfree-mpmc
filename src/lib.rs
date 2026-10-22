//! Bounded lock-free multi-producer / multi-consumer queue.
//!
//! Implements Dmitry Vyukov's classic algorithm
//! (<https://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue>):
//! each slot carries a monotonically increasing sequence number; producers and
//! consumers race with a single CAS on a shared index, then claim their slot
//! with a Release store. Head and tail sit in separate cache lines so that
//! concurrent producers and consumers do not false-share.
//!
//! Safety invariants (checked by loom under `--cfg loom` and by Miri in CI):
//! - a slot is written only after its owning producer has won the CAS on
//!   `head` and observed `seq == head`;
//! - a slot is read only after its owning consumer has won the CAS on `tail`
//!   and observed `seq == tail + 1`;
//! - the Release/Acquire pair on `seq` establishes happens-before between the
//!   write of the payload and its subsequent read.

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]
#![deny(missing_docs, rust_2018_idioms)]

mod sync;

use core::mem::MaybeUninit;
use core::ptr;
use crossbeam_utils::CachePadded;

use sync::{AtomicUsize, Ordering, UnsafeCell};

/// A single queue slot: sequence + payload.
///
/// Layout is intentionally not cache-padded per-slot: padding the indices is
/// enough to remove the hot false-sharing path, and padding every slot would
/// blow up the working set for large capacities.
struct Slot<T> {
    sequence: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

/// Bounded lock-free MPMC queue.
///
/// Capacity must be a power of two and at least 2. The queue never allocates
/// after construction; `push` returns `Err(T)` when full and `pop` returns
/// `None` when empty.
pub struct MpmcQueue<T> {
    buffer: Box<[Slot<T>]>,
    mask: usize,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
}

// Safety: every access to a slot is gated by the CAS on head/tail plus the
// Acquire load of `sequence`. The algorithm guarantees that at most one
// producer owns a given slot for writing and at most one consumer owns it for
// reading at any time. `T: Send` is therefore sufficient for both `Send` and
// `Sync` of the queue itself.
unsafe impl<T: Send> Send for MpmcQueue<T> {}
unsafe impl<T: Send> Sync for MpmcQueue<T> {}

impl<T> MpmcQueue<T> {
    /// Create a new queue with the given capacity.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is less than 2 or is not a power of two.
    pub fn new(capacity: usize) -> Self {
        assert!(
            capacity >= 2 && capacity.is_power_of_two(),
            "capacity must be a power of two and at least 2 (got {capacity})"
        );

        let mut buffer = Vec::with_capacity(capacity);
        for i in 0..capacity {
            buffer.push(Slot {
                sequence: AtomicUsize::new(i),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            });
        }

        Self {
            buffer: buffer.into_boxed_slice(),
            mask: capacity - 1,
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
        }
    }

    /// Maximum number of elements the queue can hold.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.mask + 1
    }

    /// Approximate number of elements currently in the queue.
    ///
    /// Concurrent producers/consumers may make this stale immediately; it is
    /// intended for metrics, not for control flow.
    pub fn len(&self) -> usize {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        head.wrapping_sub(tail)
    }

    /// Returns `true` if the queue appears empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Push `value` into the queue.
    ///
    /// Returns `Ok(())` on success, or `Err(value)` if the queue is currently
    /// full (the value is given back so the caller can retry or drop it).
    pub fn push(&self, value: T) -> Result<(), T> {
        let mut pos = self.head.load(Ordering::Relaxed);
        loop {
            let slot = &self.buffer[pos & self.mask];
            let seq = slot.sequence.load(Ordering::Acquire);
            // Cast through isize so the wrap-around of a full cycle is
            // interpreted as a negative difference (queue full).
            let dif = seq as isize - pos as isize;

            if dif == 0 {
                match self.head.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // We own the slot. Write the payload and publish it.
                        slot.value.with_mut(|ptr| unsafe {
                            ptr::write(ptr, MaybeUninit::new(value));
                        });
                        slot.sequence.store(pos.wrapping_add(1), Ordering::Release);
                        return Ok(());
                    }
                    Err(actual) => {
                        pos = actual;
                    }
                }
            } else if dif < 0 {
                // Slot still holds a previous item → queue is full.
                return Err(value);
            } else {
                // Another producer advanced head; reload and retry.
                pos = self.head.load(Ordering::Relaxed);
            }
        }
    }

    /// Pop one element from the queue, or `None` if it is currently empty.
    pub fn pop(&self) -> Option<T> {
        let mut pos = self.tail.load(Ordering::Relaxed);
        loop {
            let slot = &self.buffer[pos & self.mask];
            let seq = slot.sequence.load(Ordering::Acquire);
            let dif = seq as isize - pos.wrapping_add(1) as isize;

            if dif == 0 {
                match self.tail.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // We own the slot. Read the payload and release it
                        // for the next producer cycle.
                        let value = slot
                            .value
                            .with(|ptr| unsafe { ptr::read(ptr).assume_init() });
                        slot.sequence.store(
                            pos.wrapping_add(self.mask).wrapping_add(1),
                            Ordering::Release,
                        );
                        return Some(value);
                    }
                    Err(actual) => {
                        pos = actual;
                    }
                }
            } else if dif < 0 {
                // Producer has not published yet → queue is empty.
                return None;
            } else {
                pos = self.tail.load(Ordering::Relaxed);
            }
        }
    }
}

impl<T> Drop for MpmcQueue<T> {
    fn drop(&mut self) {
        // Drain remaining initialized elements so their Drop runs.
        while self.pop().is_some() {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn single_thread_roundtrip() {
        let q = MpmcQueue::new(4);
        assert!(q.is_empty());
        q.push(1).unwrap();
        q.push(2).unwrap();
        assert_eq!(q.len(), 2);
        assert_eq!(q.pop(), Some(1));
        assert_eq!(q.pop(), Some(2));
        assert!(q.pop().is_none());
    }

    #[test]
    fn full_returns_value() {
        let q = MpmcQueue::new(2);
        q.push(10).unwrap();
        q.push(20).unwrap();
        assert_eq!(q.push(30), Err(30));
        assert_eq!(q.pop(), Some(10));
        q.push(30).unwrap();
        assert_eq!(q.pop(), Some(20));
        assert_eq!(q.pop(), Some(30));
    }

    #[test]
    #[should_panic]
    fn rejects_non_power_of_two() {
        let _ = MpmcQueue::<u8>::new(3);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn multi_producer_multi_consumer() {
        const N: usize = 50_000;
        const PRODUCERS: usize = 4;
        const CONSUMERS: usize = 4;
        let q = Arc::new(MpmcQueue::new(1024));

        let producers: Vec<_> = (0..PRODUCERS)
            .map(|p| {
                let q = Arc::clone(&q);
                thread::spawn(move || {
                    let start = p * N;
                    for i in start..start + N {
                        while q.push(i).is_err() {
                            thread::yield_now();
                        }
                    }
                })
            })
            .collect();

        let consumers: Vec<_> = (0..CONSUMERS)
            .map(|_| {
                let q = Arc::clone(&q);
                thread::spawn(move || {
                    let mut got = Vec::with_capacity(N);
                    while got.len() < N {
                        if let Some(v) = q.pop() {
                            got.push(v);
                        } else {
                            thread::yield_now();
                        }
                    }
                    got
                })
            })
            .collect();

        for p in producers {
            p.join().unwrap();
        }
        let mut all = Vec::with_capacity(PRODUCERS * N);
        for c in consumers {
            all.extend(c.join().unwrap());
        }

        all.sort_unstable();
        let expected: Vec<_> = (0..PRODUCERS * N).collect();
        assert_eq!(all, expected);
    }

    #[test]
    fn drop_runs_for_remaining_items() {
        use std::sync::atomic::{AtomicUsize, Ordering as StdOrd};
        static DROPPED: AtomicUsize = AtomicUsize::new(0);

        #[derive(Debug)]
        struct Bomb;
        impl Drop for Bomb {
            fn drop(&mut self) {
                DROPPED.fetch_add(1, StdOrd::Relaxed);
            }
        }

        DROPPED.store(0, StdOrd::Relaxed);
        {
            let q = MpmcQueue::new(8);
            for _ in 0..5 {
                q.push(Bomb).unwrap();
            }
            let _ = q.pop();
            let _ = q.pop();
        }
        assert_eq!(DROPPED.load(StdOrd::Relaxed), 5);
    }
}

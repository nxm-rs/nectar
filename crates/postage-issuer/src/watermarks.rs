//! Flat per-bucket fill watermarks with lock-free allocation.
//!
//! Allocation compare-and-swaps a single counter from `n` to `n + 1`. It does
//! not fetch-add and roll back: a rollback loses a slot when two overshooters
//! interleave with a dilution.
//!
//! Safety rests on monotonicity. The capacity only grows and a watermark never
//! moves back, so a stale depth bounds the count low and refuses conservatively
//! rather than over-issuing.

use alloc::vec::Vec;
use core::fmt;

use nectar_postage::{Bucket, BucketDepth};
use nectar_primitives::{Mainnet, SwarmSpec};

use crate::counter::CounterError;

/// The interior-mutable words the table is built from: atomics where threads
/// exist, plain cells where they do not.
#[cfg(multi_thread)]
mod word {
    use portable_atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

    // Relaxed throughout. Every access is a read-modify-write of one location
    // that publishes no other data, and the depth only grows.
    const ORDER: Ordering = Ordering::Relaxed;

    #[derive(Debug)]
    pub(super) struct Counter(AtomicU32);

    impl Counter {
        pub(super) const fn new(value: u32) -> Self {
            Self(AtomicU32::new(value))
        }

        pub(super) fn get(&self) -> u32 {
            self.0.load(ORDER)
        }

        /// Moves the watermark from `from` to `to`, or reports the value that
        /// beat it.
        pub(super) fn claim(&self, from: u32, to: u32) -> Result<(), u32> {
            self.0
                .compare_exchange_weak(from, to, ORDER, ORDER)
                .map(|_| ())
        }
    }

    #[derive(Debug)]
    pub(super) struct Depth(AtomicU8);

    impl Depth {
        pub(super) const fn new(value: u8) -> Self {
            Self(AtomicU8::new(value))
        }

        pub(super) fn get(&self) -> u8 {
            self.0.load(ORDER)
        }

        /// Raises the depth, returning the depth before the call.
        pub(super) fn raise(&self, value: u8) -> u8 {
            self.0.fetch_max(value, ORDER)
        }
    }

    #[derive(Debug)]
    pub(super) struct Issued(AtomicU64);

    impl Issued {
        pub(super) const fn new(value: u64) -> Self {
            Self(AtomicU64::new(value))
        }

        pub(super) fn get(&self) -> u64 {
            self.0.load(ORDER)
        }

        pub(super) fn bump(&self) {
            self.0.fetch_add(1, ORDER);
        }
    }
}

/// The interior-mutable words the table is built from: atomics where threads
/// exist, plain cells where they do not.
#[cfg(not(multi_thread))]
mod word {
    use core::cell::Cell;

    #[derive(Debug)]
    pub(super) struct Counter(Cell<u32>);

    impl Counter {
        pub(super) const fn new(value: u32) -> Self {
            Self(Cell::new(value))
        }

        pub(super) fn get(&self) -> u32 {
            self.0.get()
        }

        /// Moves the watermark from `from` to `to`. Nothing else can hold the
        /// cell, so the claim never loses.
        pub(super) fn claim(&self, _from: u32, to: u32) -> Result<(), u32> {
            self.0.set(to);
            Ok(())
        }
    }

    #[derive(Debug)]
    pub(super) struct Depth(Cell<u8>);

    impl Depth {
        pub(super) const fn new(value: u8) -> Self {
            Self(Cell::new(value))
        }

        pub(super) fn get(&self) -> u8 {
            self.0.get()
        }

        /// Raises the depth, returning the depth before the call.
        pub(super) fn raise(&self, value: u8) -> u8 {
            let previous = self.0.get();
            self.0.set(previous.max(value));
            previous
        }
    }

    #[derive(Debug)]
    pub(super) struct Issued(Cell<u64>);

    impl Issued {
        pub(super) const fn new(value: u64) -> Self {
            Self(Cell::new(value))
        }

        pub(super) fn get(&self) -> u64 {
            self.0.get()
        }

        pub(super) fn bump(&self) {
            self.0.set(self.0.get().saturating_add(1));
        }
    }
}

use word::{Counter, Depth, Issued};

/// Per-bucket fill watermarks: `counts[b]` is the next unused slot of bucket
/// `b`, in `[0, capacity]`.
///
/// The table is flat rather than sharded, so the whole bucket space costs
/// `4 * 2^bucket_depth` bytes once instead of once per shard.
pub(crate) struct Watermarks<S: SwarmSpec = Mainnet> {
    depth: Depth,
    bucket_depth: BucketDepth<S>,
    counts: Vec<Counter>,
    issued: Issued,
}

impl<S: SwarmSpec> fmt::Debug for Watermarks<S> {
    // Geometry only: the counters are one word per bucket, so printing them
    // dumps the whole bucket space.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Watermarks")
            .field("depth", &self.depth.get())
            .field("bucket_depth", &self.bucket_depth.get())
            .field("issued", &self.issued.get())
            .finish_non_exhaustive()
    }
}

impl<S: SwarmSpec> Clone for Watermarks<S> {
    fn clone(&self) -> Self {
        Self {
            depth: Depth::new(self.depth.get()),
            bucket_depth: self.bucket_depth,
            counts: self.counts.iter().map(|c| Counter::new(c.get())).collect(),
            issued: Issued::new(self.issued.get()),
        }
    }
}

impl<S: SwarmSpec> Watermarks<S> {
    /// Creates an empty table for the given geometry.
    pub(crate) fn new(depth: u8, bucket_depth: BucketDepth<S>) -> Self {
        let buckets = 1usize << bucket_depth.get();
        let mut counts = Vec::with_capacity(buckets);
        counts.resize_with(buckets, || Counter::new(0));
        Self {
            depth: Depth::new(depth),
            bucket_depth,
            counts,
            issued: Issued::new(0),
        }
    }

    /// Returns the batch depth.
    pub(crate) fn depth(&self) -> u8 {
        self.depth.get()
    }

    /// Returns the bucket (uniformity) depth.
    pub(crate) const fn bucket_depth(&self) -> BucketDepth<S> {
        self.bucket_depth
    }

    /// Returns the number of slots per bucket, `2^(depth - bucket_depth)`.
    // An unvalidated geometry saturates instead of panicking; the constructors
    // that take a `Batch` keep the shift within range.
    pub(crate) fn bucket_capacity(&self) -> u32 {
        let slot_bits = self.depth.get().saturating_sub(self.bucket_depth.get());
        1u32.checked_shl(u32::from(slot_bits)).unwrap_or(u32::MAX)
    }

    /// Returns the watermark of a bucket.
    pub(crate) fn count(&self, bucket: u32) -> Result<u32, CounterError> {
        self.counter(bucket).map(Counter::get)
    }

    /// Returns whether a fresh slot remains in `bucket`.
    pub(crate) fn has_capacity(&self, bucket: u32) -> Result<bool, CounterError> {
        Ok(self.count(bucket)? < self.bucket_capacity())
    }

    /// Returns the highest watermark across all buckets.
    pub(crate) fn max_count(&self) -> u32 {
        self.counts.iter().map(Counter::get).max().unwrap_or(0)
    }

    /// Returns the lifetime number of slots handed out.
    pub(crate) fn total_issued(&self) -> u64 {
        self.issued.get()
    }

    /// Claims the next free slot of `bucket` and returns it.
    ///
    /// A bucket cut at another depth is refused: it is inside this table's
    /// bucket range, so a range check alone would not catch it.
    ///
    /// # Errors
    ///
    /// [`CounterError::BucketDepthMismatch`] for a bucket cut at another depth,
    /// [`CounterError::InvalidBucket`] for one outside the table, and
    /// [`CounterError::BucketFull`] once the bucket has no slot left.
    pub(crate) fn allocate(&self, bucket: Bucket<S>) -> Result<u32, CounterError> {
        if bucket.depth() != self.bucket_depth {
            return Err(CounterError::BucketDepthMismatch {
                expected: self.bucket_depth.get(),
                got: bucket.depth().get(),
            });
        }
        let bucket = bucket.value();
        let counter = self.counter(bucket)?;
        let mut count = counter.get();
        loop {
            // Re-read the capacity every attempt: a dilution that lands mid-loop
            // reopens the bucket, and a stale read only refuses early.
            let capacity = self.bucket_capacity();
            if count >= capacity {
                return Err(CounterError::BucketFull { bucket, capacity });
            }
            match counter.claim(count, count.saturating_add(1)) {
                Ok(()) => {
                    self.issued.bump();
                    return Ok(count);
                }
                Err(observed) => count = observed,
            }
        }
    }

    /// Raises the batch depth to `new_depth`, growing the per-bucket capacity
    /// without moving a watermark.
    ///
    /// The raise is a maximum, so a redelivered or reordered chain event is a
    /// no-op.
    pub(crate) fn raise_depth(&self, new_depth: u8) -> u8 {
        self.depth.raise(new_depth)
    }

    fn counter(&self, bucket: u32) -> Result<&Counter, CounterError> {
        usize::try_from(bucket)
            .ok()
            .and_then(|index| self.counts.get(index))
            .ok_or(CounterError::InvalidBucket { bucket })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bucket_depth() -> BucketDepth {
        BucketDepth::new(16).unwrap()
    }

    fn at(bucket: u32) -> Bucket {
        Bucket::checked(bucket, bucket_depth()).unwrap()
    }

    #[test]
    fn allocation_is_a_monotone_watermark_that_refuses_a_full_bucket() {
        // depth 17 over bucket depth 16 gives 2 slots per bucket.
        let table: Watermarks = Watermarks::new(17, bucket_depth());
        assert_eq!(table.allocate(at(5)), Ok(0));
        assert_eq!(table.allocate(at(5)), Ok(1));
        assert_eq!(
            table.allocate(at(5)),
            Err(CounterError::BucketFull {
                bucket: 5,
                capacity: 2
            })
        );
        assert_eq!(table.count(5), Ok(2));
        assert_eq!(table.total_issued(), 2);
        assert_eq!(table.max_count(), 2);
    }

    #[test]
    fn a_bucket_cut_at_another_depth_never_lands_in_a_counter() {
        let table: Watermarks = Watermarks::new(24, BucketDepth::new(20).unwrap());
        let shallow = Bucket::checked(5, bucket_depth()).unwrap();
        assert_eq!(
            table.allocate(shallow),
            Err(CounterError::BucketDepthMismatch {
                expected: 20,
                got: 16
            })
        );
        assert_eq!(table.count(5), Ok(0));
        assert_eq!(table.total_issued(), 0);
    }

    #[test]
    fn a_bucket_outside_the_table_is_refused() {
        let table: Watermarks = Watermarks::new(20, bucket_depth());
        assert_eq!(
            table.count(0x1_0000),
            Err(CounterError::InvalidBucket { bucket: 0x1_0000 })
        );
        assert_eq!(
            table.has_capacity(0x1_0000),
            Err(CounterError::InvalidBucket { bucket: 0x1_0000 })
        );
    }

    #[test]
    fn a_raise_takes_the_maximum_and_leaves_watermarks_alone() {
        let table: Watermarks = Watermarks::new(17, bucket_depth());
        assert_eq!(table.allocate(at(5)), Ok(0));
        assert_eq!(table.raise_depth(18), 17);
        assert_eq!(table.bucket_capacity(), 4);
        // A redelivered or reordered event never shrinks the batch.
        assert_eq!(table.raise_depth(17), 18);
        assert_eq!(table.depth(), 18);
        assert_eq!(table.count(5), Ok(1));
        assert_eq!(table.allocate(at(5)), Ok(1));
    }
}

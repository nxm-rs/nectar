//! Flat per-bucket fill watermarks with lock-free allocation.
//!
//! Allocation compare-and-swaps one counter from `n` to `n + 1`. Fetch-add and
//! roll back loses a slot when two overshooters interleave with a dilution.

use alloc::vec::Vec;
use core::fmt;

use nectar_postage::{Bucket, BucketDepth};
use nectar_primitives::{Mainnet, SwarmSpec};

use crate::counter::CounterError;

/// Core atomics, so the claim contract holds with or without threads.
mod word {
    use core::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

    // Relaxed: every access is a read-modify-write of one location and
    // publishes no other data.
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

        /// Acquire, so this is a second load and not the first one folded.
        pub(super) fn reload(&self) -> u8 {
            self.0.load(Ordering::Acquire)
        }

        /// Raises the depth, returning the depth before the call.
        pub(super) fn raise(&self, value: u8) -> u8 {
            self.0.fetch_max(value, Ordering::AcqRel)
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

use word::{Counter, Depth, Issued};

/// Per-bucket fill watermarks: `counts[b]` is the next unused slot of bucket
/// `b`, in `[0, capacity]`.
pub(crate) struct Watermarks<S: SwarmSpec = Mainnet> {
    depth: Depth,
    bucket_depth: BucketDepth<S>,
    counts: Vec<Counter>,
    issued: Issued,
}

impl<S: SwarmSpec> fmt::Debug for Watermarks<S> {
    // Counters omitted: one word per bucket is the whole bucket space.
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

    pub(crate) fn depth(&self) -> u8 {
        self.depth.get()
    }

    pub(crate) const fn bucket_depth(&self) -> BucketDepth<S> {
        self.bucket_depth
    }

    /// Slots per bucket, `2^(depth - bucket_depth)`.
    pub(crate) fn bucket_capacity(&self) -> u32 {
        self.capacity_at(self.depth.get())
    }

    fn reloaded_capacity(&self) -> u32 {
        self.capacity_at(self.depth.reload())
    }

    // An unvalidated geometry saturates rather than panicking; the `Batch`
    // constructors keep the shift in range.
    fn capacity_at(&self, depth: u8) -> u32 {
        let slot_bits = depth.saturating_sub(self.bucket_depth.get());
        1u32.checked_shl(u32::from(slot_bits)).unwrap_or(u32::MAX)
    }

    pub(crate) fn count(&self, bucket: u32) -> Result<u32, CounterError> {
        self.counter(bucket).map(Counter::get)
    }

    pub(crate) fn has_capacity(&self, bucket: u32) -> Result<bool, CounterError> {
        Ok(self.count(bucket)? < self.bucket_capacity())
    }

    pub(crate) fn max_count(&self) -> u32 {
        self.counts.iter().map(Counter::get).max().unwrap_or(0)
    }

    pub(crate) fn total_issued(&self) -> u64 {
        self.issued.get()
    }

    /// Claims the next free slot of `bucket` and returns it.
    ///
    /// A bucket cut at another depth is refused: it is inside this table's
    /// bucket range, so a range check alone would not catch it.
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
        let mut reloaded = false;
        loop {
            // A dilution landing mid-loop reopens the bucket, so a full bucket
            // is confirmed by one fresh load before it is refused. Once per
            // call: the retry is a courtesy on a path that was going to fail.
            let mut capacity = self.bucket_capacity();
            if count >= capacity {
                if reloaded {
                    return Err(CounterError::BucketFull { bucket, capacity });
                }
                reloaded = true;
                capacity = self.reloaded_capacity();
                if count >= capacity {
                    return Err(CounterError::BucketFull { bucket, capacity });
                }
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

    /// Raises the batch depth, growing the per-bucket capacity without moving a
    /// watermark. A maximum, so a redelivered or reordered event is a no-op.
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
        assert_eq!(table.raise_depth(17), 18);
        assert_eq!(table.depth(), 18);
        assert_eq!(table.count(5), Ok(1));
        assert_eq!(table.allocate(at(5)), Ok(1));
    }
}

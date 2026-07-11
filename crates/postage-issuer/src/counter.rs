//! The shared per-bucket slot counter table.
//!
//! Every issuer in this crate, and the self-hosted snapshot in
//! `nectar-postage-usage`, tracks the same per-bucket state: for each of the
//! `2^bucket_depth` collision buckets, how far issuance has advanced into the
//! bucket's `2^(depth - bucket_depth)` slots. [`CounterTable`] is that state and
//! the single counter-advance primitive behind all of them, so the advance logic
//! lives in exactly one place.
//!
//! # Representation
//!
//! `counts[b]` lives in `[0, capacity]` in both modes and is the value the
//! `nectar-postage-usage` wire format serializes verbatim:
//!
//! - [`CounterMode::Fill`]: `counts[b]` is a monotone fill watermark, the next
//!   unused slot. Issuance returns the watermark and advances it; a full bucket
//!   fails with [`CounterError::BucketFull`] rather than overwriting.
//! - [`CounterMode::Ring`]: `counts[b]` is a ring cursor. A cursor equal to
//!   `capacity` means "wrap on the next write": the wrap is deferred so the
//!   cursor stays in `[0, capacity]` exactly as it appears on the wire. Issuance
//!   wraps at the capacity, so a full bucket churns instead of failing.
//!
//! In both modes `total_issued()` is the running sum of the counters. For a fill
//! table that is the lifetime stamp count. For a ring table it is a deterministic
//! checksum, not a lifetime count: a wrapped bucket is full yet its cursor may be
//! small, so the sum does not count overwrites. The snapshot codec writes and
//! re-checks this sum in both modes.

extern crate alloc;

use alloc::vec;
use alloc::vec::Vec;

use thiserror::Error;

/// Whether a [`CounterTable`] fills each bucket once or wraps it as a ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CounterMode {
    /// Fill-only: a monotone watermark per bucket; a full bucket is refused.
    Fill,
    /// Ring: a wrapping cursor per bucket; a full bucket overwrites the oldest
    /// slot.
    Ring,
}

/// An error advancing or constructing a [`CounterTable`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum CounterError {
    /// A bucket index is outside the table's bucket range.
    #[error("bucket {bucket} out of range")]
    InvalidBucket {
        /// The offending bucket index.
        bucket: u32,
    },

    /// A fill bucket has no remaining slot. Never produced in ring mode.
    #[error("bucket {bucket} has reached capacity {capacity}")]
    BucketFull {
        /// The full bucket.
        bucket: u32,
        /// The bucket capacity.
        capacity: u32,
    },

    /// Every slot in a ring bucket is protected, so the ring cannot advance
    /// without re-emitting a protected slot. The batch geometry forbids this at
    /// real depths, so it signals a malformed reservation.
    #[error("ring bucket {bucket} has no unprotected slot to issue")]
    RingExhausted {
        /// The exhausted bucket.
        bucket: u32,
    },

    /// A counter vector does not match the bucket count.
    #[error("expected {expected} counters, got {got}")]
    CounterLength {
        /// The expected number of counters.
        expected: usize,
        /// The number of counters provided.
        got: usize,
    },

    /// A counter exceeds the per-bucket slot capacity.
    #[error("counter {count} for bucket {bucket} exceeds capacity {capacity}")]
    CounterOverflow {
        /// The offending bucket.
        bucket: u32,
        /// The counter value.
        count: u32,
        /// The bucket capacity.
        capacity: u32,
    },
}

/// Per-bucket slot counters in the `[0, capacity]` deferred-wrap representation.
///
/// This is the shared engine behind every issuer in this crate and behind the
/// self-hosted snapshot in `nectar-postage-usage`. It holds only the counters
/// and the geometry needed to advance them; it is address-agnostic, so callers
/// map a chunk address to a bucket and hand the bucket here.
///
/// The advance primitive is [`record`](Self::record). It takes a predicate that
/// answers whether a `(bucket, slot)` is protected, so a ring never re-emits a
/// reserved slot. Fill mode ignores the predicate; nothing protects a fill
/// watermark because it only ever moves forward.
#[derive(Debug, Clone)]
pub struct CounterTable {
    depth: u8,
    bucket_depth: u8,
    mode: CounterMode,
    counts: Vec<u32>,
    issued: u64,
}

impl PartialEq for CounterTable {
    fn eq(&self, other: &Self) -> bool {
        self.depth == other.depth
            && self.bucket_depth == other.bucket_depth
            && self.mode == other.mode
            && self.counts == other.counts
            && self.issued == other.issued
    }
}

impl Eq for CounterTable {}

impl CounterTable {
    /// Creates an empty table for the given geometry and mode.
    pub fn new(depth: u8, bucket_depth: u8, mode: CounterMode) -> Self {
        Self {
            depth,
            bucket_depth,
            mode,
            counts: vec![0u32; 1usize << bucket_depth],
            issued: 0,
        }
    }

    /// Creates a table from existing counters, each in `[0, capacity]`.
    ///
    /// `counts` must hold exactly `2^bucket_depth` entries. The issued total is
    /// recomputed as their sum.
    ///
    /// # Errors
    ///
    /// Returns [`CounterError::CounterLength`] if the vector length does not match
    /// the bucket count, or [`CounterError::CounterOverflow`] if any counter
    /// exceeds the bucket capacity.
    pub fn from_counts(
        depth: u8,
        bucket_depth: u8,
        mode: CounterMode,
        counts: Vec<u32>,
    ) -> Result<Self, CounterError> {
        let expected = 1usize << bucket_depth;
        if counts.len() != expected {
            return Err(CounterError::CounterLength {
                expected,
                got: counts.len(),
            });
        }
        // Batch geometry invariant: depth >= bucket_depth, enforced by callers.
        #[allow(clippy::arithmetic_side_effects)]
        let capacity = 1u32 << (depth - bucket_depth);
        let mut issued = 0u64;
        for (bucket, &count) in counts.iter().enumerate() {
            if count > capacity {
                // `bucket` indexes a vector whose length was validated above to
                // be `2^bucket_depth`; a table wider than `u32` is geometrically
                // impossible (the bucket space is addressed by `u32` throughout).
                #[allow(clippy::as_conversions)]
                return Err(CounterError::CounterOverflow {
                    bucket: bucket as u32,
                    count,
                    capacity,
                });
            }
            // Sum of at most 2^bucket_depth u32 counters cannot overflow a u64.
            #[allow(clippy::arithmetic_side_effects)]
            {
                issued += u64::from(count);
            }
        }
        Ok(Self {
            depth,
            bucket_depth,
            mode,
            counts,
            issued,
        })
    }

    /// Returns the batch depth.
    pub const fn depth(&self) -> u8 {
        self.depth
    }

    /// Returns the bucket (uniformity) depth.
    pub const fn bucket_depth(&self) -> u8 {
        self.bucket_depth
    }

    /// Returns whether this is a ring (`true`) or a fill watermark (`false`).
    pub const fn is_ring(&self) -> bool {
        matches!(self.mode, CounterMode::Ring)
    }

    /// Returns the number of collision buckets (`2^bucket_depth`).
    pub const fn bucket_count(&self) -> u32 {
        1u32 << self.bucket_depth
    }

    /// Returns the number of slots per bucket (`2^(depth - bucket_depth)`).
    // Batch geometry invariant: depth >= bucket_depth, enforced at construction.
    #[allow(clippy::arithmetic_side_effects)]
    pub const fn bucket_capacity(&self) -> u32 {
        1u32 << (self.depth - self.bucket_depth)
    }

    /// Returns the total batch capacity in slots (`2^depth`).
    pub const fn total_capacity(&self) -> u64 {
        1u64 << self.depth
    }

    /// Returns the per-bucket counters.
    pub fn counts(&self) -> &[u32] {
        &self.counts
    }

    /// Returns the counter of a bucket.
    // `u32` always fits `usize` on the >=32-bit targets this crate supports.
    #[allow(clippy::as_conversions)]
    pub fn count(&self, bucket: u32) -> Result<u32, CounterError> {
        self.counts
            .get(bucket as usize)
            .copied()
            .ok_or(CounterError::InvalidBucket { bucket })
    }

    /// Returns the highest counter across all buckets.
    pub fn max_count(&self) -> u32 {
        self.counts.iter().copied().max().unwrap_or(0)
    }

    /// Returns the lowest counter across all buckets.
    pub fn min_count(&self) -> u32 {
        self.counts.iter().copied().min().unwrap_or(0)
    }

    /// Returns the sum of the per-bucket counters: the lifetime stamp count in
    /// fill mode, a deterministic checksum in ring mode.
    pub const fn total_issued(&self) -> u64 {
        self.issued
    }

    /// Returns whether a fresh, never-written slot remains in `bucket`.
    ///
    /// A wrapped ring reports no spare capacity even though issuance into it
    /// still succeeds by overwriting an earlier slot.
    pub fn has_capacity(&self, bucket: u32) -> Result<bool, CounterError> {
        Ok(self.count(bucket)? < self.bucket_capacity())
    }

    /// Advances the counter of `bucket`, skipping any slot for which
    /// `is_protected` returns `true`, and returns the assigned slot.
    ///
    /// Fill mode returns the watermark and bumps it, failing with
    /// [`CounterError::BucketFull`] at capacity; the predicate is unused because a
    /// monotone watermark never lands on a reserved slot. Ring mode starts at the
    /// cursor (wrapping a cursor that equals the capacity), skips protected slots,
    /// returns the slot, and stores the cursor just past it in `[0, capacity]`. A
    /// ring whose every slot is protected fails with
    /// [`CounterError::RingExhausted`]; the geometry forbids this at real depths.
    pub fn record(
        &mut self,
        bucket: u32,
        is_protected: impl Fn(u32) -> bool,
    ) -> Result<u32, CounterError> {
        // `u32` always fits `usize` on the >=32-bit targets this crate supports.
        #[allow(clippy::as_conversions)]
        let bucket_idx = bucket as usize;
        if bucket_idx >= self.counts.len() {
            return Err(CounterError::InvalidBucket { bucket });
        }
        let capacity = self.bucket_capacity();

        if matches!(self.mode, CounterMode::Fill) {
            // Indexing guarded by the bucket range check at the top of `record`.
            #[allow(clippy::indexing_slicing)]
            let count = &mut self.counts[bucket_idx];
            if *count >= capacity {
                return Err(CounterError::BucketFull { bucket, capacity });
            }
            let index = *count;
            // `*count < capacity <= u32::MAX` (checked above) and the u64 issued
            // total cannot overflow before the u32 counters do.
            #[allow(clippy::arithmetic_side_effects)]
            {
                *count += 1;
                self.issued += 1;
            }
            return Ok(index);
        }

        // Indexing guarded by the bucket range check at the top of `record`.
        #[allow(clippy::indexing_slicing)]
        let old_cursor = self.counts[bucket_idx];
        // Start at the cursor; a cursor equal to capacity means "wrap on the next
        // write", resetting to 0 when the bucket bound is reached.
        let mut candidate = if old_cursor >= capacity {
            0
        } else {
            old_cursor
        };
        // Skip protected slots, wrapping. Bounded by `capacity` steps: if every
        // slot is protected we fail rather than loop.
        let mut steps = 0u32;
        // `candidate < capacity` throughout (initial value and the modulo keep it
        // there), `capacity >= 1` (a power of two), and `steps < capacity`, so
        // neither increment can overflow and the modulo divisor is nonzero.
        #[allow(clippy::arithmetic_side_effects)]
        while is_protected(candidate) {
            candidate = (candidate + 1) % capacity;
            steps += 1;
            if steps >= capacity {
                return Err(CounterError::RingExhausted { bucket });
            }
        }
        let index = candidate;
        // The new cursor points just past the slot we returned. Storing
        // `capacity` (rather than wrapping to 0 here) defers the wrap to the next
        // write, keeping the cursor in [0, capacity] as on the wire.
        // `index < capacity <= u32::MAX`, so the increment cannot overflow.
        #[allow(clippy::arithmetic_side_effects)]
        let new_cursor = index + 1;
        // Indexing guarded by the bucket range check at the top of `record`.
        #[allow(clippy::indexing_slicing)]
        {
            self.counts[bucket_idx] = new_cursor;
        }
        // Keep issued == sum(counts): fold in the signed delta (it decreases on
        // wrap, when new_cursor < old_cursor). `issued == sum(counts) >=
        // old_cursor` (it is one of the summands), so the subtraction cannot
        // underflow, and the sum of 2^bucket_depth u32 counters fits a u64.
        #[allow(clippy::arithmetic_side_effects)]
        {
            self.issued = self.issued - u64::from(old_cursor) + u64::from(new_cursor);
        }
        Ok(index)
    }

    /// Increases the batch depth after an on-chain dilution, growing the
    /// per-bucket capacity without moving any counter.
    ///
    /// The caller must reject a depth decrease before calling: the table adopts
    /// whatever depth it is handed. Issuers check the non-decrease against their
    /// own error type, and `nectar-postage-usage` validates the new geometry
    /// against the snapshot format first.
    pub const fn set_depth(&mut self, new_depth: u8) {
        self.depth = new_depth;
    }

    /// Takes the elementwise maximum of this table's counters and `other`'s,
    /// adopting `new_depth`, and recomputes the issued sum.
    ///
    /// The caller must ensure both tables share a bucket geometry and are fill
    /// tables; the elementwise maximum is only a valid join for monotone fill
    /// counters. `nectar-postage-usage` enforces that before calling.
    pub fn merge_counts_max(&mut self, other: &Self, new_depth: u8) {
        self.depth = new_depth;
        let mut issued = 0u64;
        for (mine, theirs) in self.counts.iter_mut().zip(other.counts.iter()) {
            *mine = (*mine).max(*theirs);
            // Sum of at most 2^bucket_depth u32 counters cannot overflow a u64.
            #[allow(clippy::arithmetic_side_effects)]
            {
                issued += u64::from(*mine);
            }
        }
        self.issued = issued;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn never(_slot: u32) -> bool {
        false
    }

    #[test]
    fn fill_is_a_monotone_watermark_that_refuses_a_full_bucket() {
        // depth 17, bucket depth 16 gives 2 slots per bucket.
        let mut table = CounterTable::new(17, 16, CounterMode::Fill);
        assert_eq!(table.record(5, never).unwrap(), 0);
        assert_eq!(table.record(5, never).unwrap(), 1);
        assert_eq!(
            table.record(5, never),
            Err(CounterError::BucketFull {
                bucket: 5,
                capacity: 2
            })
        );
        assert_eq!(table.count(5).unwrap(), 2);
        assert_eq!(table.total_issued(), 2);
    }

    #[test]
    fn ring_wraps_and_keeps_the_cursor_in_range() {
        let mut table = CounterTable::new(17, 16, CounterMode::Ring);
        assert_eq!(table.record(5, never).unwrap(), 0);
        assert_eq!(table.record(5, never).unwrap(), 1);
        // The cursor sits at capacity, the deferred-wrap state.
        assert_eq!(table.count(5).unwrap(), 2);
        // The next write wraps back to slot 0.
        assert_eq!(table.record(5, never).unwrap(), 0);
        assert_eq!(table.count(5).unwrap(), 1);
    }

    #[test]
    fn ring_skips_protected_slots() {
        // depth 18, bucket depth 16 gives 4 slots per bucket. Protect 1 and 3.
        let mut table = CounterTable::new(18, 16, CounterMode::Ring);
        let protected = |slot: u32| slot == 1 || slot == 3;
        for _ in 0..20 {
            let slot = table.record(0, protected).unwrap();
            assert!(slot == 0 || slot == 2, "ring emitted protected slot {slot}");
        }
    }

    #[test]
    fn ring_exhausts_when_every_slot_is_protected() {
        let mut table = CounterTable::new(17, 16, CounterMode::Ring);
        assert_eq!(
            table.record(0, |_| true),
            Err(CounterError::RingExhausted { bucket: 0 })
        );
    }

    #[test]
    fn from_counts_sums_and_rejects_overflow() {
        let mut counts = vec![0u32; 1usize << 16];
        counts[7] = 2;
        let table = CounterTable::from_counts(17, 16, CounterMode::Fill, counts).unwrap();
        assert_eq!(table.total_issued(), 2);

        let mut over = vec![0u32; 1usize << 16];
        over[7] = 3;
        assert_eq!(
            CounterTable::from_counts(17, 16, CounterMode::Fill, over),
            Err(CounterError::CounterOverflow {
                bucket: 7,
                count: 3,
                capacity: 2
            })
        );
    }
}

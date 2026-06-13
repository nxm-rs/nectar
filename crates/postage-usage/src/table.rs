//! In-memory per-bucket slot counters for a postage batch.

use alloc::collections::BTreeSet;
use alloc::vec;
use alloc::vec::Vec;

use nectar_postage::{BatchId, StampIndex, calculate_bucket};
use nectar_primitives::SwarmAddress;

use crate::{MAX_BUCKET_DEPTH, MAX_COUNTER_BITS, Result, UsageError};

/// Validates that a batch geometry is within the range supported by the
/// snapshot format: `bucket_depth <= 16` and `depth - bucket_depth <= 31`.
pub(crate) const fn validate_geometry(depth: u8, bucket_depth: u8) -> Result<()> {
    if bucket_depth > MAX_BUCKET_DEPTH
        || depth < bucket_depth
        || depth - bucket_depth > MAX_COUNTER_BITS
    {
        return Err(UsageError::InvalidGeometry {
            depth,
            bucket_depth,
        });
    }
    Ok(())
}

/// Per-bucket slot counters for a postage batch.
///
/// For each of the `2^bucket_depth` collision buckets, tracks the state needed
/// to issue collision-free stamps into its `2^(depth - bucket_depth)` slots.
///
/// - **Immutable** ([`new`](Self::new) / [`from_counts`](Self::from_counts)):
///   `counts[b]` is a monotone fill watermark, the next unused index; issuance
///   advances it and a full bucket fails rather than overwriting.
/// - **Mutable** ([`new_mutable`](Self::new_mutable) /
///   [`from_counts_mutable`](Self::from_counts_mutable)): `counts[b]` is a ring
///   cursor in `[0, capacity]` that wraps at capacity, so a full bucket churns
///   instead of failing. The cursor skips the snapshot's reserved slots
///   (installed via [`Snapshot`](crate::Snapshot)) so issuance never evicts the
///   data that records the batch state.
#[derive(Debug, Clone)]
pub struct UsageTable {
    pub(crate) batch_id: BatchId,
    pub(crate) depth: u8,
    pub(crate) bucket_depth: u8,
    pub(crate) counts: Vec<u32>,
    pub(crate) issued: u64,
    /// Whether this table is a mutable ring (true) or immutable fill watermark
    /// (false).
    pub(crate) mutable: bool,
    /// `(bucket, index)` slots reserved by the snapshot's own chunks, which the
    /// mutable issuance path skips. A derived cache (recomputed from owner and
    /// allocated slots, empty until [`set_reserved`](Self::set_reserved)
    /// installs it), so it is excluded from equality.
    pub(crate) reserved: BTreeSet<(u32, u32)>,
}

impl PartialEq for UsageTable {
    fn eq(&self, other: &Self) -> bool {
        self.batch_id == other.batch_id
            && self.depth == other.depth
            && self.bucket_depth == other.bucket_depth
            && self.counts == other.counts
            && self.issued == other.issued
            && self.mutable == other.mutable
    }
}

impl Eq for UsageTable {}

impl UsageTable {
    /// Creates an empty immutable table for a batch with the given geometry.
    pub fn new(batch_id: BatchId, depth: u8, bucket_depth: u8) -> Result<Self> {
        Self::new_with_mode(batch_id, depth, bucket_depth, false)
    }

    /// Creates an empty mutable (ring-cursor) table.
    ///
    /// Issuance is a per-bucket ring: once a bucket fills it wraps to index `0`.
    /// Install the snapshot's reserved slots through the
    /// [`Snapshot`](crate::Snapshot) entry points before issuing content, so the
    /// ring never evicts the snapshot's own chunks.
    pub fn new_mutable(batch_id: BatchId, depth: u8, bucket_depth: u8) -> Result<Self> {
        Self::new_with_mode(batch_id, depth, bucket_depth, true)
    }

    fn new_with_mode(
        batch_id: BatchId,
        depth: u8,
        bucket_depth: u8,
        mutable: bool,
    ) -> Result<Self> {
        validate_geometry(depth, bucket_depth)?;
        Ok(Self {
            batch_id,
            depth,
            bucket_depth,
            counts: vec![0; 1usize << bucket_depth],
            issued: 0,
            mutable,
            reserved: BTreeSet::new(),
        })
    }

    /// Creates an immutable table from existing counters.
    ///
    /// `counts` must hold exactly `2^bucket_depth` entries, each at most the
    /// bucket capacity.
    pub fn from_counts(
        batch_id: BatchId,
        depth: u8,
        bucket_depth: u8,
        counts: Vec<u32>,
    ) -> Result<Self> {
        Self::from_counts_with_mode(batch_id, depth, bucket_depth, counts, false)
    }

    /// Creates a mutable (ring-cursor) table from existing cursors, each in
    /// `[0, capacity]`. As with [`new_mutable`](Self::new_mutable), install the
    /// reserved slots via [`Snapshot`](crate::Snapshot) before issuing.
    pub fn from_counts_mutable(
        batch_id: BatchId,
        depth: u8,
        bucket_depth: u8,
        counts: Vec<u32>,
    ) -> Result<Self> {
        Self::from_counts_with_mode(batch_id, depth, bucket_depth, counts, true)
    }

    fn from_counts_with_mode(
        batch_id: BatchId,
        depth: u8,
        bucket_depth: u8,
        counts: Vec<u32>,
        mutable: bool,
    ) -> Result<Self> {
        validate_geometry(depth, bucket_depth)?;
        let expected = 1usize << bucket_depth;
        if counts.len() != expected {
            return Err(UsageError::CounterLength {
                expected,
                got: counts.len(),
            });
        }
        let capacity = 1u32 << (depth - bucket_depth);
        let mut issued = 0u64;
        for (bucket, &count) in counts.iter().enumerate() {
            if count > capacity {
                return Err(UsageError::CounterOverflow {
                    bucket: bucket as u32,
                    count,
                    capacity,
                });
            }
            issued += u64::from(count);
        }
        Ok(Self {
            batch_id,
            depth,
            bucket_depth,
            counts,
            issued,
            mutable,
            reserved: BTreeSet::new(),
        })
    }

    /// Returns the batch id this table describes.
    pub const fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    /// Returns whether this table is a mutable ring (`true`) or an immutable
    /// fill watermark (`false`).
    pub const fn is_mutable(&self) -> bool {
        self.mutable
    }

    /// Installs the `(bucket, index)` slots reserved by the snapshot's own
    /// chunks, replacing any previous set. The mutable issuance path skips them;
    /// immutable issuance ignores the set. [`Snapshot`](crate::Snapshot) installs
    /// it so a table recovered from the wire becomes owner-aware before any
    /// mutable issuance.
    pub(crate) fn set_reserved(&mut self, reserved: impl IntoIterator<Item = (u32, u32)>) {
        self.reserved = reserved.into_iter().collect();
    }

    /// Returns the batch depth.
    pub const fn depth(&self) -> u8 {
        self.depth
    }

    /// Returns the bucket (uniformity) depth.
    pub const fn bucket_depth(&self) -> u8 {
        self.bucket_depth
    }

    /// Returns the number of collision buckets (`2^bucket_depth`).
    pub const fn bucket_count(&self) -> u32 {
        1u32 << self.bucket_depth
    }

    /// Returns the number of slots per bucket (`2^(depth - bucket_depth)`).
    pub const fn bucket_capacity(&self) -> u32 {
        1u32 << (self.depth - self.bucket_depth)
    }

    /// Returns the total batch capacity in slots (`2^depth`).
    pub const fn total_capacity(&self) -> u64 {
        1u64 << self.depth
    }

    /// Returns the sum of the per-bucket counters.
    ///
    /// Immutable: the lifetime count of stamps issued. Mutable: the counters are
    /// ring cursors, so this is a deterministic checksum, not a lifetime count
    /// (a wrapped bucket is full yet its cursor may be small). The codec writes
    /// and verifies it as a checksum in both modes.
    pub const fn total_issued(&self) -> u64 {
        self.issued
    }

    /// Returns the per-bucket counters.
    pub fn counts(&self) -> &[u32] {
        &self.counts
    }

    /// Returns the counter of a bucket.
    pub fn count(&self, bucket: u32) -> Result<u32> {
        self.counts
            .get(bucket as usize)
            .copied()
            .ok_or(UsageError::InvalidBucket { bucket })
    }

    /// Returns the highest counter across all buckets.
    pub fn max_count(&self) -> u32 {
        self.counts.iter().copied().max().unwrap_or(0)
    }

    /// Returns the lowest counter across all buckets.
    pub fn min_count(&self) -> u32 {
        self.counts.iter().copied().min().unwrap_or(0)
    }

    /// Returns whether a bucket can accept another slot assignment.
    pub fn has_capacity(&self, bucket: u32) -> Result<bool> {
        Ok(self.count(bucket)? < self.bucket_capacity())
    }

    /// Assigns the next slot in a bucket and returns its index.
    ///
    /// Immutable: the monotone fill watermark, failing with
    /// [`BucketFull`](UsageError::BucketFull) at capacity. Mutable: a ring cursor
    /// that wraps at capacity and skips reserved slots, never returning
    /// `BucketFull` (it returns [`RingExhausted`](UsageError::RingExhausted) only
    /// if every slot is reserved, which the geometry forbids).
    pub fn record(&mut self, bucket: u32) -> Result<u32> {
        let capacity = self.bucket_capacity();
        if bucket as usize >= self.counts.len() {
            return Err(UsageError::InvalidBucket { bucket });
        }
        if self.mutable {
            return self.record_mutable(bucket, capacity);
        }
        let count = &mut self.counts[bucket as usize];
        if *count >= capacity {
            return Err(UsageError::BucketFull { bucket, capacity });
        }
        let index = *count;
        *count += 1;
        self.issued += 1;
        Ok(index)
    }

    /// Advances a mutable ring cursor, skipping reserved slots and wrapping at
    /// capacity. `capacity >= 1` always holds for supported geometry.
    fn record_mutable(&mut self, bucket: u32, capacity: u32) -> Result<u32> {
        let old_cursor = self.counts[bucket as usize];
        // Start at the cursor; a cursor equal to capacity means "wrap on the
        // next write", resetting to 0 when the bucket bound is reached.
        let mut candidate = if old_cursor >= capacity {
            0
        } else {
            old_cursor
        };
        // Skip reserved slots, wrapping. Bounded by `capacity` steps: if every
        // slot is reserved we fail rather than loop.
        let mut steps = 0u32;
        while self.reserved.contains(&(bucket, candidate)) {
            candidate = (candidate + 1) % capacity;
            steps += 1;
            if steps >= capacity {
                return Err(UsageError::RingExhausted { bucket });
            }
        }
        let index = candidate;
        // The new cursor points just past the slot we returned. Storing
        // `capacity` (rather than wrapping to 0 here) defers the wrap to the
        // next write, keeping the cursor in [0, capacity] as on the wire.
        let new_cursor = index + 1;
        self.counts[bucket as usize] = new_cursor;
        // Keep issued == sum(counts): fold in the signed delta (it decreases
        // on wrap, when new_cursor < old_cursor).
        self.issued = self.issued - u64::from(old_cursor) + u64::from(new_cursor);
        Ok(index)
    }

    /// Assigns the next unused slot for a chunk address and returns the
    /// resulting stamp index.
    pub fn record_address(&mut self, address: &SwarmAddress) -> Result<StampIndex> {
        let bucket = calculate_bucket(address, self.bucket_depth);
        let index = self.record(bucket)?;
        Ok(StampIndex::new(bucket, index))
    }

    /// Increases the batch depth after an on-chain dilution.
    ///
    /// Counters are unchanged; only the per-bucket capacity grows. The new
    /// depth must not decrease and must stay within the supported geometry.
    pub fn dilute(&mut self, new_depth: u8) -> Result<()> {
        if new_depth < self.depth {
            return Err(UsageError::DepthDecrease {
                current: self.depth,
                requested: new_depth,
            });
        }
        validate_geometry(new_depth, self.bucket_depth)?;
        self.depth = new_depth;
        Ok(())
    }

    /// Merges another table into this one by taking the elementwise maximum of
    /// the counters and the maximum depth.
    ///
    /// Immutable only: monotone counters make the elementwise maximum a
    /// well-defined join for reconciling divergent copies (it cannot undo two
    /// writers issuing the same index). Rejects with
    /// [`MutableMerge`](UsageError::MutableMerge) if either table is mutable: a
    /// ring cursor falls on wrap, so it has no maximum-based join, and mutable
    /// divergence is a conflict surfaced by the snapshot sequence number.
    pub fn merge_max(&mut self, other: &Self) -> Result<()> {
        if self.mutable || other.mutable {
            return Err(UsageError::MutableMerge);
        }
        if self.batch_id != other.batch_id || self.bucket_depth != other.bucket_depth {
            return Err(UsageError::BatchMismatch);
        }
        let depth = self.depth.max(other.depth);
        validate_geometry(depth, self.bucket_depth)?;
        self.depth = depth;
        let mut issued = 0u64;
        for (mine, theirs) in self.counts.iter_mut().zip(other.counts.iter()) {
            *mine = (*mine).max(*theirs);
            issued += u64::from(*mine);
        }
        self.issued = issued;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::b256;

    use super::*;

    fn batch_id() -> BatchId {
        b256!("0x1122334455667788112233445566778811223344556677881122334455667788")
    }

    #[test]
    fn geometry_bounds() {
        assert!(UsageTable::new(batch_id(), 20, 16).is_ok());
        assert!(UsageTable::new(batch_id(), 16, 16).is_ok());
        assert!(UsageTable::new(batch_id(), 47, 16).is_ok());
        assert!(UsageTable::new(batch_id(), 48, 16).is_err());
        assert!(UsageTable::new(batch_id(), 15, 16).is_err());
        assert!(UsageTable::new(batch_id(), 20, 17).is_err());
    }

    #[test]
    fn record_assigns_sequential_indices() {
        let mut table = UsageTable::new(batch_id(), 17, 16).unwrap();
        assert_eq!(table.bucket_capacity(), 2);
        assert_eq!(table.record(7).unwrap(), 0);
        assert_eq!(table.record(7).unwrap(), 1);
        assert_eq!(
            table.record(7),
            Err(UsageError::BucketFull {
                bucket: 7,
                capacity: 2
            })
        );
        assert_eq!(table.total_issued(), 2);
        assert_eq!(table.max_count(), 2);
        assert_eq!(table.min_count(), 0);
    }

    #[test]
    fn dilute_grows_capacity_only() {
        let mut table = UsageTable::new(batch_id(), 17, 16).unwrap();
        table.record(0).unwrap();
        table.dilute(18).unwrap();
        assert_eq!(table.bucket_capacity(), 4);
        assert_eq!(table.count(0).unwrap(), 1);
        assert_eq!(
            table.dilute(17),
            Err(UsageError::DepthDecrease {
                current: 18,
                requested: 17
            })
        );
    }

    #[test]
    fn merge_takes_elementwise_max() {
        let mut a = UsageTable::new(batch_id(), 18, 16).unwrap();
        let mut b = UsageTable::new(batch_id(), 19, 16).unwrap();
        a.record(0).unwrap();
        a.record(0).unwrap();
        b.record(0).unwrap();
        b.record(1).unwrap();
        a.merge_max(&b).unwrap();
        assert_eq!(a.depth(), 19);
        assert_eq!(a.count(0).unwrap(), 2);
        assert_eq!(a.count(1).unwrap(), 1);
        assert_eq!(a.total_issued(), 3);
    }

    #[test]
    fn mutable_ring_wraps_instead_of_failing() {
        // Capacity 2 per bucket.
        let mut table = UsageTable::new_mutable(batch_id(), 17, 16).unwrap();
        assert!(table.is_mutable());
        assert_eq!(table.bucket_capacity(), 2);
        assert_eq!(table.record(7).unwrap(), 0);
        assert_eq!(table.record(7).unwrap(), 1);
        // The ring wraps rather than returning BucketFull.
        assert_eq!(table.record(7).unwrap(), 0);
        assert_eq!(table.record(7).unwrap(), 1);
        assert_eq!(table.record(7).unwrap(), 0);
        // issued == sum(counts) at all times; cursor sits at 1 -> sum 1.
        let sum: u64 = table.counts().iter().map(|&c| u64::from(c)).sum();
        assert_eq!(table.total_issued(), sum);
    }

    #[test]
    fn mutable_record_skips_reserved_slots() {
        // Capacity 4 per bucket; reserve slot 1 in bucket 3.
        let mut table = UsageTable::new_mutable(batch_id(), 18, 16).unwrap();
        table.set_reserved([(3u32, 1u32)]);
        // Cursor starts at 0: 0, skip 1 -> 2, 3, wrap skip 1 (cursor at 0) ...
        assert_eq!(table.record(3).unwrap(), 0);
        assert_eq!(table.record(3).unwrap(), 2);
        assert_eq!(table.record(3).unwrap(), 3);
        // Cursor now 4 -> wraps to 0, then 2, 3, ... reserved slot never emitted.
        assert_eq!(table.record(3).unwrap(), 0);
        assert_eq!(table.record(3).unwrap(), 2);
        for _ in 0..50 {
            assert_ne!(
                table.record(3).unwrap(),
                1,
                "reserved slot must never be emitted"
            );
        }
    }

    #[test]
    fn mutable_dilute_changes_no_cursor() {
        let mut table = UsageTable::new_mutable(batch_id(), 17, 16).unwrap();
        table.record(0).unwrap();
        table.record(0).unwrap();
        let before: Vec<u32> = table.counts().to_vec();
        table.dilute(20).unwrap();
        assert_eq!(table.bucket_capacity(), 16);
        assert_eq!(table.counts(), before.as_slice());
    }

    #[test]
    fn merge_max_rejects_mutable() {
        let mut a = UsageTable::new_mutable(batch_id(), 18, 16).unwrap();
        let b = UsageTable::new(batch_id(), 18, 16).unwrap();
        assert_eq!(a.merge_max(&b), Err(UsageError::MutableMerge));
        let mut c = UsageTable::new(batch_id(), 18, 16).unwrap();
        let d = UsageTable::new_mutable(batch_id(), 18, 16).unwrap();
        assert_eq!(c.merge_max(&d), Err(UsageError::MutableMerge));
    }
}

//! In-memory per-bucket slot counters for a postage batch.

use alloc::vec;
use alloc::vec::Vec;

use nectar_postage::BatchId;

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
/// For each of the `2^bucket_depth` collision buckets, this records the state
/// needed to issue collision-free stamps into its `2^(depth - bucket_depth)`
/// slots. The table is an inert counters-and-geometry value: it has no method
/// that advances a counter. Issuance happens only through
/// [`Snapshot::issuer`](crate::Snapshot::issuer), which installs the reserved
/// slots first, so a bare table can never evict the chunks that record the
/// batch state.
///
/// - **Immutable** ([`new`](Self::new) / [`from_counts`](Self::from_counts)):
///   `counts[b]` is a monotone fill watermark, the next unused index; issuance
///   advances it and a full bucket fails rather than overwriting.
/// - **Mutable** ([`new_mutable`](Self::new_mutable) /
///   [`from_counts_mutable`](Self::from_counts_mutable)): `counts[b]` is a ring
///   cursor in `[0, capacity]` that wraps at capacity, so a full bucket churns
///   instead of failing. The reserved slots that the ring skips live on the
///   issuing handle, not the table, so they cannot be lost when the table is
///   moved.
///
/// A bare table cannot issue: it has no reserved-blind `record` mutator, so
/// counter advances that skip reserved installation are a compile error, not a
/// runtime check.
///
/// ```compile_fail
/// use alloy_primitives::B256;
/// use nectar_postage_usage::UsageTable;
///
/// let mut table = UsageTable::new_mutable(B256::repeat_byte(0x42), 18, 16).unwrap();
/// // `record` no longer exists on the inert table.
/// table.record(7).unwrap();
/// ```
///
/// ```compile_fail
/// use alloy_primitives::B256;
/// use nectar_postage_usage::{SwarmAddress, UsageTable};
///
/// let mut table = UsageTable::new_mutable(B256::repeat_byte(0x42), 18, 16).unwrap();
/// // `record_address` no longer exists on the inert table.
/// table.record_address(&SwarmAddress::from(B256::repeat_byte(0x99))).unwrap();
/// ```
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
    /// The ring skips the snapshot's reserved slots, which are installed onto the
    /// issuing handle by [`Snapshot::issuer`](crate::Snapshot::issuer), so the
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
    /// `[0, capacity]`. As with [`new_mutable`](Self::new_mutable), the reserved
    /// slots are installed by [`Snapshot::issuer`](crate::Snapshot::issuer)
    /// before issuing.
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

    /// Increases the batch depth after an on-chain dilution.
    ///
    /// Counters are unchanged; only the per-bucket capacity grows. The new
    /// depth must not decrease and must stay within the supported geometry.
    /// Exposed to callers through [`Snapshot::dilute`](crate::Snapshot::dilute).
    pub(crate) fn dilute(&mut self, new_depth: u8) -> Result<()> {
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
    /// Exposed to callers through
    /// [`Snapshot::merge_max`](crate::Snapshot::merge_max).
    pub(crate) fn merge_max(&mut self, other: &Self) -> Result<()> {
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

/// A borrowed, read-only window onto a [`UsageTable`].
///
/// This is what [`Snapshot::table`](crate::Snapshot::table) and
/// [`SnapshotParts::table`](crate::SnapshotParts::table) hand out. It exposes the
/// counters and geometry a caller legitimately needs to inspect (utilisation,
/// depth, mutability) but deliberately yields no owned [`UsageTable`]: it does
/// not implement [`Clone`] and does not [`Deref`](core::ops::Deref) to the table,
/// so `snapshot.table().clone()` cannot reproduce an owned table that
/// [`Snapshot::new`](crate::Snapshot::new) would accept at sequence 0. Together
/// with [`SnapshotParts`](crate::SnapshotParts) holding its table privately, this
/// closes the in-memory clone route that would otherwise downgrade a recovered
/// snapshot.
///
/// The view borrows the table, so it cannot outlive the snapshot it came from.
#[derive(Debug, Clone, Copy)]
pub struct TableView<'a> {
    table: &'a UsageTable,
}

impl<'a> TableView<'a> {
    pub(crate) const fn new(table: &'a UsageTable) -> Self {
        Self { table }
    }

    /// Returns the batch id this table describes.
    pub const fn batch_id(&self) -> BatchId {
        self.table.batch_id
    }

    /// Returns whether the table is a mutable ring (`true`) or an immutable fill
    /// watermark (`false`).
    pub const fn is_mutable(&self) -> bool {
        self.table.mutable
    }

    /// Returns the batch depth.
    pub const fn depth(&self) -> u8 {
        self.table.depth
    }

    /// Returns the bucket (uniformity) depth.
    pub const fn bucket_depth(&self) -> u8 {
        self.table.bucket_depth
    }

    /// Returns the number of collision buckets (`2^bucket_depth`).
    pub const fn bucket_count(&self) -> u32 {
        self.table.bucket_count()
    }

    /// Returns the number of slots per bucket (`2^(depth - bucket_depth)`).
    pub const fn bucket_capacity(&self) -> u32 {
        self.table.bucket_capacity()
    }

    /// Returns the total batch capacity in slots (`2^depth`).
    pub const fn total_capacity(&self) -> u64 {
        self.table.total_capacity()
    }

    /// Returns the sum of the per-bucket counters (a checksum in mutable mode).
    pub const fn total_issued(&self) -> u64 {
        self.table.issued
    }

    /// Returns the per-bucket counters.
    pub fn counts(&self) -> &[u32] {
        self.table.counts()
    }

    /// Returns the counter of a bucket.
    pub fn count(&self, bucket: u32) -> Result<u32> {
        self.table.count(bucket)
    }

    /// Returns the highest counter across all buckets.
    pub fn max_count(&self) -> u32 {
        self.table.max_count()
    }

    /// Returns the lowest counter across all buckets.
    pub fn min_count(&self) -> u32 {
        self.table.min_count()
    }

    /// Returns whether a bucket can accept another slot assignment.
    pub fn has_capacity(&self, bucket: u32) -> Result<bool> {
        self.table.has_capacity(bucket)
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
    fn from_counts_sums_issued_and_rejects_overflow() {
        let mut counts = vec![0u32; 1usize << 16];
        counts[7] = 2;
        let table = UsageTable::from_counts(batch_id(), 17, 16, counts).unwrap();
        assert_eq!(table.bucket_capacity(), 2);
        assert_eq!(table.total_issued(), 2);
        assert_eq!(table.max_count(), 2);
        assert_eq!(table.min_count(), 0);

        let mut over = vec![0u32; 1usize << 16];
        over[7] = 3; // capacity is 2
        assert_eq!(
            UsageTable::from_counts(batch_id(), 17, 16, over),
            Err(UsageError::CounterOverflow {
                bucket: 7,
                count: 3,
                capacity: 2
            })
        );
    }

    #[test]
    fn dilute_grows_capacity_only() {
        let mut counts = vec![0u32; 1usize << 16];
        counts[0] = 1;
        let mut table = UsageTable::from_counts(batch_id(), 17, 16, counts).unwrap();
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
        let mut counts_a = vec![0u32; 1usize << 16];
        counts_a[0] = 2;
        let mut counts_b = vec![0u32; 1usize << 16];
        counts_b[0] = 1;
        counts_b[1] = 1;
        let mut a = UsageTable::from_counts(batch_id(), 18, 16, counts_a).unwrap();
        let b = UsageTable::from_counts(batch_id(), 19, 16, counts_b).unwrap();
        a.merge_max(&b).unwrap();
        assert_eq!(a.depth(), 19);
        assert_eq!(a.count(0).unwrap(), 2);
        assert_eq!(a.count(1).unwrap(), 1);
        assert_eq!(a.total_issued(), 3);
    }

    #[test]
    fn merge_max_rejects_mutable() {
        let zero = || vec![0u32; 1usize << 16];
        let mut a = UsageTable::from_counts_mutable(batch_id(), 18, 16, zero()).unwrap();
        let b = UsageTable::from_counts(batch_id(), 18, 16, zero()).unwrap();
        assert_eq!(a.merge_max(&b), Err(UsageError::MutableMerge));
        let mut c = UsageTable::from_counts(batch_id(), 18, 16, zero()).unwrap();
        let d = UsageTable::from_counts_mutable(batch_id(), 18, 16, zero()).unwrap();
        assert_eq!(c.merge_max(&d), Err(UsageError::MutableMerge));
    }
}

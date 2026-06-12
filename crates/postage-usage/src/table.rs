//! In-memory per-bucket slot counters for a postage batch.

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
/// Tracks, for each of the `2^bucket_depth` collision buckets, how many of
/// its `2^(depth - bucket_depth)` storage slots have been assigned. The next
/// unused within-bucket index of a bucket always equals its counter, so this
/// table is sufficient state for issuing collision-free stamps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageTable {
    pub(crate) batch_id: BatchId,
    pub(crate) depth: u8,
    pub(crate) bucket_depth: u8,
    pub(crate) counts: Vec<u32>,
    pub(crate) issued: u64,
}

impl UsageTable {
    /// Creates an empty table for a batch with the given geometry.
    pub fn new(batch_id: BatchId, depth: u8, bucket_depth: u8) -> Result<Self> {
        validate_geometry(depth, bucket_depth)?;
        Ok(Self {
            batch_id,
            depth,
            bucket_depth,
            counts: vec![0; 1usize << bucket_depth],
            issued: 0,
        })
    }

    /// Creates a table from existing counters.
    ///
    /// `counts` must hold exactly `2^bucket_depth` entries, each at most the
    /// bucket capacity.
    pub fn from_counts(
        batch_id: BatchId,
        depth: u8,
        bucket_depth: u8,
        counts: Vec<u32>,
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
        })
    }

    /// Returns the batch id this table describes.
    pub const fn batch_id(&self) -> BatchId {
        self.batch_id
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

    /// Returns the total number of slots assigned across all buckets.
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

    /// Assigns the next unused slot in a bucket and returns its index.
    pub fn record(&mut self, bucket: u32) -> Result<u32> {
        let capacity = self.bucket_capacity();
        let count = self
            .counts
            .get_mut(bucket as usize)
            .ok_or(UsageError::InvalidBucket { bucket })?;
        if *count >= capacity {
            return Err(UsageError::BucketFull { bucket, capacity });
        }
        let index = *count;
        *count += 1;
        self.issued += 1;
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

    /// Merges another table into this one by taking the elementwise maximum
    /// of the counters and the maximum depth.
    ///
    /// Counters are monotone, so this is a well-defined join for recovering
    /// from divergent copies of the same table. It cannot undo two writers
    /// having issued the same index; see the crate documentation.
    pub fn merge_max(&mut self, other: &Self) -> Result<()> {
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
}

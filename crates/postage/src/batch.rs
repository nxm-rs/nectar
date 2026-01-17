//! Postage batch types.

use alloy_primitives::{Address, B256};

/// A 32-byte batch identifier.
pub type BatchId = B256;

/// A postage batch represents a prepaid storage allocation in the Swarm network.
///
/// Batches are created by sending BZZ tokens to the postage stamp contract.
/// Each batch has a depth that determines the maximum number of chunks it can stamp,
/// and a bucket depth that controls the uniformity of chunk distribution.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Batch {
    /// The unique identifier for this batch.
    id: BatchId,
    /// The normalized balance of the batch (value per chunk).
    value: u128,
    /// The block number when this batch was created.
    block_created: Option<u64>,
    /// The Ethereum address of the batch owner.
    owner: Address,
    /// The depth of the batch, determining total capacity (2^depth chunks).
    depth: u8,
    /// The bucket depth for collision bucket uniformity.
    bucket_depth: u8,
    /// Whether the batch is immutable (cannot be topped up).
    immutable: bool,
}

impl Batch {
    /// Creates a new batch with the given parameters.
    #[inline]
    pub const fn new(
        id: BatchId,
        value: u128,
        block_created: Option<u64>,
        owner: Address,
        depth: u8,
        bucket_depth: u8,
        immutable: bool,
    ) -> Self {
        Self {
            id,
            value,
            block_created,
            owner,
            depth,
            bucket_depth,
            immutable,
        }
    }

    /// Returns the batch ID.
    #[inline]
    pub const fn id(&self) -> BatchId {
        self.id
    }

    /// Returns the normalized value (balance per chunk).
    #[inline]
    pub const fn value(&self) -> u128 {
        self.value
    }

    /// Returns the block number when this batch was created.
    #[inline]
    pub const fn block_created(&self) -> Option<u64> {
        self.block_created
    }

    /// Returns the owner's Ethereum address.
    #[inline]
    pub const fn owner(&self) -> Address {
        self.owner
    }

    /// Returns the batch depth.
    ///
    /// The total capacity is 2^depth chunks.
    #[inline]
    pub const fn depth(&self) -> u8 {
        self.depth
    }

    /// Returns the bucket depth.
    ///
    /// This controls the uniformity of chunk distribution across collision buckets.
    #[inline]
    pub const fn bucket_depth(&self) -> u8 {
        self.bucket_depth
    }

    /// Returns whether this batch is immutable.
    ///
    /// Immutable batches cannot be topped up with additional value.
    #[inline]
    pub const fn immutable(&self) -> bool {
        self.immutable
    }

    /// Returns the maximum number of chunks per bucket.
    ///
    /// This is equal to 2^(depth - bucket_depth).
    #[inline]
    pub const fn bucket_upper_bound(&self) -> u32 {
        1u32 << (self.depth - self.bucket_depth)
    }

    /// Returns the number of collision buckets.
    ///
    /// This is equal to 2^bucket_depth.
    #[inline]
    pub const fn bucket_count(&self) -> u32 {
        1u32 << self.bucket_depth
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_creation() {
        let id = B256::ZERO;
        let batch = Batch::new(id, 1000, Some(100), Address::ZERO, 18, 16, false);

        assert_eq!(batch.id(), id);
        assert_eq!(batch.value(), 1000);
        assert_eq!(batch.block_created(), Some(100));
        assert_eq!(batch.owner(), Address::ZERO);
        assert_eq!(batch.depth(), 18);
        assert_eq!(batch.bucket_depth(), 16);
        assert!(!batch.immutable());
    }

    #[test]
    fn test_bucket_calculations() {
        let batch = Batch::new(B256::ZERO, 0, None, Address::ZERO, 18, 16, false);

        // 2^(18-16) = 2^2 = 4 chunks per bucket
        assert_eq!(batch.bucket_upper_bound(), 4);
        // 2^16 = 65536 buckets
        assert_eq!(batch.bucket_count(), 65536);
    }

    #[test]
    fn test_immutable_batch() {
        let batch = Batch::new(B256::ZERO, 0, None, Address::ZERO, 17, 16, true);
        assert!(batch.immutable());
    }
}

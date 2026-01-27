//! Postage batch types.

use alloy_primitives::{Address, B256};
use nectar_primitives::SwarmAddress;

use crate::{StampError, StampIndex, calculate_bucket};

/// A 32-byte batch identifier.
pub type BatchId = B256;

/// Parameters for creating a new batch.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BatchParams {
    /// The owner's Ethereum address.
    pub owner: Address,
    /// The depth of the batch (total capacity = 2^depth chunks).
    pub depth: u8,
    /// The bucket depth for collision bucket uniformity.
    pub bucket_depth: u8,
    /// Whether the batch is immutable.
    ///
    /// Immutable batches cannot be diluted (depth increased) and chunks cannot
    /// be overwritten. Mutable batches allow writing new chunks to the same
    /// bucket index with a later timestamp, replacing the previous chunk.
    pub immutable: bool,
    /// Initial amount to fund the batch.
    pub amount: u128,
}

impl BatchParams {
    /// Creates new batch parameters.
    pub const fn new(owner: Address, depth: u8, bucket_depth: u8, amount: u128) -> Self {
        Self {
            owner,
            depth,
            bucket_depth,
            immutable: false,
            amount,
        }
    }

    /// Sets the immutable flag.
    #[must_use]
    pub const fn immutable(mut self, immutable: bool) -> Self {
        self.immutable = immutable;
        self
    }
}

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
    start: u64,
    /// The Ethereum address of the batch owner.
    owner: Address,
    /// The depth of the batch, determining total capacity (2^depth chunks).
    depth: u8,
    /// The bucket depth for collision bucket uniformity.
    bucket_depth: u8,
    /// Whether the batch is immutable.
    ///
    /// Immutable batches cannot be diluted (depth increased) and chunks cannot
    /// be overwritten. Mutable batches allow writing new chunks to the same
    /// bucket index with a later timestamp, replacing the previous chunk.
    immutable: bool,
}

impl Batch {
    /// Creates a new batch with the given parameters.
    #[inline]
    pub const fn new(
        id: BatchId,
        value: u128,
        start: u64,
        owner: Address,
        depth: u8,
        bucket_depth: u8,
        immutable: bool,
    ) -> Self {
        Self {
            id,
            value,
            start,
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
    pub const fn start(&self) -> u64 {
        self.start
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
    /// Immutable batches cannot be diluted (depth increased) and chunks cannot
    /// be overwritten. Mutable batches allow writing new chunks to the same
    /// bucket index with a later timestamp, replacing the previous chunk.
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

    /// Updates the batch value (for top-up operations).
    #[inline]
    pub const fn set_value(&mut self, value: u128) {
        self.value = value;
    }

    /// Updates the batch depth (for dilution operations).
    #[inline]
    pub const fn set_depth(&mut self, depth: u8) {
        self.depth = depth;
    }

    /// Checks if the batch has expired given the current chain state.
    #[inline]
    pub const fn is_expired(&self, total_amount: u128) -> bool {
        self.value <= total_amount
    }

    /// Checks if the batch is usable (has enough confirmations).
    #[inline]
    pub const fn is_usable(&self, current_block: u64, threshold: u64) -> bool {
        current_block >= self.start.saturating_add(threshold)
    }

    // =========================================================================
    // Validation methods
    // =========================================================================

    /// Validates that an index is within the valid range for this batch.
    ///
    /// Checks that:
    /// - The bucket is within the valid range (< bucket_count)
    /// - The position within the bucket is within capacity (< bucket_upper_bound)
    ///
    /// # Returns
    ///
    /// `Ok(())` if the index is valid, or `Err(StampError::InvalidIndex)` otherwise.
    pub const fn validate_index(&self, index: &StampIndex) -> Result<(), StampError> {
        // Check bucket is within range
        if index.bucket() >= self.bucket_count() {
            return Err(StampError::InvalidIndex);
        }

        // Check index is within bucket capacity
        if index.index() >= self.bucket_upper_bound() {
            return Err(StampError::InvalidIndex);
        }

        Ok(())
    }

    /// Calculates which bucket a chunk address belongs to.
    ///
    /// The bucket is determined by taking the first `bucket_depth` bits of the
    /// chunk address, interpreted as a big-endian unsigned integer.
    #[inline]
    pub fn bucket_for_address(&self, address: &SwarmAddress) -> u32 {
        calculate_bucket(address, self.bucket_depth)
    }

    /// Checks if a chunk address matches the expected bucket for a stamp index.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the bucket matches, or `Err(StampError::BucketMismatch)` otherwise.
    pub fn validate_bucket(
        &self,
        index: &StampIndex,
        address: &SwarmAddress,
    ) -> Result<(), StampError> {
        let expected_bucket = self.bucket_for_address(address);
        if index.bucket() != expected_bucket {
            return Err(StampError::BucketMismatch);
        }
        Ok(())
    }
}

// Arbitrary implementations for property-based testing

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for BatchParams {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        // Generate valid depth values (bucket_depth must be <= depth)
        let depth: u8 = u.int_in_range(1..=32)?;
        let bucket_depth: u8 = u.int_in_range(1..=depth)?;

        Ok(Self {
            owner: Address::arbitrary(u)?,
            depth,
            bucket_depth,
            immutable: u.arbitrary()?,
            amount: u.arbitrary()?,
        })
    }
}

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for Batch {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        // Generate valid depth values (bucket_depth must be <= depth)
        let depth: u8 = u.int_in_range(1..=32)?;
        let bucket_depth: u8 = u.int_in_range(1..=depth)?;

        Ok(Self::new(
            B256::arbitrary(u)?,
            u.arbitrary()?,
            u.arbitrary()?,
            Address::arbitrary(u)?,
            depth,
            bucket_depth,
            u.arbitrary()?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_creation() {
        let id = B256::ZERO;
        let batch = Batch::new(id, 1000, 100, Address::ZERO, 18, 16, false);

        assert_eq!(batch.id(), id);
        assert_eq!(batch.value(), 1000);
        assert_eq!(batch.start(), 100);
        assert_eq!(batch.owner(), Address::ZERO);
        assert_eq!(batch.depth(), 18);
        assert_eq!(batch.bucket_depth(), 16);
        assert!(!batch.immutable());
    }

    #[test]
    fn test_bucket_calculations() {
        let batch = Batch::new(B256::ZERO, 0, 0, Address::ZERO, 18, 16, false);

        // 2^(18-16) = 2^2 = 4 chunks per bucket
        assert_eq!(batch.bucket_upper_bound(), 4);
        // 2^16 = 65536 buckets
        assert_eq!(batch.bucket_count(), 65536);
    }

    #[test]
    fn test_batch_expiry() {
        let batch = Batch::new(B256::ZERO, 1000, 0, Address::ZERO, 18, 16, false);

        assert!(!batch.is_expired(999));
        assert!(batch.is_expired(1000));
        assert!(batch.is_expired(1001));
    }

    #[test]
    fn test_batch_usability() {
        let batch = Batch::new(B256::ZERO, 1000, 100, Address::ZERO, 18, 16, false);

        assert!(!batch.is_usable(100, 10)); // Same block
        assert!(!batch.is_usable(109, 10)); // Not enough confirmations
        assert!(batch.is_usable(110, 10)); // Exactly threshold
        assert!(batch.is_usable(111, 10)); // Past threshold
    }

    #[test]
    fn test_batch_params_builder() {
        let params = BatchParams::new(Address::ZERO, 20, 16, 1000).immutable(true);

        assert_eq!(params.owner, Address::ZERO);
        assert_eq!(params.depth, 20);
        assert_eq!(params.bucket_depth, 16);
        assert_eq!(params.amount, 1000);
        assert!(params.immutable);
    }
}

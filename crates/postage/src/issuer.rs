//! Stamp issuer trait for tracking bucket utilization.

use crate::{BatchId, StampDigest, StampError, StampIndex};
use nectar_primitives::SwarmAddress;

/// A trait for managing stamp issuance within a batch.
///
/// The stamp issuer tracks which bucket indices have been used and allocates
/// new indices for chunks as they are stamped. This is the stateful component
/// that ensures each stamp uses a unique index within its bucket.
///
/// # Separation of Concerns
///
/// The `StampIssuer` is responsible only for tracking and allocating indices.
/// Signing is handled separately by the `Stamper` trait, allowing the same
/// issuer state to be used with different signing mechanisms.
///
/// # Example
///
/// ```ignore
/// use nectar_postage::{StampIssuer, StampDigest, StampError};
/// use nectar_primitives::SwarmAddress;
///
/// struct MyIssuer { /* ... */ }
///
/// impl StampIssuer for MyIssuer {
///     fn prepare_stamp(&mut self, address: &SwarmAddress, timestamp: u64) -> Result<StampDigest, StampError> {
///         // Allocate index and return digest
///         todo!()
///     }
///
///     fn batch_id(&self) -> BatchId {
///         todo!()
///     }
///
///     // ... other methods
/// }
/// ```
pub trait StampIssuer {
    /// Prepares a stamp digest for the given chunk address.
    ///
    /// This method:
    /// 1. Calculates which bucket the chunk belongs to
    /// 2. Allocates the next available index within that bucket
    /// 3. Returns the digest that needs to be signed
    ///
    /// The caller is then responsible for signing the digest and creating
    /// the final stamp.
    ///
    /// # Arguments
    ///
    /// * `address` - The address of the chunk to stamp
    /// * `timestamp` - The timestamp to include in the stamp
    ///
    /// # Errors
    ///
    /// Returns `StampError::BucketFull` if the bucket has no remaining capacity.
    fn prepare_stamp(
        &mut self,
        address: &SwarmAddress,
        timestamp: u64,
    ) -> Result<StampDigest, StampError>;

    /// Returns the batch ID that stamps are issued for.
    fn batch_id(&self) -> BatchId;

    /// Returns the batch depth.
    fn batch_depth(&self) -> u8;

    /// Returns the bucket depth.
    fn bucket_depth(&self) -> u8;

    /// Returns the current utilization of the most-used bucket.
    ///
    /// This is useful for monitoring batch usage and determining
    /// when a batch is approaching capacity.
    fn max_bucket_utilization(&self) -> u32;

    /// Returns the utilization of a specific bucket.
    fn bucket_utilization(&self, bucket: u32) -> u32;

    /// Checks if a bucket can accept another chunk.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket number to check
    ///
    /// # Returns
    ///
    /// `true` if the bucket has capacity for at least one more chunk,
    /// `false` if the bucket is full.
    fn bucket_has_capacity(&self, bucket: u32) -> bool;

    /// Returns the total number of stamps issued.
    fn stamps_issued(&self) -> u64;

    /// Returns the total capacity of the batch (2^depth).
    fn total_capacity(&self) -> u64 {
        1u64 << self.batch_depth()
    }

    /// Returns the bucket capacity (2^(depth - bucket_depth)).
    fn bucket_capacity(&self) -> u32 {
        1u32 << (self.batch_depth() - self.bucket_depth())
    }

    /// Returns the number of buckets (2^bucket_depth).
    fn bucket_count(&self) -> u32 {
        1u32 << self.bucket_depth()
    }

    /// Checks if the issuer is approaching capacity.
    ///
    /// Returns `true` if the most utilized bucket has reached the
    /// specified percentage of capacity (0.0 to 1.0).
    fn is_near_capacity(&self, threshold: f64) -> bool {
        let max_util = self.max_bucket_utilization() as f64;
        let capacity = self.bucket_capacity() as f64;
        max_util / capacity >= threshold
    }
}

/// An in-memory stamp issuer that tracks bucket utilization.
///
/// This implementation stores bucket indices in a vector and is suitable
/// for most use cases where the issuer state doesn't need to persist
/// across restarts.
#[derive(Debug, Clone)]
pub struct MemoryIssuer {
    /// The batch ID.
    batch_id: BatchId,
    /// The batch depth.
    depth: u8,
    /// The bucket depth.
    bucket_depth: u8,
    /// Current index for each bucket.
    bucket_indices: alloc::vec::Vec<u32>,
    /// Maximum utilization across all buckets.
    max_utilization: u32,
    /// Total stamps issued.
    stamps_issued: u64,
}

extern crate alloc;

impl MemoryIssuer {
    /// Creates a new memory issuer for the given batch.
    pub fn new(batch_id: BatchId, depth: u8, bucket_depth: u8) -> Self {
        let bucket_count = 1usize << bucket_depth;
        Self {
            batch_id,
            depth,
            bucket_depth,
            bucket_indices: alloc::vec![0u32; bucket_count],
            max_utilization: 0,
            stamps_issued: 0,
        }
    }

    /// Creates a memory issuer from a batch.
    pub fn from_batch(batch: &crate::Batch) -> Self {
        Self::new(batch.id(), batch.depth(), batch.bucket_depth())
    }
}

impl StampIssuer for MemoryIssuer {
    fn prepare_stamp(
        &mut self,
        address: &SwarmAddress,
        timestamp: u64,
    ) -> Result<StampDigest, StampError> {
        let bucket = crate::calculate_bucket(address, self.bucket_depth);
        let bucket_idx = bucket as usize;

        // Get current index for this bucket
        let current_index = self.bucket_indices[bucket_idx];

        // Check if bucket is full
        let bucket_capacity = 1u32 << (self.depth - self.bucket_depth);
        if current_index >= bucket_capacity {
            return Err(StampError::BucketFull {
                bucket,
                capacity: bucket_capacity,
            });
        }

        // Increment the bucket index
        self.bucket_indices[bucket_idx] = current_index + 1;
        self.stamps_issued += 1;

        // Update max utilization
        if current_index + 1 > self.max_utilization {
            self.max_utilization = current_index + 1;
        }

        let index = StampIndex::new(bucket, current_index);

        Ok(StampDigest::new(*address, self.batch_id, index, timestamp))
    }

    fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    fn batch_depth(&self) -> u8 {
        self.depth
    }

    fn bucket_depth(&self) -> u8 {
        self.bucket_depth
    }

    fn max_bucket_utilization(&self) -> u32 {
        self.max_utilization
    }

    fn bucket_utilization(&self, bucket: u32) -> u32 {
        self.bucket_indices
            .get(bucket as usize)
            .copied()
            .unwrap_or(0)
    }

    fn bucket_has_capacity(&self, bucket: u32) -> bool {
        let bucket_idx = bucket as usize;
        if bucket_idx >= self.bucket_indices.len() {
            return false;
        }
        let bucket_capacity = 1u32 << (self.depth - self.bucket_depth);
        self.bucket_indices[bucket_idx] < bucket_capacity
    }

    fn stamps_issued(&self) -> u64 {
        self.stamps_issued
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;

    fn test_address(leading: u16) -> SwarmAddress {
        let mut bytes = [0u8; 32];
        bytes[0] = (leading >> 8) as u8;
        bytes[1] = leading as u8;
        SwarmAddress::new(bytes)
    }

    #[test]
    fn test_memory_issuer_basic() {
        let batch_id = B256::ZERO;
        let issuer = MemoryIssuer::new(batch_id, 20, 16);

        assert_eq!(issuer.batch_id(), batch_id);
        assert_eq!(issuer.batch_depth(), 20);
        assert_eq!(issuer.bucket_depth(), 16);
        assert_eq!(issuer.max_bucket_utilization(), 0);
        assert_eq!(issuer.stamps_issued(), 0);
        assert_eq!(issuer.bucket_count(), 65536);
        assert_eq!(issuer.bucket_capacity(), 16);
    }

    #[test]
    fn test_memory_issuer_prepare_stamp() {
        let mut issuer = MemoryIssuer::new(B256::ZERO, 20, 16);

        let address = test_address(0xCBE5);
        let digest = issuer.prepare_stamp(&address, 12345).unwrap();

        assert_eq!(digest.batch_id, B256::ZERO);
        assert_eq!(digest.index.bucket(), 0xCBE5);
        assert_eq!(digest.index.index(), 0);
        assert_eq!(digest.timestamp, 12345);
        assert_eq!(issuer.stamps_issued(), 1);
        assert_eq!(issuer.max_bucket_utilization(), 1);
    }

    #[test]
    fn test_memory_issuer_increments_index() {
        let mut issuer = MemoryIssuer::new(B256::ZERO, 20, 16);

        let address = test_address(0xCBE5);

        let d1 = issuer.prepare_stamp(&address, 1).unwrap();
        let d2 = issuer.prepare_stamp(&address, 2).unwrap();
        let d3 = issuer.prepare_stamp(&address, 3).unwrap();

        assert_eq!(d1.index.index(), 0);
        assert_eq!(d2.index.index(), 1);
        assert_eq!(d3.index.index(), 2);
        assert_eq!(issuer.stamps_issued(), 3);
    }

    #[test]
    fn test_memory_issuer_bucket_full() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket
        let mut issuer = MemoryIssuer::new(B256::ZERO, 17, 16);

        let address = test_address(0xABCD);

        // First two should succeed
        assert!(issuer.prepare_stamp(&address, 1).is_ok());
        assert!(issuer.prepare_stamp(&address, 2).is_ok());

        // Third should fail
        let result = issuer.prepare_stamp(&address, 3);
        assert!(matches!(result, Err(StampError::BucketFull { bucket: 0xABCD, capacity: 2 })));
    }

    #[test]
    fn test_memory_issuer_bucket_utilization() {
        let mut issuer = MemoryIssuer::new(B256::ZERO, 20, 16);

        let addr1 = test_address(0x1234);
        let addr2 = test_address(0x5678);

        issuer.prepare_stamp(&addr1, 1).unwrap();
        issuer.prepare_stamp(&addr1, 2).unwrap();
        issuer.prepare_stamp(&addr2, 3).unwrap();

        assert_eq!(issuer.bucket_utilization(0x1234), 2);
        assert_eq!(issuer.bucket_utilization(0x5678), 1);
        assert_eq!(issuer.bucket_utilization(0x9999), 0);
    }

    #[test]
    fn test_memory_issuer_capacity_check() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket
        let mut issuer = MemoryIssuer::new(B256::ZERO, 17, 16);

        let address = test_address(0x0001);

        assert!(issuer.bucket_has_capacity(0x0001));

        issuer.prepare_stamp(&address, 1).unwrap();
        assert!(issuer.bucket_has_capacity(0x0001));

        issuer.prepare_stamp(&address, 2).unwrap();
        assert!(!issuer.bucket_has_capacity(0x0001));
    }

    #[test]
    fn test_memory_issuer_near_capacity() {
        // depth=18, bucket_depth=16 gives 4 slots per bucket
        let mut issuer = MemoryIssuer::new(B256::ZERO, 18, 16);

        let address = test_address(0x0001);

        assert!(!issuer.is_near_capacity(0.5));

        issuer.prepare_stamp(&address, 1).unwrap();
        issuer.prepare_stamp(&address, 2).unwrap();

        // 2/4 = 0.5
        assert!(issuer.is_near_capacity(0.5));
        assert!(!issuer.is_near_capacity(0.75));

        issuer.prepare_stamp(&address, 3).unwrap();

        // 3/4 = 0.75
        assert!(issuer.is_near_capacity(0.75));
    }
}

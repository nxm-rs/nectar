//! Stamper trait and bucket calculation utilities.

use crate::{Batch, Stamp, StampError, StampIndex};
use nectar_primitives::SwarmAddress;

/// Calculates which collision bucket a chunk belongs to based on its address.
///
/// The bucket is determined by taking the first `bucket_depth` bits of the
/// chunk address, interpreted as a big-endian unsigned integer.
///
/// # Arguments
///
/// * `address` - The chunk's Swarm address
/// * `bucket_depth` - The number of leading bits to use (from the batch configuration)
///
/// # Returns
///
/// The bucket number (0 to 2^bucket_depth - 1)
///
/// # Example
///
/// ```
/// use nectar_postage::calculate_bucket;
/// use nectar_primitives::SwarmAddress;
/// use alloy_primitives::B256;
///
/// let address = SwarmAddress::new([0xCB, 0xE5, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
/// let bucket = calculate_bucket(&address, 16);
/// assert_eq!(bucket, 0xCBE5);
/// ```
#[inline]
pub fn calculate_bucket(address: &SwarmAddress, bucket_depth: u8) -> u32 {
    // Take the first 4 bytes as a big-endian u32
    let leading = u32::from_be_bytes(address.as_bytes()[0..4].try_into().unwrap());
    // Shift right to get only the top `bucket_depth` bits
    leading >> (32 - bucket_depth)
}

/// A trait for entities that can stamp chunks.
///
/// Implementations of this trait manage the state needed to stamp chunks,
/// including tracking bucket usage and generating signatures.
///
/// # Type Parameters
///
/// The trait is generic over the chunk type to allow different chunk
/// representations while maintaining a consistent stamping interface.
///
/// # Example
///
/// ```ignore
/// use nectar_postage::{Stamper, Stamp, StampError};
///
/// struct MyStamper { /* ... */ }
///
/// impl Stamper for MyStamper {
///     type Error = StampError;
///
///     fn stamp(&mut self, address: &SwarmAddress) -> Result<Stamp, Self::Error> {
///         // Implementation details...
///     }
///
///     fn batch(&self) -> &Batch {
///         // Return reference to the batch
///     }
/// }
/// ```
pub trait Stamper {
    /// The error type returned when stamping fails.
    type Error: From<StampError>;

    /// Stamps a chunk identified by its address.
    ///
    /// This method:
    /// 1. Calculates the bucket for the chunk based on its address
    /// 2. Allocates the next available index within that bucket
    /// 3. Generates the stamp signature
    /// 4. Returns the complete stamp
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The bucket is full and the batch is immutable
    /// - Signature generation fails
    /// - Any other implementation-specific error occurs
    fn stamp(&mut self, address: &SwarmAddress) -> Result<Stamp, Self::Error>;

    /// Returns a reference to the underlying batch.
    fn batch(&self) -> &Batch;

    /// Returns the current utilization of the most-used bucket.
    ///
    /// This is useful for monitoring batch usage and determining
    /// when a batch is approaching capacity.
    fn max_bucket_utilization(&self) -> u32;

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
}

/// Extension trait for [`Batch`] providing bucket calculation utilities.
pub trait BatchExt {
    /// Calculates which bucket a chunk address belongs to.
    fn bucket_for_address(&self, address: &SwarmAddress) -> u32;

    /// Validates that an index is within the valid range for this batch.
    fn validate_index(&self, index: &StampIndex) -> Result<(), StampError>;
}

impl BatchExt for Batch {
    #[inline]
    fn bucket_for_address(&self, address: &SwarmAddress) -> u32 {
        calculate_bucket(address, self.bucket_depth())
    }

    fn validate_index(&self, index: &StampIndex) -> Result<(), StampError> {
        // Check bucket is within range
        if index.bucket() >= self.bucket_count() {
            return Err(StampError::BucketMismatch);
        }

        // Check index is within bucket capacity
        if index.index() >= self.bucket_upper_bound() {
            return Err(StampError::InvalidIndex);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256};

    #[test]
    fn test_calculate_bucket() {
        // Address starting with 0xCBE5...
        let address = SwarmAddress::new([
            0xCB, 0xE5, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0,
        ]);

        // With bucket_depth=16, we should get 0xCBE5
        assert_eq!(calculate_bucket(&address, 16), 0xCBE5);

        // With bucket_depth=8, we should get 0xCB
        assert_eq!(calculate_bucket(&address, 8), 0xCB);

        // With bucket_depth=4, we should get 0xC
        assert_eq!(calculate_bucket(&address, 4), 0xC);
    }

    #[test]
    fn test_batch_bucket_for_address() {
        let batch = Batch::new(B256::ZERO, 0, None, Address::ZERO, 18, 16, false);

        let address = SwarmAddress::new([
            0xCB, 0xE5, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0,
        ]);

        assert_eq!(batch.bucket_for_address(&address), 0xCBE5);
    }

    #[test]
    fn test_validate_index_valid() {
        let batch = Batch::new(B256::ZERO, 0, None, Address::ZERO, 18, 16, false);

        // Valid: bucket < 2^16, index < 2^(18-16) = 4
        let index = StampIndex::new(1000, 3);
        assert!(batch.validate_index(&index).is_ok());
    }

    #[test]
    fn test_validate_index_bucket_out_of_range() {
        let batch = Batch::new(B256::ZERO, 0, None, Address::ZERO, 18, 16, false);

        // Invalid: bucket >= 2^16 = 65536
        let index = StampIndex::new(70000, 0);
        assert!(matches!(
            batch.validate_index(&index),
            Err(StampError::BucketMismatch)
        ));
    }

    #[test]
    fn test_validate_index_position_out_of_range() {
        let batch = Batch::new(B256::ZERO, 0, None, Address::ZERO, 18, 16, false);

        // Invalid: index >= 2^(18-16) = 4
        let index = StampIndex::new(1000, 5);
        assert!(matches!(
            batch.validate_index(&index),
            Err(StampError::InvalidIndex)
        ));
    }
}

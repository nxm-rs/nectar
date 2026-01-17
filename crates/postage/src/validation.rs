//! Stamp validation traits and utilities.

use crate::{calculate_bucket, Batch, ChainState, Stamp, StampError, StampIndex};
use nectar_primitives::SwarmAddress;

/// A trait for validating postage stamps.
///
/// Implementations of this trait verify that stamps are valid for a given
/// chunk address and chain state. Validation includes checking:
///
/// - The batch exists and is not expired
/// - The stamp index is within valid bounds
/// - The chunk address matches the expected bucket
/// - The signature is valid (implementation-dependent)
///
/// # Example
///
/// ```ignore
/// use nectar_postage::{StampValidator, Stamp, ChainState};
/// use nectar_primitives::SwarmAddress;
///
/// struct MyValidator { /* ... */ }
///
/// impl StampValidator for MyValidator {
///     type Error = nectar_postage::StampError;
///
///     fn validate(&self, stamp: &Stamp, address: &SwarmAddress, state: &ChainState) -> Result<(), Self::Error> {
///         // Validation logic...
///         Ok(())
///     }
/// }
/// ```
pub trait StampValidator {
    /// The error type returned when validation fails.
    type Error: From<StampError>;

    /// Validates a stamp for a given chunk address.
    ///
    /// # Arguments
    ///
    /// * `stamp` - The stamp to validate
    /// * `address` - The address of the chunk being validated
    /// * `state` - The current chain state for expiry checks
    ///
    /// # Returns
    ///
    /// `Ok(())` if the stamp is valid, or an error describing why validation failed.
    fn validate(
        &self,
        stamp: &Stamp,
        address: &SwarmAddress,
        state: &ChainState,
    ) -> Result<(), Self::Error>;

    /// Validates only the structural properties of a stamp without signature verification.
    ///
    /// This is useful for quick validation before performing more expensive
    /// cryptographic operations. It checks:
    ///
    /// - The batch exists
    /// - The batch is usable (enough confirmations)
    /// - The batch is not expired
    /// - The stamp index is within valid bounds
    /// - The chunk address matches the expected bucket
    ///
    /// The default implementation calls `validate`, but implementations may
    /// override this for performance.
    fn validate_structure(
        &self,
        stamp: &Stamp,
        address: &SwarmAddress,
        state: &ChainState,
    ) -> Result<(), Self::Error> {
        self.validate(stamp, address, state)
    }
}

/// Extension trait for [`Batch`] providing index validation utilities.
pub trait BatchValidation {
    /// Validates that an index is within the valid range for this batch.
    fn validate_index(&self, index: &StampIndex) -> Result<(), StampError>;

    /// Calculates which bucket a chunk address belongs to.
    fn bucket_for_address(&self, address: &SwarmAddress) -> u32;

    /// Checks if a chunk address matches the expected bucket for a stamp index.
    fn validate_bucket(&self, index: &StampIndex, address: &SwarmAddress) -> Result<(), StampError>;
}

impl BatchValidation for Batch {
    fn validate_index(&self, index: &StampIndex) -> Result<(), StampError> {
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

    #[inline]
    fn bucket_for_address(&self, address: &SwarmAddress) -> u32 {
        calculate_bucket(address, self.bucket_depth())
    }

    fn validate_bucket(&self, index: &StampIndex, address: &SwarmAddress) -> Result<(), StampError> {
        let expected_bucket = self.bucket_for_address(address);
        if index.bucket() != expected_bucket {
            return Err(StampError::BucketMismatch);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256};

    #[test]
    fn test_validate_index_valid() {
        let batch = Batch::new(B256::ZERO, 0, 0, Address::ZERO, 18, 16, false);

        // Valid: bucket < 2^16, index < 2^(18-16) = 4
        let index = StampIndex::new(1000, 3);
        assert!(batch.validate_index(&index).is_ok());
    }

    #[test]
    fn test_validate_index_bucket_out_of_range() {
        let batch = Batch::new(B256::ZERO, 0, 0, Address::ZERO, 18, 16, false);

        // Invalid: bucket >= 2^16 = 65536
        let index = StampIndex::new(70000, 0);
        assert!(matches!(
            batch.validate_index(&index),
            Err(StampError::InvalidIndex)
        ));
    }

    #[test]
    fn test_validate_index_position_out_of_range() {
        let batch = Batch::new(B256::ZERO, 0, 0, Address::ZERO, 18, 16, false);

        // Invalid: index >= 2^(18-16) = 4
        let index = StampIndex::new(1000, 5);
        assert!(matches!(
            batch.validate_index(&index),
            Err(StampError::InvalidIndex)
        ));
    }

    #[test]
    fn test_bucket_for_address() {
        let batch = Batch::new(B256::ZERO, 0, 0, Address::ZERO, 18, 16, false);

        let address = SwarmAddress::new([
            0xCB, 0xE5, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0,
        ]);

        assert_eq!(batch.bucket_for_address(&address), 0xCBE5);
    }

    #[test]
    fn test_validate_bucket_match() {
        let batch = Batch::new(B256::ZERO, 0, 0, Address::ZERO, 18, 16, false);

        let address = SwarmAddress::new([
            0xCB, 0xE5, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0,
        ]);
        let index = StampIndex::new(0xCBE5, 0);

        assert!(batch.validate_bucket(&index, &address).is_ok());
    }

    #[test]
    fn test_validate_bucket_mismatch() {
        let batch = Batch::new(B256::ZERO, 0, 0, Address::ZERO, 18, 16, false);

        let address = SwarmAddress::new([
            0xCB, 0xE5, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0,
        ]);
        let index = StampIndex::new(0x1234, 0); // Wrong bucket

        assert!(matches!(
            batch.validate_bucket(&index, &address),
            Err(StampError::BucketMismatch)
        ));
    }
}

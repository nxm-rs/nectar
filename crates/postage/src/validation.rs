//! Stamp validation traits and utilities.

use crate::{PostageContext, Stamp, StampError};
use nectar_primitives::SwarmAddress;

#[cfg(any(test, feature = "std"))]
use crate::Batch;

#[cfg(test)]
use crate::StampIndex;

#[cfg(feature = "std")]
use crate::{BatchStore, BatchStoreExt};

/// A trait for validating postage stamps.
///
/// Implementations of this trait verify that stamps are valid for a given
/// chunk address and postage context. Validation includes checking:
///
/// - The batch exists and is not expired
/// - The stamp index is within valid bounds
/// - The chunk address matches the expected bucket
/// - The signature is valid (implementation-dependent)
///
/// # Example
///
/// ```ignore
/// use nectar_postage::{StampValidator, Stamp, PostageContext};
/// use nectar_primitives::SwarmAddress;
///
/// struct MyValidator { /* ... */ }
///
/// impl StampValidator for MyValidator {
///     type Error = nectar_postage::StampError;
///
///     fn validate(&self, stamp: &Stamp, address: &SwarmAddress, state: &PostageContext) -> Result<(), Self::Error> {
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
    /// * `state` - The current postage context for expiry checks
    ///
    /// # Returns
    ///
    /// `Ok(())` if the stamp is valid, or an error describing why validation failed.
    fn validate(
        &self,
        stamp: &Stamp,
        address: &SwarmAddress,
        state: &PostageContext,
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
        state: &PostageContext,
    ) -> Result<(), Self::Error> {
        self.validate(stamp, address, state)
    }
}

// Note: BatchValidation methods (validate_index, bucket_for_address, validate_bucket)
// are now implemented directly on the Batch type in batch.rs for better ergonomics.

// =============================================================================
// Store-based Validator
// =============================================================================

/// A validator that uses a [`BatchStore`] for validation.
///
/// This validator performs comprehensive validation:
/// 1. Retrieves the batch from the store
/// 2. Checks the batch is usable (enough confirmations)
/// 3. Checks the batch is not expired
/// 4. Validates the stamp index is within bounds
/// 5. Validates the bucket matches the chunk address
/// 6. Verifies the stamp signature matches the batch owner
///
/// # Example
///
/// ```ignore
/// use nectar_postage::{StoreValidator, BatchStore};
///
/// let store = MyBatchStore::new();
/// let validator = StoreValidator::new(store, 50); // 50 block confirmations
///
/// let result = validator.validate(&stamp, &address).await;
/// ```
#[derive(Debug)]
#[cfg(feature = "std")]
pub struct StoreValidator<S> {
    store: S,
    confirmation_threshold: u64,
}

#[cfg(feature = "std")]
impl<S> StoreValidator<S> {
    /// Creates a new store validator.
    ///
    /// # Arguments
    ///
    /// * `store` - The batch store to use for lookups
    /// * `confirmation_threshold` - Minimum block confirmations for a batch to be usable
    pub const fn new(store: S, confirmation_threshold: u64) -> Self {
        Self {
            store,
            confirmation_threshold,
        }
    }

    /// Returns a reference to the underlying store.
    pub const fn store(&self) -> &S {
        &self.store
    }

    /// Returns the confirmation threshold.
    pub const fn confirmation_threshold(&self) -> u64 {
        self.confirmation_threshold
    }
}

#[cfg(feature = "std")]
impl<S: BatchStore + Sync> StoreValidator<S> {
    /// Validates a stamp asynchronously.
    ///
    /// This performs full validation including signature verification.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the stamp is valid, or a [`StampError`] describing the failure.
    pub async fn validate(&self, stamp: &Stamp, address: &SwarmAddress) -> Result<(), StampError> {
        // Get the batch and verify it's usable
        let batch = self.get_batch_for_stamp(stamp).await?;

        // Validate structure
        self.validate_structure_with_batch(stamp, address, &batch)?;

        // Verify signature
        stamp.verify(address, batch.owner())?;

        Ok(())
    }

    /// Validates the structural properties without signature verification.
    ///
    /// This is faster than full validation when you only need to check
    /// that the stamp references a valid batch and bucket.
    pub async fn validate_structure(
        &self,
        stamp: &Stamp,
        address: &SwarmAddress,
    ) -> Result<(), StampError> {
        let batch = self.get_batch_for_stamp(stamp).await?;
        self.validate_structure_with_batch(stamp, address, &batch)
    }

    /// Gets and validates the batch for a stamp.
    async fn get_batch_for_stamp(&self, stamp: &Stamp) -> Result<Batch, StampError> {
        self.store
            .get_usable(&stamp.batch(), self.confirmation_threshold)
            .await
            .map_err(|e| match e {
                crate::BatchStoreError::NotFound(id) => StampError::BatchNotFound(id),
                crate::BatchStoreError::NotUsable {
                    created,
                    current,
                    threshold,
                    ..
                } => StampError::BatchNotUsable {
                    created,
                    current,
                    threshold,
                },
                crate::BatchStoreError::Expired {
                    value,
                    total_amount,
                    ..
                } => StampError::BatchExpired {
                    value,
                    total_amount,
                },
                crate::BatchStoreError::Store(_) => StampError::BatchNotFound(stamp.batch()),
            })
    }

    /// Validates structure given an already-retrieved batch.
    fn validate_structure_with_batch(
        &self,
        stamp: &Stamp,
        address: &SwarmAddress,
        batch: &Batch,
    ) -> Result<(), StampError> {
        // Validate index bounds
        batch.validate_index(&stamp.stamp_index())?;

        // Validate bucket matches address
        batch.validate_bucket(&stamp.stamp_index(), address)?;

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

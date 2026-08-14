//! Stamp validation traits and utilities.

use crate::{PostageContext, Stamp, StampError};
use nectar_primitives::ChunkAddress;

#[cfg(any(test, feature = "std"))]
use crate::{Batch, BatchId};

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
/// use nectar_primitives::ChunkAddress;
///
/// struct MyValidator { /* ... */ }
///
/// impl StampValidator for MyValidator {
///     type Error = nectar_postage::StampError;
///
///     fn validate(&self, stamp: &Stamp, address: &ChunkAddress, state: &PostageContext) -> Result<(), Self::Error> {
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
        address: &ChunkAddress,
        state: &PostageContext,
    ) -> Result<(), Self::Error>;

    /// Validates only the structural properties of a stamp without signature verification.
    ///
    /// This is useful for quick validation before performing more expensive
    /// cryptographic operations. It checks:
    ///
    /// - The batch exists
    /// - The batch is not expired
    /// - The stamp index is within valid bounds
    /// - The chunk address matches the expected bucket
    ///
    /// The default implementation calls `validate`, but implementations may
    /// override this for performance.
    fn validate_structure(
        &self,
        stamp: &Stamp,
        address: &ChunkAddress,
        state: &PostageContext,
    ) -> Result<(), Self::Error> {
        self.validate(stamp, address, state)
    }
}

// Store-based Validator

/// A validator that uses a [`BatchStore`] for validation.
///
/// This validator performs comprehensive validation:
/// 1. Retrieves the batch from the store
/// 2. Checks the batch is not expired
/// 3. Validates the stamp index is within bounds
/// 4. Validates the bucket matches the chunk address
/// 5. Verifies the stamp signature matches the batch owner
///
/// The confirmation threshold gates issuance, through
/// [`batch_for_issuance`](Self::batch_for_issuance), and never acceptance.
///
/// # Example
///
/// ```ignore
/// use nectar_postage::{StoreValidator, BatchStore};
///
/// let store = MyBatchStore::new();
/// let validator = StoreValidator::new(store, 50); // 50 confirmations to issue
///
/// let result = validator.validate(&stamp, &address);
/// ```
#[derive(Debug)]
#[cfg(feature = "std")]
pub struct StoreValidator<S> {
    store: S,
    issuance_threshold: u64,
    /// Non-zero refuses stamps the live network accepts.
    acceptance_threshold: u64,
}

#[cfg(feature = "std")]
impl<S> StoreValidator<S> {
    /// Demands `issuance_threshold` block confirmations to issue, and none to
    /// accept.
    pub const fn new(store: S, issuance_threshold: u64) -> Self {
        Self {
            store,
            issuance_threshold,
            acceptance_threshold: 0,
        }
    }

    /// Sets the confirmations demanded of an inbound stamp's batch.
    #[must_use]
    pub const fn with_acceptance_threshold(mut self, threshold: u64) -> Self {
        self.acceptance_threshold = threshold;
        self
    }

    /// Returns a reference to the underlying store.
    pub const fn store(&self) -> &S {
        &self.store
    }

    /// Returns the confirmations demanded of a batch to issue from.
    pub const fn issuance_threshold(&self) -> u64 {
        self.issuance_threshold
    }

    /// Returns the confirmations demanded of an inbound stamp's batch.
    pub const fn acceptance_threshold(&self) -> u64 {
        self.acceptance_threshold
    }
}

#[cfg(feature = "std")]
impl<S: BatchStore> StoreValidator<S> {
    /// Validates a stamp.
    ///
    /// This performs full validation including signature verification.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the stamp is valid, or a [`StampError`] describing the failure.
    pub fn validate(&self, stamp: &Stamp, address: &ChunkAddress) -> Result<(), StampError> {
        let batch = self.get_batch_for_stamp(stamp)?;
        self.validate_structure_with_batch(stamp, address, &batch)?;
        stamp.verify(address, batch.owner())?;

        Ok(())
    }

    /// Validates the structural properties without signature verification.
    ///
    /// This is faster than full validation when you only need to check
    /// that the stamp references a valid batch and bucket.
    pub fn validate_structure(
        &self,
        stamp: &Stamp,
        address: &ChunkAddress,
    ) -> Result<(), StampError> {
        let batch = self.get_batch_for_stamp(stamp)?;
        self.validate_structure_with_batch(stamp, address, &batch)
    }

    /// Returns a batch fit to issue from: known, unexpired, and confirmed to
    /// [`issuance_threshold`](Self::issuance_threshold).
    ///
    /// # Errors
    ///
    /// [`StampError::BatchNotFound`], [`StampError::BatchNotUsable`] or
    /// [`StampError::BatchExpired`].
    pub fn batch_for_issuance(&self, id: &BatchId) -> Result<Batch, StampError> {
        self.usable_batch(id, self.issuance_threshold)
    }

    fn get_batch_for_stamp(&self, stamp: &Stamp) -> Result<Batch, StampError> {
        self.usable_batch(&stamp.batch(), self.acceptance_threshold)
    }

    fn usable_batch(&self, id: &BatchId, threshold: u64) -> Result<Batch, StampError> {
        self.store.get_usable(id, threshold).map_err(|e| match e {
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
            crate::BatchStoreError::Store(_) => StampError::BatchNotFound(*id),
        })
    }

    /// Validates structure given an already-retrieved batch.
    fn validate_structure_with_batch(
        &self,
        stamp: &Stamp,
        address: &ChunkAddress,
        batch: &Batch,
    ) -> Result<(), StampError> {
        batch.validate_index(&stamp.stamp_index())?;
        batch.validate_bucket(&stamp.stamp_index(), address)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BucketDepth, generators};
    use alloy_primitives::Address;
    use alloy_signer_local::PrivateKeySigner;
    use arbitrary::Unstructured;

    const START_BLOCK: u64 = 100;
    const ISSUANCE_THRESHOLD: u64 = 50;

    #[derive(Debug)]
    struct OneBatchStore {
        batch: Batch,
        context: PostageContext,
    }

    impl BatchStore for OneBatchStore {
        type Error = core::convert::Infallible;

        fn get(&self, id: &BatchId) -> Result<Option<Batch>, Self::Error> {
            Ok((*id == self.batch.id()).then(|| self.batch.clone()))
        }

        fn put(&self, _batch: Batch) -> Result<(), Self::Error> {
            Ok(())
        }

        fn remove(&self, _id: &BatchId) -> Result<bool, Self::Error> {
            Ok(false)
        }

        fn contains(&self, id: &BatchId) -> Result<bool, Self::Error> {
            Ok(*id == self.batch.id())
        }

        fn context(&self) -> Result<PostageContext, Self::Error> {
            Ok(self.context)
        }

        fn set_context(&self, _state: PostageContext) -> Result<(), Self::Error> {
            Ok(())
        }

        fn batch_ids(&self) -> Result<Vec<BatchId>, Self::Error> {
            Ok(vec![self.batch.id()])
        }

        fn count(&self) -> Result<usize, Self::Error> {
            Ok(1)
        }
    }

    fn signer() -> PrivateKeySigner {
        PrivateKeySigner::from_slice(&[7u8; 32]).unwrap()
    }

    fn batch_started_at(owner: Address, start: u64) -> Batch {
        Batch::new(
            BatchId::ZERO,
            1_000,
            start,
            owner,
            18,
            BucketDepth::new(16).unwrap(),
            true,
        )
    }

    fn address() -> ChunkAddress {
        ChunkAddress::new([
            0xCB, 0xE5, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0,
        ])
    }

    fn stamp_for(signer: &PrivateKeySigner, batch: &Batch, address: &ChunkAddress) -> Stamp {
        let mut u = Unstructured::new(&[7u8; 32]);
        generators::signed_stamp(&mut u, signer, batch, address).unwrap()
    }

    fn unconfirmed() -> (StoreValidator<OneBatchStore>, Stamp, ChunkAddress, BatchId) {
        let signer = signer();
        let batch = batch_started_at(signer.address(), START_BLOCK);
        let address = address();
        let stamp = stamp_for(&signer, &batch, &address);
        let id = batch.id();
        let store = OneBatchStore {
            batch,
            context: PostageContext::new(START_BLOCK + 5, 0),
        };
        (
            StoreValidator::new(store, ISSUANCE_THRESHOLD),
            stamp,
            address,
            id,
        )
    }

    #[test]
    fn acceptance_ignores_the_issuance_threshold() {
        let (validator, stamp, address, _) = unconfirmed();

        assert_eq!(validator.acceptance_threshold(), 0);
        assert!(validator.validate(&stamp, &address).is_ok());
        assert!(validator.validate_structure(&stamp, &address).is_ok());
    }

    #[test]
    fn issuance_refuses_an_unconfirmed_batch() {
        let (validator, _, _, id) = unconfirmed();

        assert_eq!(validator.issuance_threshold(), ISSUANCE_THRESHOLD);
        assert!(matches!(
            validator.batch_for_issuance(&id),
            Err(StampError::BatchNotUsable { .. })
        ));
    }

    #[test]
    fn issuance_accepts_a_confirmed_batch() {
        let signer = signer();
        let batch = batch_started_at(signer.address(), START_BLOCK);
        let id = batch.id();
        let store = OneBatchStore {
            batch,
            context: PostageContext::new(START_BLOCK + ISSUANCE_THRESHOLD, 0),
        };
        let validator = StoreValidator::new(store, ISSUANCE_THRESHOLD);

        assert!(validator.batch_for_issuance(&id).is_ok());
    }

    #[test]
    fn acceptance_still_refuses_an_expired_batch() {
        let signer = signer();
        let batch = batch_started_at(signer.address(), START_BLOCK);
        let address = address();
        let stamp = stamp_for(&signer, &batch, &address);
        let total_amount = batch.value();
        let store = OneBatchStore {
            batch,
            context: PostageContext::new(START_BLOCK + 5, total_amount),
        };
        let validator = StoreValidator::new(store, ISSUANCE_THRESHOLD);

        assert!(matches!(
            validator.validate(&stamp, &address),
            Err(StampError::BatchExpired { .. })
        ));
    }

    #[test]
    fn acceptance_refuses_an_unknown_batch() {
        let (validator, _, address, _) = unconfirmed();
        let signer = signer();
        let other = Batch::new(
            BatchId::from([9u8; 32]),
            1_000,
            START_BLOCK,
            signer.address(),
            18,
            BucketDepth::new(16).unwrap(),
            true,
        );
        let stamp = stamp_for(&signer, &other, &address);

        assert!(matches!(
            validator.validate(&stamp, &address),
            Err(StampError::BatchNotFound(_))
        ));
    }

    #[test]
    fn an_opt_in_acceptance_threshold_still_gates() {
        let (validator, stamp, address, _) = unconfirmed();
        let validator = validator.with_acceptance_threshold(ISSUANCE_THRESHOLD);

        assert!(matches!(
            validator.validate(&stamp, &address),
            Err(StampError::BatchNotUsable { .. })
        ));
    }

    #[test]
    fn test_validate_index_valid() {
        let batch: Batch = Batch::new(
            BatchId::ZERO,
            0,
            0,
            Address::ZERO,
            18,
            BucketDepth::new(16).unwrap(),
            false,
        );

        // Valid: bucket < 2^16, index < 2^(18-16) = 4
        let index = StampIndex::new(1000, 3);
        assert!(batch.validate_index(&index).is_ok());
    }

    #[test]
    fn test_validate_index_bucket_out_of_range() {
        let batch: Batch = Batch::new(
            BatchId::ZERO,
            0,
            0,
            Address::ZERO,
            18,
            BucketDepth::new(16).unwrap(),
            false,
        );

        // Invalid: bucket >= 2^16 = 65536
        let index = StampIndex::new(70000, 0);
        assert!(matches!(
            batch.validate_index(&index),
            Err(StampError::InvalidIndex)
        ));
    }

    #[test]
    fn test_validate_index_position_out_of_range() {
        let batch: Batch = Batch::new(
            BatchId::ZERO,
            0,
            0,
            Address::ZERO,
            18,
            BucketDepth::new(16).unwrap(),
            false,
        );

        // Invalid: index >= 2^(18-16) = 4
        let index = StampIndex::new(1000, 5);
        assert!(matches!(
            batch.validate_index(&index),
            Err(StampError::InvalidIndex)
        ));
    }

    #[test]
    fn test_bucket_for_address() {
        let batch: Batch = Batch::new(
            BatchId::ZERO,
            0,
            0,
            Address::ZERO,
            18,
            BucketDepth::new(16).unwrap(),
            false,
        );

        let address = ChunkAddress::new([
            0xCB, 0xE5, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0,
        ]);

        assert_eq!(batch.bucket_for_address(&address).value(), 0xCBE5);
    }

    #[test]
    fn test_validate_bucket_match() {
        let batch: Batch = Batch::new(
            BatchId::ZERO,
            0,
            0,
            Address::ZERO,
            18,
            BucketDepth::new(16).unwrap(),
            false,
        );

        let address = ChunkAddress::new([
            0xCB, 0xE5, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0,
        ]);
        let index = StampIndex::new(0xCBE5, 0);

        assert!(batch.validate_bucket(&index, &address).is_ok());
    }

    #[test]
    fn test_validate_bucket_mismatch() {
        let batch: Batch = Batch::new(
            BatchId::ZERO,
            0,
            0,
            Address::ZERO,
            18,
            BucketDepth::new(16).unwrap(),
            false,
        );

        let address = ChunkAddress::new([
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

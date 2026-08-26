//! The store-free admission composite.
//!
//! The caller loads the batch the stamp claims and passes it in beside the
//! store-level [`PostageContext`]; a reserve runs admission inside its own
//! write transaction against an already-loaded batch. The load is the
//! caller's, and its miss is the caller's [`AdmissionError::UnknownBatch`].

use nectar_primitives::{ChunkAddress, SwarmSpec};

use crate::error::{AdmissionError, StampError};
use crate::{Batch, PostageContext, Stamp};

/// The admission composite: the canonical gate between a stamped chunk and
/// an already-loaded batch.
///
/// It carries no store dependency. The confirmation threshold it holds is
/// the one the usability gate applies against the store-level block height.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AdmissionValidator {
    confirmation_threshold: u64,
}

impl AdmissionValidator {
    /// The composite with the confirmation threshold its usability gate
    /// applies.
    #[inline]
    pub const fn new(confirmation_threshold: u64) -> Self {
        Self {
            confirmation_threshold,
        }
    }

    /// The confirmation threshold the usability gate applies.
    #[inline]
    pub const fn confirmation_threshold(&self) -> u64 {
        self.confirmation_threshold
    }

    /// Admit or deny the stamped chunk against the loaded batch.
    ///
    /// The gates run in a fixed order: the batch the stamp claims, the
    /// usability gate, the expiry gate, the index bounds, the bucket match
    /// and, last, the owner signature.
    ///
    /// # Errors
    ///
    /// [`AdmissionError::BatchNotUsable`] and [`AdmissionError::BatchExpired`]
    /// read the store-level context. The geometry and signature answers
    /// surface through [`AdmissionError::Stamp`], except the signature owner
    /// check, which is [`AdmissionError::OwnerMismatch`].
    pub fn validate<S: SwarmSpec>(
        &self,
        stamp: &Stamp,
        address: &ChunkAddress,
        batch: &Batch<S>,
        context: &PostageContext,
    ) -> Result<(), AdmissionError> {
        if stamp.batch() != batch.id() {
            return Err(AdmissionError::Stamp(StampError::BatchMismatch {
                expected: batch.id(),
                actual: stamp.batch(),
            }));
        }
        if !batch.is_usable(context.block(), self.confirmation_threshold) {
            return Err(AdmissionError::BatchNotUsable);
        }
        if batch.is_expired(context.total_amount()) {
            return Err(AdmissionError::BatchExpired);
        }
        batch.validate_index(&stamp.stamp_index())?;
        batch.validate_bucket(&stamp.stamp_index(), address)?;
        match stamp.verify(address, batch.owner()) {
            Ok(()) => Ok(()),
            Err(StampError::OwnerMismatch { .. }) => Err(AdmissionError::OwnerMismatch),
            Err(e) => Err(AdmissionError::Stamp(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    //! Stamps with generated provenance. Each stamp is signed at test run
    //! time by nectar's own signing path over these module's fixed inputs:
    //! the deterministic EIP-191 key pinned by bee's
    //! `pkg/crypto/signer_test.go` `TestDefaultSignerDeterministic`
    //! (upstream; the same key the postage-issuer EIP-191 interop vectors
    //! run on), a `[0x01; 32]` batch and a fixed chunk address. No upstream
    //! vector exists for the composite's answers; vertex's admission
    //! implementation is its design input and carries no pinned vectors of
    //! its own.

    use alloy_primitives::{Address, Signature, U256, eip191_hash_message, hex};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use nectar_primitives::Mainnet;

    use super::*;
    use crate::{BatchId, BucketDepth, StampDigest, StampIndex};

    /// The deterministic key the postage-issuer EIP-191 interop vectors run
    /// on.
    const OWNER_KEY: &str = "634fb5a872396d9693e5c9f9d7233cfa93f395c093371017ff44aa9ae6564cdd";

    /// The timestamp every fixture stamp carries.
    const TIMESTAMP: u64 = 1_688_492_510_651;

    /// A batch usable at `PostageContext::new(105, 0)` against a threshold of
    /// five, and not expired against that payout.
    fn batch(owner: Address, id: BatchId) -> Batch<Mainnet> {
        Batch::new(
            id,
            1000,
            100,
            owner,
            20,
            BucketDepth::new(16).expect("16 is a depth"),
            true,
        )
    }

    fn owner() -> Address {
        signer().address()
    }

    fn signer() -> PrivateKeySigner {
        PrivateKeySigner::from_slice(&hex::decode(OWNER_KEY).expect("hex")).expect("key")
    }

    fn address() -> ChunkAddress {
        ChunkAddress::new([0xAB; 32])
    }

    /// Sign the stamp digest the way the issuer does: the keccak prehash,
    /// then the EIP-191 personal-message hash of it.
    fn signed_stamp(batch: &Batch<Mainnet>, address: &ChunkAddress) -> Stamp {
        let signer = signer();
        let bucket = batch.bucket_for_address(address).value();
        signed_stamp_at(batch, address, bucket, 0, Some(signer))
    }

    fn signed_stamp_at(
        batch: &Batch<Mainnet>,
        address: &ChunkAddress,
        bucket: u32,
        index: u32,
        signer_slot: Option<PrivateKeySigner>,
    ) -> Stamp {
        let index = StampIndex::new(bucket, index);
        let digest = StampDigest::new(*address, batch.id(), index, TIMESTAMP);
        let prehash = digest.to_prehash();
        let msg_hash = eip191_hash_message(prehash.as_slice());
        let sig = signer_slot.map_or_else(
            || Signature::new(U256::ZERO, U256::from(1), false),
            |signer| signer.sign_hash_sync(&msg_hash).expect("sign"),
        );
        Stamp::with_index(batch.id(), index, TIMESTAMP, sig)
    }

    #[test]
    fn admits_a_stamp_signed_by_the_batch_owner() {
        let batch = batch(owner(), BatchId::new([0x01; 32]));
        let context = PostageContext::new(105, 0);
        let address = address();
        let stamp = signed_stamp(&batch, &address);
        let validator = AdmissionValidator::new(5);

        validator
            .validate(&stamp, &address, &batch, &context)
            .unwrap();
    }

    #[test]
    fn denies_the_batch_the_stamp_does_not_claim() {
        let claimed = batch(owner(), BatchId::new([0x01; 32]));
        let presented = batch(owner(), BatchId::new([0x02; 32]));
        let context = PostageContext::new(105, 0);
        let address = address();
        let stamp = signed_stamp(&claimed, &address);
        let validator = AdmissionValidator::new(5);

        let err = validator
            .validate(&stamp, &address, &presented, &context)
            .unwrap_err();

        // The signature is good for the presented batch's owner; the leading
        // identity gate is what turns the stamp away.
        assert!(matches!(
            err,
            AdmissionError::Stamp(StampError::BatchMismatch { .. })
        ));
    }

    #[test]
    fn denies_a_batch_below_its_confirmation_threshold() {
        let batch = batch(owner(), BatchId::new([0x01; 32]));
        // At the start block itself the batch has no confirmations.
        let context = PostageContext::new(100, 0);
        let address = address();
        let stamp = signed_stamp(&batch, &address);
        let validator = AdmissionValidator::new(5);

        assert!(matches!(
            validator.validate(&stamp, &address, &batch, &context),
            Err(AdmissionError::BatchNotUsable)
        ));
    }

    #[test]
    fn denies_a_batch_value_at_the_cumulative_payout() {
        let batch = batch(owner(), BatchId::new([0x01; 32]));
        let context = PostageContext::new(105, 1000);
        let address = address();
        let stamp = signed_stamp(&batch, &address);
        let validator = AdmissionValidator::new(5);

        assert!(matches!(
            validator.validate(&stamp, &address, &batch, &context),
            Err(AdmissionError::BatchExpired)
        ));
    }

    #[test]
    fn maps_a_foreign_owner_to_the_owner_mismatch() {
        // The batch owner is a fixed address that signed nothing; the stamp
        // is signed by the deterministic test key.
        let batch = batch(Address::new([0xEE; 20]), BatchId::new([0x01; 32]));
        let context = PostageContext::new(105, 0);
        let address = address();
        let stamp = signed_stamp(&batch, &address);
        let validator = AdmissionValidator::new(5);

        assert!(matches!(
            validator.validate(&stamp, &address, &batch, &context),
            Err(AdmissionError::OwnerMismatch)
        ));
    }

    #[test]
    fn wraps_geometry_failures_in_the_stamp_group() {
        let batch = batch(owner(), BatchId::new([0x01; 32]));
        let context = PostageContext::new(105, 0);
        let address = address();
        let bucket = batch.bucket_for_address(&address).value();
        let signer = signer();
        let validator = AdmissionValidator::new(5);

        // An index at the bucket's upper bound is out of capacity.
        let out_of_capacity = signed_stamp_at(&batch, &address, bucket, 16, Some(signer.clone()));
        let err = validator
            .validate(&out_of_capacity, &address, &batch, &context)
            .unwrap_err();
        assert!(matches!(
            err,
            AdmissionError::Stamp(StampError::InvalidIndex)
        ));

        // A wrong-bucket stamp passes the bounds and fails the match.
        let wrong_bucket = signed_stamp_at(&batch, &address, bucket + 1, 0, Some(signer));
        let err = validator
            .validate(&wrong_bucket, &address, &batch, &context)
            .unwrap_err();
        assert!(matches!(
            err,
            AdmissionError::Stamp(StampError::BucketMismatch)
        ));
    }

    #[test]
    fn wraps_an_unrecoverable_signature_in_the_stamp_group() {
        let batch = batch(owner(), BatchId::new([0x01; 32]));
        let context = PostageContext::new(105, 0);
        let address = address();
        let bucket = batch.bucket_for_address(&address).value();
        // No issuer key is passed: an `r == 0` signature fails the k256
        // conversion before any recovery can run.
        let stamp = signed_stamp_at(&batch, &address, bucket, 0, None);
        let validator = AdmissionValidator::new(5);

        let err = validator
            .validate(&stamp, &address, &batch, &context)
            .unwrap_err();
        assert!(matches!(
            err,
            AdmissionError::Stamp(StampError::InvalidSignature)
        ));
    }
}

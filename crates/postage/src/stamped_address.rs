//! Typestate validation carrier for a stamp bound to an address.
//!
//! The signature covers `address | batchID | index | timestamp`, and the
//! address is not on the wire, so the address has to travel inside the value.

use core::marker::PhantomData;

use alloy_primitives::Address;
use nectar_primitives::{ChunkAddress, SwarmSpec};

use crate::{Batch, BatchId, Stamp, StampError, StampIndex};

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Validated {}
    impl Sealed for super::Unvalidated {}
}

/// Sealed validation state of a stamp against an address: [`Validated`] or
/// [`Unvalidated`].
pub trait ValidationState: sealed::Sealed + Send + Sync + 'static {
    /// State name for diagnostics.
    const NAME: &'static str;
}

/// The stamp's geometry and the owner's signature over this address are facts.
///
/// Expiry and batch usability are not: they decay, so consumers still gate on
/// them at the moment of use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Validated;

impl ValidationState for Validated {
    const NAME: &'static str = "validated";
}

/// The pairing is a claim: nothing has checked the stamp against the address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unvalidated;

impl ValidationState for Unvalidated {
    const NAME: &'static str = "unvalidated";
}

/// A stamp together with the address it is bound to, carrying its validation
/// state in the type.
///
/// Pairing exists only at [`Unvalidated`]. Two transitions reach
/// [`Validated`]: [`validate`](Self::validate) recovers the signature, and
/// [`issued_by`](Self::issued_by) stands on a signer already bound to the
/// batch owner.
///
/// The type carries no serde impls in any state, so no wire format can hand
/// back a [`Validated`] value:
///
#[cfg_attr(feature = "serde", doc = "```compile_fail")]
#[cfg_attr(not(feature = "serde"), doc = "```ignore")]
/// use nectar_postage::{StampedAddress, Validated};
///
/// fn de<T: serde::de::DeserializeOwned>() {}
/// de::<StampedAddress<Validated>>();
/// ```
pub struct StampedAddress<V: ValidationState = Validated> {
    address: ChunkAddress,
    stamp: Stamp,
    _validation: PhantomData<V>,
}

impl StampedAddress<Unvalidated> {
    /// Pair a stamp with the address it claims to pay for.
    #[inline]
    #[must_use]
    pub const fn new(address: ChunkAddress, stamp: Stamp) -> Self {
        Self::from_parts(address, stamp)
    }

    /// Certify the pairing against `batch`: the stamp names this batch, its
    /// index and bucket fit the batch geometry, and the owner signed this
    /// address.
    ///
    /// Silent on expiry and confirmations, which decay. Dilution only raises
    /// `depth`, so a pass here holds at every later depth.
    ///
    /// # Errors
    ///
    /// [`StampError::BatchMismatch`], [`StampError::InvalidIndex`],
    /// [`StampError::BucketMismatch`], [`StampError::OwnerMismatch`] or
    /// [`StampError::InvalidSignature`].
    pub fn validate<S: SwarmSpec>(
        self,
        batch: &Batch<S>,
    ) -> Result<StampedAddress<Validated>, StampError> {
        if self.stamp.batch() != batch.id() {
            return Err(StampError::BatchMismatch {
                expected: batch.id(),
                actual: self.stamp.batch(),
            });
        }

        let index = self.stamp.stamp_index();
        batch.validate_index(&index)?;
        batch.validate_bucket(&index, &self.address)?;
        self.stamp.verify(&self.address, batch.owner())?;

        Ok(StampedAddress::from_parts(self.address, self.stamp))
    }

    /// Certify a pairing stamped here by the batch owner's own key, so the
    /// signature is bound rather than recovered.
    ///
    /// `signer` is the address of the key that produced the signature; the
    /// caller establishes that, and nothing here re-derives it.
    ///
    /// # Errors
    ///
    /// [`StampError::OwnerMismatch`], [`StampError::BatchMismatch`] or
    /// [`StampError::BucketMismatch`].
    pub fn issued_by<S: SwarmSpec>(
        self,
        batch: &Batch<S>,
        signer: Address,
    ) -> Result<StampedAddress<Validated>, StampError> {
        if signer != batch.owner() {
            return Err(StampError::OwnerMismatch {
                expected: batch.owner(),
                actual: signer,
            });
        }
        if self.stamp.batch() != batch.id() {
            return Err(StampError::BatchMismatch {
                expected: batch.id(),
                actual: self.stamp.batch(),
            });
        }
        // The position bound rises with dilution and the issuer allocates
        // against the live depth, so a batch copy can sit below it.
        batch.validate_bucket(&self.stamp.stamp_index(), &self.address)?;

        Ok(StampedAddress::from_parts(self.address, self.stamp))
    }
}

impl<V: ValidationState> StampedAddress<V> {
    /// Sound only where a transition over this same address justifies `V`.
    #[inline]
    #[must_use]
    pub(crate) const fn from_parts(address: ChunkAddress, stamp: Stamp) -> Self {
        Self {
            address,
            stamp,
            _validation: PhantomData,
        }
    }

    /// The address the stamp is bound to.
    #[inline]
    #[must_use]
    pub const fn address(&self) -> &ChunkAddress {
        &self.address
    }

    /// The stamp.
    #[inline]
    #[must_use]
    pub const fn stamp(&self) -> &Stamp {
        &self.stamp
    }

    /// The batch the stamp draws on.
    #[inline]
    #[must_use]
    pub const fn batch(&self) -> BatchId {
        self.stamp.batch()
    }

    /// The stamp's bucket and position.
    #[inline]
    #[must_use]
    pub const fn stamp_index(&self) -> StampIndex {
        self.stamp.stamp_index()
    }

    /// Split into the address and the stamp.
    #[inline]
    #[must_use]
    pub const fn into_parts(self) -> (ChunkAddress, Stamp) {
        (self.address, self.stamp)
    }
}

impl<V: ValidationState> Clone for StampedAddress<V> {
    fn clone(&self) -> Self {
        Self {
            address: self.address,
            stamp: self.stamp.clone(),
            _validation: PhantomData,
        }
    }
}

impl<V: ValidationState> PartialEq for StampedAddress<V> {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address && self.stamp == other.stamp
    }
}

impl<V: ValidationState> Eq for StampedAddress<V> {}

impl<V: ValidationState> core::fmt::Debug for StampedAddress<V> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("StampedAddress")
            .field("state", &V::NAME)
            .field("address", &self.address)
            .field("stamp", &self.stamp)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, Signature};
    use alloy_signer_local::PrivateKeySigner;
    use arbitrary::Unstructured;
    use proptest::prelude::*;

    use super::*;
    use crate::{BucketDepth, generators};

    const DEPTH: u8 = 18;
    const BUCKET_DEPTH: u8 = 16;

    fn signer() -> PrivateKeySigner {
        PrivateKeySigner::from_slice(&[7u8; 32]).unwrap()
    }

    fn other_signer() -> PrivateKeySigner {
        PrivateKeySigner::from_slice(&[9u8; 32]).unwrap()
    }

    fn batch_owned_by(owner: Address, id: BatchId) -> Batch {
        Batch::new(
            id,
            1_000,
            100,
            owner,
            DEPTH,
            BucketDepth::new(BUCKET_DEPTH).unwrap(),
            true,
        )
    }

    /// Bucket 0xCBE5 at bucket depth 16.
    fn address_with(tail: u8) -> ChunkAddress {
        let mut bytes = [0u8; 32];
        bytes[0] = 0xCB;
        bytes[1] = 0xE5;
        bytes[31] = tail;
        ChunkAddress::new(bytes)
    }

    fn stamp_for(signer: &PrivateKeySigner, batch: &Batch, address: &ChunkAddress) -> Stamp {
        let mut u = Unstructured::new(&[7u8; 32]);
        generators::signed_stamp(&mut u, signer, batch, address).unwrap()
    }

    fn coherent() -> (Batch, StampedAddress<Unvalidated>) {
        let signer = signer();
        let batch = batch_owned_by(signer.address(), BatchId::ZERO);
        let address = address_with(0);
        let stamp = stamp_for(&signer, &batch, &address);
        (batch, StampedAddress::new(address, stamp))
    }

    #[test]
    fn validate_certifies_a_coherent_pairing() {
        let (batch, pairing) = coherent();
        let address = *pairing.address();
        let stamp = pairing.stamp().clone();

        let validated = pairing.validate(&batch).unwrap();
        assert_eq!(validated.address(), &address);
        assert_eq!(validated.stamp(), &stamp);
        assert_eq!(validated.batch(), batch.id());
    }

    /// Same owner and same shape, so only the batch id can refuse it.
    #[test]
    fn validate_refuses_a_foreign_batch() {
        let signer = signer();
        let bought = batch_owned_by(signer.address(), BatchId::ZERO);
        let spent = batch_owned_by(signer.address(), BatchId::from([1u8; 32]));
        let address = address_with(0);
        let stamp = stamp_for(&signer, &bought, &address);

        assert!(matches!(
            StampedAddress::new(address, stamp).validate(&spent),
            Err(StampError::BatchMismatch { .. })
        ));
    }

    /// Same bucket, so geometry passes and the signature is what refuses it.
    #[test]
    fn validate_refuses_a_re_paired_address() {
        let (batch, pairing) = coherent();
        let (_, stamp) = pairing.into_parts();
        let elsewhere = address_with(0xFF);

        assert!(matches!(
            StampedAddress::new(elsewhere, stamp).validate(&batch),
            Err(StampError::OwnerMismatch { .. })
        ));
    }

    #[test]
    fn validate_refuses_a_bucket_mismatch() {
        let (batch, pairing) = coherent();
        let (_, stamp) = pairing.into_parts();
        let other_bucket = ChunkAddress::new([0x12u8; 32]);

        assert!(matches!(
            StampedAddress::new(other_bucket, stamp).validate(&batch),
            Err(StampError::BucketMismatch)
        ));
    }

    #[test]
    fn validate_refuses_a_position_past_capacity() {
        let (batch, pairing) = coherent();
        let (address, stamp) = pairing.into_parts();
        let past_end = StampIndex::new(stamp.bucket(), batch.bucket_upper_bound());
        let stamp = Stamp::with_index(
            stamp.batch(),
            past_end,
            stamp.timestamp(),
            *stamp.signature(),
        );

        assert!(matches!(
            StampedAddress::new(address, stamp).validate(&batch),
            Err(StampError::InvalidIndex)
        ));
    }

    #[test]
    fn validate_refuses_a_foreign_signer() {
        let signer = other_signer();
        let batch = batch_owned_by(signer.address(), BatchId::ZERO);
        let address = address_with(0);
        let stamp = stamp_for(&signer, &batch, &address);
        let owned_elsewhere = batch_owned_by(self::signer().address(), BatchId::ZERO);

        assert!(matches!(
            StampedAddress::new(address, stamp).validate(&owned_elsewhere),
            Err(StampError::OwnerMismatch { .. })
        ));
    }

    /// Dilution raises depth only, so it can never invalidate an allocated
    /// index.
    #[test]
    fn validation_survives_dilution() {
        let (mut batch, pairing) = coherent();
        batch.set_depth(DEPTH + 4);

        assert!(pairing.validate(&batch).is_ok());
    }

    /// Expiry and confirmations decay, so the marker must not certify them.
    #[test]
    fn validation_ignores_expiry_and_confirmations() {
        let signer = signer();
        let mut batch = batch_owned_by(signer.address(), BatchId::ZERO);
        let address = address_with(0);
        let stamp = stamp_for(&signer, &batch, &address);
        batch.set_value(0);

        assert!(batch.is_expired(1));
        assert!(!batch.is_usable(batch.start(), 50));
        assert!(StampedAddress::new(address, stamp).validate(&batch).is_ok());
    }

    #[test]
    fn issued_by_agrees_with_validate_on_a_coherent_pairing() {
        let (batch, pairing) = coherent();
        let owner = batch.owner();

        let bound = pairing.clone().issued_by(&batch, owner).unwrap();
        let recovered = pairing.validate(&batch).unwrap();
        assert_eq!(bound, recovered);
    }

    /// The whole point: the producer pays no `ecrecover`, so a signature that
    /// could never recover still passes once the signer is the owner.
    #[test]
    fn issued_by_pays_no_recovery() {
        let (batch, pairing) = coherent();
        let (address, stamp) = pairing.into_parts();
        let junk = Stamp::with_index(
            stamp.batch(),
            stamp.stamp_index(),
            stamp.timestamp(),
            Signature::from_raw(&[1u8; 65]).unwrap(),
        );

        assert!(
            StampedAddress::new(address, junk)
                .issued_by(&batch, batch.owner())
                .is_ok()
        );
    }

    #[test]
    fn issued_by_refuses_a_signer_that_is_not_the_owner() {
        let (batch, pairing) = coherent();

        assert!(matches!(
            pairing.issued_by(&batch, other_signer().address()),
            Err(StampError::OwnerMismatch { .. })
        ));
    }

    #[test]
    fn issued_by_refuses_a_foreign_batch() {
        let signer = signer();
        let bought = batch_owned_by(signer.address(), BatchId::ZERO);
        let spent = batch_owned_by(signer.address(), BatchId::from([1u8; 32]));
        let address = address_with(0);
        let stamp = stamp_for(&signer, &bought, &address);

        assert!(matches!(
            StampedAddress::new(address, stamp).issued_by(&spent, signer.address()),
            Err(StampError::BatchMismatch { .. })
        ));
    }

    #[test]
    fn issued_by_refuses_a_re_paired_address() {
        let (batch, pairing) = coherent();
        let (_, stamp) = pairing.into_parts();
        let other_bucket = ChunkAddress::new([0x12u8; 32]);

        assert!(matches!(
            StampedAddress::new(other_bucket, stamp).issued_by(&batch, batch.owner()),
            Err(StampError::BucketMismatch)
        ));
    }

    /// The issuer allocates against the live depth, so a batch copy taken
    /// before a dilution must not bound the position.
    #[test]
    fn issued_by_admits_a_position_the_batch_copy_cannot_bound() {
        let (batch, pairing) = coherent();
        let (address, stamp) = pairing.into_parts();
        let diluted = StampIndex::new(stamp.bucket(), batch.bucket_upper_bound());
        let stamp = Stamp::with_index(
            stamp.batch(),
            diluted,
            stamp.timestamp(),
            *stamp.signature(),
        );

        assert!(matches!(
            StampedAddress::new(address, stamp.clone()).validate(&batch),
            Err(StampError::InvalidIndex)
        ));
        assert!(
            StampedAddress::new(address, stamp)
                .issued_by(&batch, batch.owner())
                .is_ok()
        );
    }

    #[test]
    fn debug_names_the_validation_state() {
        let (batch, pairing) = coherent();
        assert!(format!("{pairing:?}").contains("unvalidated"));
        assert!(format!("{:?}", pairing.validate(&batch).unwrap()).contains("validated"));
    }

    #[test]
    fn state_names_are_distinct() {
        assert_eq!(Validated::NAME, "validated");
        assert_eq!(Unvalidated::NAME, "unvalidated");
    }

    proptest! {
        #[test]
        fn a_signed_pairing_validates_against_its_batch(
            seed in proptest::collection::vec(any::<u8>(), 128..2048),
        ) {
            let mut u = Unstructured::new(&seed);
            let signer = nectar_primitives::generators::signer(&mut u).unwrap();
            let batch = generators::batch(&mut u, signer.address()).unwrap();
            let address = ChunkAddress::new(u.arbitrary::<[u8; 32]>().unwrap());
            let stamp = generators::signed_stamp(&mut u, &signer, &batch, &address).unwrap();

            let validated = StampedAddress::new(address, stamp).validate(&batch);
            prop_assert!(validated.is_ok());
        }

        /// The re-paired address keeps the leading 4 bytes, so it shares the
        /// bucket at every bucket depth and only the signature can refuse it.
        #[test]
        fn a_re_paired_stamp_never_validates(
            seed in proptest::collection::vec(any::<u8>(), 128..2048),
            tail in any::<[u8; 28]>(),
        ) {
            let mut u = Unstructured::new(&seed);
            let signer = nectar_primitives::generators::signer(&mut u).unwrap();
            let batch = generators::batch(&mut u, signer.address()).unwrap();
            let address = ChunkAddress::new(u.arbitrary::<[u8; 32]>().unwrap());
            let stamp = generators::signed_stamp(&mut u, &signer, &batch, &address).unwrap();

            let mut bytes = *address.as_array();
            prop_assume!(bytes[4..] != tail[..]);
            bytes[4..].copy_from_slice(&tail);

            let refused = StampedAddress::new(ChunkAddress::new(bytes), stamp).validate(&batch);
            prop_assert!(
                matches!(
                    refused,
                    Err(StampError::OwnerMismatch { .. } | StampError::InvalidSignature)
                ),
                "the signature must refuse a re-paired address"
            );
        }
    }
}

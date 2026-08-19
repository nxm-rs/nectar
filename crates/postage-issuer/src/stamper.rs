//! Stamper trait and implementations for creating signed stamps.
//!
//! # EIP-191 Compatibility
//!
//! Stamps are signed using EIP-191 personal message signing. The prehash
//! (keccak256 of stamp data) is treated as the message, which gets prefixed
//! with `"\x19Ethereum Signed Message:\n32"`.
//!
//! Use alloy's [`SignerSync`] trait with `sign_message_sync(prehash.as_slice())`
//! rather than `sign_hash_sync`.

use alloy_primitives::Signature;
#[cfg(feature = "std")]
use alloy_signer::SignerSync;

use crate::StampIssuer;
#[cfg(feature = "std")]
use crate::error::SigningError;
use crate::permit::Prepared;
use nectar_clock::Clock;
#[cfg(feature = "std")]
use nectar_clock::SystemClock;
use nectar_postage::{BatchId, Stamp, StampDigest, StampError};
use nectar_primitives::ChunkAddress;

/// Reads `clock` as a stamp timestamp: nanoseconds since the unix epoch,
/// clamped to zero for pre-epoch readings.
pub(crate) fn stamp_timestamp<C: Clock + ?Sized>(clock: &C) -> u64 {
    u64::try_from(clock.now_ns()).unwrap_or(0)
}

/// A trait for entities that can stamp chunks.
///
/// Implementations of this trait manage the state needed to stamp chunks,
/// including tracking bucket usage and generating signatures.
///
/// # Example
///
/// ```ignore
/// use nectar_postage_issuer::{Stamper, Stamp, StampError, BatchId};
/// use nectar_primitives::ChunkAddress;
///
/// struct MyStamper { /* ... */ }
///
/// impl Stamper for MyStamper {
///     type Error = StampError;
///
///     fn stamp(&mut self, address: &ChunkAddress) -> Result<Stamp, Self::Error> {
///         // Implementation details...
///     }
///
///     fn batch_id(&self) -> BatchId {
///         // Return the batch ID
///     }
///     // ... other methods
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
    /// - The bucket is full
    /// - Signature generation fails
    /// - Any other implementation-specific error occurs
    fn stamp(&mut self, address: &ChunkAddress) -> Result<Stamp, Self::Error>;

    /// Returns the batch ID that stamps are issued for.
    fn batch_id(&self) -> BatchId;

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

/// A stamper that combines an issuer (for bucket tracking) with a signer.
///
/// This implementation delegates bucket/index tracking to a [`StampIssuer`]
/// and handles the signing of stamps. This composition allows using different
/// issuer implementations (e.g., `MemoryIssuer`, `RingIssuer`) with any signer.
///
/// Stamp timestamps come from the clock type parameter, defaulting to the
/// system clock; [`with_clock`](Self::with_clock) injects a deterministic
/// source.
///
/// # Example
///
/// ```ignore
/// use nectar_postage_issuer::{BatchStamper, MemoryIssuer, Stamper};
///
/// let issuer = MemoryIssuer::from_batch(&batch)?;
/// let mut stamper = BatchStamper::new(issuer, my_signer);
/// let stamp = stamper.stamp(&chunk_address)?;
/// ```
#[cfg(feature = "std")]
#[derive(Debug, Clone)]
pub struct BatchStamper<I, S, C = SystemClock> {
    /// The issuer for tracking bucket utilization.
    issuer: I,
    /// The signer used to sign stamps.
    signer: S,
    /// The timestamp source for issued stamps.
    clock: C,
}

/// Without `std` there is no default clock; construct via
/// [`with_clock`](Self::with_clock).
#[cfg(not(feature = "std"))]
#[derive(Debug, Clone)]
pub struct BatchStamper<I, S, C> {
    /// The issuer for tracking bucket utilization.
    issuer: I,
    /// The signer used to sign stamps.
    signer: S,
    /// The timestamp source for issued stamps.
    clock: C,
}

#[cfg(feature = "std")]
impl<I, S> BatchStamper<I, S> {
    /// Creates a new batch stamper with the given issuer and signer, reading
    /// stamp timestamps from the system clock.
    pub const fn new(issuer: I, signer: S) -> Self {
        Self {
            issuer,
            signer,
            clock: SystemClock,
        }
    }
}

impl<I, S, C> BatchStamper<I, S, C> {
    /// Creates a batch stamper that reads stamp timestamps from `clock`.
    pub const fn with_clock(issuer: I, signer: S, clock: C) -> Self {
        Self {
            issuer,
            signer,
            clock,
        }
    }

    /// Returns a reference to the clock.
    pub const fn clock(&self) -> &C {
        &self.clock
    }

    /// Returns a reference to the issuer.
    pub const fn issuer(&self) -> &I {
        &self.issuer
    }

    /// Returns a mutable reference to the issuer.
    pub const fn issuer_mut(&mut self) -> &mut I {
        &mut self.issuer
    }

    /// Returns a reference to the signer.
    pub const fn signer(&self) -> &S {
        &self.signer
    }

    /// Returns a mutable reference to the signer.
    pub const fn signer_mut(&mut self) -> &mut S {
        &mut self.signer
    }

    /// Splits into issuer, signer and clock, so an issuer moves between the
    /// one-off and streaming stamping doors.
    pub fn into_parts(self) -> (I, S, C) {
        (self.issuer, self.signer, self.clock)
    }

    /// Creates a stamp from a digest and signature.
    ///
    /// This is a utility function for converting an alloy `Signature` into
    /// the 65-byte format used in stamps (r || s || v).
    #[inline]
    pub const fn stamp_from_signature(digest: &StampDigest, sig: Signature) -> Stamp {
        // Signature is now stored directly in Stamp
        Stamp::with_index(digest.batch_id, digest.index, digest.timestamp, sig)
    }
}

impl<I, S, C> BatchStamper<I, S, C>
where
    I: StampIssuer,
{
    /// Claims a slot for `address` without signing it, for async signing
    /// flows.
    ///
    /// # Errors
    ///
    /// [`StampError::BucketFull`] once the bucket has no slot left.
    pub fn reserve(
        &self,
        address: &ChunkAddress,
        timestamp: u64,
    ) -> Result<Prepared<I::Spec>, StampError> {
        self.issuer.reserve(address, timestamp)
    }
}

#[cfg(feature = "std")]
impl<I, S, C> Stamper for BatchStamper<I, S, C>
where
    I: StampIssuer,
    S: SignerSync,
    C: Clock,
{
    type Error = SigningError;

    fn stamp(&mut self, address: &ChunkAddress) -> Result<Stamp, Self::Error> {
        let timestamp = stamp_timestamp(&self.clock);
        let permit = self.issuer.reserve(address, timestamp)?;
        let prehash = permit.digest().to_prehash();

        let sig = self.signer.sign_message_sync(prehash.as_slice())?;

        Ok(permit.stamp(sig))
    }

    fn batch_id(&self) -> BatchId {
        self.issuer.batch_id()
    }

    fn max_bucket_utilization(&self) -> u32 {
        self.issuer.max_bucket_utilization()
    }

    fn bucket_has_capacity(&self, bucket: u32) -> bool {
        self.issuer.bucket_has_capacity(bucket)
    }
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;
    use crate::MemoryIssuer;
    use alloy_primitives::{B256, Signature, U256};
    use nectar_postage::BucketDepth;
    use nectar_postage::StampIndex;

    /// A mock signer for testing that creates deterministic signatures.
    struct MockSigner;

    impl SignerSync for MockSigner {
        fn sign_hash_sync(&self, _hash: &B256) -> Result<Signature, alloy_signer::Error> {
            Ok(Signature::new(U256::from(1), U256::from(2), false))
        }

        fn sign_message_sync(&self, _message: &[u8]) -> Result<Signature, alloy_signer::Error> {
            Ok(Signature::new(U256::from(1), U256::from(2), false))
        }

        fn chain_id_sync(&self) -> Option<u64> {
            None
        }
    }

    #[test]
    fn test_batch_stamper_basic() {
        let issuer: MemoryIssuer =
            MemoryIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());
        let mut stamper = BatchStamper::new(issuer, MockSigner);

        let address = ChunkAddress::new([0xAB; 32]);
        let stamp = stamper.stamp(&address).unwrap();

        assert_eq!(stamp.batch(), BatchId::ZERO);
        // First stamp in bucket should have index 0
        assert_eq!(stamp.index(), 0);
    }

    #[test]
    fn test_batch_stamper_increments_index() {
        let issuer: MemoryIssuer =
            MemoryIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());
        let mut stamper = BatchStamper::new(issuer, MockSigner);

        // Use same address to hit same bucket
        let address = ChunkAddress::new([0xAB; 32]);

        let stamp1 = stamper.stamp(&address).unwrap();
        let stamp2 = stamper.stamp(&address).unwrap();
        let stamp3 = stamper.stamp(&address).unwrap();

        assert_eq!(stamp1.index(), 0);
        assert_eq!(stamp2.index(), 1);
        assert_eq!(stamp3.index(), 2);

        // All should be in the same bucket
        assert_eq!(stamp1.bucket(), stamp2.bucket());
        assert_eq!(stamp2.bucket(), stamp3.bucket());
    }

    #[test]
    fn test_batch_stamper_injected_clock() {
        use nectar_clock::ManualClock;

        let issuer: MemoryIssuer =
            MemoryIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());
        let clock = ManualClock::new(1_234_567_890);
        let mut stamper = BatchStamper::with_clock(issuer, MockSigner, &clock);

        let address = ChunkAddress::new([0xAB; 32]);
        let stamp = stamper.stamp(&address).unwrap();
        assert_eq!(stamp.timestamp(), 1_234_567_890);

        clock.set_ns(2_000_000_000);
        let stamp = stamper.stamp(&address).unwrap();
        assert_eq!(stamp.timestamp(), 2_000_000_000);

        // A pre-epoch reading clamps to zero, matching the default clock path.
        clock.set_ns(-1);
        let stamp = stamper.stamp(&address).unwrap();
        assert_eq!(stamp.timestamp(), 0);
    }

    #[test]
    fn test_batch_stamper_bucket_full() {
        use crate::error::SigningError;

        // Create an issuer with very small bucket capacity: depth=17, bucket_depth=16
        // This gives 2^(17-16) = 2 slots per bucket
        let issuer: MemoryIssuer =
            MemoryIssuer::new(BatchId::ZERO, 17, BucketDepth::new(16).unwrap());
        let mut stamper = BatchStamper::new(issuer, MockSigner);

        let address = ChunkAddress::new([0xAB; 32]);

        // First two stamps should succeed
        assert!(stamper.stamp(&address).is_ok());
        assert!(stamper.stamp(&address).is_ok());

        // Third stamp should fail - bucket is full
        let result = stamper.stamp(&address);
        assert!(matches!(
            result,
            Err(SigningError::Stamp(StampError::BucketFull { .. }))
        ));
    }

    #[test]
    fn test_batch_stamper_max_utilization() {
        let issuer: MemoryIssuer =
            MemoryIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());
        let mut stamper = BatchStamper::new(issuer, MockSigner);

        assert_eq!(stamper.max_bucket_utilization(), 0);

        let address = ChunkAddress::new([0xAB; 32]);
        stamper.stamp(&address).unwrap();
        assert_eq!(stamper.max_bucket_utilization(), 1);

        stamper.stamp(&address).unwrap();
        assert_eq!(stamper.max_bucket_utilization(), 2);
    }

    #[test]
    fn test_into_parts_keeps_issuer_state() {
        let issuer: MemoryIssuer =
            MemoryIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());
        let mut stamper = BatchStamper::new(issuer, MockSigner);

        let address = ChunkAddress::new([0xAB; 32]);
        stamper.stamp(&address).unwrap();

        let (issuer, _signer, _clock) = stamper.into_parts();
        assert_eq!(issuer.stamps_issued(), Some(1));
    }

    #[test]
    fn test_stamp_digest_prehash() {
        let address = ChunkAddress::new([0xAB; 32]);
        let batch_id = BatchId::ZERO;
        let index = StampIndex::new(100, 5);
        let timestamp = 1234567890u64;

        let digest = StampDigest::new(address, batch_id, index, timestamp);
        let prehash = digest.to_prehash();

        let prehash2 = digest.to_prehash();
        assert_eq!(prehash, prehash2);
    }

    #[test]
    fn test_eip191_signing_interop() {
        use alloy_primitives::hex;
        use alloy_signer::SignerSync;
        use alloy_signer_local::PrivateKeySigner;

        let privkey_bytes =
            hex::decode("634fb5a872396d9693e5c9f9d7233cfa93f395c093371017ff44aa9ae6564cdd")
                .unwrap();
        let message =
            hex::decode("2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae")
                .unwrap();
        let expected_sig = hex::decode(
            "336d24afef78c5883b96ad9a62552a8db3d236105cb059ddd04dc49680869dc16234f6852c277087f025d4114c4fac6b40295ecffd1194a84cdb91bd571769491b"
        ).unwrap();

        let signer = PrivateKeySigner::from_slice(&privkey_bytes).unwrap();
        let signature = signer.sign_message_sync(&message).unwrap();

        let mut sig_bytes = [0u8; 65];
        sig_bytes[..32].copy_from_slice(&signature.r().to_be_bytes::<32>());
        sig_bytes[32..64].copy_from_slice(&signature.s().to_be_bytes::<32>());
        sig_bytes[64] = u8::from(signature.v()) + 27;

        assert_eq!(
            sig_bytes.as_slice(),
            expected_sig.as_slice(),
            "Signature mismatch with test vector"
        );
    }

    #[test]
    fn test_eip191_recovery_interop() {
        use alloy_primitives::hex;
        use alloy_signer::SignerSync;
        use alloy_signer_local::PrivateKeySigner;

        let privkey_bytes =
            hex::decode("634fb5a872396d9693e5c9f9d7233cfa93f395c093371017ff44aa9ae6564cdd")
                .unwrap();
        let message =
            hex::decode("2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae")
                .unwrap();
        let expected_address = "8d3766440f0d7b949a5e32995d09619a7f86e632";

        let signer = PrivateKeySigner::from_slice(&privkey_bytes).unwrap();
        let signature = signer.sign_message_sync(&message).unwrap();
        let recovered = signature.recover_address_from_msg(&message).unwrap();

        assert_eq!(
            hex::encode(recovered.as_slice()),
            expected_address,
            "Recovered address mismatch"
        );
        assert_eq!(
            recovered,
            signer.address(),
            "Recovered address should match signer address"
        );
    }
}

//! Stamper trait and implementations for creating signed stamps.
//!
//! # EIP-191 Compatibility
//!
//! To be compatible with Go/bee implementations, stamps must be signed using
//! EIP-191 personal message signing. The prehash (keccak256 of stamp data) is
//! treated as the message, which gets prefixed with `"\x19Ethereum Signed Message:\n32"`.
//!
//! Use alloy's [`SignerSync`] trait with `sign_message_sync(prehash.as_slice())`
//! rather than `sign_hash_sync` to ensure compatibility.

use alloy_primitives::Signature;
use alloy_signer::SignerSync;

use crate::error::SigningError;
use crate::StampIssuer;
use nectar_postage::{BatchId, Stamp, StampDigest, StampError, current_timestamp};
use nectar_primitives::SwarmAddress;

/// A trait for entities that can stamp chunks.
///
/// Implementations of this trait manage the state needed to stamp chunks,
/// including tracking bucket usage and generating signatures.
///
/// # Example
///
/// ```ignore
/// use nectar_postage_issuer::{Stamper, Stamp, StampError, BatchId};
/// use nectar_primitives::SwarmAddress;
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
    fn stamp(&mut self, address: &SwarmAddress) -> Result<Stamp, Self::Error>;

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
/// issuer implementations (e.g., `MemoryIssuer`, `ShardedIssuer`) with any signer.
///
/// # Example
///
/// ```ignore
/// use nectar_postage_issuer::{BatchStamper, MemoryIssuer, Stamper};
///
/// let issuer = MemoryIssuer::from_batch(&batch);
/// let mut stamper = BatchStamper::new(issuer, my_signer);
/// let stamp = stamper.stamp(&chunk_address)?;
/// ```
#[derive(Debug, Clone)]
pub struct BatchStamper<I, S> {
    /// The issuer for tracking bucket utilization.
    issuer: I,
    /// The signer used to sign stamps.
    signer: S,
}

impl<I, S> BatchStamper<I, S> {
    /// Creates a new batch stamper with the given issuer and signer.
    pub const fn new(issuer: I, signer: S) -> Self {
        Self { issuer, signer }
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

    /// Creates a stamp from a digest and signature.
    ///
    /// This is a utility function for converting an alloy `Signature` into
    /// the 65-byte format used in stamps (r || s || v).
    #[inline]
    pub fn stamp_from_signature(digest: &StampDigest, sig: Signature) -> Stamp {
        // Signature is now stored directly in Stamp
        Stamp::with_index(digest.batch_id, digest.index, digest.timestamp, sig)
    }
}

impl<I, S> BatchStamper<I, S>
where
    I: StampIssuer,
{
    /// Prepares a stamp for the given chunk address.
    ///
    /// This allocates an index from the issuer and creates the digest,
    /// but does not sign it. Use this for async signing flows.
    pub fn prepare_stamp(
        &mut self,
        address: &SwarmAddress,
        timestamp: u64,
    ) -> Result<StampDigest, StampError> {
        self.issuer.prepare_stamp(address, timestamp)
    }
}

impl<I, S> Stamper for BatchStamper<I, S>
where
    I: StampIssuer,
    S: SignerSync,
{
    type Error = SigningError;

    fn stamp(&mut self, address: &SwarmAddress) -> Result<Stamp, Self::Error> {
        let timestamp = current_timestamp();
        let digest = self.issuer.prepare_stamp(address, timestamp)?;
        let prehash = digest.to_prehash();

        let sig = self.signer.sign_message_sync(prehash.as_slice())?;

        Ok(Self::stamp_from_signature(&digest, sig))
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
    use alloy_primitives::{B256, Signature, U256};
    use crate::MemoryIssuer;
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
        let issuer = MemoryIssuer::new(B256::ZERO, 20, 16);
        let mut stamper = BatchStamper::new(issuer, MockSigner);

        let address = SwarmAddress::new([0xAB; 32]);
        let stamp = stamper.stamp(&address).unwrap();

        assert_eq!(stamp.batch(), B256::ZERO);
        // First stamp in bucket should have index 0
        assert_eq!(stamp.index(), 0);
    }

    #[test]
    fn test_batch_stamper_increments_index() {
        let issuer = MemoryIssuer::new(B256::ZERO, 20, 16);
        let mut stamper = BatchStamper::new(issuer, MockSigner);

        // Use same address to hit same bucket
        let address = SwarmAddress::new([0xAB; 32]);

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
    fn test_batch_stamper_bucket_full() {
        use crate::error::SigningError;

        // Create an issuer with very small bucket capacity: depth=17, bucket_depth=16
        // This gives 2^(17-16) = 2 slots per bucket
        let issuer = MemoryIssuer::new(B256::ZERO, 17, 16);
        let mut stamper = BatchStamper::new(issuer, MockSigner);

        let address = SwarmAddress::new([0xAB; 32]);

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
        let issuer = MemoryIssuer::new(B256::ZERO, 20, 16);
        let mut stamper = BatchStamper::new(issuer, MockSigner);

        assert_eq!(stamper.max_bucket_utilization(), 0);

        let address = SwarmAddress::new([0xAB; 32]);
        stamper.stamp(&address).unwrap();
        assert_eq!(stamper.max_bucket_utilization(), 1);

        stamper.stamp(&address).unwrap();
        assert_eq!(stamper.max_bucket_utilization(), 2);
    }

    #[test]
    fn test_stamp_digest_prehash() {
        let address = SwarmAddress::new([0xAB; 32]);
        let batch_id = B256::ZERO;
        let index = StampIndex::new(100, 5);
        let timestamp = 1234567890u64;

        let digest = StampDigest::new(address, batch_id, index, timestamp);
        let prehash = digest.to_prehash();

        // Prehash should be deterministic
        let prehash2 = digest.to_prehash();
        assert_eq!(prehash, prehash2);
    }

    /// Test EIP-191 signing interoperability.
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
        sig_bytes[64] = signature.v() as u8 + 27;

        assert_eq!(
            sig_bytes.as_slice(),
            expected_sig.as_slice(),
            "Signature mismatch with Go/bee test vector"
        );
    }

    /// Test that signature recovery works correctly with EIP-191.
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

    /// Test verifying a stamp created by Go/bee in Rust.
    #[test]
    fn test_verify_go_created_stamp() {
        use alloy_primitives::{Address, hex};

        let chunk_addr_bytes =
            hex::decode("0000000000000000000000000000000000000000000000000000000000000002")
                .unwrap();
        let full_stamp_bytes = hex::decode(
            "000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000003496cb9ac06221d39c3f6a7dd3b9c2301c1f923162b90d5443e42023f34ff908945b0da1c297190f111b7c6ebc828648ead8f7fce06c0364cb5a833410230c5c01c"
        ).unwrap();
        let expected_owner = "8d3766440f0d7b949a5e32995d09619a7f86e632";
        let expected_digest =
            hex::decode("f4fe8b1b61d3ac2155c07fbfe445599a4119fbd29b1125b5ac0d06964f76ec20")
                .unwrap();

        let stamp = Stamp::try_from_slice(&full_stamp_bytes).unwrap();

        assert_eq!(stamp.bucket(), 0);
        assert_eq!(stamp.index(), 0);
        assert_eq!(stamp.timestamp(), 3);
        // Check v value - Go uses v=28 (0x1c) for odd y parity
        // as_bytes()[64] gives us the raw v byte which should be 28 (0x1c)
        assert_eq!(
            stamp.signature().as_bytes()[64],
            0x1c,
            "Go uses v=28 (0x1c) for odd y parity"
        );

        let chunk_address = SwarmAddress::new(chunk_addr_bytes.try_into().unwrap());
        let digest = StampDigest::new(
            chunk_address,
            stamp.batch(),
            stamp.stamp_index(),
            stamp.timestamp(),
        );
        let prehash = digest.to_prehash();

        assert_eq!(
            prehash.as_slice(),
            expected_digest.as_slice(),
            "Digest mismatch - Rust computed different prehash than Go"
        );

        // stamp.signature() already returns &Signature
        let recovered = stamp
            .signature()
            .recover_address_from_msg(prehash.as_slice())
            .expect("Failed to recover address from Go signature");

        assert_eq!(
            hex::encode(recovered.as_slice()),
            expected_owner,
            "Recovered owner address mismatch"
        );

        let expected_owner_addr: Address = expected_owner.parse().unwrap();
        assert_eq!(recovered, expected_owner_addr);
    }
}

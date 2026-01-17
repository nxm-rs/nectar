//! Stamper trait and implementations for creating signed stamps.

extern crate alloc;

use alloc::vec::Vec;
use alloy_primitives::B256;
use alloy_signer::Signature;

use crate::{calculate_bucket, Batch, Stamp, StampDigest, StampError, StampIndex};
use nectar_primitives::SwarmAddress;

/// A trait for signing stamp digests.
///
/// This trait abstracts over different signing mechanisms, allowing stamps
/// to be created with hardware wallets, remote signers, or local keys.
///
/// # EIP-191 Compatibility
///
/// To be compatible with Go/bee implementations, stamps must be signed using
/// EIP-191 personal message signing. The prehash (keccak256 of stamp data) is
/// treated as the message, which gets prefixed with `"\x19Ethereum Signed Message:\n32"`.
///
/// Implementations should use alloy's `sign_message` (or `sign_message_sync`)
/// rather than `sign_hash` to ensure compatibility.
pub trait StampSigner {
    /// The error type returned when signing fails.
    type Error;

    /// Signs a stamp digest message synchronously using EIP-191 personal signing.
    ///
    /// The prehash is the keccak256 hash of the stamp digest data. This method
    /// should apply EIP-191 message prefixing before signing to be compatible
    /// with Go/bee implementations.
    ///
    /// Use alloy's `SignerSync::sign_message_sync(prehash.as_slice())` for the
    /// implementation.
    fn sign_message(&self, prehash: &B256) -> Result<Signature, Self::Error>;
}

/// Error type for signing operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignerError;

impl core::fmt::Display for SignerError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "signing failed")
    }
}

#[cfg(feature = "std")]
impl std::error::Error for SignerError {}

/// A trait for entities that can stamp chunks.
///
/// Implementations of this trait manage the state needed to stamp chunks,
/// including tracking bucket usage and generating signatures.
///
/// # Example
///
/// ```ignore
/// use nectar_postage::{Stamper, Stamp, StampError};
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
    /// - The bucket is full
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

/// A stamper that uses a batch and tracks bucket indices.
///
/// This implementation tracks which indices have been used in each bucket
/// and creates stamps with the appropriate index for each chunk address.
#[derive(Debug, Clone)]
pub struct BatchStamper<S> {
    /// The batch to stamp with.
    batch: Batch,
    /// Current index for each bucket.
    bucket_indices: Vec<u32>,
    /// The signer used to sign stamps.
    signer: S,
    /// Maximum utilization across all buckets.
    max_utilization: u32,
}

impl<S> BatchStamper<S> {
    /// Creates a new batch stamper.
    pub fn new(batch: Batch, signer: S) -> Self {
        let bucket_count = batch.bucket_count() as usize;
        Self {
            batch,
            bucket_indices: alloc::vec![0u32; bucket_count],
            signer,
            max_utilization: 0,
        }
    }

    /// Returns a reference to the signer.
    pub fn signer(&self) -> &S {
        &self.signer
    }

    /// Returns a mutable reference to the signer.
    pub fn signer_mut(&mut self) -> &mut S {
        &mut self.signer
    }

    /// Prepares a stamp for the given chunk address.
    ///
    /// This allocates an index and creates the digest, but does not sign it.
    /// Use this for async signing flows.
    pub fn prepare_stamp(
        &mut self,
        address: &SwarmAddress,
        timestamp: u64,
    ) -> Result<StampDigest, StampError> {
        let bucket = calculate_bucket(address, self.batch.bucket_depth());

        // Get current index for this bucket
        let current_index = self.bucket_indices[bucket as usize];

        // Check if bucket is full
        if current_index >= self.batch.bucket_upper_bound() {
            return Err(StampError::BucketFull {
                bucket,
                capacity: self.batch.bucket_upper_bound(),
            });
        }

        // Increment the bucket index
        self.bucket_indices[bucket as usize] = current_index + 1;

        // Update max utilization
        if current_index + 1 > self.max_utilization {
            self.max_utilization = current_index + 1;
        }

        let index = StampIndex::new(bucket, current_index);

        Ok(StampDigest::new(*address, self.batch.id(), index, timestamp))
    }

    /// Creates a stamp from a digest and signature.
    #[inline]
    pub fn stamp_from_signature(digest: &StampDigest, sig: Signature) -> Stamp {
        // Convert alloy Signature to 65-byte array (r || s || v)
        let sig_bytes: [u8; 65] = {
            let mut bytes = [0u8; 65];
            bytes[..32].copy_from_slice(&sig.r().to_be_bytes::<32>());
            bytes[32..64].copy_from_slice(&sig.s().to_be_bytes::<32>());
            // v is y_parity as bool, convert to byte (0 or 1)
            bytes[64] = sig.v() as u8;
            bytes
        };

        Stamp::with_index(digest.batch_id, digest.index, digest.timestamp, sig_bytes)
    }
}

impl<S> Stamper for BatchStamper<S>
where
    S: StampSigner<Error = SignerError>,
{
    type Error = StampError;

    fn stamp(&mut self, address: &SwarmAddress) -> Result<Stamp, Self::Error> {
        let timestamp = current_timestamp();
        let digest = self.prepare_stamp(address, timestamp)?;
        let prehash = digest.to_prehash();

        let sig = self
            .signer
            .sign_message(&prehash)
            .map_err(|_| StampError::SigningFailed("signer returned error"))?;

        Ok(Self::stamp_from_signature(&digest, sig))
    }

    fn batch(&self) -> &Batch {
        &self.batch
    }

    fn max_bucket_utilization(&self) -> u32 {
        self.max_utilization
    }

    fn bucket_has_capacity(&self, bucket: u32) -> bool {
        if bucket as usize >= self.bucket_indices.len() {
            return false;
        }
        self.bucket_indices[bucket as usize] < self.batch.bucket_upper_bound()
    }
}

/// Returns the current timestamp in nanoseconds.
#[cfg(feature = "std")]
fn current_timestamp() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(not(feature = "std"))]
fn current_timestamp() -> u64 {
    0 // In no_std, caller should provide timestamp via prepare_stamp
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;
    use alloy_primitives::{Address, U256};

    /// A mock signer for testing that creates deterministic signatures.
    struct MockSigner;

    impl StampSigner for MockSigner {
        type Error = SignerError;

        fn sign_message(&self, _prehash: &B256) -> Result<Signature, Self::Error> {
            // Create a dummy signature for testing
            Ok(Signature::new(U256::from(1), U256::from(2), false))
        }
    }

    #[test]
    fn test_batch_stamper_basic() {
        let batch = Batch::new(B256::ZERO, 0, 0, Address::ZERO, 20, 16, false);
        let mut stamper = BatchStamper::new(batch, MockSigner);

        let address = SwarmAddress::new([0xAB; 32]);
        let stamp = stamper.stamp(&address).unwrap();

        assert_eq!(stamp.batch(), B256::ZERO);
        // First stamp in bucket should have index 0
        assert_eq!(stamp.index(), 0);
    }

    #[test]
    fn test_batch_stamper_increments_index() {
        let batch = Batch::new(B256::ZERO, 0, 0, Address::ZERO, 20, 16, false);
        let mut stamper = BatchStamper::new(batch, MockSigner);

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
        // Create a batch with very small bucket capacity: depth=17, bucket_depth=16
        // This gives 2^(17-16) = 2 slots per bucket
        let batch = Batch::new(B256::ZERO, 0, 0, Address::ZERO, 17, 16, false);
        let mut stamper = BatchStamper::new(batch, MockSigner);

        let address = SwarmAddress::new([0xAB; 32]);

        // First two stamps should succeed
        assert!(stamper.stamp(&address).is_ok());
        assert!(stamper.stamp(&address).is_ok());

        // Third stamp should fail - bucket is full
        let result = stamper.stamp(&address);
        assert!(matches!(result, Err(StampError::BucketFull { .. })));
    }

    #[test]
    fn test_batch_stamper_max_utilization() {
        let batch = Batch::new(B256::ZERO, 0, 0, Address::ZERO, 20, 16, false);
        let mut stamper = BatchStamper::new(batch, MockSigner);

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
    ///
    /// This test uses the exact test vector from bee's TestDefaultSignerDeterministic
    /// in pkg/crypto/signer_test.go to verify that our signing produces identical
    /// signatures.
    ///
    /// Test vector:
    /// - Private key: 634fb5a872396d9693e5c9f9d7233cfa93f395c093371017ff44aa9ae6564cdd
    /// - Message: 2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae
    /// - Expected signature: 336d24afef78c5883b96ad9a62552a8db3d236105cb059ddd04dc49680869dc1
    ///                       6234f6852c277087f025d4114c4fac6b40295ecffd1194a84cdb91bd57176949
    ///                       1b
    #[test]
    fn test_eip191_signing_interop() {
        use alloy_primitives::hex;
        use alloy_signer::SignerSync;
        use alloy_signer_local::PrivateKeySigner;

        // Test vector from Go/bee TestDefaultSignerDeterministic
        let privkey_bytes =
            hex::decode("634fb5a872396d9693e5c9f9d7233cfa93f395c093371017ff44aa9ae6564cdd")
                .unwrap();
        let message =
            hex::decode("2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae")
                .unwrap();
        let expected_sig = hex::decode(
            "336d24afef78c5883b96ad9a62552a8db3d236105cb059ddd04dc49680869dc16234f6852c277087f025d4114c4fac6b40295ecffd1194a84cdb91bd571769491b"
        ).unwrap();

        // Create signer from private key
        let signer = PrivateKeySigner::from_slice(&privkey_bytes).unwrap();

        // Sign using EIP-191 message signing (same as Go's signer.Sign())
        let signature = signer.sign_message_sync(&message).unwrap();

        // Convert to bytes in r || s || v format (same as Go)
        let mut sig_bytes = [0u8; 65];
        sig_bytes[..32].copy_from_slice(&signature.r().to_be_bytes::<32>());
        sig_bytes[32..64].copy_from_slice(&signature.s().to_be_bytes::<32>());
        sig_bytes[64] = signature.v() as u8 + 27; // Go uses 27/28 for v, not 0/1

        assert_eq!(
            sig_bytes.as_slice(),
            expected_sig.as_slice(),
            "Signature mismatch with Go/bee test vector.\nExpected: {}\nGot: {}",
            hex::encode(&expected_sig),
            hex::encode(&sig_bytes)
        );
    }

    /// Test that signature recovery works correctly with EIP-191.
    ///
    /// This verifies that we can recover the signer address from a signature
    /// created using EIP-191 message signing.
    #[test]
    fn test_eip191_recovery_interop() {
        use alloy_primitives::hex;
        use alloy_signer::SignerSync;
        use alloy_signer_local::PrivateKeySigner;

        // Test vector from Go/bee TestDefaultSignerDeterministic
        let privkey_bytes =
            hex::decode("634fb5a872396d9693e5c9f9d7233cfa93f395c093371017ff44aa9ae6564cdd")
                .unwrap();
        let message =
            hex::decode("2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae")
                .unwrap();

        // Expected Ethereum address for this private key (from Go test)
        let expected_address = "8d3766440f0d7b949a5e32995d09619a7f86e632";

        let signer = PrivateKeySigner::from_slice(&privkey_bytes).unwrap();
        let signature = signer.sign_message_sync(&message).unwrap();

        // Recover address from signature using EIP-191
        let recovered = signature.recover_address_from_msg(&message).unwrap();

        assert_eq!(
            hex::encode(recovered.as_slice()),
            expected_address,
            "Recovered address mismatch"
        );
        assert_eq!(recovered, signer.address(), "Recovered address should match signer address");
    }

    /// Test verifying a stamp created by Go/bee in Rust.
    ///
    /// This test uses a stamp generated by Go's TestGenerateInteropStamp test
    /// with fully deterministic values to verify cross-implementation compatibility.
    ///
    /// Test vector from Go:
    /// - Private Key: 634fb5a872396d9693e5c9f9d7233cfa93f395c093371017ff44aa9ae6564cdd
    /// - Owner Address: 8d3766440f0d7b949a5e32995d09619a7f86e632
    /// - Chunk Address: 0000...0002
    /// - Batch ID: 0000...0001
    /// - Index: 0000000000000000 (bucket=0, index=0)
    /// - Timestamp: 0000000000000003
    #[test]
    fn test_verify_go_created_stamp() {
        use alloy_primitives::{hex, Address};
        use alloy_signer::Signature;

        // Test vector generated by Go's TestGenerateInteropStamp
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

        // Parse the stamp
        let stamp = crate::Stamp::try_from_slice(&full_stamp_bytes).unwrap();

        // Verify stamp fields
        assert_eq!(
            hex::encode(stamp.batch().as_slice()),
            "0000000000000000000000000000000000000000000000000000000000000001"
        );
        assert_eq!(stamp.bucket(), 0);
        assert_eq!(stamp.index(), 0);
        assert_eq!(stamp.timestamp(), 3);

        // Verify the signature ends with 0x1c (28 in decimal, Go's v format)
        assert_eq!(stamp.signature()[64], 0x1c, "Go uses v=28 (0x1c) for odd y parity");

        // Create the chunk address
        let chunk_address = SwarmAddress::new(chunk_addr_bytes.try_into().unwrap());

        // Compute the digest (should match Go's digest)
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

        // Recover the signer from the Go-created signature using EIP-191
        let sig = Signature::from_raw(stamp.signature()).expect("Failed to parse Go signature");
        let recovered = sig
            .recover_address_from_msg(prehash.as_slice())
            .expect("Failed to recover address from Go signature");

        assert_eq!(
            hex::encode(recovered.as_slice()),
            expected_owner,
            "Recovered owner address mismatch - Rust failed to verify Go stamp"
        );

        // Also verify using the expected owner address directly
        let expected_owner_addr: Address = expected_owner.parse().unwrap();
        assert_eq!(
            recovered, expected_owner_addr,
            "Recovered address should match expected owner"
        );
    }
}

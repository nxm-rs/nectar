//! Postage stamp types.

use alloy_primitives::{Address, B256, Signature, eip191_hash_message};
use alloy_signer::k256::ecdsa::VerifyingKey;
use byteorder::{BigEndian, ByteOrder};
use nectar_primitives::SwarmAddress;

use crate::{BatchId, StampError};

/// The size of a serialized stamp in bytes.
///
/// Layout: batch_id (32) + bucket (4) + index (4) + timestamp (8) + signature (65) = 113 bytes
pub const STAMP_SIZE: usize = 113;

/// A serialized postage stamp as a fixed-size byte array.
pub type StampBytes = [u8; STAMP_SIZE];

/// A stamp index representing the position of a chunk within a batch.
///
/// The stamp index consists of two components:
/// - `bucket`: The collision bucket determined by the chunk's address (also called "x")
/// - `index`: The position within that bucket (also called "y")
///
/// # Implementation Note
///
/// The exact encoding of the stamp index into a single value is **implementation-specific**
/// and **not defined by the Swarm specifications**. This implementation encodes the index
/// as a 64-bit value by concatenating the bucket (high 32 bits) and position (low 32 bits)
/// in big-endian format. Other implementations may use different encodings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StampIndex {
    /// The collision bucket (x coordinate).
    ///
    /// Determined by the leading bits of the chunk address, specifically
    /// the first `bucket_depth` bits interpreted as a big-endian integer.
    bucket: u32,
    /// The position within the bucket (y coordinate).
    ///
    /// Assigned sequentially as chunks are added to the bucket, starting from 0.
    index: u32,
}

impl StampIndex {
    /// Creates a new stamp index.
    #[inline]
    pub const fn new(bucket: u32, index: u32) -> Self {
        Self { bucket, index }
    }

    /// Returns the collision bucket (x).
    #[inline]
    pub const fn bucket(&self) -> u32 {
        self.bucket
    }

    /// Returns the position within the bucket (y).
    #[inline]
    pub const fn index(&self) -> u32 {
        self.index
    }

    /// Encodes the stamp index as a 64-bit value for use in stamp digest calculation.
    ///
    /// # Encoding Format
    ///
    /// The encoding concatenates bucket (4 bytes BE) and index (4 bytes BE):
    /// ```text
    /// | bucket (32 bits) | index (32 bits) |
    /// |   high 32 bits   |   low 32 bits   |
    /// ```
    ///
    /// # Implementation Note
    ///
    /// This encoding is **implementation-specific** and not defined by the Swarm
    /// specifications. The Swarm protocol only specifies that the stamp contains
    /// bucket and index values; the exact wire format for the combined index
    /// used in signature computation is left to implementations.
    #[inline]
    pub const fn encode(&self) -> u64 {
        ((self.bucket as u64) << 32) | (self.index as u64)
    }

    /// Decodes a stamp index from a 64-bit encoded value.
    ///
    /// See [`encode`](Self::encode) for the encoding format.
    #[inline]
    pub const fn decode(encoded: u64) -> Self {
        Self {
            bucket: (encoded >> 32) as u32,
            index: encoded as u32,
        }
    }

    /// Converts the index to big-endian bytes (8 bytes total).
    #[inline]
    pub const fn to_be_bytes(&self) -> [u8; 8] {
        self.encode().to_be_bytes()
    }

    /// Creates a stamp index from big-endian bytes.
    #[inline]
    pub const fn from_be_bytes(bytes: [u8; 8]) -> Self {
        Self::decode(u64::from_be_bytes(bytes))
    }
}

impl From<(u32, u32)> for StampIndex {
    fn from((bucket, index): (u32, u32)) -> Self {
        Self::new(bucket, index)
    }
}

impl From<StampIndex> for (u32, u32) {
    fn from(idx: StampIndex) -> Self {
        (idx.bucket, idx.index)
    }
}

/// A postage stamp represents proof of payment for storing a chunk.
///
/// Stamps are created by signing a message containing the chunk address,
/// batch ID, stamp index, and timestamp with the batch owner's private key.
///
/// # Wire Format
///
/// A serialized stamp is 113 bytes:
/// - Batch ID: 32 bytes
/// - Bucket (x): 4 bytes, big-endian
/// - Index (y): 4 bytes, big-endian
/// - Timestamp: 8 bytes, big-endian
/// - Signature: 65 bytes (r || s || v)
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Stamp {
    /// The batch ID this stamp belongs to.
    batch: BatchId,
    /// The stamp index (bucket and position).
    index: StampIndex,
    /// Timestamp when the stamp was created (nanoseconds since epoch).
    timestamp: u64,
    /// The signature proving ownership.
    sig: Signature,
}

impl Stamp {
    /// Creates a new stamp with the given parameters.
    #[inline]
    pub fn new(
        batch: BatchId,
        bucket: u32,
        index: u32,
        timestamp: u64,
        sig: Signature,
    ) -> Self {
        Self {
            batch,
            index: StampIndex::new(bucket, index),
            timestamp,
            sig,
        }
    }

    /// Creates a new stamp from a stamp index.
    #[inline]
    pub fn with_index(batch: BatchId, index: StampIndex, timestamp: u64, sig: Signature) -> Self {
        Self {
            batch,
            index,
            timestamp,
            sig,
        }
    }

    /// Returns the batch ID.
    #[inline]
    pub const fn batch(&self) -> BatchId {
        self.batch
    }

    /// Returns the stamp index.
    #[inline]
    pub const fn stamp_index(&self) -> StampIndex {
        self.index
    }

    /// Returns the collision bucket.
    #[inline]
    pub const fn bucket(&self) -> u32 {
        self.index.bucket()
    }

    /// Returns the position within the bucket.
    #[inline]
    pub const fn index(&self) -> u32 {
        self.index.index()
    }

    /// Returns the timestamp.
    #[inline]
    pub const fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Returns the signature.
    #[inline]
    pub const fn signature(&self) -> &Signature {
        &self.sig
    }

    /// Serializes the stamp to a 113-byte array.
    #[inline]
    pub fn to_bytes(&self) -> StampBytes {
        let mut bytes = [0u8; STAMP_SIZE];
        bytes[..32].copy_from_slice(self.batch.as_slice());
        BigEndian::write_u32(&mut bytes[32..36], self.index.bucket());
        BigEndian::write_u32(&mut bytes[36..40], self.index.index());
        BigEndian::write_u64(&mut bytes[40..48], self.timestamp);
        bytes[48..STAMP_SIZE].copy_from_slice(&self.sig.as_bytes());
        bytes
    }

    /// Deserializes a stamp from a 113-byte array.
    ///
    /// Returns an error if the signature bytes are invalid.
    #[inline]
    pub fn from_bytes(bytes: &StampBytes) -> Result<Self, StampError> {
        let batch = B256::from_slice(&bytes[..32]);
        let bucket = BigEndian::read_u32(&bytes[32..36]);
        let index = BigEndian::read_u32(&bytes[36..40]);
        let timestamp = BigEndian::read_u64(&bytes[40..48]);

        let sig =
            Signature::from_raw(&bytes[48..STAMP_SIZE]).map_err(|_| StampError::InvalidSignature)?;

        Ok(Self {
            batch,
            index: StampIndex::new(bucket, index),
            timestamp,
            sig,
        })
    }

    /// Attempts to deserialize a stamp from a byte slice.
    ///
    /// Returns an error if the slice is not exactly 113 bytes or if the signature is invalid.
    #[inline]
    pub fn try_from_slice(bytes: &[u8]) -> Result<Self, StampError> {
        if bytes.len() != STAMP_SIZE {
            return Err(StampError::InvalidData("stamp must be exactly 113 bytes"));
        }

        // Safety: we verified the length above
        let mut stamp_bytes = [0u8; STAMP_SIZE];
        stamp_bytes.copy_from_slice(bytes);
        Self::from_bytes(&stamp_bytes)
    }

    /// Recovers the signer address from this stamp using EIP-191 message recovery.
    ///
    /// This computes the stamp digest from the chunk address and stamp fields,
    /// then recovers the Ethereum address that signed it.
    ///
    /// # Arguments
    ///
    /// * `chunk_address` - The address of the chunk this stamp is for
    ///
    /// # Returns
    ///
    /// The Ethereum address of the signer, or an error if recovery fails.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let stamp = Stamp::try_from_slice(&bytes)?;
    /// let signer = stamp.recover_signer(&chunk_address)?;
    /// println!("Stamp signed by: {}", signer);
    /// ```
    pub fn recover_signer(&self, chunk_address: &SwarmAddress) -> Result<Address, StampError> {
        let digest = StampDigest::new(*chunk_address, self.batch, self.index, self.timestamp);
        let prehash = digest.to_prehash();

        // Use recover_address_from_msg for EIP-191 compatibility
        self.sig
            .recover_address_from_msg(prehash.as_slice())
            .map_err(|_| StampError::InvalidSignature)
    }

    /// Verifies this stamp was signed by the expected owner.
    ///
    /// This is a convenience method that calls [`recover_signer`](Self::recover_signer)
    /// and compares the result to the expected owner address.
    ///
    /// # Arguments
    ///
    /// * `chunk_address` - The address of the chunk this stamp is for
    /// * `owner` - The expected owner/signer address
    ///
    /// # Returns
    ///
    /// `Ok(())` if the stamp was signed by the expected owner,
    /// or an error if signature recovery fails or the signer doesn't match.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let stamp = Stamp::try_from_slice(&bytes)?;
    /// stamp.verify(&chunk_address, batch.owner())?;
    /// ```
    pub fn verify(&self, chunk_address: &SwarmAddress, owner: Address) -> Result<(), StampError> {
        let recovered = self.recover_signer(chunk_address)?;
        if recovered != owner {
            return Err(StampError::OwnerMismatch {
                expected: owner,
                actual: recovered,
            });
        }
        Ok(())
    }

    /// Recovers the public key from this stamp.
    ///
    /// This is useful for caching the public key after the first verification
    /// of a batch. Subsequent stamps from the same batch can then use
    /// [`verify_with_pubkey`](Self::verify_with_pubkey) which is approximately
    /// 10x faster than full signature recovery.
    ///
    /// # Arguments
    ///
    /// * `chunk_address` - The address of the chunk this stamp is for
    ///
    /// # Returns
    ///
    /// The public key of the signer, or an error if recovery fails.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // First stamp: recover public key and cache it
    /// let pubkey = first_stamp.recover_pubkey(&first_chunk_address)?;
    ///
    /// // Subsequent stamps: fast verification with cached pubkey
    /// for (stamp, addr) in remaining_stamps {
    ///     stamp.verify_with_pubkey(&addr, &pubkey)?;
    /// }
    /// ```
    pub fn recover_pubkey(&self, chunk_address: &SwarmAddress) -> Result<VerifyingKey, StampError> {
        let digest = StampDigest::new(*chunk_address, self.batch, self.index, self.timestamp);
        let prehash = digest.to_prehash();

        // Compute EIP-191 message hash
        let msg_hash = eip191_hash_message(prehash.as_slice());

        // Convert to k256 signature (64-byte r||s)
        let k256_sig = self
            .sig
            .to_k256()
            .map_err(|_| StampError::InvalidSignature)?;

        // Get recovery id from signature
        let recovery_id = self.sig.recid();

        // Recover the public key
        VerifyingKey::recover_from_prehash(msg_hash.as_slice(), &k256_sig, recovery_id)
            .map_err(|_| StampError::InvalidSignature)
    }

    /// Verifies this stamp using a known public key.
    ///
    /// This is approximately 10x faster than [`verify`](Self::verify) or
    /// [`recover_signer`](Self::recover_signer) because it avoids the expensive
    /// ECDSA public key recovery operation.
    ///
    /// Use this when you've already recovered the owner's public key from a
    /// previous stamp in the same batch (via [`recover_pubkey`](Self::recover_pubkey)).
    ///
    /// # Arguments
    ///
    /// * `chunk_address` - The address of the chunk this stamp is for
    /// * `pubkey` - The expected signer's public key (cached from previous recovery)
    ///
    /// # Returns
    ///
    /// `Ok(())` if the signature is valid for the given public key,
    /// or an error if verification fails.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // First stamp: recover and cache the public key
    /// let pubkey = first_stamp.recover_pubkey(&first_address)?;
    /// let owner = alloy_signer::utils::public_key_to_address(&pubkey);
    ///
    /// // Fast verification for remaining stamps in the same batch
    /// second_stamp.verify_with_pubkey(&second_address, &pubkey)?;
    /// third_stamp.verify_with_pubkey(&third_address, &pubkey)?;
    /// ```
    pub fn verify_with_pubkey(
        &self,
        chunk_address: &SwarmAddress,
        pubkey: &VerifyingKey,
    ) -> Result<(), StampError> {
        use alloy_signer::k256::ecdsa::signature::hazmat::PrehashVerifier;

        let digest = StampDigest::new(*chunk_address, self.batch, self.index, self.timestamp);
        let prehash = digest.to_prehash();

        // Compute EIP-191 message hash
        let msg_hash = eip191_hash_message(prehash.as_slice());

        // Convert to k256 signature (64-byte r||s)
        let k256_sig = self
            .sig
            .to_k256()
            .map_err(|_| StampError::InvalidSignature)?;

        // Verify the signature using prehash
        pubkey
            .verify_prehash(msg_hash.as_slice(), &k256_sig)
            .map_err(|_| StampError::InvalidSignature)
    }
}

/// The digest that must be signed to create a valid stamp.
///
/// The digest is computed as: `keccak256(chunk_address || batch_id || index || timestamp)`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StampDigest {
    /// The chunk address being stamped.
    pub chunk_address: SwarmAddress,
    /// The batch ID.
    pub batch_id: BatchId,
    /// The stamp index (bucket and position).
    pub index: StampIndex,
    /// The timestamp.
    pub timestamp: u64,
}

impl StampDigest {
    /// Creates a new stamp digest.
    #[inline]
    pub const fn new(
        chunk_address: SwarmAddress,
        batch_id: BatchId,
        index: StampIndex,
        timestamp: u64,
    ) -> Self {
        Self {
            chunk_address,
            batch_id,
            index,
            timestamp,
        }
    }

    /// Computes the 32-byte hash that must be signed.
    ///
    /// Format: `keccak256(chunk_address || batch_id || index_bytes || timestamp_bytes)`
    pub fn to_prehash(&self) -> B256 {
        use alloy_primitives::keccak256;

        let mut data = [0u8; 32 + 32 + 8 + 8]; // 80 bytes
        data[..32].copy_from_slice(self.chunk_address.as_bytes());
        data[32..64].copy_from_slice(self.batch_id.as_slice());
        data[64..72].copy_from_slice(&self.index.to_be_bytes());
        data[72..80].copy_from_slice(&self.timestamp.to_be_bytes());

        keccak256(data)
    }
}

impl From<Stamp> for StampBytes {
    #[inline]
    fn from(stamp: Stamp) -> Self {
        stamp.to_bytes()
    }
}

impl TryFrom<StampBytes> for Stamp {
    type Error = StampError;

    #[inline]
    fn try_from(bytes: StampBytes) -> Result<Self, Self::Error> {
        Self::from_bytes(&bytes)
    }
}

// =============================================================================
// Arbitrary implementations for property-based testing
// =============================================================================

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for StampIndex {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self::new(u.arbitrary()?, u.arbitrary()?))
    }
}

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for Stamp {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        use alloy_primitives::U256;

        let batch: B256 = u.arbitrary()?;
        let index = StampIndex::arbitrary(u)?;
        let timestamp: u64 = u.arbitrary()?;

        // Generate a valid signature (r, s must be non-zero for a valid ECDSA signature)
        let r = U256::from_be_bytes(u.arbitrary::<[u8; 32]>()?);
        let s = U256::from_be_bytes(u.arbitrary::<[u8; 32]>()?);
        let v: bool = u.arbitrary()?;
        let sig = Signature::new(r, s, v);

        Ok(Self::with_index(batch, index, timestamp, sig))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::hex;

    const TEST_BATCH_ID: &str = "c3387832bb1b88acbcd0ffdb65a08ef077d98c08d4bee576a72dbe3d36761369";
    const TEST_STAMP: &str = "c3387832bb1b88acbcd0ffdb65a08ef077d98c08d4bee576a72dbe3d367613690000cbe5000000000000018921ff0dbb29169df9e6364e26c6ca6b17745c10b9d6a36ea38e204f2e3cc64a8373c0661f5bb0a347c61d8d1689b0dcf8354117686a6a18d08cff927f526de5fc61b2b7491b";

    #[test]
    fn test_stamp_index_encode_decode() {
        let idx = StampIndex::new(0x1234, 0x5678);
        assert_eq!(idx.encode(), 0x0000123400005678);

        let decoded = StampIndex::decode(0x0000123400005678);
        assert_eq!(decoded, idx);
    }

    #[test]
    fn test_stamp_index_bytes() {
        let idx = StampIndex::new(0x1234, 0x5678);
        let bytes = idx.to_be_bytes();
        let restored = StampIndex::from_be_bytes(bytes);
        assert_eq!(idx, restored);
    }

    #[test]
    fn test_stamp_index_conversions() {
        let idx = StampIndex::new(100, 50);
        let tuple: (u32, u32) = idx.into();
        assert_eq!(tuple, (100, 50));

        let back: StampIndex = tuple.into();
        assert_eq!(back, idx);
    }

    #[test]
    fn test_stamp_roundtrip() {
        let batch = B256::ZERO;
        let sig = Signature::test_signature();
        let stamp = Stamp::new(batch, 100, 50, 1234567890, sig);

        let bytes = stamp.to_bytes();
        let restored = Stamp::from_bytes(&bytes).unwrap();

        assert_eq!(stamp, restored);
    }

    #[test]
    fn test_stamp_from_known_data() {
        let bytes = hex::decode(TEST_STAMP).unwrap();
        let stamp = Stamp::try_from_slice(&bytes).unwrap();

        let expected_batch = B256::from_slice(&hex::decode(TEST_BATCH_ID).unwrap());
        assert_eq!(stamp.batch(), expected_batch);
        assert_eq!(stamp.bucket(), 52197); // 0x0000cbe5
        assert_eq!(stamp.index(), 0);
        assert_eq!(stamp.timestamp(), 1688492510651);
    }

    #[test]
    fn test_stamp_with_index() {
        let batch = B256::ZERO;
        let idx = StampIndex::new(100, 50);
        let sig = Signature::test_signature();
        let stamp = Stamp::with_index(batch, idx, 1234567890, sig);

        assert_eq!(stamp.stamp_index(), idx);
        assert_eq!(stamp.bucket(), 100);
        assert_eq!(stamp.index(), 50);
    }

    #[test]
    fn test_stamp_size() {
        assert_eq!(STAMP_SIZE, 113);
    }

    #[test]
    fn test_invalid_slice_size() {
        let bytes = [0u8; 100];
        let result = Stamp::try_from_slice(&bytes);
        assert!(matches!(result, Err(StampError::InvalidData(_))));
    }

    #[test]
    fn test_from_conversions() {
        let sig = Signature::test_signature();
        let stamp = Stamp::new(B256::ZERO, 1, 2, 3, sig);

        // From<Stamp> for StampBytes
        let bytes: StampBytes = stamp.clone().into();
        // TryFrom<StampBytes> for Stamp
        let back: Stamp = bytes.try_into().unwrap();
        assert_eq!(stamp, back);
    }

    /// Test recover_signer using the Go interop test vector.
    ///
    /// This uses the same test data as stamper::tests::test_verify_go_created_stamp
    /// to ensure the Stamp::recover_signer method works correctly.
    #[test]
    fn test_recover_signer() {
        // Test vector from Go's TestGenerateInteropStamp
        let chunk_addr_bytes =
            hex::decode("0000000000000000000000000000000000000000000000000000000000000002")
                .unwrap();
        let full_stamp_bytes = hex::decode(
            "000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000003496cb9ac06221d39c3f6a7dd3b9c2301c1f923162b90d5443e42023f34ff908945b0da1c297190f111b7c6ebc828648ead8f7fce06c0364cb5a833410230c5c01c"
        ).unwrap();
        let expected_owner: Address = "8d3766440f0d7b949a5e32995d09619a7f86e632".parse().unwrap();

        let chunk_address = SwarmAddress::new(chunk_addr_bytes.try_into().unwrap());
        let stamp = Stamp::try_from_slice(&full_stamp_bytes).unwrap();

        // Test recover_signer
        let recovered = stamp.recover_signer(&chunk_address).unwrap();
        assert_eq!(recovered, expected_owner);
    }

    /// Test verify method using the Go interop test vector.
    #[test]
    fn test_verify() {
        // Test vector from Go's TestGenerateInteropStamp
        let chunk_addr_bytes =
            hex::decode("0000000000000000000000000000000000000000000000000000000000000002")
                .unwrap();
        let full_stamp_bytes = hex::decode(
            "000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000003496cb9ac06221d39c3f6a7dd3b9c2301c1f923162b90d5443e42023f34ff908945b0da1c297190f111b7c6ebc828648ead8f7fce06c0364cb5a833410230c5c01c"
        ).unwrap();
        let expected_owner: Address = "8d3766440f0d7b949a5e32995d09619a7f86e632".parse().unwrap();
        let wrong_owner: Address = "0000000000000000000000000000000000000001".parse().unwrap();

        let chunk_address = SwarmAddress::new(chunk_addr_bytes.try_into().unwrap());
        let stamp = Stamp::try_from_slice(&full_stamp_bytes).unwrap();

        // Verify with correct owner should succeed
        assert!(stamp.verify(&chunk_address, expected_owner).is_ok());

        // Verify with wrong owner should fail
        let result = stamp.verify(&chunk_address, wrong_owner);
        assert!(matches!(result, Err(StampError::OwnerMismatch { .. })));
    }

    /// Test recover_pubkey using the Go interop test vector.
    #[test]
    fn test_recover_pubkey() {
        use alloy_signer::utils::public_key_to_address;

        // Test vector from Go's TestGenerateInteropStamp
        let chunk_addr_bytes =
            hex::decode("0000000000000000000000000000000000000000000000000000000000000002")
                .unwrap();
        let full_stamp_bytes = hex::decode(
            "000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000003496cb9ac06221d39c3f6a7dd3b9c2301c1f923162b90d5443e42023f34ff908945b0da1c297190f111b7c6ebc828648ead8f7fce06c0364cb5a833410230c5c01c"
        ).unwrap();
        let expected_owner: Address = "8d3766440f0d7b949a5e32995d09619a7f86e632".parse().unwrap();

        let chunk_address = SwarmAddress::new(chunk_addr_bytes.try_into().unwrap());
        let stamp = Stamp::try_from_slice(&full_stamp_bytes).unwrap();

        // Test recover_pubkey
        let pubkey = stamp.recover_pubkey(&chunk_address).unwrap();

        // Convert to address and verify
        let recovered_addr = public_key_to_address(&pubkey);
        assert_eq!(recovered_addr, expected_owner);
    }

    /// Test verify_with_pubkey using the Go interop test vector.
    #[test]
    fn test_verify_with_pubkey() {
        // Test vector from Go's TestGenerateInteropStamp
        let chunk_addr_bytes =
            hex::decode("0000000000000000000000000000000000000000000000000000000000000002")
                .unwrap();
        let full_stamp_bytes = hex::decode(
            "000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000003496cb9ac06221d39c3f6a7dd3b9c2301c1f923162b90d5443e42023f34ff908945b0da1c297190f111b7c6ebc828648ead8f7fce06c0364cb5a833410230c5c01c"
        ).unwrap();

        let chunk_address = SwarmAddress::new(chunk_addr_bytes.try_into().unwrap());
        let stamp = Stamp::try_from_slice(&full_stamp_bytes).unwrap();

        // First recover the public key
        let pubkey = stamp.recover_pubkey(&chunk_address).unwrap();

        // Now verify using the cached pubkey
        let result = stamp.verify_with_pubkey(&chunk_address, &pubkey);
        assert!(result.is_ok());
    }

    /// Test that verify_with_pubkey fails with wrong pubkey.
    #[test]
    fn test_verify_with_wrong_pubkey() {
        use alloy_signer::SignerSync;
        use alloy_signer_local::PrivateKeySigner;

        // Create a stamp with one signer
        let signer = PrivateKeySigner::random();
        let chunk_address = SwarmAddress::new([0xAB; 32]);
        let batch_id = B256::ZERO;
        let index = StampIndex::new(0, 0);
        let timestamp = 12345u64;

        let digest = StampDigest::new(chunk_address, batch_id, index, timestamp);
        let prehash = digest.to_prehash();

        // sign_message_sync returns alloy_primitives::Signature directly
        let sig = signer.sign_message_sync(prehash.as_slice()).unwrap();
        let stamp = Stamp::with_index(batch_id, index, timestamp, sig);

        // Get the correct pubkey
        let correct_pubkey = stamp.recover_pubkey(&chunk_address).unwrap();

        // Create a different signer for wrong pubkey
        let wrong_signer = PrivateKeySigner::random();
        let wrong_pubkey = wrong_signer.credential().verifying_key();

        // Verify with correct pubkey should succeed
        assert!(
            stamp
                .verify_with_pubkey(&chunk_address, &correct_pubkey)
                .is_ok()
        );

        // Verify with wrong pubkey should fail
        assert!(
            stamp
                .verify_with_pubkey(&chunk_address, wrong_pubkey)
                .is_err()
        );
    }
}

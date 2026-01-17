//! Postage stamp types.

use crate::BatchId;
use alloy_primitives::B256;
use thiserror::Error;

/// The size of a marshalled stamp in bytes.
///
/// Layout: batch_id (32) + bucket (4) + index (4) + timestamp (8) + signature (65) = 113 bytes
pub const STAMP_SIZE: usize = 113;

/// A marshalled (serialized) postage stamp.
pub type MarshalledStamp = [u8; STAMP_SIZE];

/// Errors that can occur when working with stamps.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum StampError {
    /// The owner recovered from the signature doesn't match the expected owner.
    #[error("owner mismatch: expected {expected}, got {actual}")]
    OwnerMismatch {
        /// The expected owner address.
        expected: alloy_primitives::Address,
        /// The actual owner recovered from the signature.
        actual: alloy_primitives::Address,
    },

    /// The bucket index exceeds the maximum allowed for the batch depth.
    #[error("invalid index: bucket index exceeds maximum")]
    InvalidIndex,

    /// The chunk address doesn't match the expected collision bucket.
    #[error("bucket mismatch: chunk address doesn't match expected bucket")]
    BucketMismatch,

    /// The batch was not found in the store.
    #[error("batch not found: {0}")]
    BatchNotFound(BatchId),

    /// Invalid stamp data (wrong size or format).
    #[error("invalid stamp data: {0}")]
    InvalidData(&'static str),

    /// The batch bucket is full and cannot accept more chunks.
    #[error("bucket full: bucket {bucket} has reached capacity")]
    BucketFull {
        /// The bucket that is full.
        bucket: u32,
    },
}

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
/// A marshalled stamp is 113 bytes:
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
    /// The signature proving ownership (65 bytes: r || s || v).
    sig: [u8; 65],
}

impl Stamp {
    /// Creates a new stamp with the given parameters.
    #[inline]
    pub const fn new(
        batch: BatchId,
        bucket: u32,
        index: u32,
        timestamp: u64,
        sig: [u8; 65],
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
    pub const fn with_index(
        batch: BatchId,
        index: StampIndex,
        timestamp: u64,
        sig: [u8; 65],
    ) -> Self {
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
    pub const fn signature(&self) -> &[u8; 65] {
        &self.sig
    }

    /// Serializes the stamp to a 113-byte array.
    pub fn marshal(&self) -> MarshalledStamp {
        let mut bytes = [0u8; STAMP_SIZE];
        bytes[..32].copy_from_slice(self.batch.as_slice());
        bytes[32..36].copy_from_slice(&self.index.bucket().to_be_bytes());
        bytes[36..40].copy_from_slice(&self.index.index().to_be_bytes());
        bytes[40..48].copy_from_slice(&self.timestamp.to_be_bytes());
        bytes[48..STAMP_SIZE].copy_from_slice(&self.sig);
        bytes
    }

    /// Deserializes a stamp from a 113-byte array.
    pub fn unmarshal(bytes: &MarshalledStamp) -> Self {
        let batch = B256::from_slice(&bytes[..32]);
        let bucket = u32::from_be_bytes(bytes[32..36].try_into().unwrap());
        let index = u32::from_be_bytes(bytes[36..40].try_into().unwrap());
        let timestamp = u64::from_be_bytes(bytes[40..48].try_into().unwrap());
        let mut sig = [0u8; 65];
        sig.copy_from_slice(&bytes[48..STAMP_SIZE]);

        Self {
            batch,
            index: StampIndex::new(bucket, index),
            timestamp,
            sig,
        }
    }

    /// Attempts to deserialize a stamp from a byte slice.
    ///
    /// Returns an error if the slice is not exactly 113 bytes.
    pub fn try_from_slice(bytes: &[u8]) -> Result<Self, StampError> {
        if bytes.len() != STAMP_SIZE {
            return Err(StampError::InvalidData("stamp must be exactly 113 bytes"));
        }

        let marshalled: &MarshalledStamp = bytes.try_into().unwrap();
        Ok(Self::unmarshal(marshalled))
    }
}

impl From<Stamp> for MarshalledStamp {
    fn from(stamp: Stamp) -> Self {
        stamp.marshal()
    }
}

impl From<&Stamp> for MarshalledStamp {
    fn from(stamp: &Stamp) -> Self {
        stamp.marshal()
    }
}

impl From<MarshalledStamp> for Stamp {
    fn from(bytes: MarshalledStamp) -> Self {
        Self::unmarshal(&bytes)
    }
}

impl From<&MarshalledStamp> for Stamp {
    fn from(bytes: &MarshalledStamp) -> Self {
        Self::unmarshal(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::hex;

    const TEST_BATCH_ID: &str =
        "c3387832bb1b88acbcd0ffdb65a08ef077d98c08d4bee576a72dbe3d36761369";
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
    fn test_stamp_marshal_unmarshal() {
        let batch = B256::ZERO;
        let sig = [0u8; 65];
        let stamp = Stamp::new(batch, 100, 50, 1234567890, sig);

        let marshalled = stamp.marshal();
        let unmarshalled = Stamp::unmarshal(&marshalled);

        assert_eq!(stamp, unmarshalled);
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
        let stamp = Stamp::with_index(batch, idx, 1234567890, [0u8; 65]);

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
        let stamp = Stamp::new(B256::ZERO, 1, 2, 3, [0u8; 65]);

        let marshalled: MarshalledStamp = stamp.clone().into();
        let back: Stamp = marshalled.into();
        assert_eq!(stamp, back);

        let marshalled_ref: MarshalledStamp = (&stamp).into();
        let back_ref: Stamp = (&marshalled_ref).into();
        assert_eq!(stamp, back_ref);
    }
}

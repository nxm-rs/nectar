//! Postage stamp types.

use crate::BatchId;
use alloy_primitives::B256;
use thiserror::Error;

/// The size of a marshalled stamp in bytes.
///
/// Layout: batch_id (32) + x (4) + y (4) + timestamp (8) + signature (65) = 113 bytes
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

    /// The bucket index (y) exceeds the maximum allowed for the batch depth.
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
}

/// A postage stamp represents proof of payment for storing a chunk.
///
/// Stamps are created by signing a message containing the chunk address,
/// batch ID, index, and timestamp with the batch owner's private key.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Stamp {
    /// The batch ID this stamp belongs to.
    batch: BatchId,
    /// The collision bucket index (x coordinate).
    x: u32,
    /// The index within the bucket (y coordinate).
    y: u32,
    /// Timestamp when the stamp was created.
    timestamp: u64,
    /// The signature proving ownership (65 bytes: r || s || v).
    sig: [u8; 65],
}

impl Stamp {
    /// Creates a new stamp with the given parameters.
    #[inline]
    pub const fn new(batch: BatchId, x: u32, y: u32, timestamp: u64, sig: [u8; 65]) -> Self {
        Self {
            batch,
            x,
            y,
            timestamp,
            sig,
        }
    }

    /// Returns the batch ID.
    #[inline]
    pub const fn batch(&self) -> BatchId {
        self.batch
    }

    /// Returns the collision bucket index (x).
    #[inline]
    pub const fn x(&self) -> u32 {
        self.x
    }

    /// Returns the bucket index (y).
    #[inline]
    pub const fn y(&self) -> u32 {
        self.y
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

    /// Computes the "silly index" used in the stamp digest.
    ///
    /// This is a concatenation of x (4 bytes BE) and y (4 bytes BE) as a u64.
    #[inline]
    pub const fn silly_index(&self) -> u64 {
        ((self.x as u64) << 32) | (self.y as u64)
    }

    /// Serializes the stamp to a 113-byte array.
    pub fn marshal(&self) -> MarshalledStamp {
        let mut bytes = [0u8; STAMP_SIZE];
        bytes[..32].copy_from_slice(self.batch.as_slice());
        bytes[32..36].copy_from_slice(&self.x.to_be_bytes());
        bytes[36..40].copy_from_slice(&self.y.to_be_bytes());
        bytes[40..48].copy_from_slice(&self.timestamp.to_be_bytes());
        bytes[48..STAMP_SIZE].copy_from_slice(&self.sig);
        bytes
    }

    /// Deserializes a stamp from a 113-byte array.
    pub fn unmarshal(bytes: &MarshalledStamp) -> Self {
        let batch = B256::from_slice(&bytes[..32]);
        let x = u32::from_be_bytes(bytes[32..36].try_into().unwrap());
        let y = u32::from_be_bytes(bytes[36..40].try_into().unwrap());
        let timestamp = u64::from_be_bytes(bytes[40..48].try_into().unwrap());
        let mut sig = [0u8; 65];
        sig.copy_from_slice(&bytes[48..STAMP_SIZE]);

        Self {
            batch,
            x,
            y,
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
        assert_eq!(stamp.x(), 52197); // 0x0000cbe5
        assert_eq!(stamp.y(), 0);
        assert_eq!(stamp.timestamp(), 1688492510651);
    }

    #[test]
    fn test_stamp_silly_index() {
        let stamp = Stamp::new(B256::ZERO, 0x1234, 0x5678, 0, [0u8; 65]);
        assert_eq!(stamp.silly_index(), 0x0000123400005678);
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

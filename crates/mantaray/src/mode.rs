//! Node entry types for plain and encrypted manifests.

use nectar_primitives::chunk::ChunkAddress;

use crate::error::{MantarayError, Result};

/// Trait for typed entries stored in mantaray nodes.
///
/// Implementations define the compile-time size and serialization for entries.
/// `ChunkAddress` (32 bytes) is used for plain manifests, and
/// `EncryptedChunkRef` (64 bytes) for encrypted manifests.
pub trait NodeEntry: Clone + PartialEq + Eq + core::fmt::Debug + 'static {
    /// Size of the serialized entry in bytes.
    const SIZE: usize;

    /// The chunk address component of this entry.
    fn address(&self) -> &ChunkAddress;

    /// Serialize to bytes (allocating). Prefer [`write_to`](Self::write_to) in hot paths.
    fn to_bytes(&self) -> Vec<u8>;

    /// Write serialized bytes directly into `buf` without allocating.
    fn write_to(&self, buf: &mut Vec<u8>);

    /// Deserialize from bytes.
    fn try_from_bytes(bytes: &[u8]) -> Result<Self>;
}

impl NodeEntry for ChunkAddress {
    const SIZE: usize = 32;

    fn address(&self) -> &ChunkAddress {
        self
    }

    fn to_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }

    fn write_to(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.as_bytes());
    }

    fn try_from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != Self::SIZE {
            return Err(MantarayError::EntrySizeMismatch {
                expected: Self::SIZE,
                actual: bytes.len(),
            });
        }
        #[allow(clippy::expect_used)] // infallible: bytes.len() == Self::SIZE (32) checked above
        let arr: [u8; 32] = bytes.try_into().expect("length checked");
        Ok(Self::from(arr))
    }
}

#[cfg(feature = "encryption")]
impl NodeEntry for nectar_primitives::EncryptedChunkRef {
    const SIZE: usize = 64;

    fn address(&self) -> &ChunkAddress {
        Self::address(self)
    }

    fn to_bytes(&self) -> Vec<u8> {
        Vec::from(self)
    }

    fn write_to(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.address().as_bytes());
        buf.extend_from_slice(self.key().as_bytes());
    }

    fn try_from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != Self::SIZE {
            return Err(MantarayError::EntrySizeMismatch {
                expected: Self::SIZE,
                actual: bytes.len(),
            });
        }
        Self::try_from(bytes).map_err(|_| MantarayError::EntrySizeMismatch {
            expected: Self::SIZE,
            actual: bytes.len(),
        })
    }
}

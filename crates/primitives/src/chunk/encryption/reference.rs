//! Chunk reference types for encrypted chunks.

use std::mem::size_of;

use crate::chunk::ChunkAddress;

use super::error::EncryptionError;
use super::key::EncryptionKey;

/// An encrypted chunk reference: 32-byte address + 32-byte decryption key.
///
/// This type statically guarantees the reference is encrypted,
/// eliminating runtime variant checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptedChunkRef {
    address: ChunkAddress,
    key: EncryptionKey,
}

impl EncryptedChunkRef {
    /// Serialized size: address + decryption key.
    pub const SIZE: usize = size_of::<ChunkAddress>() + EncryptionKey::SIZE;

    /// Create a new encrypted chunk reference.
    pub fn new(address: ChunkAddress, key: EncryptionKey) -> Self {
        Self { address, key }
    }

    /// Chunk address (BMT hash of ciphertext).
    pub fn address(&self) -> &ChunkAddress {
        &self.address
    }

    /// Decryption key.
    pub fn key(&self) -> &EncryptionKey {
        &self.key
    }

    /// Consume and return (address, key).
    pub fn into_parts(self) -> (ChunkAddress, EncryptionKey) {
        (self.address, self.key)
    }

    /// Write the reference into `buf`. Panics if `buf` is too small.
    pub fn write_to(&self, buf: &mut [u8]) {
        buf[..size_of::<ChunkAddress>()].copy_from_slice(self.address.as_bytes());
        buf[size_of::<ChunkAddress>()..Self::SIZE]
            .copy_from_slice(self.key.as_bytes());
    }
}

impl From<&EncryptedChunkRef> for [u8; EncryptedChunkRef::SIZE] {
    fn from(r: &EncryptedChunkRef) -> Self {
        let mut buf = [0u8; EncryptedChunkRef::SIZE];
        r.write_to(&mut buf);
        buf
    }
}

impl From<EncryptedChunkRef> for [u8; EncryptedChunkRef::SIZE] {
    fn from(r: EncryptedChunkRef) -> Self {
        (&r).into()
    }
}

impl From<&EncryptedChunkRef> for Vec<u8> {
    fn from(r: &EncryptedChunkRef) -> Self {
        let mut v = Vec::with_capacity(EncryptedChunkRef::SIZE);
        v.extend_from_slice(r.address.as_bytes());
        v.extend_from_slice(r.key.as_bytes());
        v
    }
}

impl TryFrom<&[u8]> for EncryptedChunkRef {
    type Error = EncryptionError;

    fn try_from(slice: &[u8]) -> Result<Self, Self::Error> {
        if slice.len() != Self::SIZE {
            return Err(EncryptionError::InvalidReferenceLength { len: slice.len() });
        }
        let addr = ChunkAddress::from_slice(&slice[..size_of::<ChunkAddress>()])
            .map_err(|_| EncryptionError::InvalidReferenceLength { len: slice.len() })?;
        let key = EncryptionKey::try_from(&slice[size_of::<ChunkAddress>()..])?;
        Ok(Self { address: addr, key })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;

    #[test]
    fn encrypted_roundtrip() {
        let addr = ChunkAddress::from(B256::repeat_byte(0xcd));
        let key = EncryptionKey::from([0xef; 32]);
        let enc_ref = EncryptedChunkRef::new(addr, key.clone());

        assert_eq!(enc_ref.address(), &addr);
        assert_eq!(enc_ref.key(), &key);

        let bytes: [u8; 64] = (&enc_ref).into();
        assert_eq!(bytes.len(), 64);

        let recovered = EncryptedChunkRef::try_from(bytes.as_slice()).unwrap();
        assert_eq!(recovered, enc_ref);
    }

    #[test]
    fn invalid_length() {
        let bad = [0u8; 48];
        let err = EncryptedChunkRef::try_from(bad.as_slice()).unwrap_err();
        assert!(matches!(
            err,
            EncryptionError::InvalidReferenceLength { len: 48 }
        ));
    }

    #[test]
    fn write_to_buffer() {
        let addr = ChunkAddress::from(B256::repeat_byte(0x22));
        let key = EncryptionKey::from([0x33; 32]);
        let enc_ref = EncryptedChunkRef::new(addr, key);

        let mut buf = [0u8; 64];
        enc_ref.write_to(&mut buf);
        assert_eq!(&buf[..32], &[0x22; 32]);
        assert_eq!(&buf[32..], &[0x33; 32]);
    }
}

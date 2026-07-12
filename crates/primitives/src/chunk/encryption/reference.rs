//! Chunk reference types for encrypted chunks.

use crate::chunk::reference::{RefKind, Reference, sealed};
use crate::chunk::{ChunkAddress, ChunkRef};
use crate::wire::{Cursor, Underrun};

use super::error::EncryptionError;
use super::key::EncryptionKey;

/// An encrypted chunk reference: a plain reference plus the decryption key.
///
/// This type statically guarantees the reference is encrypted, eliminating
/// runtime variant checks. It composes [`ChunkRef`] rather than restating the
/// address field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptedChunkRef {
    reference: ChunkRef,
    key: EncryptionKey,
}

impl EncryptedChunkRef {
    /// Serialized size: reference + decryption key.
    pub const SIZE: usize = ChunkRef::SIZE + EncryptionKey::SIZE;

    /// Create a new encrypted chunk reference.
    pub const fn new(address: ChunkAddress, key: EncryptionKey) -> Self {
        Self {
            reference: ChunkRef::new(address),
            key,
        }
    }

    /// The plain reference this encrypted reference extends.
    pub const fn reference(&self) -> &ChunkRef {
        &self.reference
    }

    /// Chunk address (BMT hash of ciphertext).
    pub const fn address(&self) -> &ChunkAddress {
        self.reference.address()
    }

    /// Decryption key.
    pub const fn key(&self) -> &EncryptionKey {
        &self.key
    }

    /// Consume and return (address, key).
    pub fn into_parts(self) -> (ChunkAddress, EncryptionKey) {
        (self.reference.into_address(), self.key)
    }

    /// Write the reference into `buf`. Panics if `buf` is too small.
    #[allow(clippy::indexing_slicing)] // documented contract: panics if buf.len() < Self::SIZE; both callers pass fixed [u8; Self::SIZE] buffers
    pub fn write_to(&self, buf: &mut [u8]) {
        buf[..ChunkRef::SIZE].copy_from_slice(self.address().as_bytes());
        buf[ChunkRef::SIZE..Self::SIZE].copy_from_slice(self.key.as_bytes());
    }
}

impl sealed::Sealed for EncryptedChunkRef {}

impl Reference for EncryptedChunkRef {
    const KIND: RefKind = RefKind::Encrypted;

    fn read_optional(cursor: &mut Cursor<'_>) -> Result<Option<Self>, Underrun> {
        // The sentinel is the whole slot as written on the wire: address and
        // key both all-zero.
        let addr = cursor.take::<[u8; ChunkAddress::SIZE]>()?;
        let key = cursor.take::<[u8; EncryptionKey::SIZE]>()?;
        if addr.iter().all(|&b| b == 0) && key.iter().all(|&b| b == 0) {
            Ok(None)
        } else {
            Ok(Some(Self::new(
                ChunkAddress::new(addr),
                EncryptionKey::from(key),
            )))
        }
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
        let mut v = Self::with_capacity(EncryptedChunkRef::SIZE);
        v.extend_from_slice(r.address().as_bytes());
        v.extend_from_slice(r.key.as_bytes());
        v
    }
}

impl TryFrom<&[u8]> for EncryptedChunkRef {
    type Error = EncryptionError;

    #[allow(clippy::indexing_slicing)] // slice.len() == Self::SIZE (64) is checked above with an error return, covering both the 32-byte address and 32-byte key slices
    fn try_from(slice: &[u8]) -> Result<Self, Self::Error> {
        if slice.len() != Self::SIZE {
            return Err(EncryptionError::InvalidReferenceLength { len: slice.len() });
        }
        let addr = ChunkAddress::from_slice(&slice[..ChunkRef::SIZE])
            .map_err(|_| EncryptionError::InvalidReferenceLength { len: slice.len() })?;
        let key = EncryptionKey::try_from(&slice[ChunkRef::SIZE..])?;
        Ok(Self::new(addr, key))
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
        assert_eq!(enc_ref.reference().address(), &addr);
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

    #[test]
    fn read_optional_sentinel_is_the_whole_slot() {
        let zeros = [0u8; EncryptedChunkRef::SIZE];
        let mut cur = Cursor::new(&zeros);
        assert_eq!(EncryptedChunkRef::read_optional(&mut cur).unwrap(), None);
        assert!(cur.is_empty());

        // A zero address under a nonzero key is a reference, not the sentinel.
        let mut slot = [0u8; EncryptedChunkRef::SIZE];
        slot[32..].copy_from_slice(&[0x33; 32]);
        let mut cur = Cursor::new(&slot);
        assert_eq!(
            EncryptedChunkRef::read_optional(&mut cur).unwrap(),
            Some(EncryptedChunkRef::new(
                ChunkAddress::new([0u8; 32]),
                EncryptionKey::from([0x33; 32])
            ))
        );

        // Underrun inside the slot is an error, not a sentinel.
        let short = [0u8; EncryptedChunkRef::SIZE - 1];
        let mut cur = Cursor::new(&short);
        assert!(EncryptedChunkRef::read_optional(&mut cur).is_err());
    }
}

//! Chunk reference types for encrypted chunks.

use crate::chunk::reference::{RefKind, Reference, WrongRefKind, sealed};
use crate::chunk::{ChunkAddress, ChunkRef};
use crate::file::EntryRef;
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

    /// Serialize to the fixed wire form: address followed by decryption key.
    pub const fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        let (address, key) = buf.split_at_mut(ChunkRef::SIZE);
        address.copy_from_slice(self.address().as_bytes());
        key.copy_from_slice(self.key.as_bytes());
        buf
    }

    /// Reconstruct from the fixed wire form: any 64 bytes are a valid reference.
    pub fn from_bytes(bytes: &[u8; Self::SIZE]) -> Self {
        let mut address = [0u8; ChunkRef::SIZE];
        let mut key = [0u8; EncryptionKey::SIZE];
        let (a, k) = bytes.split_at(ChunkRef::SIZE);
        address.copy_from_slice(a);
        key.copy_from_slice(k);
        Self::new(ChunkAddress::from(address), EncryptionKey::from(key))
    }
}

impl sealed::Sealed for EncryptedChunkRef {}

impl Reference for EncryptedChunkRef {
    const KIND: RefKind = RefKind::Encrypted;

    fn address(&self) -> &ChunkAddress {
        self.reference.address()
    }

    fn into_entry_ref(self) -> EntryRef {
        EntryRef::Encrypted(self)
    }

    fn from_entry_ref(entry: EntryRef) -> Result<Self, WrongRefKind> {
        match entry {
            EntryRef::Encrypted(enc) => Ok(enc),
            EntryRef::Plain(_) => Err(WrongRefKind {
                expected: Self::KIND,
                got: RefKind::Plain,
            }),
        }
    }

    fn write_to(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.address().as_bytes());
        out.extend_from_slice(self.key.as_bytes());
    }

    fn from_wire_bytes(bytes: &[u8]) -> Option<Self> {
        let bytes: &[u8; Self::SIZE] = bytes.try_into().ok()?;
        Some(Self::from_bytes(bytes))
    }

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
        r.to_bytes()
    }
}

impl From<EncryptedChunkRef> for [u8; EncryptedChunkRef::SIZE] {
    fn from(r: EncryptedChunkRef) -> Self {
        r.to_bytes()
    }
}

impl From<[u8; Self::SIZE]> for EncryptedChunkRef {
    fn from(bytes: [u8; Self::SIZE]) -> Self {
        Self::from_bytes(&bytes)
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

        assert_eq!(EncryptedChunkRef::from(bytes), enc_ref);
        assert_eq!(EncryptedChunkRef::from_bytes(&bytes), enc_ref);
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
    fn to_bytes_layout() {
        let addr = ChunkAddress::from(B256::repeat_byte(0x22));
        let key = EncryptionKey::from([0x33; 32]);
        let enc_ref = EncryptedChunkRef::new(addr, key);

        let buf = enc_ref.to_bytes();
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

//! Typed chunk references.
//!
//! A reference names a chunk by its 32-byte address; the width is a fact of the
//! reference type, not a runtime byte count. [`RefKind`] names the two widths
//! and [`Reference`] carries them at the type level, so every wire-width
//! constant in the crate derives from this single statement of the fact.

use std::mem::size_of;

use crate::chunk::ChunkAddress;

pub(crate) mod sealed {
    pub trait Sealed {}
}

/// The two reference widths: a plain address, or an address plus a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RefKind {
    /// A plain reference ([`ChunkRef`]): a 32-byte address.
    Unencrypted,
    /// An encrypted reference
    /// ([`EncryptedChunkRef`](crate::chunk::encryption::EncryptedChunkRef)):
    /// the same address plus the chunk's decryption key.
    Encrypted,
}

impl RefKind {
    /// Wire width in bytes of a reference of this kind.
    pub const fn size(self) -> usize {
        match self {
            Self::Unencrypted => ChunkRef::SIZE,
            Self::Encrypted => crate::chunk::encryption::EncryptedChunkRef::SIZE,
        }
    }
}

/// A chunk reference whose width is a compile-time fact.
///
/// Sealed: the only references are [`ChunkRef`] and
/// [`EncryptedChunkRef`](crate::chunk::encryption::EncryptedChunkRef).
pub trait Reference: sealed::Sealed {
    /// Which width this reference carries.
    const KIND: RefKind;

    /// Wire width in bytes; the width fact, derived from [`Self::KIND`].
    const SIZE: usize = Self::KIND.size();
}

/// A 32-byte reference to a chunk.
///
/// The chunk may be content-addressed or single-owner; a reference is identical
/// either way, and which kind it is is resolved on fetch and validation, never
/// from the reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChunkRef(ChunkAddress);

impl ChunkRef {
    /// Wire width in bytes.
    pub const SIZE: usize = size_of::<ChunkAddress>();

    /// Wrap an address as a reference.
    pub const fn new(address: ChunkAddress) -> Self {
        Self(address)
    }

    /// The referenced chunk address.
    pub const fn address(&self) -> &ChunkAddress {
        &self.0
    }

    /// Consume the reference, returning its address.
    pub const fn into_address(self) -> ChunkAddress {
        self.0
    }
}

impl From<ChunkAddress> for ChunkRef {
    fn from(address: ChunkAddress) -> Self {
        Self(address)
    }
}

impl sealed::Sealed for ChunkRef {}

impl Reference for ChunkRef {
    const KIND: RefKind = RefKind::Unencrypted;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::encryption::EncryptedChunkRef;
    use alloy_primitives::B256;

    #[test]
    fn chunk_ref_is_address_width() {
        assert_eq!(ChunkRef::SIZE, 32);
        assert_eq!(<ChunkRef as Reference>::SIZE, ChunkRef::SIZE);
        assert_eq!(ChunkRef::KIND, RefKind::Unencrypted);
        assert_eq!(RefKind::Unencrypted.size(), ChunkRef::SIZE);
    }

    #[test]
    fn encrypted_ref_is_address_plus_key() {
        assert_eq!(EncryptedChunkRef::SIZE, 64);
        assert_eq!(
            <EncryptedChunkRef as Reference>::SIZE,
            EncryptedChunkRef::SIZE
        );
        assert_eq!(EncryptedChunkRef::KIND, RefKind::Encrypted);
        assert_eq!(RefKind::Encrypted.size(), EncryptedChunkRef::SIZE);
    }

    #[test]
    fn round_trips_through_address() {
        let addr = ChunkAddress::from(B256::repeat_byte(0x7f));
        let reference = ChunkRef::new(addr);
        assert_eq!(reference.address(), &addr);
        assert_eq!(reference.into_address(), addr);
        assert_eq!(ChunkRef::from(addr), reference);
    }
}

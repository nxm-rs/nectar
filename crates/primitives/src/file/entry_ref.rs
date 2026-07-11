//! Unified file entry reference type.

use crate::chunk::encryption::EncryptedChunkRef;
use crate::chunk::{ChunkAddress, ChunkRef};

use super::error::FileError;

/// A typed chunk reference: either a plain 32-byte address or an encrypted 64-byte ref.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryRef {
    /// Plain 32-byte chunk address.
    Plain(ChunkAddress),
    /// Encrypted 64-byte reference (address + decryption key).
    Encrypted(EncryptedChunkRef),
}

impl EntryRef {
    /// Parse an entry reference from raw bytes.
    ///
    /// - [`ChunkRef::SIZE`] bytes → `Plain`
    /// - [`EncryptedChunkRef::SIZE`] bytes → `Encrypted`
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, FileError> {
        if let Ok(addr_bytes) = <[u8; ChunkRef::SIZE]>::try_from(bytes) {
            return Ok(Self::Plain(ChunkAddress::from(addr_bytes)));
        }
        if bytes.len() == EncryptedChunkRef::SIZE {
            let enc_ref = EncryptedChunkRef::try_from(bytes)
                .map_err(|_| FileError::InvalidEntryRef { len: bytes.len() })?;
            return Ok(Self::Encrypted(enc_ref));
        }
        Err(FileError::InvalidEntryRef { len: bytes.len() })
    }

    /// The chunk address (first 32 bytes of any reference).
    pub const fn address(&self) -> &ChunkAddress {
        match self {
            Self::Plain(addr) => addr,
            Self::Encrypted(enc) => enc.address(),
        }
    }
}

impl From<ChunkAddress> for EntryRef {
    fn from(addr: ChunkAddress) -> Self {
        Self::Plain(addr)
    }
}

impl From<EncryptedChunkRef> for EntryRef {
    fn from(enc: EncryptedChunkRef) -> Self {
        Self::Encrypted(enc)
    }
}

impl From<&EntryRef> for Vec<u8> {
    fn from(entry_ref: &EntryRef) -> Self {
        match entry_ref {
            EntryRef::Plain(addr) => addr.as_bytes().to_vec(),
            EntryRef::Encrypted(enc) => Self::from(enc),
        }
    }
}

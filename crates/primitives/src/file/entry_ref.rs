//! Unified file entry reference type.

use crate::chunk::ChunkAddress;
#[cfg(feature = "encryption")]
use crate::chunk::encryption::EncryptedChunkRef;

use super::error::FileError;

/// A typed chunk reference: either a plain 32-byte address or an encrypted 64-byte ref.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryRef {
    /// Plain 32-byte chunk address.
    Plain(ChunkAddress),
    /// Encrypted 64-byte reference (address + decryption key).
    #[cfg(feature = "encryption")]
    Encrypted(EncryptedChunkRef),
}

impl EntryRef {
    /// Parse an entry reference from raw bytes.
    ///
    /// - 32 bytes → `Plain`
    /// - 64 bytes → `Encrypted` (requires `encryption` feature)
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, FileError> {
        match bytes.len() {
            32 => {
                #[allow(clippy::expect_used)]
                // infallible: this match arm guarantees bytes.len() == 32
                let addr_bytes: [u8; 32] = bytes.try_into().expect("length checked");
                Ok(Self::Plain(ChunkAddress::from(addr_bytes)))
            }
            #[cfg(feature = "encryption")]
            64 => {
                let enc_ref = EncryptedChunkRef::try_from(bytes)
                    .map_err(|_| FileError::InvalidEntryRef { len: bytes.len() })?;
                Ok(Self::Encrypted(enc_ref))
            }
            len => Err(FileError::InvalidEntryRef { len }),
        }
    }

    /// The chunk address (first 32 bytes of any reference).
    pub const fn address(&self) -> &ChunkAddress {
        match self {
            Self::Plain(addr) => addr,
            #[cfg(feature = "encryption")]
            Self::Encrypted(enc) => enc.address(),
        }
    }
}

impl From<ChunkAddress> for EntryRef {
    fn from(addr: ChunkAddress) -> Self {
        Self::Plain(addr)
    }
}

#[cfg(feature = "encryption")]
impl From<EncryptedChunkRef> for EntryRef {
    fn from(enc: EncryptedChunkRef) -> Self {
        Self::Encrypted(enc)
    }
}

impl From<&EntryRef> for Vec<u8> {
    fn from(entry_ref: &EntryRef) -> Self {
        match entry_ref {
            EntryRef::Plain(addr) => addr.as_bytes().to_vec(),
            #[cfg(feature = "encryption")]
            EntryRef::Encrypted(enc) => Self::from(enc),
        }
    }
}

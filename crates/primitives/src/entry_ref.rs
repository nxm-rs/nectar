//! Unified file entry reference type.

use crate::chunk::encryption::EncryptedChunkRef;
use crate::chunk::{ChunkAddress, ChunkRef};

#[allow(deprecated)]
use crate::file::error::FileError;

/// A typed chunk reference: either a plain 32-byte reference or an encrypted
/// 64-byte reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryRef {
    /// Plain 32-byte chunk reference.
    Plain(ChunkRef),
    /// Encrypted 64-byte reference (address + decryption key).
    Encrypted(EncryptedChunkRef),
}

#[allow(deprecated)]
impl EntryRef {
    /// Parse an entry reference from raw bytes.
    ///
    /// - [`ChunkRef::SIZE`] bytes → `Plain`
    /// - [`EncryptedChunkRef::SIZE`] bytes → `Encrypted`
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, FileError> {
        if let Ok(addr_bytes) = <[u8; ChunkRef::SIZE]>::try_from(bytes) {
            return Ok(Self::Plain(ChunkRef::new(ChunkAddress::from(addr_bytes))));
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
            Self::Plain(reference) => reference.address(),
            Self::Encrypted(enc) => enc.address(),
        }
    }
}

impl From<ChunkRef> for EntryRef {
    fn from(reference: ChunkRef) -> Self {
        Self::Plain(reference)
    }
}

impl From<ChunkAddress> for EntryRef {
    fn from(addr: ChunkAddress) -> Self {
        Self::Plain(ChunkRef::new(addr))
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
            EntryRef::Plain(reference) => reference.address().as_bytes().to_vec(),
            EntryRef::Encrypted(enc) => Self::from(enc),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::encryption::{EncryptedChunkRef, EncryptionKey};

    /// Both widths parse and re-serialize without the `encryption` feature:
    /// the representation is unconditional, only key generation and joining
    /// are gated.
    #[test]
    fn parses_and_reserializes_both_widths() {
        let plain_bytes = [0x11u8; ChunkRef::SIZE];
        let plain = EntryRef::try_from_bytes(&plain_bytes).unwrap();
        assert_eq!(
            plain,
            EntryRef::Plain(ChunkRef::new(ChunkAddress::from(plain_bytes)))
        );
        assert_eq!(Vec::<u8>::from(&plain), plain_bytes);

        let mut enc_bytes = [0x22u8; EncryptedChunkRef::SIZE];
        enc_bytes[ChunkRef::SIZE..].fill(0x33);
        let encrypted = EntryRef::try_from_bytes(&enc_bytes).unwrap();
        let EntryRef::Encrypted(ref enc) = encrypted else {
            panic!("64 bytes must parse as an encrypted reference");
        };
        assert_eq!(enc.address().as_bytes(), &[0x22u8; ChunkRef::SIZE]);
        assert_eq!(
            enc.key(),
            &EncryptionKey::from([0x33u8; EncryptionKey::SIZE])
        );
        assert_eq!(Vec::<u8>::from(&encrypted), enc_bytes);
    }

    #[test]
    #[allow(deprecated)]
    fn rejects_other_widths() {
        for len in [0usize, 31, 33, 63, 65, 96] {
            let bytes = vec![0u8; len];
            let err = EntryRef::try_from_bytes(&bytes).unwrap_err();
            assert!(matches!(err, FileError::InvalidEntryRef { len: got } if got == len));
        }
    }

    #[test]
    fn plain_variant_converts_from_address_and_reference() {
        let addr = ChunkAddress::from([0x44u8; ChunkRef::SIZE]);
        let from_addr = EntryRef::from(addr);
        let from_ref = EntryRef::from(ChunkRef::new(addr));
        assert_eq!(from_addr, from_ref);
        assert_eq!(from_addr.address(), &addr);
    }
}

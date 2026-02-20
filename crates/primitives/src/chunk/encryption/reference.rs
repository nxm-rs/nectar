//! Chunk reference types (plain and encrypted).

use crate::chunk::ChunkAddress;

use super::error::EncryptionError;
use super::key::EncryptionKey;
use super::KEY_SIZE;

const ADDRESS_SIZE: usize = 32;

/// A reference to a chunk, either plain (32-byte address) or encrypted
/// (32-byte address + 32-byte decryption key).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkRef {
    /// Unencrypted chunk reference (32-byte address only).
    Plain(ChunkAddress),
    /// Encrypted chunk reference (32-byte address + 32-byte decryption key).
    Encrypted {
        /// Chunk address (BMT hash of ciphertext).
        address: ChunkAddress,
        /// Decryption key.
        key: EncryptionKey,
    },
}

impl ChunkRef {
    /// Returns the chunk address.
    pub const fn address(&self) -> &ChunkAddress {
        match self {
            Self::Plain(addr) => addr,
            Self::Encrypted { address, .. } => address,
        }
    }

    /// Returns the decryption key if this is an encrypted reference.
    pub const fn key(&self) -> Option<&EncryptionKey> {
        match self {
            Self::Plain(_) => None,
            Self::Encrypted { key, .. } => Some(key),
        }
    }

    /// Returns `true` if this is an encrypted reference.
    pub const fn is_encrypted(&self) -> bool {
        matches!(self, Self::Encrypted { .. })
    }

    /// Serialized size: 32 for plain, 64 for encrypted.
    pub const fn ref_size(&self) -> usize {
        match self {
            Self::Plain(_) => ADDRESS_SIZE,
            Self::Encrypted { .. } => ADDRESS_SIZE + KEY_SIZE,
        }
    }

    /// Write the reference into `buf`. Panics if `buf` is too small.
    pub fn write_to(&self, buf: &mut [u8]) {
        match self {
            Self::Plain(addr) => {
                buf[..ADDRESS_SIZE].copy_from_slice(addr.as_bytes());
            }
            Self::Encrypted { address, key } => {
                buf[..ADDRESS_SIZE].copy_from_slice(address.as_bytes());
                buf[ADDRESS_SIZE..ADDRESS_SIZE + KEY_SIZE]
                    .copy_from_slice(<EncryptionKey as AsRef<[u8]>>::as_ref(key));
            }
        }
    }

    /// Serialize to a new `Vec<u8>`.
    pub fn to_vec(&self) -> Vec<u8> {
        let mut buf = vec![0u8; self.ref_size()];
        self.write_to(&mut buf);
        buf
    }
}

/// An encrypted chunk reference: 32-byte address + 32-byte decryption key.
///
/// Unlike [`ChunkRef`] which is an enum, this type statically guarantees
/// the reference is encrypted, eliminating runtime variant checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptedChunkRef {
    /// Chunk address (BMT hash of ciphertext).
    pub address: ChunkAddress,
    /// Decryption key.
    pub key: EncryptionKey,
}

impl EncryptedChunkRef {
    /// Serialized size (always 64 bytes).
    pub const SIZE: usize = ADDRESS_SIZE + KEY_SIZE;

    /// Write the reference into `buf`. Panics if `buf` is too small.
    pub fn write_to(&self, buf: &mut [u8]) {
        buf[..ADDRESS_SIZE].copy_from_slice(self.address.as_bytes());
        buf[ADDRESS_SIZE..Self::SIZE]
            .copy_from_slice(<EncryptionKey as AsRef<[u8]>>::as_ref(&self.key));
    }

    /// Serialize to a new `Vec<u8>`.
    pub fn to_vec(&self) -> Vec<u8> {
        let mut buf = vec![0u8; Self::SIZE];
        self.write_to(&mut buf);
        buf
    }
}

impl From<EncryptedChunkRef> for ChunkRef {
    fn from(enc: EncryptedChunkRef) -> Self {
        Self::Encrypted {
            address: enc.address,
            key: enc.key,
        }
    }
}

impl TryFrom<ChunkRef> for EncryptedChunkRef {
    type Error = EncryptionError;

    fn try_from(chunk_ref: ChunkRef) -> Result<Self, Self::Error> {
        match chunk_ref {
            ChunkRef::Encrypted { address, key } => Ok(Self { address, key }),
            ChunkRef::Plain(_) => Err(EncryptionError::InvalidReferenceLength {
                len: ADDRESS_SIZE,
            }),
        }
    }
}

impl TryFrom<&[u8]> for EncryptedChunkRef {
    type Error = EncryptionError;

    fn try_from(slice: &[u8]) -> Result<Self, Self::Error> {
        if slice.len() != Self::SIZE {
            return Err(EncryptionError::InvalidReferenceLength { len: slice.len() });
        }
        let addr = ChunkAddress::from_slice(&slice[..ADDRESS_SIZE])
            .expect("slice length already verified as 32");
        let key = EncryptionKey::try_from(&slice[ADDRESS_SIZE..])?;
        Ok(Self { address: addr, key })
    }
}

impl From<ChunkAddress> for ChunkRef {
    fn from(addr: ChunkAddress) -> Self {
        Self::Plain(addr)
    }
}

impl TryFrom<&[u8]> for ChunkRef {
    type Error = EncryptionError;

    fn try_from(slice: &[u8]) -> Result<Self, Self::Error> {
        match slice.len() {
            ADDRESS_SIZE => {
                let addr = ChunkAddress::from_slice(slice)
                    .expect("slice length already verified as 32");
                Ok(Self::Plain(addr))
            }
            n if n == ADDRESS_SIZE + KEY_SIZE => {
                let addr = ChunkAddress::from_slice(&slice[..ADDRESS_SIZE])
                    .expect("slice length already verified as 32");
                let key = EncryptionKey::try_from(&slice[ADDRESS_SIZE..])?;
                Ok(Self::Encrypted { address: addr, key })
            }
            len => Err(EncryptionError::InvalidReferenceLength { len }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;

    #[test]
    fn plain_roundtrip() {
        let addr = ChunkAddress::from(B256::repeat_byte(0xab));
        let chunk_ref = ChunkRef::Plain(addr);

        assert!(!chunk_ref.is_encrypted());
        assert_eq!(chunk_ref.ref_size(), 32);
        assert!(chunk_ref.key().is_none());

        let bytes = chunk_ref.to_vec();
        assert_eq!(bytes.len(), 32);

        let recovered = ChunkRef::try_from(bytes.as_slice()).unwrap();
        assert_eq!(recovered, chunk_ref);
    }

    #[test]
    fn encrypted_roundtrip() {
        let addr = ChunkAddress::from(B256::repeat_byte(0xcd));
        let key = EncryptionKey::from([0xef; 32]);
        let chunk_ref = ChunkRef::Encrypted {
            address: addr,
            key,
        };

        assert!(chunk_ref.is_encrypted());
        assert_eq!(chunk_ref.ref_size(), 64);
        assert_eq!(chunk_ref.key(), Some(&key));

        let bytes = chunk_ref.to_vec();
        assert_eq!(bytes.len(), 64);

        let recovered = ChunkRef::try_from(bytes.as_slice()).unwrap();
        assert_eq!(recovered, chunk_ref);
    }

    #[test]
    fn from_chunk_address() {
        let addr = ChunkAddress::from(B256::repeat_byte(0x11));
        let chunk_ref = ChunkRef::from(addr);
        assert!(!chunk_ref.is_encrypted());
        assert_eq!(*chunk_ref.address(), addr);
    }

    #[test]
    fn invalid_length() {
        let bad = [0u8; 48];
        let err = ChunkRef::try_from(bad.as_slice()).unwrap_err();
        assert!(matches!(
            err,
            EncryptionError::InvalidReferenceLength { len: 48 }
        ));
    }

    #[test]
    fn write_to_buffer() {
        let addr = ChunkAddress::from(B256::repeat_byte(0x22));
        let key = EncryptionKey::from([0x33; 32]);
        let chunk_ref = ChunkRef::Encrypted {
            address: addr,
            key,
        };

        let mut buf = [0u8; 64];
        chunk_ref.write_to(&mut buf);
        assert_eq!(&buf[..32], &[0x22; 32]);
        assert_eq!(&buf[32..], &[0x33; 32]);
    }
}

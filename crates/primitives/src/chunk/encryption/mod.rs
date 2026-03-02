//! Chunk encryption using Keccak-256 counter-mode cipher.

mod cipher;
mod chunk;
mod error;
mod key;
mod reference;

pub use cipher::{transcrypt, transcrypt_in_place};
pub(crate) use chunk::decrypt_chunk_data;
#[cfg(feature = "encryption")]
pub(crate) use chunk::encrypt_chunk;
pub use error::EncryptionError;
pub use key::EncryptionKey;
pub use reference::EncryptedChunkRef;

/// Trait for encrypting chunks with a Keccak-256 counter-mode cipher.
#[cfg(feature = "encryption")]
pub trait ChunkEncrypt {
    /// The encrypted output type.
    type Encrypted;

    /// Encrypt with a caller-provided key.
    fn encrypt_with(&self, key: &EncryptionKey) -> crate::error::Result<Self::Encrypted>;

    /// Encrypt with a randomly generated key.
    fn encrypt(&self) -> crate::error::Result<Self::Encrypted> {
        self.encrypt_with(&EncryptionKey::generate())
    }
}

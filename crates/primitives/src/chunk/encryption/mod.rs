//! Chunk encryption using Keccak-256 counter-mode cipher.

mod chunk;
mod cipher;
mod key;
mod reference;

#[cfg(feature = "encryption")]
pub(crate) use chunk::{decrypt_chunk_data, encrypt_chunk};
pub use cipher::{transcrypt, transcrypt_in_place};
pub use key::EncryptionKey;
// Defined in the core crate because `PrimitivesError` wraps it.
pub use nectar_primitives_core::error::EncryptionError;
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

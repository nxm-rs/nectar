//! Chunk encryption using Keccak-256 counter-mode cipher.

mod cipher;
mod chunk;
mod error;
mod key;
mod reference;

pub use cipher::{transcrypt, transcrypt_in_place};
pub use chunk::{decrypt_chunk_data, decrypt_chunk_into};
#[cfg(feature = "encryption")]
pub use chunk::encrypt_chunk;
pub use error::EncryptionError;
pub use key::EncryptionKey;
pub use reference::EncryptedChunkRef;

/// Encryption key size in bytes.
pub const KEY_SIZE: usize = 32;
/// Encrypted reference size (address + decryption key).
pub const ENCRYPTED_REF_SIZE: usize = 64;

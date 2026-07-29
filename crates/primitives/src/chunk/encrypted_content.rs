//! The encryption layer over content chunks.
//!
//! Kept out of the verification core: a verifier certifies ciphertext chunks
//! by their own address and never needs the key or the cipher.

use bytes::Bytes;

use nectar_primitives_core::bmt::{DEFAULT_BODY_SIZE, SPAN_SIZE};
use nectar_primitives_core::cast;
use nectar_primitives_core::chunk::{ChunkOps, ContentChunk};
use nectar_primitives_core::error::Result;

use super::encryption::{
    ChunkEncrypt, EncryptedChunkRef, EncryptionKey, decrypt_chunk_data, encrypt_chunk, transcrypt,
};

/// Result of encrypting a content chunk.
#[derive(Debug, Clone)]
pub struct EncryptedContentChunk<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    chunk: ContentChunk<BODY_SIZE>,
    encrypted_ref: EncryptedChunkRef,
}

impl<const BODY_SIZE: usize> EncryptedContentChunk<BODY_SIZE> {
    /// The encrypted chunk (ciphertext hashed to a new address).
    pub const fn chunk(&self) -> &ContentChunk<BODY_SIZE> {
        &self.chunk
    }

    /// The encrypted reference (address + decryption key).
    pub const fn encrypted_ref(&self) -> &EncryptedChunkRef {
        &self.encrypted_ref
    }

    /// Consume and return (chunk, encrypted_ref).
    pub fn into_parts(self) -> (ContentChunk<BODY_SIZE>, EncryptedChunkRef) {
        (self.chunk, self.encrypted_ref)
    }

    /// Decrypt back to a plaintext `ContentChunk`.
    #[allow(clippy::indexing_slicing)] // a ContentChunk's wire bytes always start with an 8-byte span, so [..SPAN_SIZE] holds
    pub fn decrypt(&self) -> Result<ContentChunk<BODY_SIZE>> {
        let encrypted_data: Bytes = self.chunk.clone().into();
        let key = self.encrypted_ref.key();

        // Decrypt the span to learn the original data length
        // BODY_SIZE / 32 is 128 for the default 4096-byte body and stays far
        // below u32::MAX for any chunk-sized body.
        #[allow(clippy::as_conversions)]
        let span_ctr = (BODY_SIZE / EncryptionKey::SIZE) as u32;
        let mut span_buf = [0u8; SPAN_SIZE];
        transcrypt(key, span_ctr, &encrypted_data[..SPAN_SIZE], &mut span_buf)?;
        let data_length = cast::usize_from_u64(u64::from_le_bytes(span_buf));

        let decrypted = decrypt_chunk_data::<BODY_SIZE>(&encrypted_data, key, data_length)?;
        ContentChunk::try_from(Bytes::from(decrypted))
    }
}

impl<const BODY_SIZE: usize> ChunkEncrypt for ContentChunk<BODY_SIZE> {
    type Encrypted = EncryptedContentChunk<BODY_SIZE>;

    /// Encrypt this chunk with a caller-provided key.
    ///
    /// The returned `EncryptedContentChunk` contains:
    /// - `chunk`: a new `ContentChunk` whose data is the ciphertext
    /// - `encrypted_ref`: the 64-byte reference (new address + decryption key)
    ///
    /// ```
    /// # use nectar_primitives::{ChunkOps, ContentChunk};
    /// # use nectar_primitives::chunk::encryption::{ChunkEncrypt, EncryptionKey};
    /// # use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
    /// let chunk = ContentChunk::<DEFAULT_BODY_SIZE>::new(b"secret data".to_vec()).unwrap();
    /// let encrypted = chunk.encrypt().unwrap();
    ///
    /// // The encrypted chunk has a different address
    /// assert_ne!(chunk.address(), encrypted.chunk().address());
    /// ```
    fn encrypt_with(&self, key: &EncryptionKey) -> Result<EncryptedContentChunk<BODY_SIZE>> {
        let body = self.body();
        let ciphertext = encrypt_chunk::<BODY_SIZE>(&body.span_bytes(), body.data().as_ref(), key)?;
        let encrypted_chunk = Self::try_from(Bytes::from(ciphertext))?;
        let encrypted_ref = EncryptedChunkRef::new(*encrypted_chunk.address(), key.clone());
        Ok(EncryptedContentChunk {
            chunk: encrypted_chunk,
            encrypted_ref,
        })
    }
    // encrypt() uses the default impl: it generates a random key, then calls
    // encrypt_with().
}

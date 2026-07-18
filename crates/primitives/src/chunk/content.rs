//! Content-addressed chunk implementation
//!
//! This module provides the content-addressed chunk type: the [`ChunkInner`]
//! carrier under the empty [`CacHeader`], whose address is the hash of the
//! chunk's own body.

use alloy_primitives::{B256, hex};
use bytes::{Bytes, BytesMut};
use std::fmt;

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::error::Result;
use crate::wire;

use super::address::ChunkAddress;
use super::bmt_body::BmtBody;
use super::error::ChunkError;
use super::inner::ChunkInner;
use super::traits::{ChunkHeader, ChunkOps};
use super::type_id::ChunkTypeId;
use super::type_tag::ChunkVersion;

/// A content-addressed chunk (CAC) with configurable body size.
///
/// The [`ChunkInner`] carrier under a [`CacHeader`]: the address is the BMT
/// hash of the body, derived by the carrier and never caller-supplied.
pub type ContentChunk<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> =
    ChunkInner<CacHeader, BODY_SIZE>;

/// Header of a content-addressed chunk (CAC).
///
/// A CAC carries no wire header and commits to its own body hash; the empty
/// header is a type fact ([`SIZE`](ChunkHeader::SIZE) is 0), not a runtime
/// check.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacHeader;

impl ChunkHeader for CacHeader {
    const TYPE_ID: ChunkTypeId = ChunkTypeId::CONTENT;
    const VERSION: ChunkVersion = ChunkVersion::new(0);
    const NAME: &'static str = "content";
    const SIZE: usize = 0;

    /// The address is the BMT body hash itself.
    fn commit(&self, body_hash: B256) -> ChunkAddress {
        ChunkAddress::from(body_hash)
    }

    fn validate(
        &self,
        body_hash: B256,
        expected: &ChunkAddress,
    ) -> std::result::Result<(), ChunkError> {
        let actual = self.commit(body_hash);
        if actual != *expected {
            return Err(ChunkError::verification_failed(*expected, actual));
        }
        Ok(())
    }

    /// The transformed address is the anchor-keyed BMT root itself.
    fn seal_transformed(&self, _address: &ChunkAddress, transformed_root: B256) -> ChunkAddress {
        ChunkAddress::from(transformed_root)
    }

    fn encode(&self, _out: &mut BytesMut) {}

    fn decode(_cursor: &mut wire::Cursor<'_>) -> std::result::Result<Self, ChunkError> {
        Ok(Self)
    }
}

impl<const BODY_SIZE: usize> ContentChunk<BODY_SIZE> {
    /// Create a new content chunk with the given data.
    ///
    /// The span is calculated from the data length; the address is derived
    /// from the body on first use.
    ///
    /// # Errors
    ///
    /// Returns an error if `data` exceeds `BODY_SIZE`.
    #[must_use = "this returns a new chunk without modifying the input"]
    pub fn new(data: impl Into<Bytes>) -> Result<Self> {
        Ok(Self::from_body(
            BmtBody::builder().auto_from_data(data)?.build()?,
        ))
    }

    /// Create a ContentChunk from a pre-existing BmtBody.
    ///
    /// This is an advanced method for when you already have a BmtBody,
    /// such as when reconstructing chunks from storage or building
    /// intermediate nodes in a merkle tree.
    #[must_use]
    pub const fn from_body(body: BmtBody<BODY_SIZE>) -> Self {
        Self::from_header_and_body(CacHeader, body)
    }
}

/// Result of encrypting a content chunk.
#[cfg(feature = "encryption")]
#[derive(Debug, Clone)]
pub struct EncryptedContentChunk<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    chunk: ContentChunk<BODY_SIZE>,
    encrypted_ref: super::encryption::EncryptedChunkRef,
}

#[cfg(feature = "encryption")]
impl<const BODY_SIZE: usize> EncryptedContentChunk<BODY_SIZE> {
    /// The encrypted chunk (ciphertext hashed to a new address).
    pub const fn chunk(&self) -> &ContentChunk<BODY_SIZE> {
        &self.chunk
    }

    /// The encrypted reference (address + decryption key).
    pub const fn encrypted_ref(&self) -> &super::encryption::EncryptedChunkRef {
        &self.encrypted_ref
    }

    /// Consume and return (chunk, encrypted_ref).
    pub fn into_parts(
        self,
    ) -> (
        ContentChunk<BODY_SIZE>,
        super::encryption::EncryptedChunkRef,
    ) {
        (self.chunk, self.encrypted_ref)
    }

    /// Decrypt back to a plaintext `ContentChunk`.
    #[allow(clippy::indexing_slicing)] // a ContentChunk's wire bytes always start with an 8-byte span, so [..SPAN_SIZE] holds
    pub fn decrypt(&self) -> Result<ContentChunk<BODY_SIZE>> {
        use super::encryption::transcrypt;
        use crate::bmt::SPAN_SIZE;

        let encrypted_data: Bytes = self.chunk.clone().into();
        let key = self.encrypted_ref.key();

        // Decrypt the span to learn the original data length
        // BODY_SIZE / 32 is 128 for the default 4096-byte body and stays far
        // below u32::MAX for any chunk-sized body.
        #[allow(clippy::as_conversions)]
        let span_ctr = (BODY_SIZE / super::encryption::EncryptionKey::SIZE) as u32;
        let mut span_buf = [0u8; SPAN_SIZE];
        transcrypt(key, span_ctr, &encrypted_data[..SPAN_SIZE], &mut span_buf)?;
        let data_length = crate::cast::usize_from_u64(u64::from_le_bytes(span_buf));

        let decrypted =
            super::encryption::decrypt_chunk_data::<BODY_SIZE>(&encrypted_data, key, data_length)?;
        ContentChunk::try_from(Bytes::from(decrypted))
    }
}

#[cfg(feature = "encryption")]
impl<const BODY_SIZE: usize> super::encryption::ChunkEncrypt for ContentChunk<BODY_SIZE> {
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
    fn encrypt_with(
        &self,
        key: &super::encryption::EncryptionKey,
    ) -> Result<EncryptedContentChunk<BODY_SIZE>> {
        let raw: Bytes = self.clone().into(); // span || data
        let ciphertext = super::encryption::encrypt_chunk::<BODY_SIZE>(&raw, key)?;
        let encrypted_chunk = Self::try_from(Bytes::from(ciphertext))?;
        let encrypted_ref =
            super::encryption::EncryptedChunkRef::new(*encrypted_chunk.address(), key.clone());
        Ok(EncryptedContentChunk {
            chunk: encrypted_chunk,
            encrypted_ref,
        })
    }
    // encrypt() uses default impl — generates random key, calls encrypt_with()
}

impl<const BODY_SIZE: usize> fmt::Display for ContentChunk<BODY_SIZE> {
    #[allow(clippy::indexing_slicing)] // the address is a fixed 32-byte value, so [..8] holds
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ContentChunk[{}]",
            hex::encode(&self.address().as_bytes()[..8])
        )
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a, const BODY_SIZE: usize> arbitrary::Arbitrary<'a> for ContentChunk<BODY_SIZE> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        // Every content chunk is valid by construction: the address is the
        // BMT hash of the drawn body, so this impl serves both tiers.
        Ok(Self::from_body(BmtBody::<BODY_SIZE>::arbitrary(u)?))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        DEFAULT_BODY_SIZE,
        chunk::{ChunkOps, error::ChunkError},
        error::PrimitivesError,
    };

    use super::*;
    use alloy_primitives::b256;
    use proptest::prelude::*;
    use proptest_arbitrary_interop::arb;

    type DefaultContentChunk = ContentChunk<DEFAULT_BODY_SIZE>;

    // Strategy for generating ContentChunk using the Arbitrary implementation
    fn chunk_strategy() -> impl Strategy<Value = DefaultContentChunk> {
        arb::<DefaultContentChunk>()
    }

    proptest! {
        #[test]
        fn test_chunk_properties(chunk in chunk_strategy()) {
            // Test basic properties
            prop_assert!(chunk.data().len() <= DEFAULT_BODY_SIZE);
            prop_assert_eq!(chunk.size(), 8 + chunk.data().len());

            // Test round-trip conversion
            let bytes: Bytes = chunk.clone().into();
            let decoded = DefaultContentChunk::try_from(bytes).unwrap();
            prop_assert_eq!(chunk.address(), decoded.address());
            prop_assert_eq!(chunk.data(), decoded.data());
            prop_assert_eq!(chunk.span(), decoded.span());
        }

        #[test]
        fn test_from_body(chunk in chunk_strategy()) {
            // Test creating a chunk from an existing body via BmtBody
            let body_data = chunk.data().clone();
            let body_span = chunk.span();

            // Create a new chunk using from_body with a fresh BmtBody
            let new_body = BmtBody::<DEFAULT_BODY_SIZE>::try_from(Bytes::from(chunk.clone())).unwrap();
            let new_chunk = DefaultContentChunk::from_body(new_body);

            prop_assert_eq!(new_chunk.data(), &body_data);
            prop_assert_eq!(new_chunk.span(), body_span);
            prop_assert_eq!(new_chunk.address(), chunk.address());
        }

        #[test]
        fn test_new_content_chunk(data in proptest::collection::vec(any::<u8>(), 0..DEFAULT_BODY_SIZE)) {
            let chunk = DefaultContentChunk::new(data.clone()).unwrap();

            prop_assert_eq!(chunk.data(), &data);
            prop_assert_eq!(chunk.span(), data.len() as u64);
            prop_assert!(!chunk.address().is_zero());
        }

        #[test]
        fn test_chunk_size_validation(data in proptest::collection::vec(any::<u8>(), DEFAULT_BODY_SIZE + 1..DEFAULT_BODY_SIZE * 2)) {
            let result = DefaultContentChunk::new(data);
            prop_assert_eq!(matches!(result, Err(PrimitivesError::Chunk(ChunkError::InvalidSize { .. }))), true);
        }

        #[test]
        fn test_empty_and_edge_cases(size in 0usize..=10usize) {
            // Test with empty or small data
            let data = vec![0u8; size];
            let chunk = DefaultContentChunk::new(data).unwrap();

            prop_assert_eq!(chunk.data().len(), size);
            prop_assert_eq!(chunk.span(), size as u64);
            prop_assert_eq!(chunk.size(), 8 + size);
        }

        #[test]
        fn test_deserialize_invalid_chunks(data in proptest::collection::vec(any::<u8>(), 0..8)) {
            let result = DefaultContentChunk::try_from(data.as_slice());
            prop_assert_eq!(matches!(result, Err(PrimitivesError::Chunk(ChunkError::InvalidSize { .. }))), true);
        }
    }

    #[test]
    fn test_new() {
        let data = b"greaterthanspan";
        let bmt_hash = b256!("27913f1bdb6e8e52cbd5a5fd4ab577c857287edf6969b41efe926b51de0f4f23");

        let chunk = DefaultContentChunk::new(data.to_vec()).unwrap();
        assert_eq!(chunk.address().as_ref(), bmt_hash);
        assert_eq!(chunk.data(), data.as_slice());
    }

    #[test]
    fn test_from_bytes() {
        let data = b"greaterthanspan";
        let bmt_hash = b256!("95022e6af5c6d6a564ee55a67f8455a3e18c511b5697c932d9e44f07f2fb8c53");

        let chunk = DefaultContentChunk::try_from(data.as_slice()).unwrap();
        assert_eq!(chunk.address().as_ref(), bmt_hash);
        assert_eq!(
            <DefaultContentChunk as Into<Bytes>>::into(chunk),
            data.as_slice()
        );
    }

    #[test]
    fn test_specific_content_hash() {
        // Test with known valid data and hash
        let data = b"foo".to_vec();
        let expected_hash =
            b256!("2387e8e7d8a48c2a9339c97c1dc3461a9a7aa07e994c5cb8b38fd7c1b3e6ea48");

        let chunk = DefaultContentChunk::new(data).unwrap();
        assert_eq!(chunk.address().as_ref(), expected_hash);

        // Test with "Digital Freedom Now"
        let data = b"Digital Freedom Now".to_vec();
        let chunk = DefaultContentChunk::new(data).unwrap();
        assert!(chunk.address().as_ref() != ChunkAddress::default().as_ref()); // Ensure we get a non-zero hash
    }

    #[test]
    fn test_exact_span_size() {
        // Create a valid 8-byte span with no data
        let mut data = vec![0u8; 8];
        data.copy_from_slice(&0u64.to_le_bytes());

        let chunk = DefaultContentChunk::try_from(data.as_slice()).unwrap();

        assert_eq!(chunk.span(), 0);
        assert_eq!(chunk.data(), &[0u8; 0].as_slice());
        assert_eq!(chunk.size(), 8);
    }

    /// The commit rule is the body hash itself, pinned on a known vector.
    #[test]
    fn cac_header_commit_is_body_hash() {
        let chunk = DefaultContentChunk::new(b"foo".to_vec()).unwrap();
        let body_hash: B256 = chunk.body().hash().into();

        let expected = b256!("2387e8e7d8a48c2a9339c97c1dc3461a9a7aa07e994c5cb8b38fd7c1b3e6ea48");
        assert_eq!(CacHeader.commit(body_hash), ChunkAddress::from(expected));
        assert!(CacHeader.validate(body_hash, &expected.into()).is_ok());
    }

    #[test]
    fn cac_header_validate_rejects_wrong_address() {
        let body_hash = B256::repeat_byte(0x11);
        let wrong = ChunkAddress::from(B256::repeat_byte(0x22));
        assert!(matches!(
            CacHeader.validate(body_hash, &wrong),
            Err(ChunkError::VerificationFailed { .. })
        ));
    }

    /// The wire header is empty: encode writes nothing, decode consumes nothing.
    #[test]
    fn cac_header_wire_shape_is_empty() {
        let mut out = BytesMut::new();
        CacHeader.encode(&mut out);
        assert_eq!(out.len(), CacHeader::SIZE);
        assert!(out.is_empty());

        let data = [0xaau8; 4];
        let mut cursor = wire::Cursor::new(&data);
        let _ = CacHeader::decode(&mut cursor).unwrap();
        assert_eq!(cursor.remaining(), &data);
    }

    /// The transformed address is the anchor-keyed root itself; nothing is
    /// sealed over the chunk address.
    #[test]
    fn cac_header_seal_transformed_is_root() {
        let address = ChunkAddress::from(B256::repeat_byte(0x33));
        let root = B256::repeat_byte(0x44);
        assert_eq!(
            CacHeader.seal_transformed(&address, root),
            ChunkAddress::from(root)
        );
    }

    #[test]
    fn cac_header_constants() {
        assert_eq!(CacHeader::SIZE, 0);
        assert_eq!(CacHeader::TYPE_ID, ChunkTypeId::CONTENT);
        assert_eq!(CacHeader::VERSION, ChunkVersion::new(0));
        assert_eq!(CacHeader::NAME, "content");
    }
}

//! Content-addressed chunk implementation
//!
//! This module provides the implementation of content-addressed chunks,
//! which are chunks whose address is derived from the hash of their content.

use alloy_primitives::hex;
use bytes::Bytes;
use std::fmt;
use std::marker::PhantomData;

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::cache::OnceCache;
use crate::error::{PrimitivesError, Result};

use super::address::ChunkAddress;
use super::bmt_body::BmtBody;
use super::traits::{BmtChunk, Chunk, ChunkHeader};

/// A content-addressed chunk with configurable body size.
///
/// This type represents a chunk of data whose address is derived from the hash
/// of its contents. It is immutable once created.
#[derive(Debug, Clone)]
pub struct ContentChunk<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    /// The (empty) wire header
    header: ContentChunkHeader,
    /// The body of the chunk, containing the actual data
    body: BmtBody<BODY_SIZE>,
    /// Cache for the chunk's address
    address_cache: OnceCache<ChunkAddress>,
}

/// Header for a content-addressed chunk
///
/// Content chunks carry no wire header: the body (`span || payload`) is the
/// whole encoding.
#[derive(Debug, Clone, Default)]
pub struct ContentChunkHeader;

impl ContentChunkHeader {
    /// Create a new header
    pub const fn new() -> Self {
        Self
    }
}

impl ChunkHeader for ContentChunkHeader {
    fn bytes(&self) -> Bytes {
        Bytes::new()
    }
}

impl<const BODY_SIZE: usize> ContentChunk<BODY_SIZE> {
    /// Create a new content chunk with the given data.
    ///
    /// This function automatically calculates the span based on the data length.
    ///
    /// # Arguments
    ///
    /// * `data` - The raw data content to encapsulate in the chunk.
    ///
    /// # Returns
    ///
    /// A Result containing the new ContentChunk, or an error if creation fails.
    #[must_use = "this returns a new chunk without modifying the input"]
    pub fn new(data: impl Into<Bytes>) -> Result<Self> {
        Ok(ContentChunkBuilderImpl::<BODY_SIZE, _>::default()
            .auto_from_data(data)?
            .build())
    }

    /// Create a new ContentChunk with a pre-computed address.
    ///
    /// This function is useful when the address is already known, for example
    /// when retrieving a chunk from a database.
    ///
    /// # Arguments
    ///
    /// * `data` - The raw data content to encapsulate in the chunk.
    /// * `address` - The pre-computed address of the chunk.
    ///
    /// # Returns
    ///
    /// A Result containing the new ContentChunk, or an error if creation fails.
    #[must_use = "this returns a new chunk without modifying the input"]
    pub fn with_address(data: impl Into<Bytes>, address: ChunkAddress) -> Result<Self> {
        Ok(ContentChunkBuilderImpl::<BODY_SIZE, _>::default()
            .auto_from_data(data)?
            .with_address(address)
            .build())
    }

    /// Create a ContentChunk from a pre-existing BmtBody.
    ///
    /// This is an advanced method for when you already have a BmtBody,
    /// such as when reconstructing chunks from storage or building
    /// intermediate nodes in a merkle tree.
    #[must_use]
    pub const fn from_body(body: BmtBody<BODY_SIZE>) -> Self {
        Self {
            header: ContentChunkHeader::new(),
            body,
            address_cache: OnceCache::new(),
        }
    }

    /// Borrow the BMT body of this content chunk.
    ///
    /// The body carries the chunk's `span`, `payload`, and the `BODY_SIZE`
    /// const, so this is the zero-copy accessor callers use to feed the body
    /// into BMT operations (e.g. [`BmtBody::transformed_root`]) without
    /// re-slicing the span/payload back out of the wire form.
    pub const fn body(&self) -> &BmtBody<BODY_SIZE> {
        &self.body
    }

    /// Create a ContentChunk from a pre-existing BmtBody with a known address.
    ///
    /// This is an advanced method for when you already have both the body
    /// and know the chunk's address (e.g., when reconstructing from storage).
    #[must_use]
    pub fn from_body_with_address(body: BmtBody<BODY_SIZE>, address: ChunkAddress) -> Self {
        Self {
            header: ContentChunkHeader::new(),
            body,
            address_cache: OnceCache::with_value(address),
        }
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
    /// # use nectar_primitives::{Chunk, ContentChunk};
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

impl<const BODY_SIZE: usize> Chunk for ContentChunk<BODY_SIZE> {
    type Header = ContentChunkHeader;

    fn address(&self) -> &ChunkAddress {
        self.address_cache.get_or_compute(|| self.body.hash())
    }

    fn data(&self) -> &Bytes {
        self.body.data()
    }

    #[allow(clippy::arithmetic_side_effects)] // header (0 bytes) plus a body bounded by BODY_SIZE cannot overflow usize
    fn size(&self) -> usize {
        self.header().bytes().len() + self.body.size()
    }

    fn header(&self) -> &Self::Header {
        &self.header
    }
}

impl<const BODY_SIZE: usize> BmtChunk for ContentChunk<BODY_SIZE> {
    fn span(&self) -> u64 {
        self.body.span()
    }
}

impl<const BODY_SIZE: usize> From<ContentChunk<BODY_SIZE>> for Bytes {
    fn from(chunk: ContentChunk<BODY_SIZE>) -> Self {
        chunk.body.into()
    }
}

impl<const BODY_SIZE: usize> TryFrom<Bytes> for ContentChunk<BODY_SIZE> {
    type Error = PrimitivesError;

    fn try_from(bytes: Bytes) -> Result<Self> {
        Ok(Self {
            header: ContentChunkHeader::new(),
            body: BmtBody::try_from(bytes)?,
            address_cache: OnceCache::new(),
        })
    }
}

impl<const BODY_SIZE: usize> TryFrom<&[u8]> for ContentChunk<BODY_SIZE> {
    type Error = PrimitivesError;

    fn try_from(bytes: &[u8]) -> Result<Self> {
        Self::try_from(Bytes::copy_from_slice(bytes))
    }
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

impl<const BODY_SIZE: usize> PartialEq for ContentChunk<BODY_SIZE> {
    fn eq(&self, other: &Self) -> bool {
        self.address() == other.address()
    }
}

impl<const BODY_SIZE: usize> Eq for ContentChunk<BODY_SIZE> {}

impl<const BODY_SIZE: usize> super::chunk_type::ChunkType for ContentChunk<BODY_SIZE> {
    const TYPE_ID: super::type_id::ChunkTypeId = super::type_id::ChunkTypeId::CONTENT;
    const TYPE_NAME: &'static str = "content";
}

// Internal builder implementation
trait BuilderState {}

#[derive(Debug, Default)]
struct Initial;
impl BuilderState for Initial {}

#[derive(Debug)]
struct ReadyToBuild;
impl BuilderState for ReadyToBuild {}

/// Builder for ContentChunk with type state pattern
#[derive(Debug)]
struct ContentChunkBuilderImpl<const BODY_SIZE: usize, S: BuilderState = Initial> {
    /// The body to use for the chunk
    body: Option<BmtBody<BODY_SIZE>>,
    /// Pre-computed address for the chunk
    address: Option<ChunkAddress>,
    /// Marker for the builder state
    _state: PhantomData<S>,
}

impl<const BODY_SIZE: usize> Default for ContentChunkBuilderImpl<BODY_SIZE, Initial> {
    fn default() -> Self {
        Self {
            body: None,
            address: None,
            _state: PhantomData,
        }
    }
}

impl<const BODY_SIZE: usize> ContentChunkBuilderImpl<BODY_SIZE, Initial> {
    /// Initialize from data with automatically calculated span
    fn auto_from_data(
        mut self,
        data: impl Into<Bytes>,
    ) -> Result<ContentChunkBuilderImpl<BODY_SIZE, ReadyToBuild>> {
        let body = BmtBody::<BODY_SIZE>::builder()
            .auto_from_data(data)?
            .build()?;
        self.body = Some(body);

        Ok(ContentChunkBuilderImpl {
            body: self.body,
            address: self.address,
            _state: PhantomData,
        })
    }
}

impl<const BODY_SIZE: usize> ContentChunkBuilderImpl<BODY_SIZE, ReadyToBuild> {
    /// Set a pre-computed address for the chunk
    const fn with_address(mut self, address: ChunkAddress) -> Self {
        self.address = Some(address);
        self
    }

    /// Build the final ContentChunk
    #[allow(clippy::unwrap_used)] // the ReadyToBuild typestate guarantees body is Some
    fn build(self) -> ContentChunk<BODY_SIZE> {
        // This is safe as we have already checked that the body is set
        let body = self.body.unwrap();

        let address_cache = self
            .address
            .map_or_else(OnceCache::new, OnceCache::with_value);

        ContentChunk {
            header: ContentChunkHeader::new(),
            body,
            address_cache,
        }
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
    use crate::{DEFAULT_BODY_SIZE, chunk::error::ChunkError};

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
}

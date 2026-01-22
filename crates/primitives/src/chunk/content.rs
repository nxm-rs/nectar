//! Content-addressed chunk implementation
//!
//! This module provides the implementation of content-addressed chunks,
//! which are chunks whose address is derived from the hash of their content.

use alloy_primitives::hex;
use bytes::Bytes;
use std::fmt;
use std::marker::PhantomData;

use crate::cache::OnceCache;
use crate::error::{PrimitivesError, Result};

use super::bmt_body::BmtBody;
use super::traits::{BmtChunk, Chunk, ChunkAddress, ChunkHeader, ChunkMetadata};

/// A content-addressed chunk.
///
/// This type represents a chunk of data whose address is derived from the hash
/// of its contents. It is immutable once created.
#[derive(Debug, Clone)]
pub struct ContentChunk {
    /// The header containing type ID, version, and metadata
    header: ContentChunkHeader,
    /// The body of the chunk, containing the actual data
    body: BmtBody,
    /// Cache for the chunk's address
    address_cache: OnceCache<ChunkAddress>,
}

/// Metadata for a content-addressed chunk
///
/// Content chunks don't have any specific metadata, so this is empty.
#[derive(Debug, Clone)]
pub struct ContentChunkMetadata;

impl ChunkMetadata for ContentChunkMetadata {
    fn bytes(&self) -> Bytes {
        Bytes::new()
    }
}

/// Header for a content-addressed chunk
#[derive(Debug, Clone)]
pub struct ContentChunkHeader {
    metadata: ContentChunkMetadata,
}

impl ContentChunkHeader {
    /// Create a new header with default metadata
    pub fn new() -> Self {
        Self {
            metadata: ContentChunkMetadata,
        }
    }
}

impl Default for ContentChunkHeader {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkHeader for ContentChunkHeader {
    type Metadata = ContentChunkMetadata;

    fn id(&self) -> u8 {
        0
    }

    fn version(&self) -> u8 {
        1
    }

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    fn bytes(&self) -> Bytes {
        Bytes::new()
    }
}

impl ContentChunk {
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
        Ok(ContentChunkBuilderImpl::default()
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
        Ok(ContentChunkBuilderImpl::default()
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
    pub fn from_body(body: BmtBody) -> Self {
        ContentChunk {
            header: ContentChunkHeader::new(),
            body,
            address_cache: OnceCache::new(),
        }
    }

    /// Create a ContentChunk from a pre-existing BmtBody with a known address.
    ///
    /// This is an advanced method for when you already have both the body
    /// and know the chunk's address (e.g., when reconstructing from storage).
    #[must_use]
    pub fn from_body_with_address(body: BmtBody, address: ChunkAddress) -> Self {
        ContentChunk {
            header: ContentChunkHeader::new(),
            body,
            address_cache: OnceCache::with_value(address),
        }
    }
}

impl Chunk for ContentChunk {
    type Header = ContentChunkHeader;

    fn address(&self) -> &ChunkAddress {
        self.address_cache.get_or_compute(|| self.body.hash())
    }

    fn data(&self) -> &Bytes {
        self.body.data()
    }

    fn size(&self) -> usize {
        self.header().bytes().len() + self.body.size()
    }

    fn header(&self) -> &Self::Header {
        &self.header
    }
}

impl BmtChunk for ContentChunk {
    fn span(&self) -> u64 {
        self.body.span()
    }
}

impl From<ContentChunk> for Bytes {
    fn from(chunk: ContentChunk) -> Self {
        chunk.body.into()
    }
}

impl TryFrom<Bytes> for ContentChunk {
    type Error = PrimitivesError;

    fn try_from(bytes: Bytes) -> Result<Self> {
        Ok(Self {
            header: ContentChunkHeader::new(),
            body: BmtBody::try_from(bytes)?,
            address_cache: OnceCache::new(),
        })
    }
}

impl TryFrom<&[u8]> for ContentChunk {
    type Error = PrimitivesError;

    fn try_from(bytes: &[u8]) -> Result<Self> {
        Self::try_from(Bytes::copy_from_slice(bytes))
    }
}

impl fmt::Display for ContentChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ContentChunk[{}]",
            hex::encode(&self.address().as_bytes()[..8])
        )
    }
}

impl PartialEq for ContentChunk {
    fn eq(&self, other: &Self) -> bool {
        self.address() == other.address()
    }
}

impl Eq for ContentChunk {}

impl super::chunk_type::ChunkType for ContentChunk {
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
struct ContentChunkBuilderImpl<S: BuilderState = Initial> {
    /// The body to use for the chunk
    body: Option<BmtBody>,
    /// Pre-computed address for the chunk
    address: Option<ChunkAddress>,
    /// Marker for the builder state
    _state: PhantomData<S>,
}

impl Default for ContentChunkBuilderImpl<Initial> {
    fn default() -> Self {
        Self {
            body: None,
            address: None,
            _state: PhantomData,
        }
    }
}

impl ContentChunkBuilderImpl<Initial> {
    /// Initialize from data with automatically calculated span
    fn auto_from_data(
        mut self,
        data: impl Into<Bytes>,
    ) -> Result<ContentChunkBuilderImpl<ReadyToBuild>> {
        let body = BmtBody::builder().auto_from_data(data)?.build()?;
        self.body = Some(body);

        Ok(ContentChunkBuilderImpl {
            body: self.body,
            address: self.address,
            _state: PhantomData,
        })
    }
}

impl ContentChunkBuilderImpl<ReadyToBuild> {
    /// Set a pre-computed address for the chunk
    fn with_address(mut self, address: ChunkAddress) -> Self {
        self.address = Some(address);
        self
    }

    /// Build the final ContentChunk
    fn build(self) -> ContentChunk {
        // This is safe as we have already checked that the body is set
        let body = self.body.unwrap();

        let address_cache = match self.address {
            Some(addr) => OnceCache::with_value(addr),
            None => OnceCache::new(),
        };

        ContentChunk {
            header: ContentChunkHeader::new(),
            body,
            address_cache,
        }
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a> arbitrary::Arbitrary<'a> for ContentChunk {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(ContentChunk::from_body(BmtBody::arbitrary(u)?))
    }
}

#[cfg(test)]
mod tests {
    use crate::{MAX_CHUNK_SIZE, chunk::error::ChunkError};

    use super::*;
    use alloy_primitives::b256;
    use proptest::prelude::*;
    use proptest_arbitrary_interop::arb;

    // Strategy for generating ContentChunk using the Arbitrary implementation
    fn chunk_strategy() -> impl Strategy<Value = ContentChunk> {
        arb::<ContentChunk>()
    }

    proptest! {
        #[test]
        fn test_chunk_properties(chunk in chunk_strategy()) {
            // Test basic properties
            prop_assert!(chunk.span() <= u64::MAX);
            prop_assert!(chunk.data().len() <= MAX_CHUNK_SIZE);
            prop_assert_eq!(chunk.size(), 8 + chunk.data().len());

            // Test round-trip conversion
            let bytes: Bytes = chunk.clone().into();
            let decoded = ContentChunk::try_from(bytes).unwrap();
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
            let new_body = BmtBody::try_from(Bytes::from(chunk.clone())).unwrap();
            let new_chunk = ContentChunk::from_body(new_body);

            prop_assert_eq!(new_chunk.data(), &body_data);
            prop_assert_eq!(new_chunk.span(), body_span);
            prop_assert_eq!(new_chunk.address(), chunk.address());
        }

        #[test]
        fn test_new_content_chunk(data in proptest::collection::vec(any::<u8>(), 0..MAX_CHUNK_SIZE)) {
            let chunk = ContentChunk::new(data.clone()).unwrap();

            prop_assert_eq!(chunk.data(), &data);
            prop_assert_eq!(chunk.span(), data.len() as u64);
            prop_assert!(!chunk.address().is_zero());
        }

        #[test]
        fn test_chunk_size_validation(data in proptest::collection::vec(any::<u8>(), MAX_CHUNK_SIZE + 1..MAX_CHUNK_SIZE * 2)) {
            let result = ContentChunk::new(data);
            prop_assert_eq!(matches!(result, Err(PrimitivesError::Chunk(ChunkError::InvalidSize { .. }))), true);
        }

        #[test]
        fn test_empty_and_edge_cases(size in 0usize..=10usize) {
            // Test with empty or small data
            let data = vec![0u8; size];
            let chunk = ContentChunk::new(data.clone()).unwrap();

            prop_assert_eq!(chunk.data().len(), size);
            prop_assert_eq!(chunk.span(), size as u64);
            prop_assert_eq!(chunk.size(), 8 + size);
        }

        #[test]
        fn test_deserialize_invalid_chunks(data in proptest::collection::vec(any::<u8>(), 0..8)) {
            let result = ContentChunk::try_from(data.as_slice());
            prop_assert_eq!(matches!(result, Err(PrimitivesError::Chunk(ChunkError::InvalidSize { .. }))), true);
        }
    }

    #[test]
    fn test_new() {
        let data = b"greaterthanspan";
        let bmt_hash = b256!("27913f1bdb6e8e52cbd5a5fd4ab577c857287edf6969b41efe926b51de0f4f23");

        let chunk = ContentChunk::new(data.to_vec()).unwrap();
        assert_eq!(chunk.address().as_ref(), bmt_hash);
        assert_eq!(chunk.data(), data.as_slice());
    }

    #[test]
    fn test_from_bytes() {
        let data = b"greaterthanspan";
        let bmt_hash = b256!("95022e6af5c6d6a564ee55a67f8455a3e18c511b5697c932d9e44f07f2fb8c53");

        let chunk = ContentChunk::try_from(data.as_slice()).unwrap();
        assert_eq!(chunk.address().as_ref(), bmt_hash);
        assert_eq!(<ContentChunk as Into<Bytes>>::into(chunk), data.as_slice());
    }

    #[test]
    fn test_specific_content_hash() {
        // Test with known valid data and hash
        let data = b"foo".to_vec();
        let expected_hash =
            b256!("2387e8e7d8a48c2a9339c97c1dc3461a9a7aa07e994c5cb8b38fd7c1b3e6ea48");

        let chunk = ContentChunk::new(data).unwrap();
        assert_eq!(chunk.address().as_ref(), expected_hash);

        // Test with "Digital Freedom Now"
        let data = b"Digital Freedom Now".to_vec();
        let chunk = ContentChunk::new(data).unwrap();
        assert!(chunk.address().as_ref() != ChunkAddress::default().as_ref()); // Ensure we get a non-zero hash
    }

    #[test]
    fn test_exact_span_size() {
        // Create a valid 8-byte span with no data
        let mut data = vec![0u8; 8];
        data.copy_from_slice(&0u64.to_le_bytes());

        let chunk = ContentChunk::try_from(data.as_slice()).unwrap();

        assert_eq!(chunk.span(), 0);
        assert_eq!(chunk.data(), &[0u8; 0].as_slice());
        assert_eq!(chunk.size(), 8);
    }
}

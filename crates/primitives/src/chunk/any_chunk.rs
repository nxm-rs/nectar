//! Type-erased chunk type
//!
//! This module provides [`AnyChunk`], an enum that can hold any chunk type
//! for runtime polymorphism without requiring trait objects.

use bytes::Bytes;

use crate::error::Result;

use super::chunk_type::ChunkType;
use super::content::ContentChunk;
use super::single_owner::SingleOwnerChunk;
use super::traits::{Chunk, ChunkAddress};
use super::type_id::ChunkTypeId;

/// Type-erased chunk for runtime polymorphism.
///
/// This enum provides dynamic dispatch for chunks without requiring object-safe traits.
/// Use this when you need to store heterogeneous chunk types in collections or pass
/// chunks through interfaces that can't be generic.
///
/// # Why an enum instead of `Box<dyn Chunk>`?
///
/// The [`Chunk`] trait has an associated type (`type Header`) which makes it not
/// object-safe. This enum provides the same functionality while maintaining type safety.
///
/// # Examples
///
/// ```
/// use nectar_primitives::{AnyChunk, Chunk, ContentChunk, ChunkTypeId};
///
/// // Create a content chunk
/// let content = ContentChunk::new(&b"hello world"[..]).unwrap();
/// let any: AnyChunk = content.clone().into();
///
/// // Access common properties
/// assert_eq!(any.type_id(), ChunkTypeId::CONTENT);
///
/// // Get the concrete type back
/// if let Some(recovered) = any.as_content() {
///     assert_eq!(recovered.address(), content.address());
/// }
/// ```
#[derive(Debug, Clone)]
pub enum AnyChunk {
    /// A content-addressed chunk (CAC).
    Content(ContentChunk),
    /// A single-owner chunk (SOC).
    SingleOwner(SingleOwnerChunk),
    /// A custom chunk type (for extensibility).
    ///
    /// This variant allows storing chunks of types not known at compile time.
    /// The raw bytes are preserved for potential later processing.
    Custom {
        /// The chunk type identifier.
        type_id: ChunkTypeId,
        /// The chunk's address.
        address: ChunkAddress,
        /// The raw chunk data.
        data: Bytes,
    },
}

impl AnyChunk {
    /// Get the address of this chunk.
    pub fn address(&self) -> &ChunkAddress {
        match self {
            Self::Content(c) => c.address(),
            Self::SingleOwner(c) => c.address(),
            Self::Custom { address, .. } => address,
        }
    }

    /// Get the raw data contained in this chunk.
    pub fn data(&self) -> &Bytes {
        match self {
            Self::Content(c) => c.data(),
            Self::SingleOwner(c) => c.data(),
            Self::Custom { data, .. } => data,
        }
    }

    /// Get the type ID of this chunk.
    pub fn type_id(&self) -> ChunkTypeId {
        match self {
            Self::Content(_) => ChunkTypeId::CONTENT,
            Self::SingleOwner(_) => ChunkTypeId::SINGLE_OWNER,
            Self::Custom { type_id, .. } => *type_id,
        }
    }

    /// Get the total size of this chunk in bytes.
    pub fn size(&self) -> usize {
        match self {
            Self::Content(c) => c.size(),
            Self::SingleOwner(c) => c.size(),
            Self::Custom { data, .. } => data.len(),
        }
    }

    /// Get the span (logical data length) of this chunk.
    ///
    /// For content chunks and single-owner chunks, this returns the BMT span.
    /// For custom chunks, the span is not available (returns 0).
    pub fn span(&self) -> u64 {
        match self {
            Self::Content(c) => super::traits::BmtChunk::span(c),
            Self::SingleOwner(c) => super::traits::BmtChunk::span(c),
            Self::Custom { .. } => 0, // Custom chunks don't have span info
        }
    }

    /// Verify that this chunk's address matches an expected address.
    pub fn verify(&self, expected: &ChunkAddress) -> Result<()> {
        match self {
            Self::Content(c) => c.verify(expected),
            Self::SingleOwner(c) => c.verify(expected),
            Self::Custom { address, .. } => {
                if address != expected {
                    return Err(
                        super::error::ChunkError::verification_failed(*expected, *address).into(),
                    );
                }
                Ok(())
            }
        }
    }

    /// Convert this chunk into its serialized bytes representation.
    pub fn into_bytes(self) -> Bytes {
        match self {
            Self::Content(c) => c.into(),
            Self::SingleOwner(c) => c.into(),
            Self::Custom { data, .. } => data,
        }
    }

    /// Check if this chunk is of a specific type.
    pub fn is<T: ChunkType>(&self) -> bool {
        self.type_id() == T::TYPE_ID
    }

    /// Check if this is a content chunk.
    pub fn is_content(&self) -> bool {
        matches!(self, Self::Content(_))
    }

    /// Check if this is a single-owner chunk.
    pub fn is_single_owner(&self) -> bool {
        matches!(self, Self::SingleOwner(_))
    }

    /// Check if this is a custom chunk type.
    pub fn is_custom(&self) -> bool {
        matches!(self, Self::Custom { .. })
    }

    /// Get a reference to the contained ContentChunk, if this is one.
    pub fn as_content(&self) -> Option<&ContentChunk> {
        match self {
            Self::Content(c) => Some(c),
            _ => None,
        }
    }

    /// Get a reference to the contained SingleOwnerChunk, if this is one.
    pub fn as_single_owner(&self) -> Option<&SingleOwnerChunk> {
        match self {
            Self::SingleOwner(c) => Some(c),
            _ => None,
        }
    }

    /// Convert into the contained ContentChunk, if this is one.
    pub fn into_content(self) -> Option<ContentChunk> {
        match self {
            Self::Content(c) => Some(c),
            _ => None,
        }
    }

    /// Convert into the contained SingleOwnerChunk, if this is one.
    pub fn into_single_owner(self) -> Option<SingleOwnerChunk> {
        match self {
            Self::SingleOwner(c) => Some(c),
            _ => None,
        }
    }
}

impl From<ContentChunk> for AnyChunk {
    fn from(chunk: ContentChunk) -> Self {
        Self::Content(chunk)
    }
}

impl From<SingleOwnerChunk> for AnyChunk {
    fn from(chunk: SingleOwnerChunk) -> Self {
        Self::SingleOwner(chunk)
    }
}

#[cfg(test)]
mod tests {
    use super::super::traits::Chunk;
    use super::*;

    #[test]
    fn test_content_chunk_conversion() {
        let content = ContentChunk::new(&b"hello world"[..]).unwrap();
        let address = *content.address();

        let any: AnyChunk = content.into();

        assert!(any.is_content());
        assert!(!any.is_single_owner());
        assert!(!any.is_custom());
        assert_eq!(any.type_id(), ChunkTypeId::CONTENT);
        assert_eq!(*any.address(), address);
    }

    #[test]
    fn test_as_content() {
        let content = ContentChunk::new(&b"test data"[..]).unwrap();
        let expected_addr = *content.address();

        let any: AnyChunk = content.into();
        let recovered = any.as_content().unwrap();

        assert_eq!(*recovered.address(), expected_addr);
    }

    #[test]
    fn test_into_content() {
        let content = ContentChunk::new(&b"test data"[..]).unwrap();
        let expected_addr = *content.address();

        let any: AnyChunk = content.into();
        let recovered = any.into_content().unwrap();

        assert_eq!(*recovered.address(), expected_addr);
    }

    #[test]
    fn test_is_methods() {
        let content: AnyChunk = ContentChunk::new(&b"test"[..]).unwrap().into();

        assert!(content.is::<ContentChunk>());
        assert!(!content.is::<SingleOwnerChunk>());
    }

    #[test]
    fn test_clone() {
        let content = ContentChunk::new(&b"test"[..]).unwrap();
        let any: AnyChunk = content.clone().into();
        let cloned = any.clone();

        assert_eq!(any.address(), cloned.address());
        assert_eq!(any.type_id(), cloned.type_id());
    }
}

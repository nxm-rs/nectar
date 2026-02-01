//! Chunk type set trait
//!
//! This module provides the [`ChunkTypeSet`] trait for defining sets of supported
//! chunk types at compile time, and [`StandardChunkSet`] as the default implementation.

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;
use bytes::Bytes;

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::error::Result;

use super::any_chunk::AnyChunk;
use super::content::ContentChunk;
use super::error::ChunkError;
use super::single_owner::SingleOwnerChunk;
use super::type_id::ChunkTypeId;

/// Trait defining a set of supported chunk types with configurable body size.
///
/// This trait is implemented by marker types that define which chunk types
/// a system supports. It enables compile-time configuration of valid chunk types
/// while providing runtime polymorphism through [`AnyChunk`].
///
/// # Design Rationale
///
/// This trait uses associated functions (not methods) because the supported
/// types are determined at compile time, not per-instance. This allows:
///
/// - Zero-cost type checking at compile time
/// - Generic programming over chunk type sets
/// - Runtime dispatch only when necessary (deserialization)
///
/// # Example
///
/// ```ignore
/// use nectar_primitives::{ChunkTypeSet, ChunkTypeId, AnyChunk, StandardChunkSet};
///
/// // Check if a type is supported
/// assert!(StandardChunkSet::supports(ChunkTypeId::CONTENT));
/// assert!(StandardChunkSet::supports(ChunkTypeId::SINGLE_OWNER));
/// assert!(!StandardChunkSet::supports(ChunkTypeId::custom(200)));
///
/// // Get supported types
/// let types = StandardChunkSet::supported_types();
/// assert_eq!(types.len(), 2);
/// ```
pub trait ChunkTypeSet<const BODY_SIZE: usize = DEFAULT_BODY_SIZE>: Send + Sync + 'static {
    /// The chunk body size in bytes for this set.
    ///
    /// This is exposed as an associated const so consumers can access the body size
    /// at compile time through the type system.
    const BODY_SIZE: usize = BODY_SIZE;

    /// Check if a chunk type ID is supported by this set.
    ///
    /// Returns `true` if chunks with the given type ID can be
    /// deserialized and processed by this set.
    fn supports(type_id: ChunkTypeId) -> bool;

    /// Deserialize bytes into the appropriate chunk type.
    ///
    /// The first byte of the input should be the chunk type ID.
    /// Returns an error if the type is not supported or deserialization fails.
    ///
    /// # Errors
    ///
    /// Returns [`ChunkError::UnsupportedType`] if the type ID is not in this set.
    /// May return other errors from the underlying chunk deserialization.
    fn deserialize(bytes: &[u8]) -> Result<AnyChunk<BODY_SIZE>>;

    /// Get the list of all supported type IDs.
    ///
    /// This returns a static slice for efficiency in const contexts.
    fn supported_types() -> &'static [ChunkTypeId];

    /// Format the supported chunk types as a human-readable string.
    ///
    /// Returns a comma-separated list with abbreviations and hex codes,
    /// e.g., "CAC (0x00), SOC (0x01)".
    ///
    /// # Example
    ///
    /// ```
    /// use nectar_primitives::{ChunkTypeSet, StandardChunkSet, DEFAULT_BODY_SIZE};
    ///
    /// let formatted = <StandardChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::format_supported_types();
    /// assert!(formatted.contains("CAC"));
    /// assert!(formatted.contains("SOC"));
    /// ```
    fn format_supported_types() -> String {
        let types = Self::supported_types();
        let names: Vec<_> = types
            .iter()
            .map(|t| {
                let abbrev = t.abbreviation().unwrap_or("???");
                alloc::format!("{} (0x{:02x})", abbrev, t.as_u8())
            })
            .collect();
        names.join(", ")
    }
}

/// Standard Swarm chunk type set.
///
/// This set includes the two fundamental chunk types in the Swarm network:
/// - Content-addressed chunks (CAC) - [`ChunkTypeId::CONTENT`]
/// - Single-owner chunks (SOC) - [`ChunkTypeId::SINGLE_OWNER`]
///
/// This is the default chunk set used by most Swarm nodes.
///
/// # Example
///
/// ```ignore
/// use nectar_primitives::{ChunkTypeSet, ChunkTypeId, StandardChunkSet};
///
/// // Check support
/// assert!(StandardChunkSet::supports(ChunkTypeId::CONTENT));
/// assert!(StandardChunkSet::supports(ChunkTypeId::SINGLE_OWNER));
///
/// // Deserialize a chunk
/// let bytes: &[u8] = /* serialized chunk bytes */;
/// let chunk = StandardChunkSet::deserialize(bytes)?;
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct StandardChunkSet;

impl<const BODY_SIZE: usize> ChunkTypeSet<BODY_SIZE> for StandardChunkSet {
    fn supports(type_id: ChunkTypeId) -> bool {
        matches!(type_id, ChunkTypeId::CONTENT | ChunkTypeId::SINGLE_OWNER)
    }

    fn deserialize(bytes: &[u8]) -> Result<AnyChunk<BODY_SIZE>> {
        if bytes.is_empty() {
            return Err(ChunkError::invalid_format("empty chunk data").into());
        }

        // Note: For CAC/SOC, the type ID is in the header, but for raw chunk data
        // coming off the wire, we typically don't have the header prefix.
        // The actual deserialization happens based on the chunk structure.
        //
        // For CAC: just BMT body (span + data)
        // For SOC: id + signature + BMT body
        //
        // We'll try ContentChunk first (simpler structure), then SingleOwnerChunk.
        // This is a heuristic - in practice, callers should know the expected type.

        // Try as ContentChunk first
        if let Ok(content) = ContentChunk::<BODY_SIZE>::try_from(Bytes::copy_from_slice(bytes)) {
            return Ok(AnyChunk::Content(content));
        }

        // Try as SingleOwnerChunk
        if let Ok(soc) = SingleOwnerChunk::<BODY_SIZE>::try_from(Bytes::copy_from_slice(bytes)) {
            return Ok(AnyChunk::SingleOwner(soc));
        }

        // If neither worked, it's an invalid format
        Err(ChunkError::invalid_format("could not deserialize as any supported chunk type").into())
    }

    fn supported_types() -> &'static [ChunkTypeId] {
        &[ChunkTypeId::CONTENT, ChunkTypeId::SINGLE_OWNER]
    }
}

/// A chunk type set that accepts only content-addressed chunks.
///
/// This is useful for systems that only need to handle immutable content.
#[derive(Debug, Clone, Copy, Default)]
pub struct ContentOnlyChunkSet;

impl<const BODY_SIZE: usize> ChunkTypeSet<BODY_SIZE> for ContentOnlyChunkSet {
    fn supports(type_id: ChunkTypeId) -> bool {
        type_id == ChunkTypeId::CONTENT
    }

    fn deserialize(bytes: &[u8]) -> Result<AnyChunk<BODY_SIZE>> {
        if bytes.is_empty() {
            return Err(ChunkError::invalid_format("empty chunk data").into());
        }

        ContentChunk::<BODY_SIZE>::try_from(Bytes::copy_from_slice(bytes)).map(AnyChunk::Content)
    }

    fn supported_types() -> &'static [ChunkTypeId] {
        &[ChunkTypeId::CONTENT]
    }
}

#[cfg(test)]
mod tests {
    use super::super::traits::Chunk;
    use super::*;

    type DefaultContentChunk = ContentChunk<DEFAULT_BODY_SIZE>;

    #[test]
    fn test_standard_chunk_set_supports() {
        assert!(
            <StandardChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::supports(ChunkTypeId::CONTENT)
        );
        assert!(
            <StandardChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::supports(
                ChunkTypeId::SINGLE_OWNER
            )
        );
        assert!(
            !<StandardChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::supports(ChunkTypeId::custom(
                100
            ))
        );
        assert!(
            !<StandardChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::supports(ChunkTypeId::new(50))
        );
    }

    #[test]
    fn test_standard_chunk_set_supported_types() {
        let types = <StandardChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::supported_types();
        assert_eq!(types.len(), 2);
        assert!(types.contains(&ChunkTypeId::CONTENT));
        assert!(types.contains(&ChunkTypeId::SINGLE_OWNER));
    }

    #[test]
    fn test_format_supported_types() {
        let formatted =
            <StandardChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::format_supported_types();
        assert_eq!(formatted, "CAC (0x00), SOC (0x01)");

        let content_only =
            <ContentOnlyChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::format_supported_types();
        assert_eq!(content_only, "CAC (0x00)");
    }

    #[test]
    fn test_content_only_chunk_set_supports() {
        assert!(
            <ContentOnlyChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::supports(
                ChunkTypeId::CONTENT
            )
        );
        assert!(
            !<ContentOnlyChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::supports(
                ChunkTypeId::SINGLE_OWNER
            )
        );
        assert!(
            !<ContentOnlyChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::supports(
                ChunkTypeId::custom(100)
            )
        );
    }

    #[test]
    fn test_deserialize_content_chunk() {
        // Create a content chunk and serialize it
        let content = DefaultContentChunk::new(&b"hello world"[..]).unwrap();
        let bytes: Bytes = content.clone().into();

        // Deserialize through StandardChunkSet
        let any_chunk =
            <StandardChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::deserialize(&bytes).unwrap();

        assert!(any_chunk.is_content());
        assert_eq!(*any_chunk.address(), *content.address());
    }

    #[test]
    fn test_deserialize_empty_bytes_fails() {
        let result = <StandardChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::deserialize(&[]);
        assert!(result.is_err());
    }
}

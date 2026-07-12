//! Chunk type trait
//!
//! This module provides the [`ChunkType`] trait which adds compile-time type
//! information to chunk implementations.

use super::traits::HeaderedChunk;
use super::type_id::ChunkTypeId;

/// Trait for chunk types with compile-time type information.
///
/// This trait extends [`HeaderedChunk`] with static type metadata, enabling:
/// - Compile-time type identification via [`TYPE_ID`](ChunkType::TYPE_ID)
/// - Type-safe serialization/deserialization
/// - Generic programming over chunk types
///
/// # Implementing ChunkType
///
/// All implementations must also implement:
/// - [`HeaderedChunk`] trait
/// - [`TryFrom<Bytes>`] for deserialization
/// - [`Into<Bytes>`] for serialization
///
/// # Example
///
/// ```ignore
/// use nectar_primitives::{ChunkType, ChunkTypeId, HeaderedChunk};
///
/// struct MyCustomChunk { /* ... */ }
///
/// impl ChunkType for MyCustomChunk {
///     const TYPE_ID: ChunkTypeId = ChunkTypeId::custom(200);
///     const TYPE_NAME: &'static str = "my_custom";
/// }
/// ```
pub trait ChunkType: HeaderedChunk + Sized {
    /// The wire-level type identifier for this chunk type.
    ///
    /// This ID is used in chunk headers for serialization and must be unique
    /// across all chunk types in a system.
    const TYPE_ID: ChunkTypeId;

    /// Human-readable name for this chunk type.
    ///
    /// Used for logging, debugging, and error messages.
    const TYPE_NAME: &'static str;
}

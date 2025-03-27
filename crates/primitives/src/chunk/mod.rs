//! Chunk types and operations
//!
//! This module provides implementations of various chunk types used in the storage system,
//! along with functionality for creating, parsing, and verifying chunks.

mod bmt_body;
mod content;
pub(crate) mod error;
mod single_owner;
mod traits;

// Re-export the traits
pub use traits::{BmtChunk, Chunk, ChunkAddress, ChunkHeader, ChunkMetadata, ChunkSerialization};

// Re-export the concrete chunk types
pub use content::{ContentChunk, ContentChunkBuilder, ContentChunkBuilderReady};
pub use single_owner::{
    SingleOwnerChunk, SingleOwnerChunkBuilder, SingleOwnerChunkBuilderReady,
    SingleOwnerChunkBuilderWithData, SingleOwnerChunkBuilderWithId,
};

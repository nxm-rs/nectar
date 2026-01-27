//! Chunk types and operations
//!
//! This module provides implementations of various chunk types used in the storage system,
//! along with functionality for creating, parsing, and verifying chunks.
//!
//! # Chunk Type System
//!
//! The chunk system is built around a hierarchy of traits:
//!
//! - [`Chunk`] - Core trait for all chunk types
//! - [`ChunkType`] - Adds compile-time type identification
//! - [`ChunkTypeSet`] - Defines which chunk types a system supports
//!
//! # Type-Erased Chunks
//!
//! The [`AnyChunk`] enum provides runtime polymorphism for chunks without
//! requiring object-safe traits.

mod any_chunk;
mod bmt_body;
mod chunk_type;
mod chunk_type_set;
mod content;
pub(crate) mod error;
mod single_owner;
mod traits;
mod type_id;

// Re-export the core traits
pub use traits::{BmtChunk, Chunk, ChunkAddress, ChunkHeader, ChunkMetadata, ChunkSerialization};

// Re-export the type system
pub use any_chunk::AnyChunk;
pub use chunk_type::ChunkType;
pub use chunk_type_set::{ChunkTypeSet, ContentOnlyChunkSet, StandardChunkSet};
pub use type_id::ChunkTypeId;

// Re-export the concrete chunk types
pub use content::ContentChunk;
pub use single_owner::SingleOwnerChunk;

//! Chunk types and operations
//!
//! This module provides implementations of various chunk types used in the storage system,
//! along with functionality for creating, parsing, and verifying chunks.
//!
//! # Chunk Type System
//!
//! The chunk system is built around a hierarchy of traits:
//!
//! - [`ChunkHeader`] - Address-derivation and self-certification predicate of
//!   a chunk type ([`CacHeader`], [`SocHeader`])
//! - [`ChunkInner`] - The single carrier: one header plus one BMT body;
//!   [`ContentChunk`] and [`SingleOwnerChunk`] are its aliases
//! - [`ChunkOps`] - Header-free behaviour shared by concrete chunks and
//!   [`AnyChunk`]
//! - [`Chunk`] - Ties a carrier to its header type
//! - [`ChunkType`] - Adds compile-time type identification
//! - [`ChunkTypeSet`] - Defines which chunk types a system supports
//!
//! # Type-Erased Chunks
//!
//! The [`AnyChunk`] enum provides runtime polymorphism for chunks without
//! requiring object-safe traits.

mod address;
mod any_chunk;
mod bmt_body;
mod chunk_type;
mod chunk_type_set;
mod content;
pub mod encryption;
pub(crate) mod error;
mod inner;
mod reference;
mod single_owner;
mod soc_id;
mod traits;
mod type_id;
mod type_tag;

#[cfg(target_arch = "wasm32")]
pub mod wasm;

// Re-export the address type, error type, and core traits
pub use address::ChunkAddress;
pub use error::ChunkError;
pub use inner::ChunkInner;
pub use traits::{Chunk, ChunkHeader, ChunkOps};

// Re-export the reference types
pub use reference::{ChunkRef, RefKind, Reference, WrongRefKind};

// Re-export the type system
pub use any_chunk::AnyChunk;
pub use chunk_type::ChunkType;
pub use chunk_type_set::{ChunkTypeSet, ContentOnlyChunkSet, StandardChunkSet};
pub use type_id::ChunkTypeId;
pub use type_tag::{ChunkTypeTag, ChunkVersion, TagWireError};

// Re-export the concrete chunk types and their headers
#[cfg(feature = "encryption")]
pub use content::EncryptedContentChunk;
pub use content::{CacHeader, ContentChunk};
#[cfg(feature = "encryption")]
pub use encryption::ChunkEncrypt;
pub use single_owner::{SingleOwnerChunk, SocHeader};
pub use soc_id::SocId;

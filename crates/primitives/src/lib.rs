//! Core primitives for a decentralized storage system
//!
//! This crate provides the fundamental types and operations used in a decentralized
//! storage system, including chunk types, address calculations, and binary merkle trees.
//!
//! ## Key Components
//!
//! - **Chunks**: Content-addressed and signed data chunks ([`ContentChunk`], [`SingleOwnerChunk`])
//! - **Binary Merkle Tree**: Efficient content addressing and proof generation ([`bmt::Hasher`])
//! - **SwarmAddress**: 256-bit identifiers for network addressing
//!
//! ## Usage Examples
//!
//! ```
//! use nectar_primitives::{Chunk, ContentChunk, SingleOwnerChunk, SwarmAddress};
//! use alloy_signer_local::LocalSigner;
//! use alloy_primitives::FixedBytes;
//!
//! // Creating content chunks
//! let chunk = ContentChunk::new(b"Hello, world!".as_slice()).unwrap();
//! let address = chunk.address();
//!
//! // Creating content chunks with pre-computed address (e.g., from storage)
//! let address_copy = *address;
//! let chunk2 = ContentChunk::with_address(b"Hello, world!".as_slice(), address_copy).unwrap();
//!
//! // Creating signed chunks
//! let wallet = LocalSigner::random();
//! let id = FixedBytes::random();
//! let owner_chunk = SingleOwnerChunk::new(id, b"Signed data".as_slice(), &wallet).unwrap();
//! ```

// Re-export dependencies that are part of our public API
pub use bytes;

pub mod address;
pub mod bmt;
mod cache;
pub mod chunk;
pub mod error;

// Re-export core constants
pub use bmt::MAX_DATA_LENGTH as MAX_CHUNK_SIZE;

// Re-export core types
pub use address::{SwarmAddress, MAX_PO};
pub use error::{PrimitivesError, Result};

// Core BMT functionality
pub use bmt::{Hasher, HasherFactory, Proof, Prover};

// Core chunk functionality
pub use chunk::{
    // Type system
    AnyChunk,
    // Core traits
    BmtChunk,
    Chunk,
    ChunkAddress,
    ChunkSerialization,
    ChunkType,
    ChunkTypeId,
    ChunkTypeSet,
    // Concrete chunk types
    ContentChunk,
    ContentOnlyChunkSet,
    SingleOwnerChunk,
    StandardChunkSet,
};

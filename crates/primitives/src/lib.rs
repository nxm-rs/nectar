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
//! use nectar_primitives::{Chunk, DefaultContentChunk, DefaultSingleOwnerChunk};
//! use alloy_signer_local::LocalSigner;
//! use alloy_primitives::FixedBytes;
//!
//! // Creating content chunks
//! let chunk = DefaultContentChunk::new(b"Hello, world!".as_slice()).unwrap();
//! let address = chunk.address();
//!
//! // Creating content chunks with pre-computed address (e.g., from storage)
//! let address_copy = *address;
//! let chunk2 = DefaultContentChunk::with_address(b"Hello, world!".as_slice(), address_copy).unwrap();
//!
//! // Creating signed chunks
//! let wallet = LocalSigner::random();
//! let id = FixedBytes::random();
//! let owner_chunk = DefaultSingleOwnerChunk::new(id, b"Signed data".as_slice(), &wallet).unwrap();
//! ```

// Re-export dependencies that are part of our public API
pub use bytes;

pub mod address;
pub mod bmt;
mod cache;
pub mod chunk;
pub mod error;
pub mod file;
pub mod store;

#[cfg(target_arch = "wasm32")]
pub mod wasm;

// Re-export core constants
pub use bmt::DEFAULT_BODY_SIZE;

// Re-export core encryption types
pub use chunk::encryption::{ChunkRef, EncryptedChunkRef, EncryptionKey};

// Re-export core types
pub use address::{MAX_PO, SwarmAddress};
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

/// Default BMT hasher.
pub type DefaultHasher = Hasher<DEFAULT_BODY_SIZE>;
/// Default content-addressed chunk.
pub type DefaultContentChunk = ContentChunk<DEFAULT_BODY_SIZE>;
/// Default single-owner chunk.
pub type DefaultSingleOwnerChunk = SingleOwnerChunk<DEFAULT_BODY_SIZE>;
/// Default polymorphic chunk.
pub type DefaultAnyChunk = AnyChunk<DEFAULT_BODY_SIZE>;

// Chunk storage (raw + typed)
pub use store::{
    ChunkGet, ChunkGetter, ChunkHas, ChunkPut, ChunkPutter, ChunkStore, ChunkStoreError,
    MemorySink, MockChunkStore, VecSink,
};
#[cfg(feature = "async")]
pub use store::{AsyncChunkGet, AsyncChunkGetter, AsyncChunkPut, AsyncChunkPutter, AsyncChunkStore};

// File operations (algorithms only)
pub use file::{
    ChunkGetExt, ChunkPutExt, ChunkRange, EncryptedJoiner, FileError, Joiner, ParallelJoiner,
    ParallelSplitter, ReadAt, SplitBuilder, Splitter, TreeParams, join, join_encrypted, split,
    split_reader,
};
#[cfg(feature = "encryption")]
pub use file::{EncryptedSplitter, split_encrypted};
#[cfg(feature = "async")]
pub use file::{AsyncJoiner, AsyncReadAt};

/// Default file splitter.
pub type DefaultSplitter<S> = file::Splitter<S, DEFAULT_BODY_SIZE>;
/// Default file joiner.
pub type DefaultJoiner<G> = file::Joiner<G, DEFAULT_BODY_SIZE>;
/// Default encrypted file splitter.
#[cfg(feature = "encryption")]
pub type DefaultEncryptedSplitter<S> = file::EncryptedSplitter<S, DEFAULT_BODY_SIZE>;
/// Default encrypted file joiner.
pub type DefaultEncryptedJoiner<G> = file::EncryptedJoiner<G, DEFAULT_BODY_SIZE>;

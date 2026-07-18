//! Core primitives for a decentralized storage system
//!
//! This crate provides the fundamental types and operations used in a decentralized
//! storage system, including chunk types, address calculations, and binary merkle trees.
//!
//! ## Key Components
//!
//! - **Chunks**: Content-addressed and signed data chunks ([`ContentChunk`], [`SingleOwnerChunk`])
//! - **Binary Merkle Tree**: Efficient content addressing and proof generation ([`bmt::Hasher`])
//! - **OverlayAddress**: 256-bit identifiers for network addressing
//!
//! ## Usage Examples
//!
//! ```
//! use nectar_primitives::{Chunk, DefaultContentChunk, DefaultSingleOwnerChunk, SocId};
//! use alloy_signer_local::LocalSigner;
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
//! let id = SocId::random();
//! let owner_chunk = DefaultSingleOwnerChunk::new(id, b"Signed data".as_slice(), &wallet).unwrap();
//! ```

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::get_unwrap,
        clippy::indexing_slicing,
        clippy::string_slice,
        clippy::arithmetic_side_effects,
        clippy::panic,
        clippy::unreachable,
        clippy::panic_in_result_fn,
        clippy::as_conversions
    )
)]

// Re-export dependencies that are part of our public API
pub use bytes;

pub mod address;
pub mod bin;
pub mod bmt;
mod cache;
mod cast;
pub mod chunk;
pub mod error;
pub mod file;
#[cfg(any(test, feature = "arbitrary"))]
pub mod generators;
pub mod neighborhood_depth;
pub mod network_id;
pub mod nonce;
pub mod overlay;
pub mod proximity_order;
pub mod signing;
pub mod spec;
pub mod store;
pub mod timestamp;
pub mod wire;
pub mod xor_metric;

#[cfg(target_arch = "wasm32")]
pub mod wasm;

// Re-export core constants
pub use bmt::DEFAULT_BODY_SIZE;

// Re-export core encryption types
pub use chunk::encryption::{EncryptedChunkRef, EncryptionKey};
#[cfg(feature = "encryption")]
pub use chunk::{ChunkEncrypt, EncryptedContentChunk};

// Re-export core types
pub use address::OverlayAddress;
pub use bin::{Bin, BinError};
pub use error::{PrimitivesError, Result, WrongLength};
pub use neighborhood_depth::recompute_neighborhood_depth;
pub use network_id::NetworkId;
pub use nonce::Nonce;
pub use overlay::compute_overlay;
pub use proximity_order::{ProximityOrder, ProximityOrderError};
pub use spec::{MAINNET, StaticSpec, SwarmSpec, TESTNET};
pub use timestamp::{Timestamp, TimestampError};
pub use xor_metric::{EXTENDED_PO, MAX_PO, XorMetric};

/// Former name of the node-identity address kind.
#[deprecated(note = "use `OverlayAddress`; this alias is removed in the next release")]
pub type SwarmAddress = OverlayAddress;

// Core BMT functionality
pub use bmt::{Hasher, HasherFactory, Proof, Prover};

// Core chunk functionality
pub use chunk::{
    // Type system
    AnyChunk,
    // Core traits
    BmtChunk,
    CacHeader,
    Chunk,
    ChunkAddress,
    ChunkError,
    ChunkHeader,
    // Concrete chunk types
    ChunkRef,
    ChunkType,
    ChunkTypeId,
    ChunkTypeSet,
    ChunkTypeTag,
    ChunkVersion,
    ContentChunk,
    ContentOnlyChunkSet,
    RefKind,
    Reference,
    SingleOwnerChunk,
    SocHeader,
    SocId,
    StandardChunkSet,
    TagWireError,
    WrongRefKind,
};

/// Default BMT hasher.
pub type DefaultHasher = Hasher<DEFAULT_BODY_SIZE>;
/// Default content-addressed chunk.
pub type DefaultContentChunk = ContentChunk<DEFAULT_BODY_SIZE>;
/// Default single-owner chunk.
pub type DefaultSingleOwnerChunk = SingleOwnerChunk<DEFAULT_BODY_SIZE>;
/// Default polymorphic chunk.
pub type DefaultAnyChunk = AnyChunk<DEFAULT_BODY_SIZE>;
/// Default in-memory chunk store.
pub type DefaultMemoryStore = MemoryStore<DEFAULT_BODY_SIZE>;

// Chunk storage traits
pub use store::{
    ChunkGet, ChunkHas, ChunkPut, ChunkStoreError, MemoryStore, RetryConfig, RetryingChunkGet,
    Sleeper,
};

// File joining (async)
#[cfg(feature = "encryption")]
pub use file::EncryptedJoiner;
#[cfg(feature = "tokio")]
pub use file::JoinerReader;
pub use file::{
    ChunkGetExt, ChunkPutExt, ChunkRange, EntryRef, FileError, GenericJoiner, JoinRef, Joiner,
    TreeParams, join,
};

// File splitting (CPU-bound, rayon)
#[cfg(feature = "encryption")]
pub use file::{EncryptedParallelSplitter, EncryptedSplitter, split_encrypted};
pub use file::{ParallelSplitter, ReadAt, Splitter, split};

/// Default sync file splitter.
pub type DefaultSplitter = file::Splitter<DEFAULT_BODY_SIZE>;
/// Default async file joiner.
pub type DefaultJoiner<G> = file::Joiner<G, DEFAULT_BODY_SIZE>;

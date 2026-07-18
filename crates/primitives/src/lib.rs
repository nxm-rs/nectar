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
//! use nectar_primitives::{ChunkOps, DefaultContentChunk, DefaultSingleOwnerChunk, SocId};
//! use alloy_signer_local::LocalSigner;
//!
//! // Creating content chunks; the address is derived from the content
//! let chunk = DefaultContentChunk::new(b"Hello, world!".as_slice()).unwrap();
//! let address = *chunk.address();
//!
//! // Reconstructing a chunk (e.g. from storage) and certifying it
//! let chunk2 = DefaultContentChunk::new(b"Hello, world!".as_slice()).unwrap();
//! chunk2.verify(&address).unwrap();
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
// The synthesized test harness references every `#[test]` inside the
// deprecated legacy file module, so the warning would fire on each of them.
#![cfg_attr(test, allow(deprecated))]

extern crate alloc;

// Re-export dependencies that are part of our public API
pub use bytes;

pub mod address;
pub mod bin;
pub mod bmt;
mod cache;
mod cast;
pub mod chunk;
pub mod entry_ref;
pub mod error;
#[deprecated(note = "superseded by the nectar-file streaming pipeline")]
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
pub use chunk::encryption::{EncryptedChunkRef, EncryptionKey, transcrypt, transcrypt_in_place};
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
    AnyChunkSet,
    CacHeader,
    // The typestate chunk currency
    Chunk,
    ChunkAddress,
    ChunkError,
    // Core traits
    ChunkHeader,
    // Concrete chunk types
    ChunkInner,
    ChunkOps,
    ChunkRef,
    ChunkRegistry,
    ChunkType,
    ChunkTypeId,
    ChunkTypeInfo,
    ChunkTypeTag,
    ChunkVersion,
    ContentChunk,
    ContentOnlyChunkSet,
    HeaderedChunk,
    IntoVerified,
    RefKind,
    Reference,
    SingleOwnerChunk,
    SocHeader,
    SocId,
    StandardChunkSet,
    TagWireError,
    TrustState,
    TrustedSource,
    Unverified,
    Verified,
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
pub type DefaultMemoryStore = MemoryStore<StandardChunkSet>;

// Chunk storage traits
pub use store::{
    ChunkGet, ChunkHas, ChunkPut, ChunkStoreError, MemoryStore, RetryConfig, RetryingChunkGet,
    Sleeper, TrustedGet,
};

// The width-agnostic reference union: the manifest-to-file bridge type.
pub use entry_ref::EntryRef;

// File joining (async)
#[allow(deprecated)]
#[cfg(feature = "encryption")]
pub use file::EncryptedJoiner;
#[allow(deprecated)]
#[cfg(feature = "tokio")]
pub use file::JoinerReader;
#[allow(deprecated)]
pub use file::{
    ChunkGetExt, ChunkPutExt, ChunkRange, FileError, GenericJoiner, JoinRef, Joiner, TreeParams,
    join,
};

// File splitting (CPU-bound, rayon)
#[allow(deprecated)]
#[cfg(feature = "encryption")]
pub use file::{EncryptedParallelSplitter, EncryptedSplitter, split_encrypted};
#[allow(deprecated)]
pub use file::{ParallelSplitter, ReadAt, Splitter, split};

/// Default sync file splitter.
#[allow(deprecated)]
pub type DefaultSplitter = file::Splitter<DEFAULT_BODY_SIZE>;
/// Default async file joiner.
#[allow(deprecated)]
pub type DefaultJoiner<G> = file::Joiner<G, DEFAULT_BODY_SIZE>;

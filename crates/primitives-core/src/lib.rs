//! Chunk verification core for a decentralized storage system.
//!
//! The transitive closure needed to certify a chunk, and nothing else: the
//! binary merkle tree ([`bmt::Hasher`], [`bmt::Proof`]), the content-addressed
//! and single-owner carriers with their acceptance rules, single-owner owner
//! recovery, and the address, error and wire types those need.
//!
//! Storage, encryption, envelopes, ECIES and the routing metrics live in
//! `nectar-primitives`, which depends on this crate and re-exports every item
//! at its original path.
//!
//! ## Usage Examples
//!
//! ```
//! use nectar_primitives_core::{ChunkOps, DefaultContentChunk};
//!
//! // Creating content chunks; the address is derived from the content
//! let chunk = DefaultContentChunk::new(b"Hello, world!".as_slice()).unwrap();
//! let address = *chunk.address();
//!
//! // Reconstructing a chunk (e.g. from storage) and certifying it
//! let chunk2 = DefaultContentChunk::new(b"Hello, world!".as_slice()).unwrap();
//! chunk2.verify(&address).unwrap();
//! ```

#![cfg_attr(not(feature = "std"), no_std)]
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
extern crate alloc;

// Signature recovery runs through alloy-primitives' k256 backend. The direct
// dependency exists only so the `std` feature can switch that same k256 on to
// its precomputed tables; nothing here names the crate.
use k256 as _;

// Re-export dependencies that are part of our public API
pub use bytes;

pub mod bmt;
pub(crate) mod cache;
pub mod chunk;
pub mod error;
#[cfg(any(test, feature = "arbitrary"))]
pub mod generators;
pub mod marker;
pub mod nonce;
#[cfg(any(test, feature = "arbitrary"))]
pub mod oracles;
pub mod wire;

/// Explicit-cast helpers shared with `nectar-primitives`. Not part of the
/// supported surface.
#[doc(hidden)]
pub mod cast;

// Re-export core constants
pub use bmt::DEFAULT_BODY_SIZE;

// Re-export core types
pub use error::{PrimitivesError, Result, WrongLength};
pub use nonce::Nonce;

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
    SingleOwnerChunk,
    SingleOwnerOnlyChunkSet,
    SocHeader,
    SocId,
    StandardChunkSet,
    TagWireError,
    TrustState,
    TrustedSource,
    Unverified,
    Verified,
};

/// Default BMT hasher.
pub type DefaultHasher = Hasher<DEFAULT_BODY_SIZE>;
/// Default content-addressed chunk.
pub type DefaultContentChunk = ContentChunk<DEFAULT_BODY_SIZE>;
/// Default single-owner chunk.
pub type DefaultSingleOwnerChunk = SingleOwnerChunk<DEFAULT_BODY_SIZE>;
/// Default polymorphic chunk.
pub type DefaultAnyChunk = AnyChunk<DEFAULT_BODY_SIZE>;

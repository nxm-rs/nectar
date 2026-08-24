//! Postage stamp primitives for Ethereum Swarm.
//!
//! This crate provides the core types and traits for postage stamps in the Swarm network.
//! It is optimized for verification use cases (such as `vertex` nodes).
//!
//! For stamp issuing and signing, use the
//! [`nectar-postage-issuer`](https://docs.rs/nectar-postage-issuer) crate.
//!
//! # Core Types
//!
//! - [`Batch`]: A postage batch representing prepaid storage
//! - [`BucketDepth`]: A collision-bucket depth a network accepts, checked
//!   against the [`SwarmSpec`](nectar_primitives::SwarmSpec) it is built for
//! - [`Bucket`]: A collision bucket carrying the depth that cut it
//! - [`BatchDepth`]: A batch depth carrying the bucket depth beneath it
//! - [`Stamp`]: A postage stamp proving payment for chunk storage
//! - [`StampIndex`]: The bucket and position index within a stamp
//! - [`StampDigest`]: The data to be signed when creating a stamp
//! - [`StampedAddress`]: A stamp bound to an address, and the authority that
//!   validates the pairing
//! - [`StampedChunk`]: A chunk and its stamp, carrying the chunk's trust state
//!   and the stamp's validation state
//! - [`PostageContext`]: Context for batch expiry calculations
//! - [`BatchEvent`]: Events emitted by the postage stamp contract (requires `std`)
//!
//! # Traits
//!
//! - [`ChunkPut`](nectar_primitives::ChunkPut) over a [`StampedChunk`]: the
//!   sink for a paid chunk, with [`StampIndifferent`] bridging a plain chunk
//!   store into it
//! - [`BatchStore`]: Persist and retrieve batches (requires `std`). The trait is
//!   synchronous and, having an associated `Error` and no generic methods, is
//!   naturally object-safe; drive it from an async edge (a gRPC service, an FFI
//!   boundary) where async is genuinely needed, rather than colouring the core.
//! - [`BatchEventHandler`]: Handle batch events from the blockchain (requires `std`)
//!
//! # Features
//!
//! - `std` (default): Enable standard library support, BatchStore, events
//! - `serde`: Enable serde serialization/deserialization
//! - `parallel`: Enable parallel verification with rayon
//! - `arbitrary`: Raw `Arbitrary` impls plus the valid-by-construction
//!   `generators` module for property-based testing and fuzzing

#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
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

// `alloc` is required by the stamped-chunk codec (`Vec`). `nectar-primitives`,
// a hard dependency, also already requires an allocator, so this adds no new
// constraint to the `no_std` build.
extern crate alloc;

mod batch;
mod error;
#[cfg(any(test, feature = "arbitrary"))]
pub mod generators;
mod geometry;
#[cfg(any(test, feature = "arbitrary"))]
pub mod oracles;
mod sink;
mod stamp;
mod stamped;
mod stamped_address;
mod util;

// Storage, validation and events (std only)
#[cfg(feature = "std")]
mod events;
#[cfg(feature = "std")]
mod store;
#[cfg(feature = "std")]
mod validation;

// Parallel verification (requires rayon)
#[cfg(feature = "parallel")]
pub mod parallel;

// Core types
pub use batch::{Batch, BatchId, BatchParams};
pub use error::StampError;
pub use geometry::{BatchDepth, Bucket, BucketDepth, calculate_bucket};
pub use sink::StampIndifferent;
pub use stamp::{STAMP_SIZE, Stamp, StampBytes, StampDigest, StampIndex};
pub use stamped::StampedChunk;
pub use stamped_address::{StampedAddress, Unvalidated, Validated, ValidationState};
pub use util::PostageContext;
#[cfg(feature = "std")]
pub use validation::StoreValidator;

// Storage and events (std only)
#[cfg(feature = "std")]
pub use events::{BatchEvent, BatchEventHandler};
#[cfg(feature = "std")]
pub use store::{BatchStore, BatchStoreError, BatchStoreExt};

// Re-export VerifyingKey for cached pubkey verification optimization
pub use k256::ecdsa::VerifyingKey;

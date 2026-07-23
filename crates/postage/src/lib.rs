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
//! - [`Stamp`]: A postage stamp proving payment for chunk storage
//! - [`StampIndex`]: The bucket and position index within a stamp
//! - [`StampDigest`]: The data to be signed when creating a stamp
//! - [`PostageContext`]: Context for batch expiry calculations
//! - [`BatchEvent`]: Events emitted by the postage stamp contract (requires `std`)
//!
//! # Traits
//!
//! - [`StampValidator`]: Validate stamps against batches
//! - [`BatchStore`]: Persist and retrieve batches (requires `std`). The trait is
//!   synchronous and, having an associated `Error` and no generic methods, is
//!   naturally object-safe; drive it from an async edge (a gRPC service, an FFI
//!   boundary) where async is genuinely needed, rather than colouring the core.
//! - [`SnapshotStore`]: Cache recovered issuer snapshot state by batch id (requires `std`)
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
        clippy::panic_in_result_fn
    )
)]

// k256 is a dependency only to enable the precomputed-tables feature for faster ECDSA
#[cfg(not(test))]
use k256 as _;

// `alloc` is required by the stamped-chunk codec (`Vec`). `nectar-primitives`,
// a hard dependency, also already requires an allocator, so this adds no new
// constraint to the `no_std` build.
extern crate alloc;

mod batch;
mod error;
#[cfg(any(test, feature = "arbitrary"))]
pub mod generators;
mod stamp;
mod stamped;
mod util;
mod validation;

// Storage and events (std only)
#[cfg(feature = "std")]
mod events;
#[cfg(feature = "std")]
mod snapshot_store;
#[cfg(feature = "std")]
mod store;

// Parallel verification (requires rayon)
#[cfg(feature = "parallel")]
pub mod parallel;

// Core types
pub use batch::{Batch, BatchId, BatchParams, BucketDepth};
pub use error::StampError;
pub use stamp::{STAMP_SIZE, Stamp, StampBytes, StampDigest, StampIndex};
pub use stamped::StampedChunk;
pub use util::{PostageContext, calculate_bucket, current_timestamp};
pub use validation::StampValidator;
#[cfg(feature = "std")]
pub use validation::StoreValidator;

// Storage and events (std only)
#[cfg(feature = "std")]
pub use events::{BatchEvent, BatchEventHandler};
#[cfg(feature = "std")]
pub use snapshot_store::SnapshotStore;
#[cfg(feature = "std")]
pub use store::{BatchStore, BatchStoreError, BatchStoreExt};

// Re-export VerifyingKey for cached pubkey verification optimization
pub use alloy_signer::k256::ecdsa::VerifyingKey;

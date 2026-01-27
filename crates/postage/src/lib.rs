//! Postage stamp primitives for Ethereum Swarm.
//!
//! This crate provides the core types and traits for postage stamps in the Swarm network.
//! It is optimized for verification use cases (such as `vertex` nodes).
//!
//! For stamp issuing and signing, use the [`nectar-postage-issuer`] crate.
//!
//! # Core Types
//!
//! - [`Batch`]: A postage batch representing prepaid storage
//! - [`Stamp`]: A postage stamp proving payment for chunk storage
//! - [`StampIndex`]: The bucket and position index within a stamp
//! - [`StampDigest`]: The data to be signed when creating a stamp
//! - [`PostageContext`]: Context for batch expiry calculations
//! - [`BatchEvent`]: Events emitted by the postage stamp contract (requires `std`)
//!
//! # Traits
//!
//! - [`StampValidator`]: Validate stamps against batches
//! - [`BatchStore`]: Persist and retrieve batches (requires `std`)
//! - [`BatchEventHandler`]: Handle batch events from the blockchain (requires `std`)
//!
//! # Features
//!
//! - `std` (default): Enable standard library support, BatchStore, events
//! - `serde`: Enable serde serialization/deserialization
//! - `parallel`: Enable parallel verification with rayon

#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

// k256 is a dependency only to enable the precomputed-tables feature for faster ECDSA
#[cfg(not(test))]
use k256 as _;

mod batch;
mod error;
mod stamp;
mod util;
mod validation;

// Storage and events (std only)
#[cfg(feature = "std")]
mod events;
#[cfg(feature = "std")]
mod store;

// Parallel verification (requires rayon)
#[cfg(feature = "parallel")]
pub mod parallel;

// Core types
pub use batch::{Batch, BatchId, BatchParams};
pub use error::StampError;
pub use stamp::{STAMP_SIZE, Stamp, StampBytes, StampDigest, StampIndex};
pub use util::{PostageContext, calculate_bucket, current_timestamp};
pub use validation::StampValidator;
#[cfg(feature = "std")]
pub use validation::StoreValidator;

// Storage and events (std only)
#[cfg(feature = "std")]
pub use events::{BatchEvent, BatchEventHandler};
#[cfg(feature = "std")]
pub use store::{BatchStore, BatchStoreError, BatchStoreExt};

// Re-export VerifyingKey for cached pubkey verification optimization
pub use alloy_signer::k256::ecdsa::VerifyingKey;

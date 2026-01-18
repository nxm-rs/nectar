//! Postage stamp primitives for Ethereum Swarm.
//!
//! This crate provides the core types and traits for postage stamps in the Swarm network.
//!
//! # Core Types
//!
//! - [`Batch`]: A postage batch representing prepaid storage
//! - [`Stamp`]: A postage stamp proving payment for chunk storage
//! - [`StampIndex`]: The bucket and position index within a stamp
//! - [`StampDigest`]: The data to be signed when creating a stamp
//! - [`ChainState`]: Blockchain state for batch expiry calculations
//!
//! # Traits
//!
//! - [`StampValidator`]: Validate stamps against batches
//! - [`StampIssuer`]: Track bucket utilization and prepare stamps
//! - [`Stamper`]: Issue and sign stamps
//! - [`BatchStore`]: Persist and retrieve batches (requires `std`)
//! - [`BatchFactory`]: Create batches on-chain or in-memory (requires `std`)
//!
//! # Features
//!
//! - `std` (default): Enable standard library support, BatchStore, BatchFactory
//! - `serde`: Enable serde serialization/deserialization
//! - `local-signer`: Enable local key signing for testing
//! - `parallel`: Enable batch-collect parallel operations with rayon (sync)
//! - `streaming`: Enable streaming parallel operations with tokio (async)

#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

mod batch;
mod error;
mod stamp;
mod util;
mod validation;

// Issuing and stamping
mod issuer;
mod stamper;

// Storage and factory (std only)
#[cfg(feature = "std")]
mod events;
#[cfg(feature = "std")]
mod factory;
#[cfg(feature = "std")]
mod store;

// Parallel stamping and verification (requires rayon)
#[cfg(feature = "parallel")]
pub mod parallel;

// Streaming parallel operations (requires tokio)
#[cfg(feature = "streaming")]
pub mod streaming;

// Core types
pub use batch::{Batch, BatchId, BatchParams};
pub use error::StampError;
pub use stamp::{Stamp, StampBytes, StampDigest, StampIndex, STAMP_SIZE};
pub use util::{calculate_bucket, current_timestamp, ChainState};
pub use validation::StampValidator;
#[cfg(feature = "std")]
pub use validation::StoreValidator;

// Issuing
pub use issuer::{MemoryIssuer, StampIssuer};
pub use stamper::{BatchStamper, StampSigner, Stamper};

// Storage and factory (std only)
#[cfg(feature = "std")]
pub use events::{BatchEvent, BatchEventHandler};
#[cfg(feature = "std")]
pub use factory::{BatchFactory, CreateResult, MemoryBatchError, MemoryBatchFactory};
#[cfg(feature = "std")]
pub use store::{BatchStore, BatchStoreError, BatchStoreExt};

// Re-export alloy-signer-local for convenience when local-signer feature is enabled
#[cfg(feature = "local-signer")]
pub use alloy_signer_local;

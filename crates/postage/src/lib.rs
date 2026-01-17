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
//! - `std` (default): Enable standard library support, async traits, storage
//! - `serde`: Enable serde serialization/deserialization

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

// Core types
pub use batch::{Batch, BatchId, BatchParams};
pub use error::StampError;
pub use stamp::{Stamp, StampBytes, StampDigest, StampIndex, STAMP_SIZE};
pub use util::{calculate_bucket, ChainState};
pub use validation::{BatchValidation, StampValidator};

// Issuing
pub use issuer::{MemoryIssuer, StampIssuer};
pub use stamper::{BatchStamper, SignerError, StampSigner, Stamper};

// Storage and factory (std only)
#[cfg(feature = "std")]
pub use events::{BatchEvent, BatchEventHandler};
#[cfg(feature = "std")]
pub use factory::{BatchFactory, CreateResult, MemoryBatchError, MemoryBatchFactory};
#[cfg(feature = "std")]
pub use store::{BatchStore, BatchStoreError, BatchStoreExt};

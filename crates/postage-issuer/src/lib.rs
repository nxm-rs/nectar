//! Postage stamp issuing and signing for Ethereum Swarm.
//!
//! This crate provides the issuing and signing functionality for postage stamps,
//! designed for use by CLI tools (like `dipper`) that create and sign stamps.
//!
//! For verification-only use cases (like `vertex` nodes), use [`nectar-postage`] directly.
//!
//! # Features
//!
//! - `std` (default) - Enables standard library support
//! - `serde` - Enables serialization/deserialization
//! - `local-signer` - Enables local key signing with `alloy-signer-local`
//! - `parallel` - Enables parallel signing with rayon
//!
//! # Example
//!
//! ```ignore
//! use nectar_postage_issuer::{BatchStamper, MemoryIssuer, Stamper};
//! use nectar_primitives::SwarmAddress;
//! use alloy_primitives::B256;
//! use alloy_signer_local::PrivateKeySigner;
//!
//! // Create an issuer for a batch
//! let issuer = MemoryIssuer::new(B256::ZERO, 20, 16);
//!
//! // Combine with any SignerSync implementation to create a stamper
//! let signer = PrivateKeySigner::random();
//! let mut stamper = BatchStamper::new(issuer, signer);
//!
//! // Stamp chunks
//! let stamp = stamper.stamp(&chunk_address)?;
//! ```

#![cfg_attr(not(feature = "std"), no_std)]

mod error;
mod factory;
mod issuer;
mod sharded;
mod stamper;

// Re-export core types from nectar-postage (includes BatchEvent, BatchEventHandler)
pub use nectar_postage::*;

// Errors (override nectar_postage::StampError with our own that includes signing)
pub use error::SigningError;

// Issuing
pub use issuer::{MemoryIssuer, StampIssuer};
pub use sharded::ShardedIssuer;
pub use stamper::{BatchStamper, Stamper};

// Factory (std only)
#[cfg(feature = "std")]
pub use factory::{BatchFactory, CreateResult, MemoryBatchError, MemoryBatchFactory};

// Parallel signing (requires parallel feature)
#[cfg(feature = "parallel")]
pub use sharded::{StampResult, sign_stamps_parallel};

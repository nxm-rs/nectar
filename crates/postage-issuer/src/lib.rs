//! Postage stamp issuing and signing for Ethereum Swarm.
//!
//! This crate provides the issuing and signing functionality for postage stamps,
//! designed for use by CLI tools (like `dipper`) that create and sign stamps.
//!
//! For verification-only use cases (like `vertex` nodes), use
//! [`nectar-postage`](nectar_postage) directly.
//!
//! # Immutable and mutable issuance
//!
//! Immutable batches are fill-only: every slot is written at most once and a
//! full bucket is refused. Use [`MemoryIssuer`] (or [`ShardedIssuer`] for
//! parallel stamping). Their `from_batch` constructors deliberately refuse a
//! mutable batch with [`IssuerError::MutableNotSupported`], so a ring is never
//! produced by accident from the generic constructor.
//!
//! Mutable batches are overwrite-aware: a later chunk may reuse the slot held
//! by an older one. This is the ring issuance in [`RingIssuer`] (and
//! [`ShardedRingIssuer`] for parallel stamping), and it must be requested by
//! name. A ring carries its reservation policy in a type parameter so a
//! reserved-blind ring can never be used in a self-hosting context:
//!
//! - [`RingIssuer::external`] builds a [`RingIssuer<Unreserved>`] for external
//!   tracking: the caller keeps usage state outside the batch and nothing in
//!   the batch is protected.
//! - [`RingIssuer::reserved`] builds a [`RingIssuer<Reserved>`] for
//!   self-hosting: the protected slots come from `nectar-postage-usage`, and
//!   the ring never re-emits one even after it wraps.
//!
//! There is no public conversion from [`Unreserved`] to [`Reserved`], so a
//! self-hosting context that demands a [`RingIssuer<Reserved>`] cannot be handed
//! a reserved-blind ring. The following does not compile:
//!
//! ```compile_fail
//! use nectar_postage_issuer::{RingIssuer, Reserved, Unreserved};
//! use nectar_postage::{Batch, BatchId};
//!
//! fn self_hosting_sink(_ring: RingIssuer<Reserved>) {}
//!
//! let batch = Batch::new(BatchId::ZERO, 0, 0, Default::default(), 20, 16, false);
//! let unreserved: RingIssuer<Unreserved> = RingIssuer::external(&batch).unwrap();
//! // A reserved-blind ring is not a Reserved ring, and there is no conversion.
//! self_hosting_sink(unreserved);
//! ```
//!
//! # Features
//!
//! - `std` (default) - Enables standard library support
//! - `local-signer` - Enables local key signing with `alloy-signer-local`
//! - `parallel` - Enables parallel signing with rayon
//!
//! # Example
//!
//! ```ignore
//! use nectar_postage_issuer::{BatchId, BatchStamper, MemoryIssuer, Stamper};
//! use nectar_primitives::ChunkAddress;
//! use alloy_signer_local::PrivateKeySigner;
//!
//! // Create an issuer for a batch
//! let issuer = MemoryIssuer::new(BatchId::ZERO, 20, 16);
//!
//! // Combine with any SignerSync implementation to create a stamper
//! let signer = PrivateKeySigner::random();
//! let mut stamper = BatchStamper::new(issuer, signer);
//!
//! // Stamp chunks
//! let stamp = stamper.stamp(&chunk_address)?;
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
        clippy::panic_in_result_fn
    )
)]

mod counter;
#[cfg(feature = "std")]
mod dilute_handler;
mod error;
mod factory;
mod issuer;
mod ring;
mod sharded;
mod sharded_ring;
mod stamper;

// Re-export core types from nectar-postage (includes BatchEvent, BatchEventHandler)
pub use nectar_postage::*;

// Errors (override nectar_postage::StampError with our own that includes signing)
pub use error::{IssuerError, SigningError};

// The shared per-bucket counter table behind every issuer and the snapshot.
pub use counter::{CounterError, CounterMode, CounterTable};

// Wiring on-chain depth-increase events through to issuer dilution (std only).
#[cfg(feature = "std")]
pub use dilute_handler::{Dilutable, IssuerRegistry};

// Issuing
pub use issuer::{MemoryIssuer, StampIssuer};
pub use sharded::ShardedIssuer;
pub use stamper::{BatchStamper, Stamper};

// Mutable (ring) issuing with a type-state reservation guard
pub use ring::{Reservation, Reserved, RingIssuer, Unreserved};
pub use sharded_ring::ShardedRingIssuer;

// Factory (std only)
#[cfg(feature = "std")]
pub use factory::{BatchFactory, CreateResult, MemoryBatchFactory};

// Parallel signing (requires parallel feature)
#[cfg(feature = "parallel")]
pub use sharded::{StampResult, sign_stamps_parallel};

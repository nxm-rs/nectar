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
//! use nectar_postage::{Batch, BatchId, BucketDepth};
//!
//! fn self_hosting_sink(_ring: RingIssuer<Reserved>) {}
//!
//! let bucket_depth = BucketDepth::new(16).unwrap();
//! let batch = Batch::new(BatchId::ZERO, 0, 0, Default::default(), 20, bucket_depth, false);
//! let unreserved: RingIssuer<Unreserved> = RingIssuer::external(&batch).unwrap();
//! // A reserved-blind ring is not a Reserved ring, and there is no conversion.
//! self_hosting_sink(unreserved);
//! ```
//!
//! # Networks
//!
//! An issuer is parameterized by the [`SwarmSpec`] its batch was built for, and
//! carries it in the [`BucketDepth`] it is constructed from, so a depth the
//! network refuses never reaches an issuer. The generic type takes the `...For`
//! name ([`MemoryIssuerFor`], [`RingIssuerFor`], [`ShardedIssuerFor`],
//! [`ShardedRingIssuerFor`], [`CounterTableFor`]) and the bare name is the
//! mainnet alias, so ordinary call sites need no type annotation:
//!
//! ```
//! use nectar_postage_issuer::{BatchId, BucketDepth, MemoryIssuer, MemoryIssuerFor, Testnet};
//!
//! let mainnet = MemoryIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16)?);
//! let testnet = MemoryIssuerFor::<Testnet>::new(BatchId::ZERO, 20, BucketDepth::new(16)?);
//! # Ok::<(), nectar_postage_issuer::StampError>(())
//! ```
//!
//! [`StampIssuer`], [`Stamper`] and [`Dilutable`] stay spec-agnostic: they only
//! read scalar geometry, so a `dyn Dilutable` registry can hold issuers for
//! different networks.
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
//! use nectar_postage_issuer::{BatchId, BatchStamper, BucketDepth, MemoryIssuer, Stamper};
//! use nectar_primitives::ChunkAddress;
//! use alloy_signer_local::PrivateKeySigner;
//!
//! // Create an issuer for a batch
//! let issuer = MemoryIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());
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
        clippy::panic_in_result_fn,
        clippy::as_conversions
    )
)]

mod counter;
#[cfg(feature = "std")]
mod dilute_handler;
mod error;
mod factory;
mod issuer;
#[cfg(feature = "parallel")]
mod prepared;
mod ring;
mod sharded;
mod sharded_ring;
mod stamper;

// Re-export core types from nectar-postage (includes BatchEvent, BatchEventHandler)
pub use nectar_postage::*;

// The network specs the issuers are parameterized by.
pub use nectar_primitives::{Mainnet, NetworkId, SwarmSpec, Testnet};

// Errors (override nectar_postage::StampError with our own that includes signing)
pub use error::{IssuerError, SigningError};

// The shared per-bucket counter table behind every issuer and the snapshot.
pub use counter::{CounterError, CounterMode, CounterTable, CounterTableFor};

// Wiring on-chain depth-increase events through to issuer dilution (std only).
#[cfg(feature = "std")]
pub use dilute_handler::{Dilutable, IssuerRegistry};

// Issuing
pub use issuer::{MemoryIssuer, MemoryIssuerFor, StampIssuer};
pub use sharded::{ShardedIssuer, ShardedIssuerFor};
pub use stamper::{BatchStamper, Stamper};

// Mutable (ring) issuing with a type-state reservation guard
pub use ring::{Reservation, Reserved, RingIssuer, RingIssuerFor, Unreserved};
pub use sharded_ring::{ShardedRingIssuer, ShardedRingIssuerFor};

// Factory (std only)
#[cfg(feature = "std")]
pub use factory::{
    BatchFactory, CreateResult, CreateResultFor, MemoryBatchFactory, MemoryBatchFactoryFor,
};

// Parallel signing (requires parallel feature)
#[cfg(feature = "parallel")]
pub use prepared::{StampPreparation, prepare_stamps, sign_prepared_parallel, stamp_parallel};
#[cfg(feature = "parallel")]
pub use sharded::{StampResult, sign_stamps_parallel};

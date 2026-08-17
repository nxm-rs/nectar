//! Postage stamp issuing and signing for Ethereum Swarm.
//!
//! This crate provides the issuing and signing functionality for postage stamps,
//! designed for use by CLI tools (like `dipper`) that create and sign stamps.
//!
//! For verification-only use cases (like `vertex` nodes), use
//! [`nectar-postage`](nectar_postage) directly.
//!
//! # Front door
//!
//! - [`StampPipeline`]: the many-chunk entry; streams any address iterator
//!   and yields results unordered, tagged by address.
//! - [`BatchStamper`]: the one-off entry for stamping a single chunk;
//!   [`BatchStamper::into_parts`] moves an issuer between the doors.
//! - [`StampedPut`]: the store decorator; wraps a stamped sink so every
//!   `ChunkPut` call site stamps at put.
//! - [`StagedPut`]: the same decoration with signing in a stage of its own,
//!   so a put slot holds store latency alone. Its puts resolve at admission,
//!   so [`StagedPut::flush`] is part of the contract.
//!
//! # Self-issued stamps
//!
//! [`BatchSigner::bind`] checks once that the signer is the batch owner. What
//! a bound signer seals is then validated by construction, with no signature
//! recovery on the hot path.
//!
//! # Immutable and mutable issuance
//!
//! Immutable batches are fill-only: every slot is written at most once and a
//! full bucket is refused. Use [`MemoryIssuer`]. Its `from_batch` constructor
//! deliberately refuses a mutable batch with
//! [`IssuerError::MutableNotSupported`], so a ring is never produced by
//! accident from the generic constructor.
//!
//! Mutable batches are overwrite-aware: a later chunk may reuse the slot held
//! by an older one. This is the ring issuance in [`RingIssuer`], and it must be
//! requested by name. A ring carries its reservation policy in a type parameter
//! so a reserved-blind ring can never be used in a self-hosting context:
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
//! let batch: Batch = Batch::new(BatchId::ZERO, 0, 0, Default::default(), 20, bucket_depth, false);
//! let unreserved: RingIssuer<Unreserved> = RingIssuer::external(&batch).unwrap();
//! // A reserved-blind ring is not a Reserved ring, and there is no conversion.
//! self_hosting_sink(unreserved);
//! ```
//!
//! # Networks
//!
//! An issuer is parameterized by the [`SwarmSpec`] its batch was built for, and
//! carries it in the [`BucketDepth`] it is constructed from, so a depth the
//! network refuses never reaches an issuer. The spec parameter defaults to
//! [`Mainnet`] in type position only; a default drives no inference, so a
//! construction site still names the type it builds:
//!
//! ```
//! use nectar_postage_issuer::{BatchId, BucketDepth, MemoryIssuer, Testnet};
//!
//! let mainnet: MemoryIssuer = MemoryIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16)?);
//! let testnet = MemoryIssuer::<Testnet>::new(BatchId::ZERO, 20, BucketDepth::new(16)?);
//! # Ok::<(), nectar_postage_issuer::StampError>(())
//! ```
//!
//! [`StampIssuer`], [`Stamper`] and [`Dilutable`] stay spec-agnostic: they only
//! read scalar geometry, so a `dyn Dilutable` registry can hold issuers for
//! different networks.
//!
//! # The permit seam
//!
//! [`StampIssuer::reserve`] takes `&self` and returns a [`Prepared`] permit:
//! constructing it consumed the slot. Signing happens outside every lock, then
//! [`Prepared::stamp`] or [`Prepared::seal`] mints the result. Dropping a
//! permit returns its [`WindowToken`] to the [`AdmissionWindow`] and burns the
//! slot, so a cancelled future recovers backpressure but never capacity.
//!
//! # Online dilution
//!
//! [`Dilutable::dilute`] takes `&self` and [`IssuerRegistry`] holds
//! [`IssuerHandle`]s, so a chain event dilutes an issuer a pipeline is signing
//! through. The registry withholds the widened capacity until the dilution
//! block carries its confirmations, because a peer that has not ingested the
//! event rejects a stamp minted into that range. A gated registry therefore
//! needs [`IssuerRegistry::advance_to`]: without head progress the last
//! dilution never applies.
//!
//! # Parallel stamping
//!
//! [`MemoryIssuer`] allocates without a lock, so any number of threads may
//! stamp through one issuer. Ring issuance stays sequential, because skipping
//! reserved slots reads more state than one word; a [`RingIssuer`] serializes
//! itself in a cell and is `!Sync`, so move one between threads behind a lock.
//!
//! Where there are no threads (`unsync`, wasm32, bare metal) the same table is
//! plain cells, which leaves [`MemoryIssuer`] `!Sync` there.
//!
//! # Features
//!
//! - `std` (default) - Standard library support. Without it the signer stack,
//!   the factory and the dilution registry are gated out, construction takes
//!   explicit clocks, and a signer panic propagates instead of being caught
//!   into a result.
//! - `local-signer` - Enables local key signing with `alloy-signer-local`
//! - `parallel` - Enables the pipeline's parallel signing engine with rayon,
//!   and implies `sign-parallel`
//! - `sign-parallel` - Signs each admission batch across the rayon pool; the
//!   signer seam otherwise runs its serial default
//! - `unsync` - Relaxes the signer thread-safety bounds on single-threaded
//!   targets; mutually exclusive with `parallel` and `sign-parallel`
//!
//! # Example
//!
//! ```ignore
//! use nectar_postage_issuer::{BatchId, BatchStamper, BucketDepth, MemoryIssuer, Stamper};
//! use nectar_primitives::ChunkAddress;
//! use alloy_signer_local::PrivateKeySigner;
//!
//! // Create an issuer for a batch
//! let issuer: MemoryIssuer = MemoryIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());
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

extern crate alloc;

// The parallel engine spawns onto rayon and needs real `Send`; `unsync`
// relaxes that bound away. Enabling both (directly or through feature
// unification) is a build error with a clear cause rather than a deep one at
// the spawn site.
#[cfg(all(feature = "parallel", feature = "unsync"))]
compile_error!("features `parallel` and `unsync` are mutually exclusive");

#[cfg(all(feature = "sign-parallel", feature = "unsync"))]
compile_error!("features `sign-parallel` and `unsync` are mutually exclusive");

mod counter;
#[cfg(feature = "std")]
mod dilute_handler;
mod error;
#[cfg(feature = "std")]
mod factory;
mod issuer;
mod permit;
mod pipeline;
mod ring;
mod stamper;
#[cfg(test)]
mod testing;
mod watermarks;

// Re-export core types from nectar-postage (includes BatchEvent, BatchEventHandler)
pub use nectar_postage::*;

// The network specs the issuers are parameterized by.
pub use nectar_primitives::{Mainnet, NetworkId, SwarmSpec, Testnet};

// Errors (override nectar_postage::StampError with our own that includes signing)
pub use error::{IssuerError, RingExhausted, SigningError};

// The shared per-bucket counter table behind every issuer and the snapshot.
pub use counter::{CounterError, CounterMode, CounterTable};

// Wiring on-chain depth-increase events through to issuer dilution (std only).
#[cfg(feature = "std")]
pub use dilute_handler::{Dilutable, IssuerHandle, IssuerRegistry};

// Issuing
pub use issuer::{MemoryIssuer, StampIssuer};
pub use permit::{AdmissionWindow, Prepared, WindowToken};
pub use stamper::{BatchStamper, Stamper};

// The streaming stamp pipeline; its sign window is the governor window.
pub use nectar_governor::Window;
pub use pipeline::{
    BatchSigner, BoundSigner, Eip191, SignPrehash, StampPipeline, StampResult, Stamped,
};
#[cfg(any(feature = "std", not(multi_thread)))]
pub use pipeline::{IssuedBound, StampedPut, StampedPutError};
#[cfg(feature = "std")]
pub use pipeline::{SealResult, SignStage, StagedPut, StampSink};

// Mutable (ring) issuing with a type-state reservation guard
pub use ring::{Reservation, Reserved, RingIssuer, Unreserved};

// Factory (std only)
#[cfg(feature = "std")]
pub use factory::{BatchFactory, CreateResult, MemoryBatchFactory};

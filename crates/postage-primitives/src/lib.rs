//! The data half of the postage domain.
//!
//! Stamps, batches, bucket geometry, the stamped-address typestate and the
//! signature recovery that binds a stamp to an address. This is the half a
//! guest links; the store-backed put seam and the event surface live in
//! `nectar-postage`, which re-exports every item here at its original path.
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
//!
//! # Features
//!
//! - `std` (default): Enable standard library support
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
#[cfg(feature = "parallel")]
pub mod parallel;
mod sink;
mod stamp;
mod stamped;
mod stamped_address;
mod util;

// Core types
pub use batch::{Batch, BatchId, BatchParams};
pub use error::StampError;
pub use geometry::{BatchDepth, Bucket, BucketDepth, calculate_bucket};
pub use stamp::{STAMP_SIZE, Stamp, StampBytes, StampDigest, StampIndex};
pub use stamped::StampedChunk;
pub use stamped_address::{StampedAddress, Unvalidated, Validated, ValidationState};
pub use util::PostageContext;

// Re-export VerifyingKey for cached pubkey verification optimization
pub use k256::ecdsa::VerifyingKey;

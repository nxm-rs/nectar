//! The behaviour half of the postage domain.
//!
//! The data half (stamps, batches, bucket geometry, the stamped-address
//! typestate and the stamp-to-address signature recovery) lives in
//! [nectar-postage-primitives](https://docs.rs/nectar-postage-primitives);
//! this crate re-exports it at the original paths and adds the store-backed
//! put seam and the event surface.
//!
//! For stamp issuing and signing, use the
//! [`nectar-postage-issuer`](https://docs.rs/nectar-postage-issuer) crate.
//!
//! # Traits
//!
//! - [`ChunkPut`](nectar_primitives::ChunkPut) over a [`StampedChunk`]: the
//!   sink for a paid chunk, with [`StampIndifferent`] bridging a plain chunk
//!   store into it
//! - [`BatchEventHandler`]: Handle batch events from the blockchain (requires `std`)
//!
//! # Features
//!
//! - `std` (default): Enable standard library support and events
//! - `serde`: Enable serde serialization/deserialization
//! - `parallel`: Enable parallel verification with rayon
//! - `arbitrary`: The data half's raw `Arbitrary` impls and the
//!   valid-by-construction `generators` module

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

mod sink;

// Events (std only)
#[cfg(feature = "std")]
mod events;

// The data half, re-exported at its original paths.
#[cfg(any(test, feature = "arbitrary"))]
pub use nectar_postage_primitives::generators;
#[cfg(any(test, feature = "arbitrary"))]
pub use nectar_postage_primitives::oracles;
#[cfg(feature = "parallel")]
pub use nectar_postage_primitives::parallel;
pub use nectar_postage_primitives::{
    Batch, BatchDepth, BatchId, BatchParams, Bucket, BucketDepth, PostageContext, STAMP_SIZE,
    Stamp, StampBytes, StampDigest, StampError, StampIndex, StampedAddress, StampedChunk,
    Unvalidated, Validated, ValidationState, VerifyingKey, calculate_bucket,
};

pub use sink::StampIndifferent;

// Events (std only)
#[cfg(feature = "std")]
pub use events::{BatchEvent, BatchEventHandler};

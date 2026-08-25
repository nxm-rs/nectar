//! The authentication layer for Swarm descent proofs.
//!
//! How one step's bytes bind to a trusted address, and the replay loop that
//! walks the steps. Two proof kinds are in scope: a segment of a chunk, and
//! membership of a key in an ldb store.
//!
//! # Core Types
//!
//! - [`Authenticate`]: The seam a descent proof verifies against
//! - [`Segment`]: The chunk segment kind, replaying the BMT segment proof
//!   from `nectar-primitives` to the chunk root
//!
//! # Features
//!
//! - `std` (default): Enable standard library support

#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::string_slice,
        clippy::arithmetic_side_effects,
        clippy::panic,
        clippy::unreachable,
        clippy::as_conversions
    )
)]

pub mod auth;
pub mod segment;

pub use crate::auth::Authenticate;
pub use crate::segment::Segment;

//! Canonical type definitions for Ethereum Swarm networks.
//!
//! This crate provides types for identifying Swarm networks, similar to how
//! `alloy-chains` provides types for Ethereum chains.
//!
//! # Features
//!
//! - `std` (default): Enable standard library support
//! - `serde`: Enable serde serialization/deserialization
//! - `arbitrary`: Enable arbitrary trait implementations for testing

#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
// Test code may freely unwrap/index/panic; the runtime-safety restriction
// lints target production code paths.
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

mod named;
mod swarm;

pub use named::NamedSwarm;
pub use swarm::{Swarm, SwarmKind};

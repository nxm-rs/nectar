//! Intrinsic comparison harness: manifest 1.0 vs mantaray 0.2 and the
//! streaming file pipeline vs the legacy splitter and joiner, all driven
//! through one unification layer over the same in-memory chunk store.

// Bench harness code: unwraps, indexing and casts are setup, not shipped
// surface.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::panic,
    clippy::panic_in_result_fn,
    clippy::as_conversions,
    clippy::missing_panics_doc
)]

pub mod corpus;
pub mod file_api;
pub mod manifest_api;
pub mod results;
pub mod store;

//! Bounded-admission kernel beneath the streaming walkers: the
//! fixed-membership [`InFlight`] set, the read-ahead [`Window`], the
//! head-slot [`Admission`] predicate, the [`AdmitPolicy`] adaptive-window
//! seam, and the [`BoxFuture`] alias the sets hold.
//!
//! The walker engines stay bespoke in their own crates; this crate carries
//! only the admission layer they share.

#![no_std]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
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
        clippy::panic_in_result_fn,
        clippy::as_conversions
    )
)]

extern crate alloc;
#[cfg(test)]
extern crate std;

// The marker predicate is what the build script's `multi_thread` alias
// mirrors; no marker item is named directly.
use nectar_marker as _;

mod admission;
#[cfg(feature = "chunk")]
mod chunk;
mod future;
mod inflight;
mod policy;
mod window;

pub use admission::Admission;
#[cfg(feature = "chunk")]
#[cfg_attr(docsrs, doc(cfg(feature = "chunk")))]
pub use chunk::get_verified;
pub use future::BoxFuture;
pub use inflight::InFlight;
pub use policy::{AdmitPolicy, Fixed, FromFn, Observations, from_fn};
pub use window::Window;

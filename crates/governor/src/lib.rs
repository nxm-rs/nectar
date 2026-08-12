//! Bounded-admission governor beneath the streaming walkers: the read-ahead
//! [`Window`], the head-slot [`Admission`] predicate, the [`AdmitPolicy`]
//! adaptive-window seam, and the write-side [`PutSink`].
//!
//! Admission only: `futures_util` is the walk substrate, and each walker
//! owns its own loop over a `FuturesUnordered` set, with its frontier,
//! ordering, and completion fold bespoke in its own crate. This crate says
//! nothing but when one more fetch may start.
//!
//! A consumer therefore takes the in-flight set from `futures_util`
//! directly, and the boxed-future alias from `nectar-tasks`, which owns it:
//! neither is re-exported here, so neither of the following compiles.
//!
//! ```compile_fail
//! use nectar_governor::FuturesUnordered;
//! ```
//!
//! ```compile_fail
//! use nectar_governor::BoxFuture;
//! ```
//!
//! The shared walk loop, and the per-walker trait it once ran over, are
//! gone with it: the surface below is all the crate exports.

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

#[cfg(test)]
extern crate std;

// The marker predicate is what the build script's `multi_thread` alias
// mirrors; no marker item is named directly.
use nectar_marker as _;

mod admission;
mod policy;
mod put_sink;
mod window;

pub use admission::Admission;
pub use policy::{AdmitPolicy, Fixed, FromFn, Observations, from_fn};
pub use put_sink::PutSink;
pub use window::Window;

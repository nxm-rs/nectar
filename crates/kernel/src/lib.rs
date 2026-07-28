//! Bounded-admission kernel beneath the streaming walkers: the shared
//! [`Driver`] loop over a [`WalkPolicy`], the [`FuturesUnordered`] in-flight
//! set it drains, the write-side [`PutSink`] over the same set, the read-ahead
//! [`Window`], the head-slot [`Admission`] predicate, the [`AdmitPolicy`]
//! adaptive-window seam, and the [`BoxFuture`] alias the sets hold.
//!
//! The driver owns the `admit`/`take`/`poll` loop; each walker's frontier,
//! ordering, and completion fold stay bespoke as a [`WalkPolicy`] impl in its
//! own crate, and a monomorphised driver is the hand-rolled walk.

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
#[cfg(feature = "chunk")]
mod chunk;
mod driver;
mod future;
mod policy;
mod put_sink;
mod window;

pub use admission::Admission;
#[cfg(feature = "chunk")]
#[cfg_attr(docsrs, doc(cfg(feature = "chunk")))]
pub use chunk::get_verified;
pub use driver::{Driver, StaticDriver, WalkPolicy};
pub use future::BoxFuture;
pub use futures_util::stream::FuturesUnordered;
pub use policy::{AdmitPolicy, Fixed, FromFn, Observations, from_fn};
pub use put_sink::PutSink;
pub use window::Window;

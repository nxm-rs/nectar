//! Streaming file pipeline for Swarm chunk trees: bounded reads and writes
//! over a chunk store.
//!
//! [`File`] is the whole surface: bind a store to a [`Policy`], then
//! `load` a root into a positional [`DataSink`] or `save` a [`Source`] into
//! a fresh tree. The sink is positional on purpose: frames land at their
//! offsets in completion order, which is what makes unordered retrieval
//! possible.
//!
//! The rest of the crate is supporting cast: per-profile tree [`geometry`]
//! pinned at compile time, the [`config`] admission budgets the engines
//! drain against, the [`sink`] targets a restartable load writes into, the
//! [`source`] adapters a save pulls from, and the [`sync`] driver for
//! Ready-only guests. The walk and split engines and their builders are
//! crate-private; `tokio` is an optional adapter shim over the same
//! handles.

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
#[cfg(any(test, feature = "std"))]
extern crate std;

// The marker traits bound only the engine surfaces behind `primitives`.
#[cfg(not(feature = "primitives"))]
use nectar_marker as _;

#[cfg(all(feature = "rayon", target_arch = "wasm32"))]
compile_error!("feature `rayon` needs a native thread pool; wasm32 builds must disable it");

#[cfg(all(feature = "rayon", feature = "unsync"))]
compile_error!("feature `rayon` needs `Send` chunks and errors; it excludes the `unsync` escape");

pub mod config;
#[cfg(any(test, feature = "arbitrary"))]
#[cfg_attr(docsrs, doc(cfg(feature = "arbitrary")))]
pub mod generators;
pub mod geometry;
// The engines and their builders are internal: `pub` inside a crate-private
// module is crate visibility, and only the handle seam is re-exported.
#[cfg(feature = "primitives")]
mod handle;
#[cfg(feature = "primitives")]
mod num;
/// Shared fuzz and test oracle for the malformed-intermediate walk.
/// Compiled for in-crate tests and for fuzz builds (`arbitrary`); exempt
/// from semver guarantees.
#[cfg(any(all(test, feature = "primitives"), feature = "arbitrary"))]
#[doc(hidden)]
pub mod oracles;
#[cfg(feature = "primitives")]
#[allow(unreachable_pub, reason = "crate-private module: `pub` is crate visibility")]
pub(crate) mod read;
pub mod sink;
#[cfg(feature = "primitives")]
#[cfg_attr(docsrs, doc(cfg(feature = "primitives")))]
pub mod source;
#[cfg(feature = "primitives")]
#[allow(unreachable_pub, reason = "crate-private module: `pub` is crate visibility")]
pub(crate) mod split;
pub mod sync;
#[cfg(all(test, feature = "primitives"))]
mod testutil;
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub mod tokio;
#[cfg(feature = "primitives")]
#[allow(unreachable_pub, reason = "crate-private module: `pub` is crate visibility")]
pub(crate) mod walk;

#[cfg(feature = "tokio")]
pub use self::tokio::{SeekOverflow, TokioReader};
pub use config::{BranchBudget, HashWindow, PutWindow, Window};
pub use geometry::{DEFAULT_BODY_SIZE, Mode, branches, max_depth};
#[cfg(feature = "primitives")]
pub use handle::{File, Policy, Reader, Segments};
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub use read::AdaptiveWindow;
#[cfg(feature = "primitives")]
pub use read::{CollectError, LoadError, OpenError, Progress, ProgressFn, SeekPastEnd};
#[cfg(feature = "std")]
pub use sink::FsSink;
pub use sink::{DataSink, MemSink, MemSinkError};
#[cfg(feature = "primitives")]
pub use source::Source;
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub use source::{ReadAt, ReadAtError, ReadAtSource};
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub use source::AsyncReadSource;
#[cfg(all(feature = "primitives", feature = "encryption"))]
#[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
pub use split::{KeyError, KeySource, RandomKeys};
#[cfg(feature = "primitives")]
pub use split::{SaveError, SealError, SplitError, SplitMode, SplitStats};
#[cfg(feature = "primitives")]
pub use walk::{
    DecodeError, Encrypted, Frame, Observations, Plain, ShapeError, WalkError, WalkStats,
};

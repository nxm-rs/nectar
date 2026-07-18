//! Streaming file pipeline for Swarm chunk trees: bounded reads and writes
//! over a chunk store.
//!
//! This crate carries the pipeline's foundations: per-profile tree
//! [`geometry`] pinned at compile time, the [`config`] admission budgets the
//! engines drain against, the poll-native [`walk`] engine every read mode
//! drains, the poll-native [`split`] engine every write mode feeds, and the
//! [`read`] facade that opens files by either reference width and drains the
//! walk in file order.

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

#[cfg(feature = "std")]
extern crate alloc;
#[cfg(test)]
extern crate std;

pub mod config;
pub mod geometry;
#[cfg(feature = "std")]
mod num;
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub mod read;
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub mod split;
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub mod walk;

pub use config::{BranchBudget, PutWindow, Window};
pub use geometry::{DEFAULT_BODY_SIZE, Mode, branches, max_depth};
#[cfg(feature = "std")]
pub use read::{AnyFile, File, FileReader, FileStream, OpenError, ReadBuilder, SeekPastEnd};
#[cfg(feature = "std")]
pub use split::{Sealed, Split, SplitError, SplitMode, SplitStats};
#[cfg(feature = "std")]
pub use walk::{
    DecodeError, Encrypted, Frame, Plain, ShapeError, Walk, WalkError, WalkMode, WalkStats,
};

//! Streaming file pipeline for Swarm chunk trees: bounded reads and writes
//! over a chunk store.
//!
//! This crate carries the pipeline's foundations: per-profile tree
//! [`geometry`] pinned at compile time, the [`config`] admission budgets the
//! engines drain against, the poll-native [`walk`] engine every read mode
//! drains, the poll-native [`split`] engine every write mode feeds, the
//! [`read`] facade that opens files by either reference width and drains the
//! walk in file order, the [`sink`] targets a restartable download writes
//! into, the [`store`] erasure that makes file handles nameable, the
//! [`sync`] driver for Ready-only guests, and the `parallel` batch ingest
//! over a random-access source (behind the `rayon` feature).

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

#[cfg(all(feature = "rayon", target_arch = "wasm32"))]
compile_error!("feature `rayon` needs a native thread pool; wasm32 builds must disable it");

#[cfg(all(feature = "rayon", feature = "unsync"))]
compile_error!("feature `rayon` needs `Send` chunks and errors; it excludes the `unsync` escape");

pub mod config;
pub mod geometry;
#[cfg(feature = "std")]
mod num;
#[cfg(all(
    feature = "rayon",
    not(target_arch = "wasm32"),
    not(feature = "unsync")
))]
#[cfg_attr(docsrs, doc(cfg(feature = "rayon")))]
pub mod parallel;
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub mod read;
pub mod sink;
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub mod split;
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub mod store;
pub mod sync;
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub mod tokio;
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub mod walk;

#[cfg(feature = "tokio")]
pub use self::tokio::{SeekOverflow, TokioReader};
#[cfg(all(
    feature = "tokio",
    not(any(target_arch = "wasm32", feature = "unsync"))
))]
pub use self::tokio::{SpawnedReader, TokioWriter};
pub use config::{BranchBudget, PutWindow, Window};
pub use geometry::{DEFAULT_BODY_SIZE, Mode, branches, max_depth};
#[cfg(all(
    feature = "rayon",
    not(target_arch = "wasm32"),
    not(feature = "unsync")
))]
pub use parallel::{ReadAt, ReadAtError, split_read_at};
#[cfg(feature = "std")]
pub use read::{
    AnyFile, CollectError, DownloadBuilder, DownloadError, File, FileFrames, FileReader,
    FileStream, OpenError, Progress, ProgressFn, ReadBuilder, SeekPastEnd,
};
#[cfg(feature = "std")]
pub use sink::FsSink;
pub use sink::{DataSink, MemSink, MemSinkError};
#[cfg(feature = "std")]
pub use split::{Sealed, Split, SplitError, SplitMode, SplitStats};
#[cfg(feature = "std")]
pub use store::{BoxedStore, BoxedStoreError, DynAnyFile, DynFile, DynFileReader, DynFileStream};
#[cfg(feature = "std")]
pub use walk::{
    DecodeError, Encrypted, Frame, Plain, ShapeError, Walk, WalkError, WalkMode, WalkStats,
};

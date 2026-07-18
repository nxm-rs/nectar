//! File facade over the walk engine: open a tree by either reference width
//! and read it in order.
//!
//! [`File`] pins the mode at the type level; [`AnyFile`] dispatches it at
//! runtime from an [`EntryRef`](nectar_primitives::EntryRef) wire reference.
//! [`ReadBuilder`] and [`DownloadBuilder`] ranges use clip semantics:
//! out-of-file bounds shrink the read instead of failing, and the clipped
//! length is readable as [`FileReader::effective_len`]. Only
//! [`FileReader::seek`] is typed-strict: it never clamps.
//! [`ReadBuilder::collect`] assembles a bounded in-memory copy, typed
//! [`CollectError::TooLarge`] past its bound.

mod download;
mod error;
mod file;
mod frames;
mod reader;
#[cfg(test)]
mod tests;

pub use download::{DownloadBuilder, Progress, ProgressFn};
pub use error::{CollectError, DownloadError, OpenError, SeekPastEnd};
pub use file::{AnyFile, File};
pub use frames::FileFrames;
pub use reader::{FileReader, FileStream, ReadBuilder};

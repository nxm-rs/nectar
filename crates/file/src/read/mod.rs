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

use core::num::NonZeroUsize;

#[cfg(test)]
mod cancel;
mod download;
mod error;
mod file;
mod frames;
mod reader;
#[cfg(test)]
mod tests;

/// The profile's body size as a typed nonzero. A zero profile never walks;
/// the floor only keeps the conversion total.
const fn body_size<const B: usize>() -> NonZeroUsize {
    match NonZeroUsize::new(B) {
        Some(body) => body,
        None => NonZeroUsize::MIN,
    }
}

pub use download::{DownloadBuilder, Progress, ProgressFn};
pub use error::{CollectError, DownloadError, OpenError, SeekPastEnd};
pub use file::{AnyFile, File};
pub use frames::FileFrames;
pub use reader::{FileReader, FileStream, ReadBuilder};

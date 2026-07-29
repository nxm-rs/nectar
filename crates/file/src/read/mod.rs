//! Internal read facade over the walk engine: open a tree by either
//! reference width and read it in order.
//!
//! [`Opened`] pins the grammar at the type level; [`AnyOpened`] dispatches
//! it at runtime from an [`EntryRef`](nectar_primitives::EntryRef) wire
//! reference. The builders are crate-private: the public seam is
//! [`File`](crate::File), which wires them to one policy.

use core::num::NonZeroUsize;

#[cfg(feature = "std")]
mod adaptive;
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
pub(crate) const fn body_size<const B: usize>() -> NonZeroUsize {
    match NonZeroUsize::new(B) {
        Some(body) => body,
        None => NonZeroUsize::MIN,
    }
}

#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub use adaptive::AdaptiveWindow;
pub use download::{DownloadBuilder, Progress, ProgressFn};
pub use error::{CollectError, LoadError, OpenError, SeekPastEnd};
pub use file::{AnyOpened, Opened};
#[cfg(test)]
pub use frames::FileFrames;
pub use reader::{FileReader, FileStream, ReadBuilder};

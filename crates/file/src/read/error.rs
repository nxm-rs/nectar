//! Typed failures of the read facade.

use nectar_primitives::chunk::ChunkAddress;

use crate::walk::{DecodeError, WalkError};

/// Failure opening a file at its root chunk.
#[derive(Debug, thiserror::Error)]
pub enum OpenError<E> {
    /// The root fetch failed.
    #[error("root fetch failed at {address}")]
    Fetch {
        /// Requested root address.
        address: ChunkAddress,
        /// Store error behind the failure.
        source: E,
    },
    /// The store completed the root fetch with a chunk at a different
    /// address.
    #[error("store returned {returned} for requested {requested}")]
    AddressMismatch {
        /// Address the open asked for.
        requested: ChunkAddress,
        /// Address of the chunk the store handed back.
        returned: ChunkAddress,
    },
    /// The root body cannot be decoded under the mode's reference grammar.
    #[error(transparent)]
    Decode(#[from] DecodeError),
}

/// Terminal failure of one download run; a full re-run restarts it.
#[derive(Debug, thiserror::Error)]
pub enum DownloadError<E, SE> {
    /// The walk failed before the range was tiled.
    #[error(transparent)]
    Walk(#[from] WalkError<E>),
    /// The sink rejected a write.
    #[error("sink write failed at {offset}")]
    Sink {
        /// Range-relative offset of the failed write.
        offset: u64,
        /// Sink error behind the failure.
        source: SE,
    },
}

/// A seek past the reader's effective length; the reader never clamps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("seek to {requested} past effective length {effective_len}")]
pub struct SeekPastEnd {
    /// Requested position, relative to the clipped range.
    pub requested: u64,
    /// The reader's effective length.
    pub effective_len: u64,
}

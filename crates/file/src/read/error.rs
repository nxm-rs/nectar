//! Typed failures of the read facade.

use nectar_primitives::chunk::ChunkAddress;

use crate::walk::DecodeError;

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

/// A seek past the reader's effective length; the reader never clamps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("seek to {requested} past effective length {effective_len}")]
pub struct SeekPastEnd {
    /// Requested position, relative to the clipped range.
    pub requested: u64,
    /// The reader's effective length.
    pub effective_len: u64,
}

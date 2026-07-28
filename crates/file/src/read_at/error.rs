//! Typed ingest failures; every error is terminal.

use std::io;

use crate::split::SplitError;

/// Terminal read-at ingest failure.
#[derive(Debug, thiserror::Error)]
pub enum ReadAtError<E> {
    /// Sizing the source failed.
    #[error("source length unavailable")]
    Length {
        /// Io error behind the failure.
        source: io::Error,
    },
    /// Reading a leaf body from the source failed.
    #[error("read failed at offset {offset}")]
    Read {
        /// Offset of the failed read.
        offset: u64,
        /// Io error behind the failure.
        source: io::Error,
    },
    /// The source reported its end before a leaf filled.
    #[error("short read at offset {offset}: {remaining} bytes missing")]
    ShortRead {
        /// Offset of the zero-length read.
        offset: u64,
        /// Leaf bytes still unread.
        remaining: usize,
    },
    /// The source reported more bytes than the read buffer holds.
    #[error("read overrun at offset {offset}: {count} bytes into {capacity}")]
    ReadOverrun {
        /// Offset of the overlong read.
        offset: u64,
        /// Byte count the source reported.
        count: usize,
        /// Buffer bytes the read had to fill.
        capacity: usize,
    },
    /// The split ascent failed; a dropped pool seal arrives here as
    /// [`SplitError::PoolDropped`].
    #[error(transparent)]
    Split(#[from] SplitError<E>),
}

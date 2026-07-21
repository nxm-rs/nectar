//! Typed ingest failures; every error is terminal.

use std::io;

use crate::split::{SealError, SplitError};

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
    /// The split ascent failed.
    #[error(transparent)]
    Split(#[from] SplitError<E>),
    /// The pool dropped a batch without replying; a worker died mid-job.
    #[error("hash pool dropped a batch")]
    PoolDropped,
}

/// A failure sealing one leaf on a pool worker; carried across the handoff.
#[derive(Debug)]
pub(super) enum LeafError {
    /// Reading the leaf body failed.
    Read { offset: u64, source: io::Error },
    /// The source ended before the leaf filled.
    Short { offset: u64, remaining: usize },
    /// The source reported more bytes than the buffer holds.
    Overrun {
        offset: u64,
        count: usize,
        capacity: usize,
    },
    /// Sealing the leaf payload failed.
    Seal(SealError),
}

impl<E> From<LeafError> for ReadAtError<E> {
    fn from(error: LeafError) -> Self {
        match error {
            LeafError::Read { offset, source } => Self::Read { offset, source },
            LeafError::Short { offset, remaining } => Self::ShortRead { offset, remaining },
            LeafError::Overrun {
                offset,
                count,
                capacity,
            } => Self::ReadOverrun {
                offset,
                count,
                capacity,
            },
            LeafError::Seal(source) => Self::Split(SplitError::Seal(source)),
        }
    }
}

//! Error types for file splitting and joining operations.

use crate::ChunkAddress;
use crate::store::{BoxedError, MaybeSend, MaybeSync};
use thiserror::Error;

/// Errors from file splitting and joining operations.
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum FileError {
    /// Write exceeded the declared span length.
    #[error("write past span length: wrote {written} bytes, span is {span}")]
    WritePastSpan {
        /// Declared span length.
        span: u64,
        /// Bytes written so far.
        written: u64,
    },

    /// Chunk data exceeds maximum allowed size.
    #[error("chunk too large: max {max}, got {actual}")]
    ChunkTooLarge {
        /// Maximum allowed size.
        max: usize,
        /// Actual size.
        actual: usize,
    },

    /// Chunk store failed to store a chunk.
    #[error("store error")]
    Store(#[source] BoxedError),

    /// Write sink failed. Rendered to a string so the sink's own error type,
    /// which may be `!Send` (a single-threaded browser writable), never
    /// enters `FileError`.
    #[error("sink error: {0}")]
    Sink(String),

    /// Chunk getter failed to retrieve a chunk.
    #[error("getter error")]
    Getter(#[source] BoxedError),

    /// Invalid chunk reference encountered during tree traversal.
    #[error("invalid reference at level {level}")]
    InvalidReference {
        /// Tree level where the invalid reference was found.
        level: usize,
    },

    /// Required chunk was not found.
    #[error("chunk not found: {0}")]
    ChunkNotFound(ChunkAddress),

    /// Span value doesn't match expected value.
    #[error("span mismatch: expected {expected}, got {actual}")]
    SpanMismatch {
        /// Expected span value.
        expected: u64,
        /// Actual span value.
        actual: u64,
    },

    /// Underlying chunk error.
    #[error("chunk error: {0}")]
    Chunk(#[from] crate::chunk::error::ChunkError),

    /// Encryption error.
    #[error("encryption error: {0}")]
    Encryption(#[from] crate::chunk::encryption::EncryptionError),

    /// A byte slice had the wrong width for a fixed-width type.
    #[error(transparent)]
    WrongLength(#[from] crate::error::WrongLength),

    /// Invalid entry reference length (expected 32 or 64 bytes).
    #[error("invalid entry reference length: {len} (expected 32 or 64)")]
    InvalidEntryRef {
        /// Actual byte length of the reference.
        len: usize,
    },
}

impl FileError {
    /// Create a store error from any error type.
    pub fn store<E: core::error::Error + MaybeSend + MaybeSync + 'static>(err: E) -> Self {
        Self::Store(Box::new(err))
    }

    /// Create a getter error from any error type.
    pub fn getter<E: core::error::Error + MaybeSend + MaybeSync + 'static>(err: E) -> Self {
        Self::Getter(Box::new(err))
    }

    /// Create a sink error by rendering any sink error to a string, so a
    /// `!Send` sink error never enters `FileError`.
    pub fn sink<E: std::error::Error>(err: E) -> Self {
        Self::Sink(err.to_string())
    }
}

/// Carries the typed error as the payload.
#[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
impl From<FileError> for std::io::Error {
    fn from(e: FileError) -> Self {
        Self::other(e)
    }
}

/// The relaxed shape may hold a `!Send` source, which `std::io::Error` cannot
/// carry, so the error is rendered to a string.
#[cfg(any(target_arch = "wasm32", feature = "unsync"))]
impl From<FileError> for std::io::Error {
    fn from(e: FileError) -> Self {
        Self::other(e.to_string())
    }
}

/// Result type for file operations.
pub type Result<T> = std::result::Result<T, FileError>;

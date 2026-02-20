//! Error types for file splitting and joining operations.

use crate::ChunkAddress;
use thiserror::Error;

/// Errors from file splitting and joining operations.
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

    /// Chunk sink failed to store a chunk.
    #[error("sink error: {0}")]
    Sink(Box<dyn std::error::Error + Send + Sync>),

    /// Chunk getter failed to retrieve a chunk.
    #[error("getter error: {0}")]
    Getter(Box<dyn std::error::Error + Send + Sync>),

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
}

impl FileError {
    /// Create a sink error from any error type.
    pub fn sink<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::Sink(Box::new(err))
    }

    /// Create a getter error from any error type.
    pub fn getter<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::Getter(Box::new(err))
    }
}

/// Result type for file operations.
pub type Result<T> = std::result::Result<T, FileError>;

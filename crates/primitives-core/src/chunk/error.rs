use alloc::string::String;

use super::address::ChunkAddress;
use thiserror::Error;

use super::type_tag::ChunkTypeTag;

/// Result type for chunk operations
pub(crate) type Result<T> = core::result::Result<T, ChunkError>;

/// Errors specific to chunk operations
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum ChunkError {
    /// Chunk body exceeds the maximum body size
    #[error("Chunk body too large: maximum {max} bytes, got {actual}")]
    BodyTooLarge {
        /// Maximum body size in bytes
        max: usize,
        /// Byte length actually observed
        actual: usize,
    },

    /// Buffer too short to carry a span
    #[error("Truncated span: expected {expected} bytes, got {actual}")]
    TruncatedSpan {
        /// Byte width a span requires
        expected: usize,
        /// Byte length actually observed
        actual: usize,
    },

    /// Span disagrees with the data length it describes
    #[error("Span mismatch: span says {span} bytes, data is {actual}")]
    SpanMismatch {
        /// Length the span claims
        span: u64,
        /// Data length actually observed
        actual: usize,
    },

    /// Chunk format is invalid
    #[error("Invalid chunk format: {0}")]
    InvalidFormat(String),

    /// Chunk address verification failed
    #[error("Chunk address verification failed: expected {expected}, got {actual}")]
    VerificationFailed {
        /// Address the chunk was checked against
        expected: ChunkAddress,
        /// Address the chunk actually derives
        actual: ChunkAddress,
    },

    /// Signature errors from the crypto library
    #[error("Signature error: {0}")]
    Signature(#[from] alloy_primitives::SignatureError),

    /// Signer errors
    #[cfg(feature = "std")]
    #[error("Signer error: {0}")]
    Signer(#[from] alloy_signer::Error),

    /// Chunk signature is invalid
    #[error("Invalid chunk signature: {0}")]
    InvalidSignature(String),

    /// Unsupported chunk type tag: an unknown id, or an unknown version of a
    /// known id (each `(id, version)` pair is a distinct acceptance rule)
    #[error("Unsupported chunk type tag: {0}")]
    UnsupportedTag(ChunkTypeTag),

    /// Wire buffer underrun
    #[error(transparent)]
    Underrun(#[from] crate::wire::Underrun),
}

impl ChunkError {
    /// Construct an [`InvalidFormat`](Self::InvalidFormat) error
    pub fn invalid_format<S: Into<String>>(msg: S) -> Self {
        Self::InvalidFormat(msg.into())
    }

    /// Construct a [`VerificationFailed`](Self::VerificationFailed) error
    pub const fn verification_failed(expected: ChunkAddress, actual: ChunkAddress) -> Self {
        Self::VerificationFailed { expected, actual }
    }

    /// Construct an [`InvalidSignature`](Self::InvalidSignature) error
    pub fn invalid_signature<S: Into<String>>(msg: S) -> Self {
        Self::InvalidSignature(msg.into())
    }

    /// Construct an [`UnsupportedTag`](Self::UnsupportedTag) error
    pub const fn unsupported_tag(tag: ChunkTypeTag) -> Self {
        Self::UnsupportedTag(tag)
    }
}

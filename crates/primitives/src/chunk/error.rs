use super::address::ChunkAddress;
use thiserror::Error;

use super::type_id::ChunkTypeId;

/// Result type for chunk operations
pub(crate) type Result<T> = std::result::Result<T, ChunkError>;

/// Errors specific to chunk operations
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum ChunkError {
    /// Chunk size is invalid
    #[error("Invalid chunk size: {message} (expected: {expected}, got: {actual})")]
    InvalidSize {
        /// What was being sized when the mismatch was found
        message: &'static str,
        /// Byte width the format requires
        expected: usize,
        /// Byte width actually observed
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
    #[error("Signer error: {0}")]
    Signer(#[from] alloy_signer::Error),

    /// Chunk signature is invalid
    #[error("Invalid chunk signature: {0}")]
    InvalidSignature(String),

    /// Unsupported chunk type
    #[error("Unsupported chunk type: {0}")]
    UnsupportedType(ChunkTypeId),

    /// Wire buffer underrun
    #[error(transparent)]
    Underrun(#[from] crate::wire::Underrun),
}

impl ChunkError {
    /// Construct an [`InvalidSize`](Self::InvalidSize) error
    pub const fn invalid_size(message: &'static str, expected: usize, actual: usize) -> Self {
        Self::InvalidSize {
            message,
            expected,
            actual,
        }
    }

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

    /// Construct an [`UnsupportedType`](Self::UnsupportedType) error
    pub const fn unsupported_type(type_id: ChunkTypeId) -> Self {
        Self::UnsupportedType(type_id)
    }
}

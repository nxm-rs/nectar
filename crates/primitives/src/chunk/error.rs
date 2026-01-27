use crate::SwarmAddress;
use thiserror::Error;

use super::type_id::ChunkTypeId;

/// Result type for chunk operations
pub(crate) type Result<T> = std::result::Result<T, ChunkError>;

/// Errors specific to chunk operations
#[derive(Error, Debug)]
pub enum ChunkError {
    /// Chunk size is invalid
    #[error("Invalid chunk size: {message} (expected: {expected}, got: {actual})")]
    InvalidSize {
        message: &'static str,
        expected: usize,
        actual: usize,
    },

    /// Chunk format is invalid
    #[error("Invalid chunk format: {0}")]
    InvalidFormat(String),

    /// Chunk address verification failed
    #[error("Chunk address verification failed: expected {expected}, got {actual}")]
    VerificationFailed {
        expected: SwarmAddress,
        actual: SwarmAddress,
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
}

impl ChunkError {
    pub fn invalid_size(message: &'static str, expected: usize, actual: usize) -> Self {
        Self::InvalidSize {
            message,
            expected,
            actual,
        }
    }

    pub fn invalid_format<S: Into<String>>(msg: S) -> Self {
        Self::InvalidFormat(msg.into())
    }

    pub fn verification_failed(expected: SwarmAddress, actual: SwarmAddress) -> Self {
        Self::VerificationFailed { expected, actual }
    }

    pub fn invalid_signature<S: Into<String>>(msg: S) -> Self {
        Self::InvalidSignature(msg.into())
    }

    pub fn unsupported_type(type_id: ChunkTypeId) -> Self {
        Self::UnsupportedType(type_id)
    }
}

//! Encryption error types.

use thiserror::Error;

/// Errors from encryption operations.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum EncryptionError {
    /// Input data is shorter than the required minimum.
    #[error("data too short: {len} bytes, minimum {min}")]
    DataTooShort {
        /// Actual length.
        len: usize,
        /// Minimum required length.
        min: usize,
    },

    /// Input data exceeds the maximum allowed length.
    #[error("data too long: {len} bytes, maximum {max}")]
    DataTooLong {
        /// Actual length.
        len: usize,
        /// Maximum allowed length.
        max: usize,
    },

    /// Output buffer is too small for decryption.
    #[error("output buffer too small: {len} bytes, need {required}")]
    OutputBufferTooSmall {
        /// Actual buffer length.
        len: usize,
        /// Required buffer length.
        required: usize,
    },
}

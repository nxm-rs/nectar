//! Error types for postage issuing operations.

use thiserror::Error;

/// Errors that can occur when signing stamps.
#[derive(Debug, Error)]
pub enum SigningError {
    /// A stamp-related error occurred.
    #[error(transparent)]
    Stamp(#[from] nectar_postage::StampError),

    /// Signing operation failed.
    #[error(transparent)]
    Signer(#[from] alloy_signer::Error),
}

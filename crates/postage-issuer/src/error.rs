//! Error types for postage issuing operations.

use thiserror::Error;

/// Errors that can occur when constructing a stamp issuer.
#[derive(Debug, Error)]
pub enum IssuerError {
    /// Mutable batches require reserved-slot awareness that this primitive issuer cannot provide.
    #[error(
        "mutable batch issuance requires reserved-slot awareness; build a nectar_postage_usage::Snapshot for the batch and stamp through Snapshot::issuer(owner) / SnapshotIssuer"
    )]
    MutableNotSupported,
}

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

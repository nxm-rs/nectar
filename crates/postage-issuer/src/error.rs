//! Error types for postage issuing operations.

use thiserror::Error;

/// Errors that can occur when constructing a stamp issuer.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum IssuerError {
    /// Mutable batches require reserved-slot awareness that this primitive issuer cannot provide.
    #[error(
        "mutable batch issuance requires reserved-slot awareness; build a nectar_postage_usage::Snapshot for the batch and stamp through Snapshot::issuer(owner) / SnapshotIssuer"
    )]
    MutableNotSupported,

    /// An immutable batch was given to a ring issuer.
    ///
    /// Ring issuance is overwrite-aware and only valid for a mutable batch. An
    /// immutable batch is fill-only and must use `MemoryIssuer`.
    #[error(
        "immutable batch cannot be stamped with a ring issuer; immutable batches are fill-only, use MemoryIssuer::from_batch"
    )]
    ImmutableNotSupported,

    /// Dilution may only increase the batch depth.
    #[error("batch depth may not decrease ({current} -> {requested})")]
    DepthDecrease {
        /// The current depth.
        current: u8,
        /// The requested depth.
        requested: u8,
    },

    /// A ring bucket had no unprotected slot to issue.
    ///
    /// Every slot in the bucket is reserved, so the ring cannot advance without
    /// re-emitting a protected slot. This is geometrically impossible at real
    /// batch depths and signals a malformed reservation.
    #[error("ring bucket {bucket} has no unprotected slot to issue")]
    RingExhausted {
        /// The bucket that had no unprotected slot.
        bucket: u32,
    },
}

/// Errors that can occur when signing stamps.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum SigningError {
    /// A stamp-related error occurred.
    #[error(transparent)]
    Stamp(#[from] nectar_postage::StampError),

    /// Signing operation failed.
    #[error(transparent)]
    Signer(#[from] alloy_signer::Error),
}

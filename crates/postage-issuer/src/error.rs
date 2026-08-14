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
    #[error("ring issuance failed")]
    RingExhausted(#[from] RingExhausted),

    /// The batch depth decoded from chain is not one a counter table can hold.
    #[error("invalid batch geometry")]
    Geometry(#[from] nectar_postage::StampError),
}

/// Every slot in a ring bucket is reserved, so the ring cannot advance without
/// re-emitting a protected slot. Real batch depths make this geometrically
/// impossible, so it signals a malformed reservation.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("ring bucket {bucket} has no unprotected slot to issue")]
pub struct RingExhausted {
    /// The exhausted bucket.
    pub bucket: u32,
}

impl RingExhausted {
    /// The condition for `bucket`.
    #[must_use]
    pub const fn new(bucket: u32) -> Self {
        Self { bucket }
    }
}

/// Errors that can occur when signing stamps.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum SigningError {
    /// A stamp-related error occurred.
    ///
    /// Allocation failures such as `BucketFull` consume no index; a retry is
    /// free.
    #[error(transparent)]
    Stamp(#[from] nectar_postage::StampError),

    /// Signing operation failed.
    ///
    /// The allocated index is burnt; a retry allocates a fresh one.
    #[cfg(feature = "std")]
    #[error(transparent)]
    Signer(#[from] alloy_signer::Error),

    /// The sign task ended without producing a signature (signer panic).
    ///
    /// The allocated index is burnt; a retry allocates a fresh one.
    #[error("sign task dropped before producing a signature")]
    Dropped,

    /// The pipeline stopped before admitting this address.
    ///
    /// No allocation happened; a retry is free.
    #[error("address not admitted before the pipeline stopped")]
    NotAdmitted,
}

impl SigningError {
    /// Whether the signer itself failed, as opposed to a per-item refusal.
    pub const fn is_systemic(&self) -> bool {
        #[cfg(feature = "std")]
        {
            matches!(self, Self::Signer(_) | Self::Dropped)
        }
        #[cfg(not(feature = "std"))]
        {
            matches!(self, Self::Dropped)
        }
    }
}

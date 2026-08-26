//! Error types for postage operations.

use crate::BatchId;
use alloy_primitives::Address;
use nectar_primitives::{ChunkAddress, error::BoxedError, wire::Underrun};
use thiserror::Error;

/// Errors that can occur when working with stamps.
///
/// No `Clone`, `PartialEq` or `Eq`: the `External` variant holds a boxed
/// source that is none of them.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum StampError {
    /// The owner recovered from the signature doesn't match the batch owner.
    #[error("owner mismatch: expected {expected}, got {actual}")]
    OwnerMismatch {
        /// The expected owner address.
        expected: Address,
        /// The actual owner recovered from the signature.
        actual: Address,
    },

    /// The stamp index exceeds the maximum allowed for the batch depth.
    #[error("invalid index: index exceeds batch capacity")]
    InvalidIndex,

    /// The chunk address doesn't match the expected collision bucket.
    #[error("bucket mismatch: chunk address doesn't belong to stamp bucket")]
    BucketMismatch,

    /// The bucket depth is outside the range a bucket key can address.
    #[error("invalid bucket depth {bucket_depth}: must be in 1..=32")]
    InvalidBucketDepth {
        /// The rejected bucket depth.
        bucket_depth: u8,
    },

    /// The bucket depth is below the minimum the network spec sets.
    #[error("bucket depth {bucket_depth} below the spec minimum {minimum}")]
    BucketDepthBelowMinimum {
        /// The rejected bucket depth.
        bucket_depth: u8,
        /// The minimum the spec sets.
        minimum: u8,
    },

    /// The batch depth leaves no room above the bucket depth.
    #[error("batch depth {depth} below bucket depth {bucket_depth}")]
    DepthBelowBucketDepth {
        /// The rejected batch depth.
        depth: u8,
        /// The bucket depth it has to reach.
        bucket_depth: u8,
    },

    /// The stamp names a different batch from the one it was checked against.
    #[error("batch mismatch: expected {expected}, got {actual}")]
    BatchMismatch {
        /// The batch the stamp was checked against.
        expected: BatchId,
        /// The batch the stamp names.
        actual: BatchId,
    },

    /// The batch depth exceeds the bucket depth by more bits than a `u32` slot
    /// count holds.
    #[error("batch depth {depth} exceeds bucket depth {bucket_depth} by more than {max} bits")]
    SlotsTooWide {
        /// The rejected batch depth.
        depth: u8,
        /// The bucket depth beneath it.
        bucket_depth: u8,
        /// The widest difference a slot count holds.
        max: u8,
    },

    /// Invalid stamp data format.
    #[error("invalid stamp data: {0}")]
    InvalidData(&'static str),

    /// The batch bucket is full and cannot accept more chunks.
    #[error("bucket full: bucket {bucket} has reached capacity {capacity}")]
    BucketFull {
        /// The bucket that is full.
        bucket: u32,
        /// Maximum capacity of the bucket.
        capacity: u32,
    },

    /// Signature verification failed.
    #[error("invalid signature")]
    InvalidSignature,

    /// The chunk offered does not match the address the slot was allocated
    /// for.
    #[error("address mismatch: slot allocated for {expected}, offered {offered}")]
    AddressMismatch {
        /// The address the slot was allocated for.
        expected: ChunkAddress,
        /// The address of the chunk offered.
        offered: ChunkAddress,
    },

    /// The wire buffer ended before a field was fully read.
    #[error("buffer underrun: need {expected} bytes, have {available}")]
    Underrun {
        /// Bytes the field required.
        expected: usize,
        /// Bytes remaining in the buffer.
        available: usize,
    },

    /// A chunk operation in `nectar-primitives` failed (for example decoding or
    /// address verification of the chunk half of a stamped chunk).
    #[error("chunk error: {0}")]
    Chunk(&'static str),

    /// An error produced beyond this crate's boundary, by a crate that depends
    /// on it.
    ///
    /// The concrete error is kept as the source so its message and type survive
    /// the boundary for logging and downcast.
    #[error("stamp crate failure")]
    External(#[source] BoxedError),
}

impl From<Underrun> for StampError {
    fn from(underrun: Underrun) -> Self {
        Self::Underrun {
            expected: underrun.expected,
            available: underrun.available,
        }
    }
}

/// The denial side of the admission composite, one variant per way a stamped
/// chunk is turned away.
///
/// The context group reads the store-level block height and cumulative
/// payout, so its answer can change as the context moves. The signature and
/// geometry answers are deterministic facts and deny the same way every
/// time.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum AdmissionError {
    /// The store holds no batch for the id the stamp claims.
    ///
    /// The store-free composite never yields this: the caller's own load
    /// miss produces it.
    #[error("unknown batch {0}")]
    UnknownBatch(BatchId),

    /// The batch has not accumulated enough confirmations at the context's
    /// block height.
    #[error("batch not usable")]
    BatchNotUsable,

    /// The batch value sits at or below the context's cumulative payout.
    #[error("batch expired")]
    BatchExpired,

    /// The signature recovered to someone who is not the batch owner.
    #[error("owner mismatch")]
    OwnerMismatch,

    /// A geometry or signature failure from the leaf predicate.
    #[error("stamp rejected")]
    Stamp(#[from] StampError),
}

impl AdmissionError {
    /// Whether the outcome can change as the chain context moves.
    ///
    /// A denial that is true of the context, not of the chunk. The context
    /// group answers `true` because a later block height or a catch-up store
    /// can admit what it denied; the signature and geometry answers answer
    /// `false` because they are facts of the bytes.
    pub const fn is_context_dependent(&self) -> bool {
        match self {
            Self::UnknownBatch(_) | Self::BatchNotUsable | Self::BatchExpired => true,
            Self::OwnerMismatch | Self::Stamp(_) => false,
        }
    }
}

#[cfg(test)]
mod admission_error_tests {
    use super::*;
    use core::error::Error as _;

    /// The compiler forces this match to cover every variant, so a new
    /// variant cannot be added without a classification decision here.
    fn expected_group(err: &AdmissionError) -> bool {
        match err {
            AdmissionError::UnknownBatch(_)
            | AdmissionError::BatchNotUsable
            | AdmissionError::BatchExpired => true,
            AdmissionError::OwnerMismatch | AdmissionError::Stamp(_) => false,
        }
    }

    #[test]
    fn every_variant_is_classified_into_exactly_one_group() {
        let variants = [
            AdmissionError::UnknownBatch(BatchId::ZERO),
            AdmissionError::BatchNotUsable,
            AdmissionError::BatchExpired,
            AdmissionError::OwnerMismatch,
            AdmissionError::Stamp(StampError::InvalidSignature),
        ];
        for err in &variants {
            assert_eq!(
                err.is_context_dependent(),
                expected_group(err),
                "{err:?} is misclassified"
            );
        }
    }

    #[test]
    fn the_stamp_group_carries_its_source() {
        let err = AdmissionError::from(StampError::BucketFull {
            bucket: 0,
            capacity: 0,
        });
        assert!(matches!(err, AdmissionError::Stamp(_)));
        let source = err.source().expect("the wrap carries its source");
        assert!(source.downcast_ref::<StampError>().is_some());
        assert!(!err.is_context_dependent());
    }
}

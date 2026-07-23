//! Error types for postage operations.

use crate::BatchId;
use alloy_primitives::Address;
use nectar_primitives::wire::Underrun;
use thiserror::Error;

/// Errors that can occur when working with stamps.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Error)]
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

    /// The batch was not found.
    #[error("batch not found: {0}")]
    BatchNotFound(BatchId),

    /// The batch is not yet usable (needs more confirmations).
    #[error(
        "batch not usable: created at block {created}, current block {current}, need {threshold} confirmations"
    )]
    BatchNotUsable {
        /// Block when batch was created.
        created: u64,
        /// Current block number.
        current: u64,
        /// Required confirmations.
        threshold: u64,
    },

    /// The batch has expired.
    #[error("batch expired: value {value} <= total_amount {total_amount}")]
    BatchExpired {
        /// Current batch value.
        value: u128,
        /// Total amount consumed.
        total_amount: u128,
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
    ///
    /// The variant carries a `&'static str` context rather than embedding the
    /// underlying [`nectar_primitives::PrimitivesError`]: [`StampError`] is
    /// `Clone`, `PartialEq` and `Eq`, whereas `PrimitivesError` is none of these
    /// (it carries `std::io::Error` among others), and this crate is `no_std`
    /// without `alloc`, so an owned `String` message is not available either.
    #[error("chunk error: {0}")]
    Chunk(&'static str),
}

impl From<Underrun> for StampError {
    fn from(underrun: Underrun) -> Self {
        Self::Underrun {
            expected: underrun.expected,
            available: underrun.available,
        }
    }
}

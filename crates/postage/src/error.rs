//! Error types for postage operations.

use crate::BatchId;
use alloy_primitives::Address;
use thiserror::Error;

/// Errors that can occur when working with stamps.
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
}

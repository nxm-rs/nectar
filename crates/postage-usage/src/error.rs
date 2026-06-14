//! Error types for usage table operations and the snapshot codec.

use thiserror::Error;

/// Errors produced by usage table operations and snapshot encoding/decoding.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum UsageError {
    /// The batch geometry is outside the range supported by the format.
    #[error("unsupported batch geometry: depth {depth}, bucket depth {bucket_depth}")]
    InvalidGeometry {
        /// The batch depth.
        depth: u8,
        /// The bucket (uniformity) depth.
        bucket_depth: u8,
    },

    /// A bucket index is outside the table's bucket range.
    #[error("bucket {bucket} out of range")]
    InvalidBucket {
        /// The offending bucket index.
        bucket: u32,
    },

    /// A bucket has no remaining storage slots.
    #[error("bucket {bucket} has reached capacity {capacity}")]
    BucketFull {
        /// The full bucket.
        bucket: u32,
        /// The bucket capacity.
        capacity: u32,
    },

    /// A counter exceeds the per-bucket slot capacity.
    #[error("counter {count} for bucket {bucket} exceeds capacity {capacity}")]
    CounterOverflow {
        /// The offending bucket.
        bucket: u32,
        /// The counter value.
        count: u32,
        /// The bucket capacity.
        capacity: u32,
    },

    /// A counter vector does not match the bucket count.
    #[error("expected {expected} counters, got {got}")]
    CounterLength {
        /// The expected number of counters.
        expected: usize,
        /// The number of counters provided.
        got: usize,
    },

    /// Dilution may only increase the batch depth.
    #[error("batch depth may not decrease ({current} -> {requested})")]
    DepthDecrease {
        /// The current depth.
        current: u8,
        /// The requested depth.
        requested: u8,
    },

    /// The operands describe different batches.
    #[error("operands describe different batches")]
    BatchMismatch,

    /// `merge_max` was called with a mutable table on either side. A ring
    /// cursor is not monotone (it falls on wrap), so the elementwise maximum
    /// is not a valid join; mutable divergence is resolved by sequence.
    #[error("merge_max is not defined for mutable batches")]
    MutableMerge,

    /// A mutable bucket has no free ring slot because every slot is reserved
    /// by the snapshot's own chunks. The batch geometry forbids this, so it
    /// signals an internal inconsistency rather than an expected condition.
    #[error("mutable bucket {bucket} has no free ring slot")]
    RingExhausted {
        /// The exhausted bucket.
        bucket: u32,
    },

    /// A within-bucket slot index is outside the bucket capacity.
    #[error("slot {slot} exceeds bucket capacity {capacity}")]
    InvalidSlot {
        /// The offending slot index.
        slot: u32,
        /// The bucket capacity.
        capacity: u32,
    },

    /// A payload has the wrong length for its declared structure.
    #[error("payload length mismatch: expected {expected} bytes, got {got}")]
    PayloadLength {
        /// The expected byte length.
        expected: usize,
        /// The byte length provided.
        got: usize,
    },

    /// The root payload does not start with the snapshot magic.
    #[error("bad snapshot magic")]
    BadMagic,

    /// The root payload violates a structural rule of the format.
    #[error("malformed snapshot: {0}")]
    Malformed(&'static str),

    /// A leaf payload does not match the digest committed in the root.
    #[error("leaf {index} digest mismatch")]
    LeafDigestMismatch {
        /// The zero-based leaf index.
        index: u16,
    },

    /// A leaf payload has the wrong length.
    #[error("leaf {index} length mismatch: expected {expected} bytes, got {got}")]
    LeafLength {
        /// The zero-based leaf index.
        index: u16,
        /// The expected byte length.
        expected: usize,
        /// The byte length provided.
        got: usize,
    },

    /// The number of leaf payloads does not match the root.
    #[error("expected {expected} leaves, got {got}")]
    LeafCount {
        /// The expected number of leaves.
        expected: usize,
        /// The number of leaves provided.
        got: usize,
    },

    /// The issued total in the header does not equal the counter sum.
    #[error("issued total mismatch: header says {header}, counters sum to {sum}")]
    IssuedMismatch {
        /// The total declared in the root header.
        header: u64,
        /// The sum of the reconstructed counters.
        sum: u64,
    },

    /// A planned persist's next sequence does not strictly exceed the published
    /// floor the consumer read from the live network, so publishing it would
    /// overwrite a newer published snapshot in place.
    #[error("persist sequence {next} does not exceed published floor {floor}")]
    StaleSequence {
        /// The sequence the persist would have published.
        next: u64,
        /// The published floor read live from the network.
        floor: u64,
    },
}

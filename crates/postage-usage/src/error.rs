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

impl UsageError {
    /// Returns whether the error means the bytes a decode was handed are bad or
    /// stale: the snapshot the network served cannot be trusted as a faithful
    /// copy of what was published.
    ///
    /// A corruption error is not something the caller can fix by adjusting its
    /// own input: the payload itself is wrong (a forged or truncated chunk, a
    /// leaf that does not match the digest the root committed to, a counter sum
    /// that does not add up). The right response is to refetch the chunk, or, if
    /// it keeps failing verification, to treat that snapshot version as
    /// unrecoverable rather than issue from it. These are exactly the errors
    /// [`is_recoverable`](Self::is_recoverable) reports `false` for among the
    /// decode failures.
    ///
    /// The set is the decode-time integrity failures: [`BadMagic`](Self::BadMagic),
    /// [`Malformed`](Self::Malformed), [`LeafDigestMismatch`](Self::LeafDigestMismatch),
    /// [`IssuedMismatch`](Self::IssuedMismatch), and the length and count
    /// mismatches [`PayloadLength`](Self::PayloadLength),
    /// [`LeafLength`](Self::LeafLength), and [`LeafCount`](Self::LeafCount).
    pub const fn is_corruption(&self) -> bool {
        match self {
            // Corruption: the fetched bytes are bad or stale.
            Self::BadMagic
            | Self::Malformed(_)
            | Self::LeafDigestMismatch { .. }
            | Self::IssuedMismatch { .. }
            | Self::PayloadLength { .. }
            | Self::LeafLength { .. }
            | Self::LeafCount { .. } => true,

            // Caller-fixable or expected control flow, and the internal-invariant
            // bug: none of these say the fetched bytes are corrupt.
            Self::BucketFull { .. }
            | Self::DepthDecrease { .. }
            | Self::BatchMismatch
            | Self::MutableMerge
            | Self::StaleSequence { .. }
            | Self::InvalidGeometry { .. }
            | Self::CounterLength { .. }
            | Self::InvalidBucket { .. }
            | Self::InvalidSlot { .. }
            | Self::CounterOverflow { .. }
            | Self::RingExhausted { .. } => false,
        }
    }

    /// Returns whether the caller can do something about the error: fix the
    /// input it supplied, or back off and retry under different conditions.
    ///
    /// This is `true` for the caller-fixable and expected-control-flow errors and
    /// `false` for both corruption and internal-invariant errors, so the three
    /// groups partition cleanly:
    ///
    /// - **Recoverable** (`true`): the input or the timing is at fault and the
    ///   caller can change it. A full bucket ([`BucketFull`](Self::BucketFull)),
    ///   a depth that would decrease ([`DepthDecrease`](Self::DepthDecrease)),
    ///   mismatched batches ([`BatchMismatch`](Self::BatchMismatch)), a mutable
    ///   merge ([`MutableMerge`](Self::MutableMerge)), a stale persist
    ///   ([`StaleSequence`](Self::StaleSequence): reread the live floor and
    ///   retry), an unsupported geometry ([`InvalidGeometry`](Self::InvalidGeometry)),
    ///   a wrong counter vector length ([`CounterLength`](Self::CounterLength)),
    ///   an out-of-range bucket ([`InvalidBucket`](Self::InvalidBucket)) or slot
    ///   ([`InvalidSlot`](Self::InvalidSlot)), or a counter past capacity
    ///   ([`CounterOverflow`](Self::CounterOverflow)).
    /// - **Corruption** (`false`): the fetched bytes are bad or stale; see
    ///   [`is_corruption`](Self::is_corruption). The caller cannot fix the input,
    ///   only refetch or abandon the version.
    /// - **Internal invariant** (`false`): [`RingExhausted`](Self::RingExhausted)
    ///   cannot occur for any supported geometry, so reaching it is a bug to
    ///   report, not something a caller can recover from.
    pub const fn is_recoverable(&self) -> bool {
        match self {
            // Caller-fixable or expected control flow: the caller can change its
            // input or back off and retry.
            Self::BucketFull { .. }
            | Self::DepthDecrease { .. }
            | Self::BatchMismatch
            | Self::MutableMerge
            | Self::StaleSequence { .. }
            | Self::InvalidGeometry { .. }
            | Self::CounterLength { .. }
            | Self::InvalidBucket { .. }
            | Self::InvalidSlot { .. }
            | Self::CounterOverflow { .. } => true,

            // Corruption: refetch or treat the snapshot as unrecoverable. Nothing
            // the caller adjusts about its own input changes the outcome.
            Self::BadMagic
            | Self::Malformed(_)
            | Self::LeafDigestMismatch { .. }
            | Self::IssuedMismatch { .. }
            | Self::PayloadLength { .. }
            | Self::LeafLength { .. }
            | Self::LeafCount { .. } => false,

            // Internal invariant: a bug to report, not a recoverable condition.
            Self::RingExhausted { .. } => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::UsageError;

    #[test]
    fn classification_of_a_representative_from_each_group() {
        // Corruption: bad bytes, not recoverable by the caller.
        let corruption = UsageError::BadMagic;
        assert!(corruption.is_corruption());
        assert!(!corruption.is_recoverable());

        // Caller-fixable / expected control flow: recoverable, not corruption.
        let recoverable = UsageError::StaleSequence { next: 1, floor: 1 };
        assert!(!recoverable.is_corruption());
        assert!(recoverable.is_recoverable());

        // Internal invariant: neither corruption nor recoverable.
        let internal = UsageError::RingExhausted { bucket: 0 };
        assert!(!internal.is_corruption());
        assert!(!internal.is_recoverable());
    }

    /// Guards that every variant is classified into exactly one of the three
    /// groups. The match is exhaustive, so adding a variant without slotting it
    /// here fails to compile, and the assertions confirm the groups stay
    /// disjoint: corruption and recoverable are never both true, and every
    /// variant lands in corruption, recoverable, or the internal-invariant
    /// remainder.
    #[test]
    fn every_variant_is_classified_into_exactly_one_group() {
        fn assert_classified(err: &UsageError) {
            let corruption = err.is_corruption();
            let recoverable = err.is_recoverable();
            // No variant is both corrupt and caller-recoverable.
            assert!(
                !(corruption && recoverable),
                "{err:?} is both corruption and recoverable"
            );
            // A variant that is neither corruption nor recoverable is the
            // internal-invariant group; assert it really is the only member, so a
            // future unclassified variant cannot hide here silently.
            if !corruption && !recoverable {
                assert!(
                    matches!(err, UsageError::RingExhausted { .. }),
                    "{err:?} is unclassified: neither corruption, recoverable, nor the known internal invariant"
                );
            }
        }

        // The compiler forces this match to cover every variant, so a new
        // variant cannot be added without listing it here and giving it a
        // representative value to classify.
        let variants = [
            UsageError::InvalidGeometry {
                depth: 0,
                bucket_depth: 0,
            },
            UsageError::InvalidBucket { bucket: 0 },
            UsageError::BucketFull {
                bucket: 0,
                capacity: 0,
            },
            UsageError::CounterOverflow {
                bucket: 0,
                count: 0,
                capacity: 0,
            },
            UsageError::CounterLength {
                expected: 0,
                got: 0,
            },
            UsageError::DepthDecrease {
                current: 0,
                requested: 0,
            },
            UsageError::BatchMismatch,
            UsageError::MutableMerge,
            UsageError::RingExhausted { bucket: 0 },
            UsageError::InvalidSlot {
                slot: 0,
                capacity: 0,
            },
            UsageError::PayloadLength {
                expected: 0,
                got: 0,
            },
            UsageError::BadMagic,
            UsageError::Malformed("x"),
            UsageError::LeafDigestMismatch { index: 0 },
            UsageError::LeafLength {
                index: 0,
                expected: 0,
                got: 0,
            },
            UsageError::LeafCount {
                expected: 0,
                got: 0,
            },
            UsageError::IssuedMismatch { header: 0, sum: 0 },
            UsageError::StaleSequence { next: 0, floor: 0 },
        ];

        // Exhaustiveness guard: this match must name every variant, so a new
        // variant added to `UsageError` will not compile until it is handled.
        // The arms double-check the array above covers each group.
        for err in &variants {
            match err {
                UsageError::BadMagic
                | UsageError::Malformed(_)
                | UsageError::LeafDigestMismatch { .. }
                | UsageError::IssuedMismatch { .. }
                | UsageError::PayloadLength { .. }
                | UsageError::LeafLength { .. }
                | UsageError::LeafCount { .. } => {
                    assert!(err.is_corruption() && !err.is_recoverable());
                }
                UsageError::BucketFull { .. }
                | UsageError::DepthDecrease { .. }
                | UsageError::BatchMismatch
                | UsageError::MutableMerge
                | UsageError::StaleSequence { .. }
                | UsageError::InvalidGeometry { .. }
                | UsageError::CounterLength { .. }
                | UsageError::InvalidBucket { .. }
                | UsageError::InvalidSlot { .. }
                | UsageError::CounterOverflow { .. } => {
                    assert!(!err.is_corruption() && err.is_recoverable());
                }
                UsageError::RingExhausted { .. } => {
                    assert!(!err.is_corruption() && !err.is_recoverable());
                }
            }
            assert_classified(err);
        }
    }
}

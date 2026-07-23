//! In-memory per-bucket slot counters for a postage batch.

use alloc::vec::Vec;

use nectar_postage::{Batch, BatchId};
use nectar_postage_issuer::{CounterError, CounterMode, CounterTable};

use crate::{MAX_BUCKET_DEPTH, MAX_COUNTER_BITS, Result, UsageError};

/// Maps a shared-counter-table error onto a usage error. The usage table only
/// ever feeds the shared table valid lengths and geometries, so the construction
/// variants surface as their usage equivalents and the issuance variants cannot
/// arise here.
pub(crate) const fn map_counter_error(err: CounterError) -> UsageError {
    match err {
        CounterError::InvalidBucket { bucket } => UsageError::InvalidBucket { bucket },
        CounterError::BucketFull { bucket, capacity } => {
            UsageError::BucketFull { bucket, capacity }
        }
        CounterError::RingExhausted { bucket } => UsageError::RingExhausted { bucket },
        CounterError::CounterLength { expected, got } => {
            UsageError::CounterLength { expected, got }
        }
        CounterError::CounterOverflow {
            bucket,
            count,
            capacity,
        } => UsageError::CounterOverflow {
            bucket,
            count,
            capacity,
        },
        _ => UsageError::Malformed("unexpected counter error"),
    }
}

/// Validates that a batch geometry is within the range supported by the
/// snapshot format: `1 <= bucket_depth <= 16` and `depth - bucket_depth <= 31`.
///
/// A zero bucket depth is rejected: a batch with no collision buckets is
/// meaningless, and `nectar_postage::calculate_bucket` shifts a `u32` right by
/// `32 - bucket_depth`, so `bucket_depth == 0` would overflow the shift on the
/// persist and issue paths.
// `depth - bucket_depth` is short-circuit guarded by the preceding
// `depth < bucket_depth` disjunct, so it cannot underflow.
#[allow(clippy::arithmetic_side_effects)]
pub(crate) const fn validate_geometry(depth: u8, bucket_depth: u8) -> Result<()> {
    if bucket_depth == 0
        || bucket_depth > MAX_BUCKET_DEPTH
        || depth < bucket_depth
        || depth - bucket_depth > MAX_COUNTER_BITS
    {
        return Err(UsageError::InvalidGeometry {
            depth,
            bucket_depth,
        });
    }
    Ok(())
}

/// Whether a usage table is an immutable fill watermark or a mutable ring.
///
/// The fill-or-ring choice is a runtime flag, not a type parameter: the SBU1
/// codec decodes it from the wire at runtime (see
/// [`RootInfo::assemble`](crate::RootInfo::assemble)), so a type-state parameter
/// would fight the wire decode. This enum makes the choice an explicit, named
/// argument to the table constructors instead of a bare bool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Mutability {
    /// `counts[b]` is a monotone fill watermark, the next unused index; issuance
    /// advances it and a full bucket fails rather than overwriting.
    #[default]
    Immutable,
    /// `counts[b]` is a ring cursor in `[0, capacity]` that wraps at capacity, so
    /// a full bucket churns instead of failing.
    Mutable,
}

impl Mutability {
    /// Picks the mutability from a batch: an immutable batch yields the fill
    /// watermark table, a mutable batch (`batch.immutable() == false`) yields the
    /// wrapping ring.
    pub const fn from_batch(batch: &Batch) -> Self {
        if batch.immutable() {
            Self::Immutable
        } else {
            Self::Mutable
        }
    }

    /// Returns whether this is the mutable (ring) variant.
    pub const fn is_mutable(self) -> bool {
        matches!(self, Self::Mutable)
    }

    /// Maps the mutability onto the shared counter table's mode.
    const fn mode(self) -> CounterMode {
        match self {
            Self::Immutable => CounterMode::Fill,
            Self::Mutable => CounterMode::Ring,
        }
    }
}

/// Per-bucket slot counters for a postage batch.
///
/// For each of the `2^bucket_depth` collision buckets, this records the state
/// needed to issue collision-free stamps into its `2^(depth - bucket_depth)`
/// slots. The table is an inert counters-and-geometry value: it has no method
/// that advances a counter. Issuance happens only through
/// [`Snapshot::issuer`](crate::Snapshot::issuer), which installs the reserved
/// slots first, so a bare table can never evict the chunks that record the
/// batch state.
///
/// The fill-or-ring choice is a runtime [`Mutability`] flag passed to
/// [`new`](Self::new) and [`from_counts`](Self::from_counts), or derived from a
/// batch by [`from_batch`](Self::from_batch):
///
/// - **Immutable** ([`Mutability::Immutable`]): `counts[b]` is a monotone fill
///   watermark, the next unused index; issuance advances it and a full bucket
///   fails rather than overwriting.
/// - **Mutable** ([`Mutability::Mutable`]): `counts[b]` is a ring cursor in
///   `[0, capacity]` that wraps at capacity, so a full bucket churns instead of
///   failing. The reserved slots that the ring skips live on the issuing handle,
///   not the table, so they cannot be lost when the table is moved.
///
/// A bare table cannot issue: it has no reserved-blind `record` mutator, so
/// counter advances that skip reserved installation are a compile error, not a
/// runtime check.
///
/// ```compile_fail
/// use nectar_postage_usage::{BatchId, Mutability, UsageTable};
///
/// let mut table = UsageTable::new(BatchId::new([0x42; 32]), 18, 16, Mutability::Mutable).unwrap();
/// // `record` no longer exists on the inert table.
/// table.record(7).unwrap();
/// ```
///
/// ```compile_fail
/// use alloy_primitives::B256;
/// use nectar_postage_usage::{BatchId, Mutability, ChunkAddress, UsageTable};
///
/// let mut table = UsageTable::new(BatchId::new([0x42; 32]), 18, 16, Mutability::Mutable).unwrap();
/// // `record_address` no longer exists on the inert table.
/// table.record_address(&ChunkAddress::from(B256::repeat_byte(0x99))).unwrap();
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageTable {
    pub(crate) batch_id: BatchId,
    /// The shared per-bucket counter table. It holds the counters, the issued
    /// sum, the geometry, and the fill-or-ring mode, in the same `[0, capacity]`
    /// representation the wire format serializes. The snapshot wraps this table
    /// rather than carrying its own copy of the counter logic.
    pub(crate) counters: CounterTable,
}

impl UsageTable {
    /// Creates an empty table for a batch with the given geometry and
    /// [`Mutability`].
    ///
    /// An immutable table is a monotone fill watermark; a mutable table is a
    /// per-bucket ring that wraps once a bucket fills. The ring skips the
    /// snapshot's reserved slots, which are installed onto the issuing handle by
    /// [`Snapshot::issuer`](crate::Snapshot::issuer), so it never evicts the
    /// snapshot's own chunks.
    pub fn new(
        batch_id: BatchId,
        depth: u8,
        bucket_depth: u8,
        mutability: Mutability,
    ) -> Result<Self> {
        validate_geometry(depth, bucket_depth)?;
        Ok(Self {
            batch_id,
            counters: CounterTable::new(depth, bucket_depth, mutability.mode()),
        })
    }

    /// Creates an empty table whose geometry and [`Mutability`] are read from a
    /// [`Batch`].
    ///
    /// An immutable batch yields a fill-watermark table; a mutable batch
    /// (`batch.immutable() == false`) yields a wrapping ring. This lets a caller
    /// holding a `Batch` build the matching table without restating the geometry
    /// or polarity by hand.
    pub fn from_batch(batch: &Batch) -> Result<Self> {
        Self::new(
            batch.id(),
            batch.depth(),
            batch.bucket_depth().get(),
            Mutability::from_batch(batch),
        )
    }

    /// Creates a table from existing counters with the given [`Mutability`].
    ///
    /// `counts` must hold exactly `2^bucket_depth` entries, each in
    /// `[0, capacity]`. For a mutable table the reserved slots are installed by
    /// [`Snapshot::issuer`](crate::Snapshot::issuer) before issuing.
    pub fn from_counts(
        batch_id: BatchId,
        depth: u8,
        bucket_depth: u8,
        counts: Vec<u32>,
        mutability: Mutability,
    ) -> Result<Self> {
        validate_geometry(depth, bucket_depth)?;
        let counters = CounterTable::from_counts(depth, bucket_depth, mutability.mode(), counts)
            .map_err(map_counter_error)?;
        Ok(Self { batch_id, counters })
    }

    /// Returns the batch id this table describes.
    pub const fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    /// Returns whether this table is a mutable ring (`true`) or an immutable
    /// fill watermark (`false`).
    pub const fn is_mutable(&self) -> bool {
        self.counters.is_ring()
    }

    /// Returns the batch depth.
    pub const fn depth(&self) -> u8 {
        self.counters.depth()
    }

    /// Returns the bucket (uniformity) depth.
    pub const fn bucket_depth(&self) -> u8 {
        self.counters.bucket_depth()
    }

    /// Returns the number of collision buckets (`2^bucket_depth`).
    pub const fn bucket_count(&self) -> u32 {
        self.counters.bucket_count()
    }

    /// Returns the number of slots per bucket (`2^(depth - bucket_depth)`).
    pub const fn bucket_capacity(&self) -> u32 {
        self.counters.bucket_capacity()
    }

    /// Returns the total batch capacity in slots (`2^depth`).
    pub const fn total_capacity(&self) -> u64 {
        self.counters.total_capacity()
    }

    /// Returns the sum of the per-bucket counters.
    ///
    /// Immutable: the lifetime count of stamps issued. Mutable: the counters are
    /// ring cursors, so this is a deterministic checksum, not a lifetime count
    /// (a wrapped bucket is full yet its cursor may be small). The codec writes
    /// and verifies it as a checksum in both modes.
    pub const fn total_issued(&self) -> u64 {
        self.counters.total_issued()
    }

    /// Returns the per-bucket counters.
    pub fn counts(&self) -> &[u32] {
        self.counters.counts()
    }

    /// Returns the counter of a bucket.
    pub fn count(&self, bucket: u32) -> Result<u32> {
        self.counters.count(bucket).map_err(map_counter_error)
    }

    /// Returns the highest counter across all buckets.
    pub fn max_count(&self) -> u32 {
        self.counters.max_count()
    }

    /// Returns the lowest counter across all buckets.
    pub fn min_count(&self) -> u32 {
        self.counters.min_count()
    }

    /// Returns whether a bucket can accept another slot assignment.
    pub fn has_capacity(&self, bucket: u32) -> Result<bool> {
        self.counters
            .has_capacity(bucket)
            .map_err(map_counter_error)
    }

    /// Returns the shared counter table backing this usage table, for the
    /// snapshot's counter-advance and merge paths.
    pub(crate) const fn counters(&self) -> &CounterTable {
        &self.counters
    }

    /// Returns a mutable reference to the shared counter table, the snapshot's
    /// single counter-advance handle.
    pub(crate) const fn counters_mut(&mut self) -> &mut CounterTable {
        &mut self.counters
    }

    /// Increases the batch depth after an on-chain dilution.
    ///
    /// Counters are unchanged; only the per-bucket capacity grows. The new
    /// depth must not decrease and must stay within the supported geometry.
    /// Exposed to callers through [`Snapshot::dilute`](crate::Snapshot::dilute).
    pub(crate) fn dilute(&mut self, new_depth: u8) -> Result<()> {
        let current = self.counters.depth();
        if new_depth < current {
            return Err(UsageError::DepthDecrease {
                current,
                requested: new_depth,
            });
        }
        validate_geometry(new_depth, self.counters.bucket_depth())?;
        // The geometry is validated against the snapshot format above, so the
        // shared table only needs to adopt the new depth.
        self.counters.set_depth(new_depth);
        Ok(())
    }

    /// Merges another table into this one by taking the elementwise maximum of
    /// the counters and the maximum depth.
    ///
    /// Immutable only: monotone counters make the elementwise maximum a
    /// well-defined join for reconciling divergent copies (it cannot undo two
    /// writers issuing the same index). Rejects with
    /// [`MutableMerge`](UsageError::MutableMerge) if either table is mutable: a
    /// ring cursor falls on wrap, so it has no maximum-based join, and mutable
    /// divergence is a conflict surfaced by the snapshot sequence number.
    /// Exposed to callers through
    /// [`Snapshot::merge_max`](crate::Snapshot::merge_max).
    pub(crate) fn merge_max(&mut self, other: &Self) -> Result<()> {
        if self.is_mutable() || other.is_mutable() {
            return Err(UsageError::MutableMerge);
        }
        if self.batch_id != other.batch_id || self.bucket_depth() != other.bucket_depth() {
            return Err(UsageError::BatchMismatch);
        }
        let depth = self.depth().max(other.depth());
        validate_geometry(depth, self.bucket_depth())?;
        self.counters.merge_counts_max(other.counters(), depth);
        Ok(())
    }
}

/// A borrowed, read-only window onto a [`UsageTable`].
///
/// This is what [`Snapshot::table`](crate::Snapshot::table) and
/// [`SnapshotParts::table`](crate::SnapshotParts::table) hand out. It exposes the
/// counters and geometry a caller legitimately needs to inspect (utilization,
/// depth, mutability) but deliberately yields no owned [`UsageTable`]: it only
/// borrows the table and does not [`Deref`](core::ops::Deref) to it, so cloning
/// or copying the view produces another borrowed view, never an owned table that
/// [`Snapshot::new`](crate::Snapshot::new) would accept at sequence 0. Together
/// with [`SnapshotParts`](crate::SnapshotParts) holding its table privately, this
/// closes the in-memory clone route that would otherwise downgrade a recovered
/// snapshot.
///
/// The view borrows the table, so it cannot outlive the snapshot it came from.
#[derive(Debug, Clone, Copy)]
pub struct TableView<'a> {
    table: &'a UsageTable,
}

impl<'a> TableView<'a> {
    pub(crate) const fn new(table: &'a UsageTable) -> Self {
        Self { table }
    }

    /// Returns the batch id this table describes.
    pub const fn batch_id(&self) -> BatchId {
        self.table.batch_id
    }

    /// Returns whether the table is a mutable ring (`true`) or an immutable fill
    /// watermark (`false`).
    pub const fn is_mutable(&self) -> bool {
        self.table.is_mutable()
    }

    /// Returns the batch depth.
    pub const fn depth(&self) -> u8 {
        self.table.depth()
    }

    /// Returns the bucket (uniformity) depth.
    pub const fn bucket_depth(&self) -> u8 {
        self.table.bucket_depth()
    }

    /// Returns the number of collision buckets (`2^bucket_depth`).
    pub const fn bucket_count(&self) -> u32 {
        self.table.bucket_count()
    }

    /// Returns the number of slots per bucket (`2^(depth - bucket_depth)`).
    pub const fn bucket_capacity(&self) -> u32 {
        self.table.bucket_capacity()
    }

    /// Returns the total batch capacity in slots (`2^depth`).
    pub const fn total_capacity(&self) -> u64 {
        self.table.total_capacity()
    }

    /// Returns the sum of the per-bucket counters (a checksum in mutable mode).
    pub const fn total_issued(&self) -> u64 {
        self.table.total_issued()
    }

    /// Returns the per-bucket counters.
    pub fn counts(&self) -> &[u32] {
        self.table.counts()
    }

    /// Returns the counter of a bucket.
    pub fn count(&self, bucket: u32) -> Result<u32> {
        self.table.count(bucket)
    }

    /// Returns the highest counter across all buckets.
    pub fn max_count(&self) -> u32 {
        self.table.max_count()
    }

    /// Returns the lowest counter across all buckets.
    pub fn min_count(&self) -> u32 {
        self.table.min_count()
    }

    /// Returns whether a bucket can accept another slot assignment.
    pub fn has_capacity(&self, bucket: u32) -> Result<bool> {
        self.table.has_capacity(bucket)
    }
}

/// `Arbitrary` implementations that generate *valid* tables: the geometry is
/// within the format bounds ([`validate_geometry`]) and every counter is
/// within `[0, capacity]`, so a generated table always encodes, and a
/// structured fuzz target can assert a full round trip instead of merely "no
/// panic". Counters cluster around a base with a few outliers, matching the
/// frame-of-reference packing the codec applies.
#[cfg(any(test, feature = "arbitrary"))]
mod arbitrary_impls {
    use alloc::vec;
    use arbitrary::{Arbitrary, Result as ArbitraryResult, Unstructured};
    use nectar_postage::BatchId;

    use super::{Mutability, UsageTable};
    use crate::{MAX_BUCKET_DEPTH, MAX_COUNTER_BITS};

    impl<'a> Arbitrary<'a> for Mutability {
        fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
            Ok(if u.arbitrary::<bool>()? {
                Self::Mutable
            } else {
                Self::Immutable
            })
        }
    }

    impl<'a> Arbitrary<'a> for UsageTable {
        fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
            let batch_id = BatchId::new(u.arbitrary::<[u8; 32]>()?);
            // `bucket_depth == 0` (a zero-width bucket) is invalid geometry:
            // `validate_geometry` rejects it because `nectar_postage::
            // calculate_bucket` shifts a u32 right by `32 - bucket_depth`, so
            // depth 0 would overflow the shift. The range below is therefore
            // exactly the format invariant, not a generator restriction.
            let bucket_depth = u.int_in_range(1..=MAX_BUCKET_DEPTH)?;
            let counter_bits = u.int_in_range(0..=MAX_COUNTER_BITS)?;
            // `bucket_depth <= 16` and `counter_bits <= 31`, so the u8 sum
            // is at most 47.
            #[allow(clippy::arithmetic_side_effects)]
            let depth = bucket_depth + counter_bits;
            let capacity = 1u32 << counter_bits;
            let mutability = Mutability::arbitrary(u)?;

            let buckets = 1usize << bucket_depth;
            let base = u.int_in_range(0..=capacity)?;
            let mut counts = vec![base; buckets];
            let outliers = u.int_in_range(0..=buckets.min(16))?;
            for _ in 0..outliers {
                let bucket = u.choose_index(buckets)?;
                // `choose_index(buckets)` returns an index below `buckets`,
                // the length of `counts`.
                #[allow(clippy::indexing_slicing)]
                {
                    counts[bucket] = u.int_in_range(0..=capacity)?;
                }
            }

            // Cannot fail for the geometry and counters generated above; map
            // defensively rather than panicking inside the generator.
            Self::from_counts(batch_id, depth, bucket_depth, counts, mutability)
                .map_err(|_| arbitrary::Error::IncorrectFormat)
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, b256};
    use nectar_postage::{Batch, BucketDepth};

    use super::*;

    fn batch_id() -> BatchId {
        BatchId::from(b256!(
            "0x1122334455667788112233445566778811223344556677881122334455667788"
        ))
    }

    fn immutable_counts() -> Vec<u32> {
        vec![0u32; 1usize << 16]
    }

    fn batch_with(depth: u8, bucket_depth: u8, immutable: bool) -> Batch {
        Batch::new(
            batch_id(),
            1_000,
            0,
            Address::repeat_byte(0x11),
            depth,
            BucketDepth::new(bucket_depth).unwrap(),
            immutable,
        )
    }

    #[test]
    fn geometry_bounds() {
        let imm = Mutability::Immutable;
        assert!(UsageTable::new(batch_id(), 20, 16, imm).is_ok());
        assert!(UsageTable::new(batch_id(), 16, 16, imm).is_ok());
        assert!(UsageTable::new(batch_id(), 47, 16, imm).is_ok());
        assert!(UsageTable::new(batch_id(), 48, 16, imm).is_err());
        assert!(UsageTable::new(batch_id(), 15, 16, imm).is_err());
        assert!(UsageTable::new(batch_id(), 20, 17, imm).is_err());
    }

    #[test]
    fn geometry_rejects_zero_bucket_depth() {
        // A zero bucket depth would overflow `calculate_bucket`'s
        // `32 - bucket_depth` shift on the persist and issue paths, so the
        // geometry validator rejects it outright.
        for depth in [0u8, 1, 20, 31] {
            assert_eq!(
                validate_geometry(depth, 0),
                Err(UsageError::InvalidGeometry {
                    depth,
                    bucket_depth: 0,
                })
            );
        }
        // Both constructors go through the validator.
        assert_eq!(
            UsageTable::new(batch_id(), 20, 0, Mutability::Immutable),
            Err(UsageError::InvalidGeometry {
                depth: 20,
                bucket_depth: 0,
            })
        );
        assert_eq!(
            UsageTable::from_counts(batch_id(), 5, 0, vec![0u32], Mutability::Immutable),
            Err(UsageError::InvalidGeometry {
                depth: 5,
                bucket_depth: 0,
            })
        );
    }

    #[test]
    fn mutability_enum_matches_old_constructors() {
        // The enum constructors produce the same tables the old `new`/`new_mutable`
        // pair did: an immutable table is a fill watermark, a mutable table a ring.
        let immutable = UsageTable::new(batch_id(), 20, 16, Mutability::Immutable).unwrap();
        assert!(!immutable.is_mutable());

        let mutable = UsageTable::new(batch_id(), 20, 16, Mutability::Mutable).unwrap();
        assert!(mutable.is_mutable());

        let from_counts_imm = UsageTable::from_counts(
            batch_id(),
            17,
            16,
            immutable_counts(),
            Mutability::Immutable,
        )
        .unwrap();
        assert!(!from_counts_imm.is_mutable());

        let from_counts_mut =
            UsageTable::from_counts(batch_id(), 17, 16, immutable_counts(), Mutability::Mutable)
                .unwrap();
        assert!(from_counts_mut.is_mutable());
    }

    #[test]
    fn from_batch_picks_table_from_polarity() {
        // An immutable batch yields a fill table; a mutable batch yields a ring.
        let immutable_batch = batch_with(20, 16, true);
        let immutable_table = UsageTable::from_batch(&immutable_batch).unwrap();
        assert!(!immutable_table.is_mutable());
        assert_eq!(immutable_table.batch_id(), batch_id());
        assert_eq!(immutable_table.depth(), 20);
        assert_eq!(immutable_table.bucket_depth(), 16);

        let mutable_batch = batch_with(20, 16, false);
        let mutable_table = UsageTable::from_batch(&mutable_batch).unwrap();
        assert!(mutable_table.is_mutable());
        assert_eq!(mutable_table.depth(), 20);
    }

    #[test]
    fn from_counts_sums_issued_and_rejects_overflow() {
        let mut counts = vec![0u32; 1usize << 16];
        counts[7] = 2;
        let table =
            UsageTable::from_counts(batch_id(), 17, 16, counts, Mutability::Immutable).unwrap();
        assert_eq!(table.bucket_capacity(), 2);
        assert_eq!(table.total_issued(), 2);
        assert_eq!(table.max_count(), 2);
        assert_eq!(table.min_count(), 0);

        let mut over = vec![0u32; 1usize << 16];
        over[7] = 3; // capacity is 2
        assert_eq!(
            UsageTable::from_counts(batch_id(), 17, 16, over, Mutability::Immutable),
            Err(UsageError::CounterOverflow {
                bucket: 7,
                count: 3,
                capacity: 2
            })
        );
    }

    #[test]
    fn dilute_grows_capacity_only() {
        let mut counts = vec![0u32; 1usize << 16];
        counts[0] = 1;
        let mut table =
            UsageTable::from_counts(batch_id(), 17, 16, counts, Mutability::Immutable).unwrap();
        table.dilute(18).unwrap();
        assert_eq!(table.bucket_capacity(), 4);
        assert_eq!(table.count(0).unwrap(), 1);
        assert_eq!(
            table.dilute(17),
            Err(UsageError::DepthDecrease {
                current: 18,
                requested: 17
            })
        );
    }

    #[test]
    fn merge_takes_elementwise_max() {
        let mut counts_a = vec![0u32; 1usize << 16];
        counts_a[0] = 2;
        let mut counts_b = vec![0u32; 1usize << 16];
        counts_b[0] = 1;
        counts_b[1] = 1;
        let mut a =
            UsageTable::from_counts(batch_id(), 18, 16, counts_a, Mutability::Immutable).unwrap();
        let b =
            UsageTable::from_counts(batch_id(), 19, 16, counts_b, Mutability::Immutable).unwrap();
        a.merge_max(&b).unwrap();
        assert_eq!(a.depth(), 19);
        assert_eq!(a.count(0).unwrap(), 2);
        assert_eq!(a.count(1).unwrap(), 1);
        assert_eq!(a.total_issued(), 3);
    }

    #[test]
    fn merge_max_rejects_mutable() {
        let zero = || vec![0u32; 1usize << 16];
        let mut a =
            UsageTable::from_counts(batch_id(), 18, 16, zero(), Mutability::Mutable).unwrap();
        let b = UsageTable::from_counts(batch_id(), 18, 16, zero(), Mutability::Immutable).unwrap();
        assert_eq!(a.merge_max(&b), Err(UsageError::MutableMerge));
        let mut c =
            UsageTable::from_counts(batch_id(), 18, 16, zero(), Mutability::Immutable).unwrap();
        let d = UsageTable::from_counts(batch_id(), 18, 16, zero(), Mutability::Mutable).unwrap();
        assert_eq!(c.merge_max(&d), Err(UsageError::MutableMerge));
    }
}

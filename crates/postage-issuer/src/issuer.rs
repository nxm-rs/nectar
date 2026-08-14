//! Stamp issuer trait for tracking bucket utilization.

use crate::error::IssuerError;
use crate::permit::Prepared;
use crate::watermarks::Watermarks;
use nectar_postage::{Batch, BatchDepth, BatchId, BucketDepth, StampError, calculate_bucket};
use nectar_primitives::{ChunkAddress, Mainnet, SwarmSpec};

/// Slot allocation within a batch: the reserve half of the three phases, with
/// signing and commit outside it.
pub trait StampIssuer {
    /// The network the batch was bought on.
    type Spec: SwarmSpec;

    /// Claims a slot in the bucket `address` falls into and returns the
    /// permit for it.
    ///
    /// # Errors
    ///
    /// [`StampError::BucketFull`] once the bucket has no slot left.
    fn reserve(
        &self,
        address: &ChunkAddress,
        timestamp: u64,
    ) -> Result<Prepared<Self::Spec>, StampError>;

    /// Returns the batch ID that stamps are issued for.
    fn batch_id(&self) -> BatchId;

    /// Returns the batch depth.
    fn batch_depth(&self) -> u8;

    /// Returns the bucket depth.
    fn bucket_depth(&self) -> u8;

    /// Returns the current utilization of the most-used bucket.
    ///
    /// This is useful for monitoring batch usage and determining
    /// when a batch is approaching capacity.
    fn max_bucket_utilization(&self) -> u32;

    /// Returns the utilization of a specific bucket.
    fn bucket_utilization(&self, bucket: u32) -> u32;

    /// Checks if a bucket can accept another chunk.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket number to check
    ///
    /// # Returns
    ///
    /// `true` if the bucket has capacity for at least one more chunk,
    /// `false` if the bucket is full.
    fn bucket_has_capacity(&self, bucket: u32) -> bool;

    /// Returns the lifetime number of stamps issued, if the issuer tracks one.
    ///
    /// A fill issuer tracks a true monotone count and returns `Some`. A mutable
    /// ring issuer that keeps only a wrapping cursor has no lifetime count to
    /// give and returns `None` rather than forwarding a checksum sum as if it
    /// were a count: a wrapped bucket is full, yet the sum of its cursors does
    /// not count the overwrites. Read saturation through
    /// [`max_bucket_utilization`](Self::max_bucket_utilization) instead, which is
    /// honest in both modes.
    fn stamps_issued(&self) -> Option<u64>;

    /// Returns the total capacity of the batch (2^depth).
    fn total_capacity(&self) -> u64 {
        1u64 << self.batch_depth()
    }

    /// Returns the bucket capacity (2^(depth - bucket_depth)).
    // Batch geometry invariant: depth >= bucket_depth for every issuer.
    #[allow(clippy::arithmetic_side_effects)]
    fn bucket_capacity(&self) -> u32 {
        1u32 << (self.batch_depth() - self.bucket_depth())
    }

    /// Returns the number of buckets (2^bucket_depth).
    fn bucket_count(&self) -> u32 {
        1u32 << self.bucket_depth()
    }

    /// Checks if the issuer is approaching capacity.
    ///
    /// Returns `true` if the most utilized bucket has reached the
    /// specified percentage of capacity (0.0 to 1.0).
    fn is_near_capacity(&self, threshold: f64) -> bool {
        let max_util = f64::from(self.max_bucket_utilization());
        let capacity = f64::from(self.bucket_capacity());
        max_util / capacity >= threshold
    }
}

/// An in-memory stamp issuer that tracks bucket utilization.
///
/// This implementation stores bucket indices in a vector and is suitable
/// for most use cases where the issuer state doesn't need to persist
/// across restarts.
///
/// Issuance is fill-only: every slot is written at most once and the bucket is
/// refused with [`StampError::BucketFull`] once full. Mutable, overwrite-aware
/// issuance is intentionally absent from this crate; it requires reserved-slot
/// awareness that lives in `nectar-postage-usage`. See the crate-root
/// documentation for the steer toward `Snapshot::issuer` / `SnapshotIssuer`.
///
/// Allocation and dilution take `&self` and no lock, so one issuer serves every
/// thread stamping into its batch.
///
/// The network is a type parameter that reaches the issuer through its
/// [`BucketDepth`].
#[derive(Debug)]
pub struct MemoryIssuer<S: SwarmSpec = Mainnet> {
    batch_id: BatchId,
    watermarks: Watermarks<S>,
}

// The spec is a type-level tag, so this carries no bound on `S` beyond
// `SwarmSpec`; deriving would demand `S: Clone` of a marker type that holds no
// data.
impl<S: SwarmSpec> Clone for MemoryIssuer<S> {
    fn clone(&self) -> Self {
        Self {
            batch_id: self.batch_id,
            watermarks: self.watermarks.clone(),
        }
    }
}

impl<S: SwarmSpec> MemoryIssuer<S> {
    /// Creates a new fill-only memory issuer for the given batch geometry.
    pub fn new(batch_id: BatchId, depth: u8, bucket_depth: BucketDepth<S>) -> Self {
        Self {
            batch_id,
            watermarks: Watermarks::new(depth, bucket_depth),
        }
    }

    /// Applies an on-chain dilution, growing the per-bucket capacity without
    /// moving any watermark.
    ///
    /// The new depth must not decrease. Diluting later is the prerequisite for
    /// topping up a batch in place, so this mirrors the snapshot's dilution.
    ///
    /// # Errors
    ///
    /// Returns [`IssuerError::DepthDecrease`] if `new_depth` is below the current
    /// depth, or [`IssuerError::Geometry`] if it leaves more slots per bucket
    /// than a counter holds.
    pub fn dilute(&self, new_depth: u8) -> Result<(), IssuerError> {
        let current = self.watermarks.depth();
        if new_depth < current {
            return Err(IssuerError::DepthDecrease {
                current,
                requested: new_depth,
            });
        }
        BatchDepth::new(new_depth, self.watermarks.bucket_depth())?;
        self.watermarks.raise_depth(new_depth);
        Ok(())
    }

    /// Claims a slot in the bucket `address` falls into.
    ///
    /// # Errors
    ///
    /// [`StampError::BucketFull`] once the bucket has no slot left.
    pub fn reserve(
        &self,
        address: &ChunkAddress,
        timestamp: u64,
    ) -> Result<Prepared<S>, StampError> {
        let bucket_depth = self.watermarks.bucket_depth();
        let bucket = calculate_bucket(address, bucket_depth);
        let position = self
            .watermarks
            .allocate(bucket)
            .map_err(|err| StampError::BucketFull {
                bucket: bucket.value(),
                capacity: match err {
                    crate::counter::CounterError::BucketFull { capacity, .. } => capacity,
                    _ => self.watermarks.bucket_capacity(),
                },
            })?;
        // Read after the claim: depth only rises, so it is never narrower than
        // the bound the claim was checked against.
        let depth = BatchDepth::new(self.watermarks.depth(), bucket_depth)?;

        Ok(Prepared::new(
            *address,
            self.batch_id,
            bucket,
            depth,
            position,
            timestamp,
        ))
    }

    /// Creates a memory issuer from a batch.
    ///
    /// Immutable batches yield a fill-only issuer identical to
    /// [`MemoryIssuer::new`] for the same geometry. Mutable batches are refused
    /// with [`IssuerError::MutableNotSupported`] so a ring is never produced by
    /// accident: overwrite-aware issuance must be requested by name through
    /// [`RingIssuer::external`](crate::RingIssuer::external) for external
    /// tracking, or [`RingIssuer::reserved`](crate::RingIssuer::reserved) for
    /// self-hosting, where the protected slots come from `nectar-postage-usage`.
    pub fn from_batch(batch: &Batch<S>) -> Result<Self, IssuerError> {
        if batch.immutable() {
            // Chain decode leaves the depth a raw byte; an unheld geometry would
            // otherwise overflow the counter table's capacity shift.
            batch.geometry()?;
            Ok(Self::new(batch.id(), batch.depth(), batch.bucket_depth()))
        } else {
            Err(IssuerError::MutableNotSupported)
        }
    }
}

impl<S: SwarmSpec> StampIssuer for MemoryIssuer<S> {
    type Spec = S;

    fn reserve(&self, address: &ChunkAddress, timestamp: u64) -> Result<Prepared<S>, StampError> {
        // The inherent method; the trait method of the same name would recurse.
        Self::reserve(self, address, timestamp)
    }

    fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    fn batch_depth(&self) -> u8 {
        self.watermarks.depth()
    }

    fn bucket_depth(&self) -> u8 {
        self.watermarks.bucket_depth().get()
    }

    fn max_bucket_utilization(&self) -> u32 {
        // Fill watermarks are monotone, so the current maximum is the historical
        // maximum.
        self.watermarks.max_count()
    }

    fn bucket_utilization(&self, bucket: u32) -> u32 {
        self.watermarks.count(bucket).unwrap_or(0)
    }

    fn bucket_has_capacity(&self, bucket: u32) -> bool {
        self.watermarks.has_capacity(bucket).unwrap_or(false)
    }

    fn stamps_issued(&self) -> Option<u64> {
        Some(self.watermarks.total_issued())
    }
}

/// Shared-handle issuance: several pipelines may admit from one issuer.
impl<I: StampIssuer + ?Sized> StampIssuer for &I {
    type Spec = I::Spec;

    fn reserve(
        &self,
        address: &ChunkAddress,
        timestamp: u64,
    ) -> Result<Prepared<Self::Spec>, StampError> {
        (**self).reserve(address, timestamp)
    }

    fn batch_id(&self) -> BatchId {
        (**self).batch_id()
    }

    fn batch_depth(&self) -> u8 {
        (**self).batch_depth()
    }

    fn bucket_depth(&self) -> u8 {
        (**self).bucket_depth()
    }

    fn max_bucket_utilization(&self) -> u32 {
        (**self).max_bucket_utilization()
    }

    fn bucket_utilization(&self, bucket: u32) -> u32 {
        (**self).bucket_utilization(bucket)
    }

    fn bucket_has_capacity(&self, bucket: u32) -> bool {
        (**self).bucket_has_capacity(bucket)
    }

    fn stamps_issued(&self) -> Option<u64> {
        (**self).stamps_issued()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_address(leading: u16) -> ChunkAddress {
        let mut bytes = [0u8; 32];
        // Big-endian split of a u16: `leading >> 8` is <= 0xFF and the low-byte
        // truncation is the intended extraction; both casts are lossless.
        #[allow(clippy::as_conversions)]
        {
            bytes[0] = (leading >> 8) as u8;
            bytes[1] = leading as u8;
        }
        ChunkAddress::new(bytes)
    }

    #[test]
    fn test_memory_issuer_basic() {
        let batch_id = BatchId::ZERO;
        let issuer: MemoryIssuer = MemoryIssuer::new(batch_id, 20, BucketDepth::new(16).unwrap());

        assert_eq!(issuer.batch_id(), batch_id);
        assert_eq!(issuer.batch_depth(), 20);
        assert_eq!(issuer.bucket_depth(), 16);
        assert_eq!(issuer.max_bucket_utilization(), 0);
        assert_eq!(issuer.stamps_issued(), Some(0));
        assert_eq!(issuer.bucket_count(), 65536);
        assert_eq!(issuer.bucket_capacity(), 16);
    }

    #[test]
    fn test_memory_issuer_reserve() {
        let issuer: MemoryIssuer =
            MemoryIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());

        let address = test_address(0xCBE5);
        let permit = issuer.reserve(&address, 12345).unwrap();

        assert_eq!(permit.batch(), BatchId::ZERO);
        assert_eq!(permit.bucket().value(), 0xCBE5);
        assert_eq!(permit.index().index(), 0);
        assert_eq!(permit.timestamp(), 12345);
        assert_eq!(permit.depth().get(), 20);
        assert_eq!(issuer.stamps_issued(), Some(1));
        assert_eq!(issuer.max_bucket_utilization(), 1);
    }

    #[test]
    fn test_memory_issuer_increments_index() {
        let issuer: MemoryIssuer =
            MemoryIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());

        let address = test_address(0xCBE5);

        let d1 = issuer.reserve(&address, 1).unwrap();
        let d2 = issuer.reserve(&address, 2).unwrap();
        let d3 = issuer.reserve(&address, 3).unwrap();

        assert_eq!(d1.index().index(), 0);
        assert_eq!(d2.index().index(), 1);
        assert_eq!(d3.index().index(), 2);
        assert_eq!(issuer.stamps_issued(), Some(3));
    }

    #[test]
    fn test_memory_issuer_bucket_full() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket
        let issuer: MemoryIssuer =
            MemoryIssuer::new(BatchId::ZERO, 17, BucketDepth::new(16).unwrap());

        let address = test_address(0xABCD);

        // First two should succeed
        assert!(issuer.reserve(&address, 1).is_ok());
        assert!(issuer.reserve(&address, 2).is_ok());

        // Third should fail
        let result = issuer.reserve(&address, 3);
        assert!(matches!(
            result,
            Err(StampError::BucketFull {
                bucket: 0xABCD,
                capacity: 2
            })
        ));
    }

    #[test]
    fn test_memory_issuer_bucket_utilization() {
        let issuer: MemoryIssuer =
            MemoryIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());

        let addr1 = test_address(0x1234);
        let addr2 = test_address(0x5678);

        issuer.reserve(&addr1, 1).unwrap();
        issuer.reserve(&addr1, 2).unwrap();
        issuer.reserve(&addr2, 3).unwrap();

        assert_eq!(issuer.bucket_utilization(0x1234), 2);
        assert_eq!(issuer.bucket_utilization(0x5678), 1);
        assert_eq!(issuer.bucket_utilization(0x9999), 0);
    }

    #[test]
    fn test_memory_issuer_capacity_check() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket
        let issuer: MemoryIssuer =
            MemoryIssuer::new(BatchId::ZERO, 17, BucketDepth::new(16).unwrap());

        let address = test_address(0x0001);

        assert!(issuer.bucket_has_capacity(0x0001));

        issuer.reserve(&address, 1).unwrap();
        assert!(issuer.bucket_has_capacity(0x0001));

        issuer.reserve(&address, 2).unwrap();
        assert!(!issuer.bucket_has_capacity(0x0001));
    }

    #[test]
    fn test_memory_issuer_near_capacity() {
        // depth=18, bucket_depth=16 gives 4 slots per bucket
        let issuer: MemoryIssuer =
            MemoryIssuer::new(BatchId::ZERO, 18, BucketDepth::new(16).unwrap());

        let address = test_address(0x0001);

        assert!(!issuer.is_near_capacity(0.5));

        issuer.reserve(&address, 1).unwrap();
        issuer.reserve(&address, 2).unwrap();

        // 2/4 = 0.5
        assert!(issuer.is_near_capacity(0.5));
        assert!(!issuer.is_near_capacity(0.75));

        issuer.reserve(&address, 3).unwrap();

        // 3/4 = 0.75
        assert!(issuer.is_near_capacity(0.75));
    }

    #[test]
    fn test_memory_issuer_from_batch_mutable_refused() {
        use nectar_postage::Batch;

        // A mutable batch must never yield an issuer: the obvious constructor
        // refuses it instead of handing back a reserved-blind ring that would
        // silently overwrite a self-hosted snapshot's own chunks.
        let mutable: Batch = Batch::new(
            BatchId::ZERO,
            0,
            0,
            Default::default(),
            20,
            BucketDepth::new(16).unwrap(),
            false,
        );

        assert!(matches!(
            MemoryIssuer::from_batch(&mutable),
            Err(IssuerError::MutableNotSupported)
        ));
    }

    #[test]
    fn from_batch_refuses_a_depth_no_counter_table_can_hold() {
        use nectar_postage::Batch;

        // Chain decode is total, so a depth this far above the bucket depth
        // reaches here; building the table would overflow its capacity shift.
        for depth in [8u8, 48, u8::MAX] {
            let batch: Batch = Batch::new(
                BatchId::ZERO,
                0,
                0,
                Default::default(),
                depth,
                BucketDepth::new(16).unwrap(),
                true,
            );
            assert!(matches!(
                MemoryIssuer::from_batch(&batch),
                Err(IssuerError::Geometry(_))
            ));
        }
    }

    #[test]
    fn test_memory_issuer_from_batch_immutable_parity_with_new() {
        use nectar_postage::Batch;

        // An immutable batch yields a fill-only issuer byte-for-byte identical
        // to `new` for the same geometry: same indices and the same digest.
        let batch_id = BatchId::new([0x11u8; 32]);
        let immutable: Batch = Batch::new(
            batch_id,
            0,
            0,
            Default::default(),
            17,
            BucketDepth::new(16).unwrap(),
            true,
        );

        let from_batch = MemoryIssuer::from_batch(&immutable).unwrap();
        let from_new: MemoryIssuer = MemoryIssuer::new(batch_id, 17, BucketDepth::new(16).unwrap());

        for ts in 0..2u64 {
            for leading in [0xCBE5u16, 0x0001, 0xABCD] {
                let address = test_address(leading);
                let a = from_batch.reserve(&address, ts).unwrap();
                let b = from_new.reserve(&address, ts).unwrap();
                assert_eq!(a.bucket().value(), b.bucket().value());
                assert_eq!(a.index().index(), b.index().index());
                assert_eq!(a.digest().to_prehash(), b.digest().to_prehash());
            }
        }

        assert_eq!(
            from_batch.max_bucket_utilization(),
            from_new.max_bucket_utilization()
        );
        assert_eq!(from_batch.stamps_issued(), from_new.stamps_issued());
    }

    #[test]
    fn test_memory_issuer_dilute_grows_capacity_only() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket.
        let issuer: MemoryIssuer =
            MemoryIssuer::new(BatchId::ZERO, 17, BucketDepth::new(16).unwrap());
        let address = test_address(0xABCD);

        // Fill the bucket, then a dilution to depth 18 (4 slots) reopens it
        // without moving the existing watermark.
        issuer.reserve(&address, 1).unwrap();
        issuer.reserve(&address, 2).unwrap();
        assert!(issuer.reserve(&address, 3).is_err());

        issuer.dilute(18).unwrap();
        assert_eq!(issuer.bucket_capacity(), 4);
        // The watermark is unchanged, so the next slot is 2, not 0.
        let d = issuer.reserve(&address, 4).unwrap();
        assert_eq!(d.index().index(), 2);
        assert_eq!(issuer.stamps_issued(), Some(3));

        // Dilution may never decrease the depth.
        assert!(matches!(
            issuer.dilute(17),
            Err(IssuerError::DepthDecrease {
                current: 18,
                requested: 17
            })
        ));
    }

    #[test]
    fn dilute_refuses_a_depth_no_counter_can_hold() {
        let issuer: MemoryIssuer =
            MemoryIssuer::new(BatchId::ZERO, 17, BucketDepth::new(16).unwrap());

        // 16 + 31 is the widest bucket a u32 slot count holds.
        assert!(issuer.dilute(47).is_ok());
        assert!(matches!(issuer.dilute(48), Err(IssuerError::Geometry(_))));
        assert_eq!(issuer.batch_depth(), 47);
    }

    mod proptests {
        use proptest::prelude::*;
        use std::collections::BTreeMap;

        use super::*;

        /// An interleaved issuance operation.
        #[derive(Debug, Clone, Copy)]
        enum Op {
            /// Allocate a slot in the bucket named by the address prefix.
            Allocate(u16),
            /// Dilute the batch to the given depth.
            Dilute(u8),
        }

        fn op_strategy() -> impl Strategy<Value = Op> {
            prop_oneof![
                (0u16..4).prop_map(Op::Allocate),
                (16u8..=24).prop_map(Op::Dilute),
            ]
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(128))]

            /// Under any allocate and dilute interleaving, a dilution grows the
            /// capacity without moving a watermark (a depth decrease is refused
            /// and changes nothing), refusal happens exactly at the current
            /// capacity, and reopened buckets continue from their watermark.
            #[test]
            fn dilution_grows_capacity_without_moving_watermarks(
                bucket_depth in 16u8..=18,
                ops in proptest::collection::vec(op_strategy(), 1..120),
            ) {
                let bucket_depth: BucketDepth = BucketDepth::new(bucket_depth).unwrap();
                let mut depth = bucket_depth.get() + 1;
                let issuer: MemoryIssuer = MemoryIssuer::new(BatchId::ZERO, depth, bucket_depth);
                let mut marks = BTreeMap::<u16, u32>::new();
                let mut issued = 0u64;
                let mut ts = 0u64;
                for &op in &ops {
                    let capacity = 1u32 << (depth - bucket_depth.get());
                    match op {
                        Op::Dilute(new_depth) if new_depth < depth => {
                            let refused = matches!(
                                issuer.dilute(new_depth),
                                Err(IssuerError::DepthDecrease { current, requested })
                                    if current == depth && requested == new_depth
                            );
                            prop_assert!(refused, "depth decrease was not refused");
                        }
                        Op::Dilute(new_depth) => {
                            prop_assert!(issuer.dilute(new_depth).is_ok());
                            depth = new_depth;
                        }
                        Op::Allocate(lead) => {
                            ts += 1;
                            let bucket = u32::from(lead) << (bucket_depth.get() - 16);
                            let mark = marks.entry(lead).or_insert(0);
                            match issuer.reserve(&test_address(lead), ts) {
                                Ok(permit) => {
                                    prop_assert!(*mark < capacity);
                                    prop_assert_eq!(permit.bucket().value(), bucket);
                                    prop_assert_eq!(permit.index().index(), *mark);
                                    *mark += 1;
                                    issued += 1;
                                }
                                Err(err) => {
                                    prop_assert_eq!(*mark, capacity);
                                    prop_assert_eq!(
                                        err,
                                        StampError::BucketFull { bucket, capacity }
                                    );
                                }
                            }
                            prop_assert_eq!(issuer.bucket_utilization(bucket), *mark);
                        }
                    }
                    prop_assert_eq!(issuer.batch_depth(), depth);
                    prop_assert_eq!(issuer.bucket_capacity(), 1u32 << (depth - bucket_depth.get()));
                    prop_assert_eq!(issuer.stamps_issued(), Some(issued));
                }
            }
        }
    }
}

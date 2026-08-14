//! Sharded parallel issuance over a sequential issuer.
//!
//! A shard owns a contiguous bucket range and one sequential issuer behind its
//! own lock, so every bucket keeps a single writer. The inner issuer sets the
//! issuance mode.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use parking_lot::Mutex;

use nectar_postage::{Batch, BatchId, BucketDepth, StampDigest, StampError, calculate_bucket};
use nectar_primitives::{ChunkAddress, Mainnet, SwarmSpec};

use crate::error::IssuerError;
use crate::issuer::{MemoryIssuer, StampIssuer};
use crate::ring::{Reservation, Reserved, RingIssuer, Unreserved};

/// Shards per issuer. A power of two, so a bucket's shard is a shift and a mask.
const DEFAULT_SHARD_COUNT: usize = 16;

/// A sharded issuer: one sequential issuer per contiguous bucket range.
///
/// Allocation takes `&self`, so several threads may stamp through one issuer.
/// The inner issuer `I` sets the issuance mode: [`ShardedIssuer`] is
/// fill-only, [`ShardedRingIssuer`] is overwrite-aware.
pub struct Sharded<I, S: SwarmSpec = Mainnet> {
    batch_id: BatchId,
    depth: u8,
    bucket_depth: BucketDepth<S>,
    bucket_capacity: u32,
    shards: Vec<Mutex<I>>,
    shard_mask: u32,
    shard_shift: u32,
    max_utilization: AtomicU32,
    stamps_issued: AtomicU64,
}

// Geometry only: deriving would dump every shard's whole counter table.
impl<I, S: SwarmSpec> core::fmt::Debug for Sharded<I, S> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Sharded")
            .field("batch_id", &self.batch_id)
            .field("depth", &self.depth)
            .field("bucket_depth", &self.bucket_depth.get())
            .field("bucket_capacity", &self.bucket_capacity)
            .field("shard_count", &self.shards.len())
            .field("stamps_issued", &self.stamps_issued)
            .finish_non_exhaustive()
    }
}

/// A fill-only sharded issuer: the parallel counterpart of [`MemoryIssuer`].
pub type ShardedIssuer<S = Mainnet> = Sharded<MemoryIssuer<S>, S>;

/// A sharded mutable (ring) issuer: the parallel counterpart of
/// [`RingIssuer`].
///
/// The reservation policy rides on the inner ring, so a sink that demands a
/// reserved ring cannot be handed a reserved-blind one:
///
/// ```compile_fail
/// use nectar_postage_issuer::{
///     Batch, BatchId, BucketDepth, Reserved, ShardedRingIssuer, Unreserved,
/// };
///
/// fn self_hosting_sink(_ring: ShardedRingIssuer<Reserved>) {}
///
/// let bucket_depth = BucketDepth::new(16).unwrap();
/// let batch: Batch = Batch::new(BatchId::ZERO, 0, 0, Default::default(), 20, bucket_depth, false);
/// let unreserved: ShardedRingIssuer<Unreserved> = ShardedRingIssuer::external(&batch).unwrap();
/// self_hosting_sink(unreserved);
/// ```
pub type ShardedRingIssuer<R = Unreserved, S = Mainnet> = Sharded<RingIssuer<R, S>, S>;

impl<I: StampIssuer, S: SwarmSpec> Sharded<I, S> {
    /// Builds `shard_count` shards, each from `make_shard` applied to the
    /// `[base, end)` bucket range it owns.
    // `shard_count` is a nonzero power of two clamped to `2^bucket_depth`, and
    // `depth >= bucket_depth` for every batch.
    #[allow(clippy::arithmetic_side_effects)]
    fn with_shards(
        batch_id: BatchId,
        depth: u8,
        bucket_depth: BucketDepth<S>,
        shard_count: usize,
        make_shard: impl Fn(u32, u32) -> I,
    ) -> Self {
        assert!(
            shard_count.is_power_of_two(),
            "shard_count must be a power of 2"
        );

        let total_buckets = 1u32 << bucket_depth.get();
        let shard_count = u32::try_from(shard_count)
            .unwrap_or(total_buckets)
            .min(total_buckets);
        let buckets_per_shard = total_buckets / shard_count;

        let shards = (0..shard_count)
            .map(|i| {
                let base = i * buckets_per_shard;
                Mutex::new(make_shard(base, base + buckets_per_shard))
            })
            .collect();

        Self {
            batch_id,
            depth,
            bucket_depth,
            bucket_capacity: 1u32 << (depth - bucket_depth.get()),
            shards,
            shard_mask: shard_count - 1,
            shard_shift: u32::from(bucket_depth.get()) - shard_count.trailing_zeros(),
            max_utilization: AtomicU32::new(0),
            stamps_issued: AtomicU64::new(0),
        }
    }

    /// The shard owning `bucket`.
    // Ranges are contiguous and equal, so the owner is the bucket's top
    // `log2(shard_count)` bits; the mask keeps the index in range for any `u32`.
    #[allow(clippy::indexing_slicing, clippy::as_conversions)]
    #[inline]
    fn shard(&self, bucket: u32) -> &Mutex<I> {
        &self.shards[((bucket >> self.shard_shift) & self.shard_mask) as usize]
    }

    /// Prepares a stamp digest for `address`.
    ///
    /// # Errors
    ///
    /// [`StampError::BucketFull`] when the owning shard refuses the bucket.
    pub fn prepare_stamp(
        &self,
        address: &ChunkAddress,
        timestamp: u64,
    ) -> Result<StampDigest, StampError> {
        let bucket = calculate_bucket(address, self.bucket_depth).value();
        let (digest, fill) = {
            let mut issuer = self.shard(bucket).lock();
            let digest = issuer.prepare_stamp(address, timestamp)?;
            (digest, issuer.bucket_utilization(bucket))
        };
        self.stamps_issued.fetch_add(1, Ordering::Relaxed);
        self.max_utilization.fetch_max(fill, Ordering::Relaxed);
        Ok(digest)
    }

    /// Batch ID.
    pub const fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    /// Batch depth.
    pub const fn batch_depth(&self) -> u8 {
        self.depth
    }

    /// Bucket depth.
    pub const fn bucket_depth(&self) -> u8 {
        self.bucket_depth.get()
    }

    /// Bucket capacity, `2^(depth - bucket_depth)`.
    pub const fn bucket_capacity(&self) -> u32 {
        self.bucket_capacity
    }

    /// Number of shards.
    pub const fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Maximum bucket utilization observed across all shards.
    pub fn max_bucket_utilization(&self) -> u32 {
        self.max_utilization.load(Ordering::Relaxed)
    }

    /// Current utilization of `bucket`.
    pub fn bucket_utilization(&self, bucket: u32) -> u32 {
        self.shard(bucket).lock().bucket_utilization(bucket)
    }

    /// Whether `bucket` has a fresh, never-written slot left.
    pub fn bucket_has_capacity(&self, bucket: u32) -> bool {
        self.shard(bucket).lock().bucket_has_capacity(bucket)
    }

    /// Total stamps issued.
    pub fn stamps_issued(&self) -> u64 {
        self.stamps_issued.load(Ordering::Relaxed)
    }
}

impl<S: SwarmSpec> Sharded<MemoryIssuer<S>, S> {
    /// Creates a fill-only sharded issuer with the default shard count.
    pub fn new(batch_id: BatchId, depth: u8, bucket_depth: BucketDepth<S>) -> Self {
        Self::with_shard_count(batch_id, depth, bucket_depth, DEFAULT_SHARD_COUNT)
    }

    /// Creates a fill-only sharded issuer with a given shard count, clamped to
    /// the bucket count.
    ///
    /// Every shard carries a full-width counter table, so the count trades
    /// address space against lock contention.
    ///
    /// # Panics
    ///
    /// Panics if `shard_count` is not a power of two.
    pub fn with_shard_count(
        batch_id: BatchId,
        depth: u8,
        bucket_depth: BucketDepth<S>,
        shard_count: usize,
    ) -> Self {
        Self::with_shards(batch_id, depth, bucket_depth, shard_count, |_, _| {
            MemoryIssuer::new(batch_id, depth, bucket_depth)
        })
    }

    /// Creates a fill-only sharded issuer from a batch.
    ///
    /// # Errors
    ///
    /// [`IssuerError::MutableNotSupported`] for a mutable batch: overwrite-aware
    /// issuance is requested by name through [`ShardedRingIssuer::external`] or
    /// [`ShardedRingIssuer::reserved`].
    pub fn from_batch(batch: &Batch<S>) -> Result<Self, IssuerError> {
        if batch.immutable() {
            Ok(Self::new(batch.id(), batch.depth(), batch.bucket_depth()))
        } else {
            Err(IssuerError::MutableNotSupported)
        }
    }

    /// Applies an on-chain dilution to every shard, growing the per-bucket
    /// capacity without moving a watermark.
    ///
    /// # Errors
    ///
    /// [`IssuerError::DepthDecrease`] if `new_depth` is below the current depth.
    pub fn dilute(&mut self, new_depth: u8) -> Result<(), IssuerError> {
        if new_depth < self.depth {
            return Err(IssuerError::DepthDecrease {
                current: self.depth,
                requested: new_depth,
            });
        }
        for shard in &self.shards {
            shard.lock().dilute(new_depth)?;
        }
        self.depth = new_depth;
        // `new_depth >= depth >= bucket_depth` by the check above.
        #[allow(clippy::arithmetic_side_effects)]
        {
            self.bucket_capacity = 1u32 << (new_depth - self.bucket_depth.get());
        }
        Ok(())
    }
}

impl<S: SwarmSpec> Sharded<RingIssuer<Unreserved, S>, S> {
    /// Builds an externally tracked sharded ring for a mutable batch.
    ///
    /// # Errors
    ///
    /// [`IssuerError::ImmutableNotSupported`] if the batch is immutable.
    pub fn external(batch: &Batch<S>) -> Result<Self, IssuerError> {
        Self::for_mutable_batch(batch, |_, _| Unreserved)
    }
}

impl<S: SwarmSpec> Sharded<RingIssuer<Reserved, S>, S> {
    /// Builds a self-hosting sharded ring for a mutable batch, protecting
    /// `slots` from re-issuance.
    ///
    /// # Errors
    ///
    /// [`IssuerError::ImmutableNotSupported`] if the batch is immutable.
    pub fn reserved(
        batch: &Batch<S>,
        slots: impl IntoIterator<Item = (u32, u32)>,
    ) -> Result<Self, IssuerError> {
        let slots: Vec<(u32, u32)> = slots.into_iter().collect();
        // A shard can only emit slots in its own range, so it carries only those.
        Self::for_mutable_batch(batch, |base, end| {
            Reserved::new(
                slots
                    .iter()
                    .copied()
                    .filter(|&(bucket, _)| bucket >= base && bucket < end),
            )
        })
    }
}

impl<R: Reservation, S: SwarmSpec> Sharded<RingIssuer<R, S>, S> {
    fn for_mutable_batch(
        batch: &Batch<S>,
        make_reservation: impl Fn(u32, u32) -> R,
    ) -> Result<Self, IssuerError> {
        if batch.immutable() {
            return Err(IssuerError::ImmutableNotSupported);
        }
        Ok(Self::with_shards(
            batch.id(),
            batch.depth(),
            batch.bucket_depth(),
            DEFAULT_SHARD_COUNT,
            |base, end| {
                RingIssuer::with_reservation(
                    batch.id(),
                    batch.depth(),
                    batch.bucket_depth(),
                    make_reservation(base, end),
                )
            },
        ))
    }
}

impl<I: StampIssuer, S: SwarmSpec> StampIssuer for Sharded<I, S> {
    fn prepare_stamp(
        &mut self,
        address: &ChunkAddress,
        timestamp: u64,
    ) -> Result<StampDigest, StampError> {
        // Every body here names the inherent method; the trait method of the
        // same name would recurse.
        Self::prepare_stamp(self, address, timestamp)
    }

    fn batch_id(&self) -> BatchId {
        Self::batch_id(self)
    }

    fn batch_depth(&self) -> u8 {
        Self::batch_depth(self)
    }

    fn bucket_depth(&self) -> u8 {
        Self::bucket_depth(self)
    }

    fn max_bucket_utilization(&self) -> u32 {
        Self::max_bucket_utilization(self)
    }

    fn bucket_utilization(&self, bucket: u32) -> u32 {
        Self::bucket_utilization(self, bucket)
    }

    fn bucket_has_capacity(&self, bucket: u32) -> bool {
        Self::bucket_has_capacity(self, bucket)
    }

    fn stamps_issued(&self) -> Option<u64> {
        Some(Self::stamps_issued(self))
    }
}

/// Shared-handle issuance: several pipelines may admit from one issuer.
impl<I: StampIssuer, S: SwarmSpec> StampIssuer for &Sharded<I, S> {
    fn prepare_stamp(
        &mut self,
        address: &ChunkAddress,
        timestamp: u64,
    ) -> Result<StampDigest, StampError> {
        Sharded::prepare_stamp(self, address, timestamp)
    }

    fn batch_id(&self) -> BatchId {
        Sharded::batch_id(self)
    }

    fn batch_depth(&self) -> u8 {
        Sharded::batch_depth(self)
    }

    fn bucket_depth(&self) -> u8 {
        Sharded::bucket_depth(self)
    }

    fn max_bucket_utilization(&self) -> u32 {
        Sharded::max_bucket_utilization(self)
    }

    fn bucket_utilization(&self, bucket: u32) -> u32 {
        Sharded::bucket_utilization(self, bucket)
    }

    fn bucket_has_capacity(&self, bucket: u32) -> bool {
        Sharded::bucket_has_capacity(self, bucket)
    }

    fn stamps_issued(&self) -> Option<u64> {
        Some(Sharded::stamps_issued(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryIssuer, RingIssuer};
    use alloy_primitives::B256;

    fn test_address(leading: u16) -> ChunkAddress {
        let mut bytes = [0u8; 32];
        bytes[..2].copy_from_slice(&leading.to_be_bytes());
        ChunkAddress::new(bytes)
    }

    fn batch(depth: u8, immutable: bool) -> Batch {
        Batch::new(
            BatchId::ZERO,
            0,
            0,
            Default::default(),
            depth,
            BucketDepth::new(16).unwrap(),
            immutable,
        )
    }

    fn bucket_depth() -> BucketDepth {
        BucketDepth::new(16).unwrap()
    }

    #[test]
    fn the_aliases_name_the_generic() {
        fn self_hosting_sink(ring: ShardedRingIssuer<Reserved>) -> u64 {
            ring.stamps_issued()
        }

        let fill: Sharded<MemoryIssuer> =
            ShardedIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());
        assert_eq!(fill.shard_count(), DEFAULT_SHARD_COUNT);

        let mutable = batch(17, false);
        let ring: Sharded<RingIssuer<Reserved>> =
            ShardedRingIssuer::reserved(&mutable, [(0u32, 0u32)]).unwrap();
        assert_eq!(self_hosting_sink(ring), 0);
    }

    #[test]
    fn from_batch_refuses_a_mutable_batch() {
        assert!(matches!(
            ShardedIssuer::from_batch(&batch(20, false)),
            Err(IssuerError::MutableNotSupported)
        ));
    }

    #[test]
    fn from_batch_accepts_an_immutable_batch() {
        assert!(ShardedIssuer::from_batch(&batch(20, true)).is_ok());
    }

    #[test]
    fn a_sharded_issuer_reports_its_geometry() {
        let issuer: ShardedIssuer =
            ShardedIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());

        assert_eq!(issuer.batch_id(), BatchId::ZERO);
        assert_eq!(issuer.batch_depth(), 20);
        assert_eq!(issuer.bucket_depth(), 16);
        assert_eq!(issuer.bucket_capacity(), 16);
        assert_eq!(issuer.shard_count(), DEFAULT_SHARD_COUNT);
    }

    #[test]
    fn a_sharded_issuer_stamps_and_counts() {
        let issuer: ShardedIssuer =
            ShardedIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());
        let address = ChunkAddress::from(B256::random());

        let digest = issuer.prepare_stamp(&address, 12345).unwrap();

        assert_eq!(digest.batch_id, BatchId::ZERO);
        assert_eq!(digest.timestamp, 12345);
        assert_eq!(issuer.stamps_issued(), 1);
        assert_eq!(issuer.max_bucket_utilization(), 1);
    }

    #[test]
    fn a_smaller_shard_count_still_routes_every_bucket() {
        let issuer: ShardedIssuer =
            ShardedIssuer::with_shard_count(BatchId::ZERO, 20, BucketDepth::new(16).unwrap(), 4);
        assert_eq!(issuer.shard_count(), 4);
        for lead in [0x0000u16, 0x3FFF, 0x4000, 0xBFFF, 0xC000, 0xFFFF] {
            let digest = issuer.prepare_stamp(&test_address(lead), 1).unwrap();
            assert_eq!(digest.index.index(), 0);
        }
        assert_eq!(issuer.stamps_issued(), 6);
    }

    #[test]
    fn dilution_grows_capacity_only() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket.
        let mut issuer: ShardedIssuer =
            ShardedIssuer::new(BatchId::ZERO, 17, BucketDepth::new(16).unwrap());
        let address = test_address(0xABCD);
        let bucket = calculate_bucket(&address, bucket_depth()).value();

        issuer.prepare_stamp(&address, 1).unwrap();
        issuer.prepare_stamp(&address, 2).unwrap();
        assert!(issuer.prepare_stamp(&address, 3).is_err());

        issuer.dilute(18).unwrap();
        assert_eq!(issuer.bucket_capacity(), 4);
        assert_eq!(issuer.batch_depth(), 18);
        // The watermark is unchanged, so the next slot is 2.
        let digest = issuer.prepare_stamp(&address, 4).unwrap();
        assert_eq!(digest.index.index(), 2);
        assert_eq!(issuer.bucket_utilization(bucket), 3);

        assert!(matches!(
            issuer.dilute(17),
            Err(IssuerError::DepthDecrease {
                current: 18,
                requested: 17
            })
        ));
    }

    #[test]
    fn dilution_reaches_every_shard() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket; one address per shard.
        let mut issuer: ShardedIssuer =
            ShardedIssuer::new(BatchId::ZERO, 17, BucketDepth::new(16).unwrap());
        let addresses: Vec<_> = (0..DEFAULT_SHARD_COUNT)
            .map(|shard| test_address(u16::try_from(shard).unwrap() << 12))
            .collect();

        for address in &addresses {
            issuer.prepare_stamp(address, 1).unwrap();
            issuer.prepare_stamp(address, 2).unwrap();
            assert!(issuer.prepare_stamp(address, 3).is_err());
        }

        issuer.dilute(18).unwrap();

        for address in &addresses {
            assert_eq!(issuer.prepare_stamp(address, 4).unwrap().index.index(), 2);
        }
    }

    #[test]
    fn stamping_is_concurrent_over_shared_handles() {
        use std::thread;

        let issuer: ShardedIssuer =
            ShardedIssuer::new(BatchId::ZERO, 24, BucketDepth::new(16).unwrap());
        let threads = 8u64;
        let per_thread = 1000u64;

        thread::scope(|scope| {
            for _ in 0..threads {
                scope.spawn(|| {
                    for _ in 0..per_thread {
                        let address = ChunkAddress::from(B256::random());
                        issuer.prepare_stamp(&address, 0).unwrap();
                    }
                });
            }
        });

        assert_eq!(issuer.stamps_issued(), threads * per_thread);
    }

    #[test]
    fn one_bucket_under_contention_hands_out_each_slot_once() {
        use std::sync::Mutex as StdMutex;
        use std::thread;

        // depth=24, bucket_depth=16 gives 256 slots, which 8 threads fill exactly.
        let issuer: ShardedIssuer =
            ShardedIssuer::new(BatchId::ZERO, 24, BucketDepth::new(16).unwrap());
        let address = test_address(0x9BCD);
        let slots = StdMutex::new(Vec::new());

        thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for ts in 0..32u64 {
                        let digest = issuer.prepare_stamp(&address, ts).unwrap();
                        slots.lock().unwrap().push(digest.index.index());
                    }
                });
            }
        });

        let mut slots = slots.into_inner().unwrap();
        slots.sort_unstable();
        assert_eq!(slots, (0..256).collect::<Vec<_>>());
        assert!(issuer.prepare_stamp(&address, 0).is_err());
    }

    #[test]
    fn a_bucket_outside_the_bucket_space_reads_as_empty() {
        let issuer: ShardedIssuer =
            ShardedIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());

        assert_eq!(issuer.bucket_utilization(0x1_0000), 0);
        assert!(!issuer.bucket_has_capacity(0x1_0000));
    }

    #[test]
    fn every_trait_method_reaches_the_inherent_one() {
        // A body that resolved back into the trait would recurse until the stack died.
        let mut issuer: ShardedIssuer =
            ShardedIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());
        let address = test_address(0x1234);
        let bucket = calculate_bucket(&address, bucket_depth()).value();

        let digest = StampIssuer::prepare_stamp(&mut issuer, &address, 7).unwrap();

        assert_eq!(digest.index.bucket(), bucket);
        assert_eq!(StampIssuer::batch_id(&issuer), BatchId::ZERO);
        assert_eq!(StampIssuer::batch_depth(&issuer), 20);
        assert_eq!(StampIssuer::bucket_depth(&issuer), 16);
        assert_eq!(StampIssuer::max_bucket_utilization(&issuer), 1);
        assert_eq!(StampIssuer::bucket_utilization(&issuer, bucket), 1);
        assert!(StampIssuer::bucket_has_capacity(&issuer, bucket));
        assert_eq!(StampIssuer::stamps_issued(&issuer), Some(1));
    }

    #[test]
    fn a_shared_handle_implements_stamp_issuer() {
        let issuer: ShardedIssuer =
            ShardedIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());
        let mut handle = &issuer;

        let address = ChunkAddress::from(B256::random());
        let digest = StampIssuer::prepare_stamp(&mut handle, &address, 42).unwrap();

        assert_eq!(digest.batch_id, BatchId::ZERO);
        assert_eq!(digest.timestamp, 42);
        assert_eq!(issuer.stamps_issued(), 1);
        assert_eq!(StampIssuer::stamps_issued(&handle), Some(1));
        assert!(StampIssuer::bucket_has_capacity(
            &handle,
            calculate_bucket(&address, bucket_depth()).value()
        ));
    }

    #[test]
    fn an_external_sharded_ring_wraps_and_reuses_slots() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket.
        let mutable = batch(17, false);
        let issuer = ShardedRingIssuer::external(&mutable).unwrap();
        let address = test_address(0xABCD);

        let d0 = issuer.prepare_stamp(&address, 1).unwrap();
        let d1 = issuer.prepare_stamp(&address, 2).unwrap();
        let d2 = issuer.prepare_stamp(&address, 3).unwrap();

        assert_eq!(d0.index.index(), 0);
        assert_eq!(d1.index.index(), 1);
        assert_eq!(d2.index.index(), 0);
        assert_eq!(issuer.stamps_issued(), 3);
        // Utilization saturates rather than counting the overwrite.
        assert_eq!(issuer.bucket_utilization(d0.index.bucket()), 2);
        assert_eq!(issuer.max_bucket_utilization(), 2);
    }

    #[test]
    fn a_reserved_sharded_ring_never_emits_a_protected_slot() {
        // depth=18, bucket_depth=16 gives 4 slots per bucket; protecting 1 and 3
        // leaves only 0 and 2.
        let mutable = batch(18, false);
        let address = test_address(0x00AA);
        let bucket = calculate_bucket(&address, bucket_depth()).value();
        let issuer = ShardedRingIssuer::reserved(&mutable, [(bucket, 1), (bucket, 3)]).unwrap();

        for ts in 0..50u64 {
            let index = issuer.prepare_stamp(&address, ts).unwrap().index.index();
            assert!(
                index == 0 || index == 2,
                "sharded ring emitted protected or out-of-range slot {index}"
            );
        }
    }

    #[test]
    fn a_reserved_sharded_ring_routes_slots_to_the_owning_shard() {
        // 0x0001 and 0xF001 sit in distinct shards and protect different slots.
        let mutable = batch(17, false);
        let bucket_lo = 0x0001u32;
        let bucket_hi = 0xF001u32;
        let issuer =
            ShardedRingIssuer::reserved(&mutable, [(bucket_lo, 0), (bucket_hi, 1)]).unwrap();

        for ts in 0..10u64 {
            let lo = issuer.prepare_stamp(&test_address(0x0001), ts).unwrap();
            let hi = issuer.prepare_stamp(&test_address(0xF001), ts).unwrap();
            assert_eq!(lo.index.index(), 1);
            assert_eq!(hi.index.index(), 0);
        }
    }

    #[test]
    fn a_ring_refuses_an_immutable_batch() {
        assert!(matches!(
            ShardedRingIssuer::external(&batch(20, true)),
            Err(IssuerError::ImmutableNotSupported)
        ));
        assert!(matches!(
            ShardedRingIssuer::reserved(&batch(20, true), [(0u32, 0u32)]),
            Err(IssuerError::ImmutableNotSupported)
        ));
    }

    #[test]
    fn a_fully_protected_bucket_surfaces_as_bucket_full() {
        let mutable = batch(17, false);
        let address = test_address(0x0001);
        let bucket = calculate_bucket(&address, bucket_depth()).value();
        let issuer = ShardedRingIssuer::reserved(&mutable, [(bucket, 0), (bucket, 1)]).unwrap();

        assert!(matches!(
            issuer.prepare_stamp(&address, 1),
            Err(StampError::BucketFull { bucket: b, capacity: 2 }) if b == bucket
        ));
        assert_eq!(issuer.stamps_issued(), 0);
    }

    mod proptests {
        use proptest::prelude::*;

        use super::*;

        /// Buckets spread over several shards, with collisions inside each.
        fn leads() -> impl Strategy<Value = Vec<u16>> {
            let bucket = prop::sample::select(vec![
                0x0000u16, 0x0001, 0x1000, 0x1001, 0x4001, 0x8000, 0xF000, 0xFFFF,
            ]);
            proptest::collection::vec(bucket, 1..160)
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(64))]

            /// A sharded fill issuer allocates exactly what its sequential
            /// counterpart does.
            #[test]
            fn sharded_fill_matches_the_sequential_issuer(
                excess in 0u8..=2,
                leads in leads(),
            ) {
                let bucket_depth: BucketDepth = BucketDepth::new(16).unwrap();
                let depth = 16 + excess;
                let sharded: ShardedIssuer = ShardedIssuer::new(BatchId::ZERO, depth, bucket_depth);
                let mut sequential: MemoryIssuer = MemoryIssuer::new(BatchId::ZERO, depth, bucket_depth);

                let mut ts = 0u64;
                for &lead in &leads {
                    ts += 1;
                    let address = test_address(lead);
                    let bucket = calculate_bucket(&address, bucket_depth).value();
                    let mine = sharded.prepare_stamp(&address, ts);
                    let theirs = StampIssuer::prepare_stamp(&mut sequential, &address, ts);
                    prop_assert_eq!(mine, theirs);
                    prop_assert_eq!(
                        sharded.bucket_utilization(bucket),
                        sequential.bucket_utilization(bucket)
                    );
                    prop_assert_eq!(
                        sharded.bucket_has_capacity(bucket),
                        sequential.bucket_has_capacity(bucket)
                    );
                }

                prop_assert_eq!(
                    sharded.max_bucket_utilization(),
                    sequential.max_bucket_utilization()
                );
                prop_assert_eq!(
                    Some(sharded.stamps_issued()),
                    StampIssuer::stamps_issued(&sequential)
                );
            }

            /// A sharded ring allocates exactly what its sequential counterpart
            /// does, protected slots included. This is what pins the routing
            /// function, which fill parity alone cannot.
            #[test]
            fn sharded_ring_matches_the_sequential_ring(
                excess in 1u8..=3,
                protect in 0u32..2,
                leads in leads(),
            ) {
                let mutable = batch(16 + excess, false);
                // Leave at least one slot issuable, so neither ring exhausts.
                let slots: Vec<(u32, u32)> = (0..=0xFFFFu32)
                    .step_by(0x1000)
                    .map(|bucket| (bucket, protect))
                    .collect();
                let sharded = ShardedRingIssuer::reserved(&mutable, slots.clone()).unwrap();
                let mut sequential = RingIssuer::reserved(&mutable, slots).unwrap();

                let mut ts = 0u64;
                for &lead in &leads {
                    ts += 1;
                    let address = test_address(lead);
                    let bucket = calculate_bucket(&address, mutable.bucket_depth()).value();
                    let mine = sharded.prepare_stamp(&address, ts);
                    let theirs = StampIssuer::prepare_stamp(&mut sequential, &address, ts);
                    prop_assert_eq!(mine, theirs);
                    prop_assert_eq!(
                        sharded.bucket_utilization(bucket),
                        sequential.bucket_utilization(bucket)
                    );
                    prop_assert_eq!(
                        sharded.bucket_has_capacity(bucket),
                        sequential.bucket_has_capacity(bucket)
                    );
                }

                prop_assert_eq!(
                    sharded.max_bucket_utilization(),
                    sequential.max_bucket_utilization()
                );
                prop_assert_eq!(
                    Some(sharded.stamps_issued()),
                    StampIssuer::stamps_issued(&sequential)
                );
            }
        }
    }
}

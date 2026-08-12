//! Sharded parallel issuance over a sequential issuer.
//!
//! The bucket space is cut into contiguous ranges, one per shard, and each shard
//! holds its own sequential issuer behind its own lock. Threads stamping into
//! different shards do not contend, and every bucket is owned by exactly one
//! shard, so its cursor keeps a single writer.
//!
//! ```text
//! buckets [0 .. 2^bucket_depth)
//!   shard 0: [0 .. n)        lock A
//!   shard 1: [n .. 2n)       lock B
//!   ...
//! ```
//!
//! The inner issuer decides the issuance mode, so the fill and ring variants are
//! aliases rather than separate implementations.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

use nectar_postage::{
    Batch, BatchId, BucketDepth, StampDigest, StampError, calculate_bucket,
};
use nectar_primitives::{ChunkAddress, Mainnet, SwarmSpec};

use crate::error::IssuerError;
use crate::issuer::{MemoryIssuerFor, StampIssuer};
use crate::ring::{Reservation, Reserved, RingIssuerFor, Unreserved};

/// Shards per issuer. A power of two, so a bucket's shard is a shift and a mask.
const DEFAULT_SHARD_COUNT: usize = 16;

// Lock poisoning means another stamping thread already panicked; propagating the
// panic is the intended behaviour.
#[allow(clippy::expect_used)]
fn lock<T>(shard: &Mutex<T>) -> MutexGuard<'_, T> {
    shard.lock().expect("shard lock poisoned")
}

/// A sharded issuer: one sequential issuer per contiguous bucket range.
///
/// Allocation takes `&self`, so several threads may stamp through one issuer,
/// each holding its own shared handle. The inner issuer `I` sets the issuance
/// mode; see [`ShardedIssuerFor`] for fill-only and [`ShardedRingIssuerFor`] for
/// overwrite-aware issuance.
#[derive(Debug)]
pub struct ShardedFor<S: SwarmSpec, I> {
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

/// The [`ShardedFor`] of the mainnet spec.
pub type Sharded<I> = ShardedFor<Mainnet, I>;

/// A fill-only sharded issuer: the parallel counterpart of [`MemoryIssuerFor`].
pub type ShardedIssuerFor<S = Mainnet> = ShardedFor<S, MemoryIssuerFor<S>>;

/// The [`ShardedIssuerFor`] of the mainnet spec.
pub type ShardedIssuer = ShardedIssuerFor<Mainnet>;

/// A sharded mutable (ring) issuer: the parallel counterpart of
/// [`RingIssuerFor`].
///
/// The reservation policy rides on the inner ring, so a self-hosting context
/// that demands a reserved ring cannot be handed a reserved-blind one:
///
/// ```compile_fail
/// use nectar_postage_issuer::{
///     Batch, BatchId, BucketDepth, Reserved, ShardedRingIssuer, Unreserved,
/// };
///
/// fn self_hosting_sink(_ring: ShardedRingIssuer<Reserved>) {}
///
/// let bucket_depth = BucketDepth::new(16).unwrap();
/// let batch = Batch::new(BatchId::ZERO, 0, 0, Default::default(), 20, bucket_depth, false);
/// let unreserved: ShardedRingIssuer<Unreserved> = ShardedRingIssuer::external(&batch).unwrap();
/// self_hosting_sink(unreserved);
/// ```
pub type ShardedRingIssuerFor<S = Mainnet, R = Unreserved> = ShardedFor<S, RingIssuerFor<S, R>>;

/// The [`ShardedRingIssuerFor`] of the mainnet spec.
pub type ShardedRingIssuer<R = Unreserved> = ShardedRingIssuerFor<Mainnet, R>;

impl<S: SwarmSpec, I: StampIssuer> ShardedFor<S, I> {
    /// Builds `shard_count` shards, each from `make_shard` applied to the
    /// `[base, end)` bucket range it owns.
    // Validated shard geometry: `shard_count` is a nonzero power of two clamped
    // to `2^bucket_depth`, and `depth >= bucket_depth` for every batch, so no
    // division by zero, underflow or overflow is reachable.
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
        let shard_count = shard_count.min(usize::try_from(total_buckets).unwrap_or(usize::MAX));
        let shard_count = u32::try_from(shard_count).unwrap_or(u32::MAX);
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
    // Shard ranges are contiguous and equal, so the owner is the bucket's top
    // `log2(shard_count)` bits; the mask is `shards.len() - 1`, so the index is
    // in range even for a bucket outside the bucket space.
    #[allow(clippy::indexing_slicing, clippy::as_conversions)]
    #[inline]
    fn shard(&self, bucket: u32) -> &Mutex<I> {
        &self.shards[((bucket >> self.shard_shift) & self.shard_mask) as usize]
    }

    /// Prepares a stamp digest for `address`.
    ///
    /// # Errors
    ///
    /// Returns [`StampError::BucketFull`] when the owning shard refuses the
    /// bucket.
    ///
    /// # Panics
    ///
    /// Panics if a shard lock is poisoned.
    pub fn prepare_stamp(
        &self,
        address: &ChunkAddress,
        timestamp: u64,
    ) -> Result<StampDigest, StampError> {
        let bucket = calculate_bucket(address, self.bucket_depth.get());
        let (digest, fill) = {
            let mut issuer = lock(self.shard(bucket));
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
    ///
    /// # Panics
    ///
    /// Panics if a shard lock is poisoned.
    pub fn bucket_utilization(&self, bucket: u32) -> u32 {
        lock(self.shard(bucket)).bucket_utilization(bucket)
    }

    /// Whether `bucket` has a fresh, never-written slot left.
    ///
    /// # Panics
    ///
    /// Panics if a shard lock is poisoned.
    pub fn bucket_has_capacity(&self, bucket: u32) -> bool {
        lock(self.shard(bucket)).bucket_has_capacity(bucket)
    }

    /// Total stamps issued.
    pub fn stamps_issued(&self) -> u64 {
        self.stamps_issued.load(Ordering::Relaxed)
    }
}

impl<S: SwarmSpec> ShardedFor<S, MemoryIssuerFor<S>> {
    /// Creates a fill-only sharded issuer with the default shard count.
    pub fn new(batch_id: BatchId, depth: u8, bucket_depth: BucketDepth<S>) -> Self {
        Self::with_shard_count(batch_id, depth, bucket_depth, DEFAULT_SHARD_COUNT)
    }

    /// Creates a fill-only sharded issuer with a given shard count, clamped to
    /// the bucket count.
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
            MemoryIssuerFor::new(batch_id, depth, bucket_depth)
        })
    }

    /// Creates a fill-only sharded issuer from a batch.
    ///
    /// # Errors
    ///
    /// Returns [`IssuerError::MutableNotSupported`] for a mutable batch, as
    /// [`MemoryIssuerFor::from_batch`] does, so overwrite-aware issuance must be
    /// requested by name through [`ShardedRingIssuer::external`] or
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
    /// Returns [`IssuerError::DepthDecrease`] if `new_depth` is below the current
    /// depth.
    ///
    /// # Panics
    ///
    /// Panics if a shard lock is poisoned.
    pub fn dilute(&mut self, new_depth: u8) -> Result<(), IssuerError> {
        if new_depth < self.depth {
            return Err(IssuerError::DepthDecrease {
                current: self.depth,
                requested: new_depth,
            });
        }
        for shard in &self.shards {
            lock(shard).dilute(new_depth)?;
        }
        self.depth = new_depth;
        // `new_depth >= depth >= bucket_depth` by the check above and the batch
        // geometry invariant.
        #[allow(clippy::arithmetic_side_effects)]
        {
            self.bucket_capacity = 1u32 << (new_depth - self.bucket_depth.get());
        }
        Ok(())
    }
}

impl<S: SwarmSpec> ShardedFor<S, RingIssuerFor<S, Unreserved>> {
    /// Builds an externally tracked sharded ring for a mutable batch.
    ///
    /// # Errors
    ///
    /// Returns [`IssuerError::ImmutableNotSupported`] if the batch is immutable.
    pub fn external(batch: &Batch<S>) -> Result<Self, IssuerError> {
        Self::for_mutable_batch(batch, |_, _| Unreserved)
    }
}

impl<S: SwarmSpec> ShardedFor<S, RingIssuerFor<S, Reserved>> {
    /// Builds a self-hosting sharded ring for a mutable batch, protecting
    /// `slots` from re-issuance.
    ///
    /// # Errors
    ///
    /// Returns [`IssuerError::ImmutableNotSupported`] if the batch is immutable.
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

impl<S: SwarmSpec, R: Reservation> ShardedFor<S, RingIssuerFor<S, R>> {
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
                RingIssuerFor::with_reservation(
                    batch.id(),
                    batch.depth(),
                    batch.bucket_depth(),
                    make_reservation(base, end),
                )
            },
        ))
    }
}

impl<S: SwarmSpec, I: StampIssuer> StampIssuer for ShardedFor<S, I> {
    fn prepare_stamp(
        &mut self,
        address: &ChunkAddress,
        timestamp: u64,
    ) -> Result<StampDigest, StampError> {
        // Every body here names the inherent method, which shadows the trait
        // method of the same name.
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

/// Shared-handle issuance: allocation needs only `&self`, so several pipelines
/// may admit concurrently from one issuer.
impl<S: SwarmSpec, I: StampIssuer> StampIssuer for &ShardedFor<S, I> {
    fn prepare_stamp(
        &mut self,
        address: &ChunkAddress,
        timestamp: u64,
    ) -> Result<StampDigest, StampError> {
        ShardedFor::prepare_stamp(self, address, timestamp)
    }

    fn batch_id(&self) -> BatchId {
        ShardedFor::batch_id(self)
    }

    fn batch_depth(&self) -> u8 {
        ShardedFor::batch_depth(self)
    }

    fn bucket_depth(&self) -> u8 {
        ShardedFor::bucket_depth(self)
    }

    fn max_bucket_utilization(&self) -> u32 {
        ShardedFor::max_bucket_utilization(self)
    }

    fn bucket_utilization(&self, bucket: u32) -> u32 {
        ShardedFor::bucket_utilization(self, bucket)
    }

    fn bucket_has_capacity(&self, bucket: u32) -> bool {
        ShardedFor::bucket_has_capacity(self, bucket)
    }

    fn stamps_issued(&self) -> Option<u64> {
        Some(ShardedFor::stamps_issued(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryIssuer, RingIssuer};
    use alloy_primitives::B256;

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
        // A reserved-blind ring would silently overwrite a self-hosted
        // snapshot's own chunks, so the fill constructor refuses one.
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
        let issuer = ShardedIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());

        assert_eq!(issuer.batch_id(), BatchId::ZERO);
        assert_eq!(issuer.batch_depth(), 20);
        assert_eq!(issuer.bucket_depth(), 16);
        assert_eq!(issuer.bucket_capacity(), 16);
        assert_eq!(issuer.shard_count(), DEFAULT_SHARD_COUNT);
    }

    #[test]
    fn a_sharded_issuer_stamps_and_counts() {
        let issuer = ShardedIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());
        let address = ChunkAddress::from(B256::random());

        let digest = issuer.prepare_stamp(&address, 12345).unwrap();

        assert_eq!(digest.batch_id, BatchId::ZERO);
        assert_eq!(digest.timestamp, 12345);
        assert_eq!(issuer.stamps_issued(), 1);
        assert_eq!(issuer.max_bucket_utilization(), 1);
    }

    #[test]
    fn a_smaller_shard_count_still_routes_every_bucket() {
        let issuer =
            ShardedIssuer::with_shard_count(BatchId::ZERO, 20, BucketDepth::new(16).unwrap(), 4);
        assert_eq!(issuer.shard_count(), 4);
        // Every bucket still routes to a shard that owns it.
        for lead in [0x0000u16, 0x3FFF, 0x4000, 0xBFFF, 0xC000, 0xFFFF] {
            let digest = issuer.prepare_stamp(&test_address(lead), 1).unwrap();
            assert_eq!(digest.index.index(), 0);
        }
        assert_eq!(issuer.stamps_issued(), 6);
    }

    #[test]
    fn dilution_grows_capacity_only() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket.
        let mut issuer = ShardedIssuer::new(BatchId::ZERO, 17, BucketDepth::new(16).unwrap());
        let address = test_address(0xABCD);
        let bucket = calculate_bucket(&address, 16);

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
        // One address per shard: a dilution that stopped at the first shard
        // would leave the others refusing at the old capacity.
        let mut issuer = ShardedIssuer::new(BatchId::ZERO, 17, BucketDepth::new(16).unwrap());
        #[allow(clippy::as_conversions)] // shard index shifted into the top bits
        let addresses: Vec<_> = (0..DEFAULT_SHARD_COUNT)
            .map(|shard| test_address((shard as u16) << 12))
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

        let issuer = ShardedIssuer::new(BatchId::ZERO, 24, BucketDepth::new(16).unwrap());
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
    fn every_trait_method_reaches_the_inherent_one() {
        // Each `StampIssuer` body delegates to an inherent method of the same
        // name, so a body that resolved back into the trait would recurse until
        // the stack died.
        let mut issuer = ShardedIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());
        let address = test_address(0x1234);
        let bucket = calculate_bucket(&address, 16);

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
        let issuer = ShardedIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());
        let mut handle = &issuer;

        let address = ChunkAddress::from(B256::random());
        let digest = StampIssuer::prepare_stamp(&mut handle, &address, 42).unwrap();

        assert_eq!(digest.batch_id, BatchId::ZERO);
        assert_eq!(digest.timestamp, 42);
        assert_eq!(issuer.stamps_issued(), 1);
        assert_eq!(StampIssuer::stamps_issued(&handle), Some(1));
        assert!(StampIssuer::bucket_has_capacity(
            &handle,
            calculate_bucket(&address, 16)
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
        // depth=18, bucket_depth=16 gives 4 slots per bucket. Protect slots 1
        // and 3, so the ring may only ever emit 0 and 2.
        let mutable = batch(18, false);
        let address = test_address(0x00AA);
        let bucket = calculate_bucket(&address, 16);
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
        // Two buckets in distinct shards protect different slots; each shard
        // must apply only its own bucket's protection.
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
        let bucket = calculate_bucket(&address, 16);
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
            /// counterpart does, and reports the same geometry and totals.
            #[test]
            fn sharded_fill_matches_the_sequential_issuer(
                excess in 0u8..=2,
                leads in leads(),
            ) {
                let bucket_depth = BucketDepth::new(16).unwrap();
                let depth = 16 + excess;
                let sharded = ShardedIssuer::new(BatchId::ZERO, depth, bucket_depth);
                let mut sequential = MemoryIssuer::new(BatchId::ZERO, depth, bucket_depth);

                let mut ts = 0u64;
                for &lead in &leads {
                    ts += 1;
                    let address = test_address(lead);
                    let bucket = calculate_bucket(&address, 16);
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
            /// does, protected slots included.
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
                    let bucket = calculate_bucket(&address, 16);
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

//! Sharded type-state mutable (ring) stamp issuance.
//!
//! [`ShardedRingIssuer`] is the parallel counterpart of
//! [`RingIssuer`](crate::RingIssuer): it partitions the bucket space across
//! shards so multiple threads can stamp into distinct shards concurrently,
//! while each bucket still advances as a ring cursor that wraps at the bucket
//! capacity.
//!
//! The reservation policy is carried in the same type parameter `R` as the
//! single-threaded ring, so the self-hosting guarantee is identical:
//!
//! - [`ShardedRingIssuer::external`] builds a [`ShardedRingIssuer<Unreserved>`]
//!   that protects nothing.
//! - [`ShardedRingIssuer::reserved`] builds a [`ShardedRingIssuer<Reserved>`]
//!   that never emits a protected slot, distributing the supplied slots to the
//!   shard that owns each bucket.

use std::sync::Mutex;

use nectar_postage::{Batch, BatchId, StampDigest, StampError, StampIndex, calculate_bucket};
use nectar_primitives::ChunkAddress;

use crate::error::IssuerError;
use crate::ring::{Reservation, Reserved, Unreserved};

/// Number of shards for bucket partitioning. Must be a power of two.
const DEFAULT_SHARD_COUNT: usize = 16;

/// A shard owning the ring cursors for a contiguous range of buckets.
///
/// Each shard carries the reservation entries for its own bucket range and is
/// guarded by its own lock, so threads stamping into different shards do not
/// contend.
#[derive(Debug)]
struct RingShard<R> {
    /// Base global bucket index for this shard.
    base_bucket: u32,
    /// Ring cursor and saturation state for each bucket in this shard.
    state: Mutex<RingShardState>,
    /// The reservation entries that fall within this shard's bucket range.
    reservation: R,
}

#[derive(Debug)]
struct RingShardState {
    /// Ring cursor per local bucket. Wraps modulo the bucket capacity.
    cursors: Vec<u32>,
    /// Whether each local bucket has been written to capacity at least once.
    saturated: Vec<bool>,
}

impl<R: Reservation> RingShard<R> {
    fn new(base_bucket: u32, buckets_per_shard: u32, reservation: R) -> Self {
        // `u32` always fits `usize` on the >=32-bit targets this crate supports.
        #[allow(clippy::as_conversions)]
        let count = buckets_per_shard as usize;
        Self {
            base_bucket,
            state: Mutex::new(RingShardState {
                cursors: alloc_zeroed_u32(count),
                saturated: vec![false; count],
            }),
            reservation,
        }
    }

    // Shard routing invariant: callers only pass buckets owned by this shard, so
    // `bucket >= base_bucket` and the subtraction cannot underflow. The offset
    // always fits `usize` on the >=32-bit targets this crate supports (const fn,
    // so `usize::try_from` is unavailable).
    #[allow(clippy::arithmetic_side_effects, clippy::as_conversions)]
    #[inline]
    const fn local_index(&self, bucket: u32) -> usize {
        (bucket - self.base_bucket) as usize
    }

    /// Advances the ring for `bucket` to the next unprotected slot under the
    /// shard lock, returning the slot to emit.
    ///
    /// Returns [`IssuerError::RingExhausted`] if every slot in the bucket is
    /// protected.
    // Shard routing invariant: `local < cursors.len() == saturated.len()` because
    // this shard owns that bucket range. The cursor is kept strictly below
    // `bucket_capacity` (reset to 0 on wrap), so `position + 1` cannot overflow.
    #[allow(clippy::indexing_slicing, clippy::arithmetic_side_effects)]
    fn next_slot(&self, bucket: u32, bucket_capacity: u32) -> Result<u32, IssuerError> {
        let local = self.local_index(bucket);
        // Lock poisoning means another thread already panicked; propagating the
        // panic is the intended behavior.
        #[allow(clippy::expect_used)]
        let mut state = self.state.lock().expect("ring shard lock poisoned");
        for _ in 0..bucket_capacity {
            let position = state.cursors[local];

            let next = position + 1;
            if next >= bucket_capacity {
                state.saturated[local] = true;
                state.cursors[local] = 0;
            } else {
                state.cursors[local] = next;
            }

            if !self.reservation.is_protected(bucket, position) {
                return Ok(position);
            }
        }
        Err(IssuerError::RingExhausted { bucket })
    }

    /// Returns the number of distinct slots written in a bucket, saturating at
    /// the bucket capacity.
    // Shard routing invariant: `local < cursors.len() == saturated.len()` because
    // this shard owns that bucket range.
    #[allow(clippy::indexing_slicing)]
    fn utilization(&self, bucket: u32, bucket_capacity: u32) -> u32 {
        let local = self.local_index(bucket);
        // Lock poisoning means another thread already panicked; propagating the
        // panic is the intended behavior.
        #[allow(clippy::expect_used)]
        let state = self.state.lock().expect("ring shard lock poisoned");
        if state.saturated[local] {
            bucket_capacity
        } else {
            state.cursors[local]
        }
    }
}

fn alloc_zeroed_u32(count: usize) -> Vec<u32> {
    vec![0u32; count]
}

/// A sharded mutable (ring) stamp issuer for high-throughput parallel stamping.
///
/// The bucket space is partitioned across shards, each guarded by its own lock,
/// so threads stamping into distinct shards proceed without contention. Within
/// a bucket, issuance advances a ring cursor that wraps at the bucket capacity,
/// so a later chunk overwrites the slot held by an earlier one.
///
/// The reservation policy `R` decides which slots, if any, the ring must skip:
///
/// - [`ShardedRingIssuer::external`] protects nothing.
/// - [`ShardedRingIssuer::reserved`] never emits a protected slot, routing each
///   protected slot to the shard that owns its bucket.
///
/// Both constructors require a mutable batch; an immutable batch is refused with
/// [`IssuerError::ImmutableNotSupported`].
#[derive(Debug)]
pub struct ShardedRingIssuer<R = Unreserved> {
    /// The batch ID.
    batch_id: BatchId,
    /// The batch depth.
    depth: u8,
    /// The bucket depth.
    bucket_depth: u8,
    /// The bucket capacity, `2^(depth - bucket_depth)`.
    bucket_capacity: u32,
    /// The shards, each owning a contiguous bucket range.
    shards: Vec<RingShard<R>>,
    /// Mask for mapping a bucket to its shard index.
    shard_mask: u32,
    /// Bits to shift a bucket by to obtain its shard index.
    shard_shift: u32,
    /// Maximum utilization observed, guarded for cross-shard aggregation.
    max_utilization: Mutex<u32>,
    /// Total stamps issued, guarded for cross-shard aggregation.
    stamps_issued: Mutex<u64>,
}

impl ShardedRingIssuer<Unreserved> {
    /// Builds an externally tracked sharded ring for a mutable batch.
    ///
    /// The ring protects nothing. The caller tracks usage outside the batch.
    ///
    /// # Errors
    ///
    /// Returns [`IssuerError::ImmutableNotSupported`] if the batch is immutable.
    pub fn external(batch: &Batch) -> Result<Self, IssuerError> {
        Self::for_mutable_batch(batch, |_, _, _| Unreserved)
    }
}

impl ShardedRingIssuer<Reserved> {
    /// Builds a self-hosting sharded ring for a mutable batch with a set of
    /// protected slots.
    ///
    /// Each protected slot is routed to the shard that owns its bucket, so the
    /// ring never emits a protected slot regardless of which thread stamps into
    /// that shard.
    ///
    /// # Errors
    ///
    /// Returns [`IssuerError::ImmutableNotSupported`] if the batch is immutable.
    pub fn reserved(
        batch: &Batch,
        slots: impl IntoIterator<Item = (u32, u32)>,
    ) -> Result<Self, IssuerError> {
        let slots: Vec<(u32, u32)> = slots.into_iter().collect();
        Self::for_mutable_batch(batch, |base, end, _idx| {
            // Distribute the reserved slots to the shard that owns each bucket.
            Reserved::new(
                slots
                    .iter()
                    .copied()
                    .filter(|&(bucket, _)| bucket >= base && bucket < end),
            )
        })
    }
}

impl<R: Reservation> ShardedRingIssuer<R> {
    fn for_mutable_batch(
        batch: &Batch,
        make_reservation: impl Fn(u32, u32, usize) -> R,
    ) -> Result<Self, IssuerError> {
        if batch.immutable() {
            return Err(IssuerError::ImmutableNotSupported);
        }
        Ok(Self::with_shard_count(
            batch.id(),
            batch.depth(),
            batch.bucket_depth(),
            DEFAULT_SHARD_COUNT,
            make_reservation,
        ))
    }

    // All arithmetic is on validated shard geometry: `shard_count` is a nonzero
    // power of two clamped to `total_buckets = 2^bucket_depth`, so the division,
    // the `shard_count - 1` mask, `bucket_depth - shard_bits`, and the
    // `i * buckets_per_shard` shard bases (bounded by `total_buckets`) cannot
    // divide by zero, underflow, or overflow; `depth >= bucket_depth` is the
    // batch geometry invariant.
    #[allow(clippy::arithmetic_side_effects)]
    fn with_shard_count(
        batch_id: BatchId,
        depth: u8,
        bucket_depth: u8,
        shard_count: usize,
        make_reservation: impl Fn(u32, u32, usize) -> R,
    ) -> Self {
        assert!(
            shard_count.is_power_of_two(),
            "shard_count must be a power of 2"
        );

        let total_buckets = 1u32 << bucket_depth;
        // `u32` always fits `usize` on the >=32-bit targets this crate supports.
        #[allow(clippy::as_conversions)]
        let shard_count = shard_count.min(total_buckets as usize);
        // `shard_count <= total_buckets <= u32::MAX` after the clamp above, so
        // the narrowing is lossless.
        #[allow(clippy::as_conversions)]
        let shard_count_u32 = shard_count as u32;
        let buckets_per_shard = total_buckets / shard_count_u32;
        let bucket_capacity = 1u32 << (depth - bucket_depth);

        let shard_bits = shard_count_u32.trailing_zeros();
        let shard_shift = u32::from(bucket_depth) - shard_bits;
        let shard_mask = shard_count_u32 - 1;

        let shards: Vec<_> = (0..shard_count)
            .map(|i| {
                // `i < shard_count <= u32::MAX`, so the narrowing is lossless.
                #[allow(clippy::as_conversions)]
                let base = i as u32 * buckets_per_shard;
                let end = base + buckets_per_shard;
                RingShard::new(base, buckets_per_shard, make_reservation(base, end, i))
            })
            .collect();

        Self {
            batch_id,
            depth,
            bucket_depth,
            bucket_capacity,
            shards,
            shard_mask,
            shard_shift,
            max_utilization: Mutex::new(0),
            stamps_issued: Mutex::new(0),
        }
    }

    // The masked value always fits `usize` on the >=32-bit targets this crate
    // supports (const fn, so `usize::try_from` is unavailable).
    #[allow(clippy::as_conversions)]
    #[inline]
    const fn shard_index(&self, bucket: u32) -> usize {
        ((bucket >> self.shard_shift) & self.shard_mask) as usize
    }

    /// Prepares a stamp digest for the given chunk address.
    ///
    /// Thread-safe: it may be called concurrently from multiple threads, with
    /// contention limited to the shard owning the chunk's bucket.
    ///
    /// # Errors
    ///
    /// Returns [`StampError::BucketFull`] only when every slot in the target
    /// bucket is protected, which is geometrically impossible at real batch
    /// depths.
    ///
    /// # Panics
    ///
    /// Panics if an internal lock is poisoned, i.e. another stamping thread
    /// already panicked; propagating the panic is the intended behavior.
    pub fn prepare_stamp(
        &self,
        address: &ChunkAddress,
        timestamp: u64,
    ) -> Result<StampDigest, StampError> {
        let bucket = calculate_bucket(address, self.bucket_depth);
        // `shard_index` masks with `shard_mask = shards.len() - 1`, so the index
        // is always in range.
        #[allow(clippy::indexing_slicing)]
        let shard = &self.shards[self.shard_index(bucket)];

        let position =
            shard
                .next_slot(bucket, self.bucket_capacity)
                .map_err(|_| StampError::BucketFull {
                    bucket,
                    capacity: self.bucket_capacity,
                })?;

        {
            // Lock poisoning means another thread already panicked; propagating
            // the panic is the intended behavior.
            #[allow(clippy::expect_used)]
            let mut issued = self.stamps_issued.lock().expect("stamps lock poisoned");
            // Monotone u64 issuance counter; one increment per stamp cannot
            // realistically overflow 2^64.
            #[allow(clippy::arithmetic_side_effects)]
            {
                *issued += 1;
            }
        }
        let fill = shard.utilization(bucket, self.bucket_capacity);
        {
            // Lock poisoning means another thread already panicked; propagating
            // the panic is the intended behavior.
            #[allow(clippy::expect_used)]
            let mut max = self
                .max_utilization
                .lock()
                .expect("max utilization lock poisoned");
            if fill > *max {
                *max = fill;
            }
        }

        let index = StampIndex::new(bucket, position);
        Ok(StampDigest::new(*address, self.batch_id, index, timestamp))
    }

    /// Returns the batch ID.
    pub const fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    /// Returns the batch depth.
    pub const fn batch_depth(&self) -> u8 {
        self.depth
    }

    /// Returns the bucket depth.
    pub const fn bucket_depth(&self) -> u8 {
        self.bucket_depth
    }

    /// Returns the bucket capacity, `2^(depth - bucket_depth)`.
    pub const fn bucket_capacity(&self) -> u32 {
        self.bucket_capacity
    }

    /// Returns the number of shards.
    pub const fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Returns the current utilization of a specific bucket, saturating at the
    /// bucket capacity once the ring has wrapped.
    pub fn bucket_utilization(&self, bucket: u32) -> u32 {
        // `shard_index` masks with `shard_mask = shards.len() - 1`, so the index
        // is always in range.
        #[allow(clippy::indexing_slicing)]
        let shard = &self.shards[self.shard_index(bucket)];
        shard.utilization(bucket, self.bucket_capacity)
    }

    /// Returns the maximum bucket utilization observed across all buckets.
    ///
    /// # Panics
    ///
    /// Panics if the utilization lock is poisoned, i.e. another stamping thread
    /// already panicked; propagating the panic is the intended behavior.
    #[allow(clippy::expect_used)]
    pub fn max_bucket_utilization(&self) -> u32 {
        *self
            .max_utilization
            .lock()
            .expect("max utilization lock poisoned")
    }

    /// Returns the total number of stamps issued.
    ///
    /// # Panics
    ///
    /// Panics if the issuance counter lock is poisoned, i.e. another stamping
    /// thread already panicked; propagating the panic is the intended behavior.
    #[allow(clippy::expect_used)]
    pub fn stamps_issued(&self) -> u64 {
        *self.stamps_issued.lock().expect("stamps lock poisoned")
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

    fn mutable_batch(depth: u8, bucket_depth: u8) -> Batch {
        Batch::new(
            BatchId::ZERO,
            0,
            0,
            Default::default(),
            depth,
            bucket_depth,
            false,
        )
    }

    fn immutable_batch(depth: u8, bucket_depth: u8) -> Batch {
        Batch::new(
            BatchId::ZERO,
            0,
            0,
            Default::default(),
            depth,
            bucket_depth,
            true,
        )
    }

    #[test]
    fn external_sharded_ring_wraps_and_reuses_slots() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket.
        let batch = mutable_batch(17, 16);
        let issuer = ShardedRingIssuer::external(&batch).unwrap();

        let address = test_address(0xABCD);

        let d0 = issuer.prepare_stamp(&address, 1).unwrap();
        let d1 = issuer.prepare_stamp(&address, 2).unwrap();
        let d2 = issuer.prepare_stamp(&address, 3).unwrap();
        assert_eq!(d0.index.index(), 0);
        assert_eq!(d1.index.index(), 1);
        assert_eq!(d2.index.index(), 0);
        assert_eq!(issuer.stamps_issued(), 3);
    }

    #[test]
    fn reserved_sharded_ring_never_emits_a_protected_slot() {
        // depth=18, bucket_depth=16 gives 4 slots per bucket. Protect slots 1
        // and 3 in the target bucket; the ring may only ever emit 0 and 2.
        let batch = mutable_batch(18, 16);
        let bucket = calculate_bucket(&test_address(0x00AA), 16);
        let issuer = ShardedRingIssuer::reserved(&batch, [(bucket, 1), (bucket, 3)]).unwrap();

        let address = test_address(0x00AA);
        for ts in 0..50u64 {
            let digest = issuer.prepare_stamp(&address, ts).unwrap();
            let index = digest.index.index();
            assert!(
                index == 0 || index == 2,
                "sharded ring emitted protected or out-of-range slot {index}"
            );
        }
    }

    #[test]
    fn reserved_sharded_ring_routes_slots_to_the_owning_shard() {
        // Two buckets in distinct shards each protect a different slot. Each
        // shard must apply only its own bucket's protection.
        let batch = mutable_batch(17, 16);
        let bucket_lo = 0x0001u32;
        let bucket_hi = 0xF001u32;
        let issuer = ShardedRingIssuer::reserved(&batch, [(bucket_lo, 0), (bucket_hi, 1)]).unwrap();

        #[allow(clippy::as_conversions)] // 0x0001 fits u16, lossless
        let addr_lo = test_address(bucket_lo as u16);
        #[allow(clippy::as_conversions)] // 0xF001 fits u16, lossless
        let addr_hi = test_address(bucket_hi as u16);

        // bucket_lo protects slot 0, so it may only emit slot 1.
        for ts in 0..10u64 {
            assert_eq!(issuer.prepare_stamp(&addr_lo, ts).unwrap().index.index(), 1);
        }
        // bucket_hi protects slot 1, so it may only emit slot 0.
        for ts in 0..10u64 {
            assert_eq!(issuer.prepare_stamp(&addr_hi, ts).unwrap().index.index(), 0);
        }
    }

    #[test]
    fn external_refuses_immutable_batch() {
        let batch = immutable_batch(20, 16);
        assert!(matches!(
            ShardedRingIssuer::external(&batch),
            Err(IssuerError::ImmutableNotSupported)
        ));
    }

    #[test]
    fn reserved_refuses_immutable_batch() {
        let batch = immutable_batch(20, 16);
        assert!(matches!(
            ShardedRingIssuer::reserved(&batch, [(0u32, 0u32)]),
            Err(IssuerError::ImmutableNotSupported)
        ));
    }

    #[test]
    fn reserved_sharded_ring_exhausts_when_every_slot_is_protected() {
        let batch = mutable_batch(17, 16);
        let bucket = calculate_bucket(&test_address(0x0001), 16);
        let issuer = ShardedRingIssuer::reserved(&batch, [(bucket, 0), (bucket, 1)]).unwrap();

        let address = test_address(0x0001);
        assert!(matches!(
            issuer.prepare_stamp(&address, 1),
            Err(StampError::BucketFull { bucket: b, capacity: 2 }) if b == bucket
        ));
    }
}

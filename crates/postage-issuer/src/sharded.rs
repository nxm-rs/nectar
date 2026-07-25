//! Sharded issuer for high-throughput parallel stamping.
//!
//! The [`ShardedIssuer`] partitions buckets across multiple shards, where each shard
//! is protected by its own lock. This allows multiple threads to stamp chunks
//! simultaneously as long as they target different shards.
//!
//! ```text
//! Bucket Space: [0...65535]
//!              ↓
//! Shard 0: [0...16383]     ← Lock A
//! Shard 1: [16384...32767] ← Lock B
//! Shard 2: [32768...49151] ← Lock C
//! Shard 3: [49152...65535] ← Lock D
//! ```

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::error::IssuerError;
use crate::issuer::StampIssuer;
use nectar_postage::{
    Batch, BatchId, BucketDepth, StampDigest, StampError, StampIndex, calculate_bucket,
};
use nectar_primitives::{ChunkAddress, Mainnet, SwarmSpec};

/// Number of shards for bucket partitioning.
/// Must be a power of 2 for efficient bucket-to-shard mapping.
const DEFAULT_SHARD_COUNT: usize = 16;

/// A shard containing bucket indices for a subset of the bucket space.
#[derive(Debug)]
struct BucketShard {
    /// Base bucket index for this shard.
    base_bucket: u32,
    /// Current index for each bucket in this shard.
    /// Uses atomic u32 for lock-free updates within the shard.
    indices: Vec<AtomicU32>,
}

impl BucketShard {
    fn new(base_bucket: u32, bucket_count: u32) -> Self {
        let indices = (0..bucket_count).map(|_| AtomicU32::new(0)).collect();
        Self {
            base_bucket,
            indices,
        }
    }

    /// Returns the local index within this shard for a given global bucket.
    // Shard routing invariant: callers only pass buckets owned by this shard, so
    // `bucket >= base_bucket` and the subtraction cannot underflow. The offset
    // always fits `usize` on the >=32-bit targets this crate supports (const fn,
    // so `usize::try_from` is unavailable).
    #[allow(clippy::arithmetic_side_effects, clippy::as_conversions)]
    #[inline]
    const fn local_index(&self, bucket: u32) -> usize {
        (bucket - self.base_bucket) as usize
    }

    /// Allocates the next index for a bucket, returning the allocated index.
    /// Returns None if the bucket is full.
    // Shard routing invariant: `local_index(bucket) < indices.len()` because this
    // shard owns buckets `[base_bucket, base_bucket + indices.len())`.
    #[allow(clippy::indexing_slicing)]
    #[inline]
    fn allocate(&self, bucket: u32, bucket_capacity: u32) -> Option<u32> {
        let local_idx = self.local_index(bucket);
        let current = self.indices[local_idx].fetch_add(1, Ordering::Relaxed);
        if current >= bucket_capacity {
            // Roll back - bucket is full
            self.indices[local_idx].fetch_sub(1, Ordering::Relaxed);
            None
        } else {
            Some(current)
        }
    }

    /// Gets the current utilization of a bucket.
    // Shard routing invariant: `local_index(bucket) < indices.len()` because this
    // shard owns buckets `[base_bucket, base_bucket + indices.len())`.
    #[allow(clippy::indexing_slicing)]
    #[inline]
    fn utilization(&self, bucket: u32) -> u32 {
        let local_idx = self.local_index(bucket);
        self.indices[local_idx].load(Ordering::Relaxed)
    }
}

/// A sharded stamp issuer for high-throughput parallel stamping.
///
/// This issuer partitions the bucket space across multiple shards, allowing
/// concurrent access from multiple threads with minimal contention.
///
/// # Example
///
/// The network is a type parameter and reaches the issuer through its
/// [`BucketDepth`]; [`ShardedIssuer`] is the mainnet issuer.
///
/// ```ignore
/// use nectar_postage_issuer::{BatchId, BucketDepth, ShardedIssuer};
///
/// let issuer = ShardedIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());
/// // Safe to stamp from multiple threads, each holding a `&ShardedIssuer`.
/// ```
#[derive(Debug)]
pub struct ShardedIssuerFor<S: SwarmSpec = Mainnet> {
    /// The batch ID.
    batch_id: BatchId,
    /// The batch depth.
    depth: u8,
    /// The bucket depth.
    bucket_depth: BucketDepth<S>,
    /// The bucket capacity (2^(depth - bucket_depth)).
    bucket_capacity: u32,
    /// The shards containing bucket indices.
    shards: Vec<BucketShard>,
    /// Mask for mapping bucket to shard (shard_count - 1).
    shard_mask: u32,
    /// Bits to shift for shard index.
    shard_shift: u32,
    /// Maximum utilization tracker (atomic for thread-safety).
    max_utilization: AtomicU32,
    /// Total stamps issued (atomic for thread-safety).
    stamps_issued: AtomicU64,
}

/// The [`ShardedIssuerFor`] of the mainnet spec.
pub type ShardedIssuer = ShardedIssuerFor<Mainnet>;

impl<S: SwarmSpec> ShardedIssuerFor<S> {
    /// Creates a new sharded issuer with the default number of shards.
    pub fn new(batch_id: BatchId, depth: u8, bucket_depth: BucketDepth<S>) -> Self {
        Self::with_shard_count(batch_id, depth, bucket_depth, DEFAULT_SHARD_COUNT)
    }

    /// Creates a new sharded issuer with a specific number of shards.
    ///
    /// # Panics
    ///
    /// Panics if `shard_count` is not a power of 2 or is greater than the bucket count.
    // All arithmetic is on validated shard geometry: `shard_count` is a nonzero
    // power of two clamped to `total_buckets = 2^bucket_depth`, so the division,
    // the `shard_count - 1` mask, `bucket_depth - shard_bits`, and the
    // `i * buckets_per_shard` shard bases (bounded by `total_buckets`) cannot
    // divide by zero, underflow, or overflow; `depth >= bucket_depth` is the
    // batch geometry invariant.
    #[allow(clippy::arithmetic_side_effects)]
    pub fn with_shard_count(
        batch_id: BatchId,
        depth: u8,
        bucket_depth: BucketDepth<S>,
        shard_count: usize,
    ) -> Self {
        assert!(
            shard_count.is_power_of_two(),
            "shard_count must be a power of 2"
        );

        let total_buckets = 1u32 << bucket_depth.get();
        // `u32` always fits `usize` on the >=32-bit targets this crate supports.
        #[allow(clippy::as_conversions)]
        let shard_count = shard_count.min(total_buckets as usize);
        // `shard_count <= total_buckets <= u32::MAX` after the clamp above, so
        // the narrowing is lossless.
        #[allow(clippy::as_conversions)]
        let shard_count_u32 = shard_count as u32;
        let buckets_per_shard = total_buckets / shard_count_u32;
        let bucket_capacity = 1u32 << (depth - bucket_depth.get());

        // Calculate shard_shift: how many bits to shift bucket to get shard index
        // For bucket_depth=16 and shard_count=16, we take top 4 bits: shift = 16 - 4 = 12
        let shard_bits = shard_count_u32.trailing_zeros();
        let shard_shift = u32::from(bucket_depth.get()) - shard_bits;
        let shard_mask = shard_count_u32 - 1;

        let shards: Vec<_> = (0..shard_count)
            .map(|i| {
                // `i < shard_count <= u32::MAX`, so the narrowing is lossless.
                #[allow(clippy::as_conversions)]
                let base = i as u32 * buckets_per_shard;
                BucketShard::new(base, buckets_per_shard)
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
            max_utilization: AtomicU32::new(0),
            stamps_issued: AtomicU64::new(0),
        }
    }

    /// Creates a sharded issuer from a batch.
    ///
    /// Immutable batches yield a fill-only issuer. Mutable batches are refused
    /// with [`IssuerError::MutableNotSupported`], matching
    /// [`MemoryIssuer::from_batch`](crate::MemoryIssuer::from_batch), so a ring
    /// is never produced by accident. Overwrite-aware parallel issuance must be
    /// requested by name through
    /// [`ShardedRingIssuer::external`](crate::ShardedRingIssuer::external) for
    /// external tracking, or
    /// [`ShardedRingIssuer::reserved`](crate::ShardedRingIssuer::reserved) for
    /// self-hosting, where the protected slots come from `nectar-postage-usage`.
    pub fn from_batch(batch: &Batch<S>) -> Result<Self, IssuerError> {
        if batch.immutable() {
            Ok(Self::new(batch.id(), batch.depth(), batch.bucket_depth()))
        } else {
            Err(IssuerError::MutableNotSupported)
        }
    }

    /// Applies an on-chain dilution, growing the per-bucket capacity without
    /// moving any watermark.
    ///
    /// The new depth must not decrease. Counters are untouched; only the
    /// capacity that bounds them grows, so the next stamp in a previously full
    /// bucket succeeds. This mirrors [`MemoryIssuer::dilute`](crate::MemoryIssuer::dilute)
    /// for the parallel issuer and is the prerequisite for topping up a batch in
    /// place.
    ///
    /// # Errors
    ///
    /// Returns [`IssuerError::DepthDecrease`] if `new_depth` is below the current
    /// depth.
    pub const fn dilute(&mut self, new_depth: u8) -> Result<(), IssuerError> {
        if new_depth < self.depth {
            return Err(IssuerError::DepthDecrease {
                current: self.depth,
                requested: new_depth,
            });
        }
        self.depth = new_depth;
        // `new_depth >= self.depth >= self.bucket_depth` (checked above plus the
        // batch geometry invariant), so the subtraction cannot underflow.
        #[allow(clippy::arithmetic_side_effects)]
        {
            self.bucket_capacity = 1u32 << (new_depth - self.bucket_depth.get());
        }
        Ok(())
    }

    /// Maps a bucket to its shard index.
    // The masked value always fits `usize` on the >=32-bit targets this crate
    // supports (const fn, so `usize::try_from` is unavailable).
    #[allow(clippy::as_conversions)]
    #[inline]
    const fn shard_index(&self, bucket: u32) -> usize {
        ((bucket >> self.shard_shift) & self.shard_mask) as usize
    }

    /// Prepares a stamp digest for the given chunk address.
    ///
    /// This is thread-safe and can be called concurrently from multiple threads.
    pub fn prepare_stamp(
        &self,
        address: &ChunkAddress,
        timestamp: u64,
    ) -> Result<StampDigest, StampError> {
        let bucket = calculate_bucket(address, self.bucket_depth.get());
        let shard_idx = self.shard_index(bucket);
        // `shard_index` masks with `shard_mask = shards.len() - 1`, so the index
        // is always in range.
        #[allow(clippy::indexing_slicing)]
        let shard = &self.shards[shard_idx];

        let position =
            shard
                .allocate(bucket, self.bucket_capacity)
                .ok_or(StampError::BucketFull {
                    bucket,
                    capacity: self.bucket_capacity,
                })?;

        // Update stats (relaxed ordering is fine for stats)
        self.stamps_issued.fetch_add(1, Ordering::Relaxed);

        // Update max utilization (compare-and-swap loop).
        // `position < bucket_capacity <= u32::MAX` (allocate returned Some), so
        // the increment cannot overflow.
        #[allow(clippy::arithmetic_side_effects)]
        let new_util = position + 1;
        let mut current_max = self.max_utilization.load(Ordering::Relaxed);
        while new_util > current_max {
            match self.max_utilization.compare_exchange_weak(
                current_max,
                new_util,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }

        let index = StampIndex::new(bucket, position);
        Ok(StampDigest::new(*address, self.batch_id, index, timestamp))
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

    /// Maximum bucket utilization observed across all buckets.
    pub fn max_bucket_utilization(&self) -> u32 {
        self.max_utilization.load(Ordering::Relaxed)
    }

    /// Current utilization of a specific bucket.
    // `shard_index` masks with `shard_mask = shards.len() - 1`, so the index is
    // always in range.
    #[allow(clippy::indexing_slicing)]
    pub fn bucket_utilization(&self, bucket: u32) -> u32 {
        let shard_idx = self.shard_index(bucket);
        self.shards[shard_idx].utilization(bucket)
    }

    /// Total stamps issued.
    pub fn stamps_issued(&self) -> u64 {
        self.stamps_issued.load(Ordering::Relaxed)
    }

    /// Bucket capacity.
    pub const fn bucket_capacity(&self) -> u32 {
        self.bucket_capacity
    }

    /// Number of shards.
    pub const fn shard_count(&self) -> usize {
        self.shards.len()
    }
}

impl<S: SwarmSpec> StampIssuer for ShardedIssuerFor<S> {
    fn prepare_stamp(
        &mut self,
        address: &ChunkAddress,
        timestamp: u64,
    ) -> Result<StampDigest, StampError> {
        // The inherent shared-reference method; inherent methods shadow the
        // trait method here.
        (*self).prepare_stamp(address, timestamp)
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
        Self::bucket_utilization(self, bucket) < Self::bucket_capacity(self)
    }

    fn stamps_issued(&self) -> Option<u64> {
        Some(Self::stamps_issued(self))
    }
}

/// Shared-handle issuance: allocation needs only `&self`, so several
/// pipelines may admit concurrently from one issuer, each holding its own
/// `&ShardedIssuerFor`.
impl<S: SwarmSpec> StampIssuer for &ShardedIssuerFor<S> {
    fn prepare_stamp(
        &mut self,
        address: &ChunkAddress,
        timestamp: u64,
    ) -> Result<StampDigest, StampError> {
        (**self).prepare_stamp(address, timestamp)
    }

    fn batch_id(&self) -> BatchId {
        ShardedIssuerFor::batch_id(self)
    }

    fn batch_depth(&self) -> u8 {
        ShardedIssuerFor::batch_depth(self)
    }

    fn bucket_depth(&self) -> u8 {
        ShardedIssuerFor::bucket_depth(self)
    }

    fn max_bucket_utilization(&self) -> u32 {
        ShardedIssuerFor::max_bucket_utilization(self)
    }

    fn bucket_utilization(&self, bucket: u32) -> u32 {
        ShardedIssuerFor::bucket_utilization(self, bucket)
    }

    fn bucket_has_capacity(&self, bucket: u32) -> bool {
        ShardedIssuerFor::bucket_utilization(self, bucket) < ShardedIssuerFor::bucket_capacity(self)
    }

    fn stamps_issued(&self) -> Option<u64> {
        Some(ShardedIssuerFor::stamps_issued(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;

    #[test]
    fn test_sharded_issuer_from_batch_mutable_refused() {
        use nectar_postage::Batch;

        // The parallel constructor refuses a mutable batch for the same reason
        // as MemoryIssuer: a reserved-blind ring would silently overwrite a
        // self-hosted snapshot's own chunks.
        let mutable = Batch::new(
            BatchId::ZERO,
            0,
            0,
            Default::default(),
            20,
            BucketDepth::new(16).unwrap(),
            false,
        );
        assert!(matches!(
            ShardedIssuer::from_batch(&mutable),
            Err(IssuerError::MutableNotSupported)
        ));
    }

    #[test]
    fn test_sharded_issuer_from_batch_immutable_ok() {
        use nectar_postage::Batch;

        let immutable = Batch::new(
            BatchId::ZERO,
            0,
            0,
            Default::default(),
            20,
            BucketDepth::new(16).unwrap(),
            true,
        );
        assert!(ShardedIssuer::from_batch(&immutable).is_ok());
    }

    #[test]
    fn test_sharded_issuer_basic() {
        let issuer = ShardedIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());

        assert_eq!(issuer.batch_id(), BatchId::ZERO);
        assert_eq!(issuer.batch_depth(), 20);
        assert_eq!(issuer.bucket_depth(), 16);
        assert_eq!(issuer.bucket_capacity(), 16); // 2^(20-16) = 16
        assert_eq!(issuer.shard_count(), DEFAULT_SHARD_COUNT);
    }

    #[test]
    fn test_sharded_issuer_prepare_stamp() {
        let issuer = ShardedIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());
        let address = ChunkAddress::from(B256::random());

        let digest = issuer.prepare_stamp(&address, 12345).unwrap();

        assert_eq!(digest.batch_id, BatchId::ZERO);
        assert_eq!(digest.timestamp, 12345);
        assert_eq!(issuer.stamps_issued(), 1);
    }

    #[test]
    fn test_sharded_issuer_dilute_grows_capacity_only() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket.
        let mut issuer = ShardedIssuer::new(BatchId::ZERO, 17, BucketDepth::new(16).unwrap());
        let address = ChunkAddress::from(B256::repeat_byte(0xAB));
        let bucket = calculate_bucket(&address, 16);

        issuer.prepare_stamp(&address, 1).unwrap();
        issuer.prepare_stamp(&address, 2).unwrap();
        assert!(issuer.prepare_stamp(&address, 3).is_err());

        issuer.dilute(18).unwrap();
        assert_eq!(issuer.bucket_capacity(), 4);
        assert_eq!(issuer.batch_depth(), 18);
        // The watermark is unchanged, so the next slot is 2.
        let d = issuer.prepare_stamp(&address, 4).unwrap();
        assert_eq!(d.index.index(), 2);
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
    fn test_sharded_issuer_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let issuer = Arc::new(ShardedIssuer::new(
            BatchId::ZERO,
            24,
            BucketDepth::new(16).unwrap(),
        ));
        let num_threads = 8;
        let stamps_per_thread = 1000;

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let issuer = Arc::clone(&issuer);
                thread::spawn(move || {
                    for _ in 0..stamps_per_thread {
                        let addr = ChunkAddress::from(B256::random());
                        issuer.prepare_stamp(&addr, 0).unwrap();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // `8 * 1000` is positive and fits `u64`; the cast is lossless.
        #[allow(clippy::as_conversions)]
        let expected = (num_threads * stamps_per_thread) as u64;
        assert_eq!(issuer.stamps_issued(), expected);
    }

    #[test]
    fn test_shared_handle_implements_stamp_issuer() {
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
}

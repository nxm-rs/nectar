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

use alloy_primitives::B256;
use alloy_signer::Signature;
use nectar_postage::{
    Batch, BatchId, Stamp, StampDigest, StampError, StampIndex, calculate_bucket, current_timestamp,
};
use nectar_primitives::SwarmAddress;

use crate::error::SigningError;

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
    #[inline]
    const fn local_index(&self, bucket: u32) -> usize {
        (bucket - self.base_bucket) as usize
    }

    /// Allocates the next index for a bucket, returning the allocated index.
    /// Returns None if the bucket is full.
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
/// ```ignore
/// use nectar_postage_issuer::ShardedIssuer;
/// use alloy_primitives::B256;
///
/// let issuer = ShardedIssuer::new(B256::ZERO, 20, 16);
/// // Now safe to use from multiple threads via sign_stamps_parallel
/// ```
#[derive(Debug)]
pub struct ShardedIssuer {
    /// The batch ID.
    batch_id: BatchId,
    /// The batch depth.
    depth: u8,
    /// The bucket depth.
    bucket_depth: u8,
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

impl ShardedIssuer {
    /// Creates a new sharded issuer with the default number of shards.
    pub fn new(batch_id: BatchId, depth: u8, bucket_depth: u8) -> Self {
        Self::with_shard_count(batch_id, depth, bucket_depth, DEFAULT_SHARD_COUNT)
    }

    /// Creates a new sharded issuer with a specific number of shards.
    ///
    /// # Panics
    ///
    /// Panics if `shard_count` is not a power of 2 or is greater than the bucket count.
    pub fn with_shard_count(
        batch_id: BatchId,
        depth: u8,
        bucket_depth: u8,
        shard_count: usize,
    ) -> Self {
        assert!(
            shard_count.is_power_of_two(),
            "shard_count must be a power of 2"
        );

        let total_buckets = 1u32 << bucket_depth;
        let shard_count = shard_count.min(total_buckets as usize);
        let buckets_per_shard = total_buckets / shard_count as u32;
        let bucket_capacity = 1u32 << (depth - bucket_depth);

        // Calculate shard_shift: how many bits to shift bucket to get shard index
        // For bucket_depth=16 and shard_count=16, we take top 4 bits: shift = 16 - 4 = 12
        let shard_bits = (shard_count as u32).trailing_zeros();
        let shard_shift = bucket_depth as u32 - shard_bits;
        let shard_mask = (shard_count - 1) as u32;

        let shards: Vec<_> = (0..shard_count)
            .map(|i| BucketShard::new(i as u32 * buckets_per_shard, buckets_per_shard))
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
    pub fn from_batch(batch: &Batch) -> Self {
        Self::new(batch.id(), batch.depth(), batch.bucket_depth())
    }

    /// Maps a bucket to its shard index.
    #[inline]
    const fn shard_index(&self, bucket: u32) -> usize {
        ((bucket >> self.shard_shift) & self.shard_mask) as usize
    }

    /// Prepares a stamp digest for the given chunk address.
    ///
    /// This is thread-safe and can be called concurrently from multiple threads.
    pub fn prepare_stamp(
        &self,
        address: &SwarmAddress,
        timestamp: u64,
    ) -> Result<StampDigest, StampError> {
        let bucket = calculate_bucket(address, self.bucket_depth);
        let shard_idx = self.shard_index(bucket);
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

        // Update max utilization (compare-and-swap loop)
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

    /// Returns the maximum bucket utilization observed.
    pub fn max_bucket_utilization(&self) -> u32 {
        self.max_utilization.load(Ordering::Relaxed)
    }

    /// Returns the utilization of a specific bucket.
    pub fn bucket_utilization(&self, bucket: u32) -> u32 {
        let shard_idx = self.shard_index(bucket);
        self.shards[shard_idx].utilization(bucket)
    }

    /// Returns the total number of stamps issued.
    pub fn stamps_issued(&self) -> u64 {
        self.stamps_issued.load(Ordering::Relaxed)
    }

    /// Returns the bucket capacity.
    pub const fn bucket_capacity(&self) -> u32 {
        self.bucket_capacity
    }

    /// Returns the number of shards.
    pub const fn shard_count(&self) -> usize {
        self.shards.len()
    }
}

// SAFETY: ShardedIssuer uses atomic operations for all mutable state
unsafe impl Sync for ShardedIssuer {}
unsafe impl Send for ShardedIssuer {}

/// Result of a parallel stamp operation.
#[derive(Debug)]
pub struct StampResult {
    /// The chunk address that was stamped.
    pub address: SwarmAddress,
    /// The resulting stamp, or error message.
    pub result: Result<Stamp, SigningError>,
}

/// Signs multiple chunks in parallel using the provided signer.
///
/// This function distributes the work across multiple threads using rayon.
/// The signer must be `Sync` as it will be shared across threads.
///
/// # EIP-191 Compatibility
///
/// The signer function receives the prehash (32-byte keccak256 of stamp data)
/// and should sign it using EIP-191 personal message signing to be compatible
/// with Go/bee implementations. Use `SignerSync::sign_message_sync(prehash.as_slice())`.
///
/// # Arguments
///
/// * `issuer` - The sharded issuer for allocating bucket indices
/// * `signer` - A synchronous signer that implements `Sync`. Should use EIP-191 signing.
/// * `addresses` - The chunk addresses to stamp
///
/// # Returns
///
/// A vector of stamp results in the same order as the input addresses.
///
/// # Example
///
/// ```ignore
/// use nectar_postage_issuer::{sign_stamps_parallel, ShardedIssuer};
/// use alloy_primitives::B256;
/// use alloy_signer::SignerSync;
///
/// let issuer = ShardedIssuer::new(B256::ZERO, 20, 16);
/// let addresses: Vec<SwarmAddress> = /* ... */;
/// // Use sign_message_sync for EIP-191 compatibility with Go/bee
/// let signer_fn = |prehash: &B256| signer.sign_message_sync(prehash.as_slice());
/// let results = sign_stamps_parallel(&issuer, &signer_fn, &addresses);
/// ```
#[cfg(feature = "parallel")]
pub fn sign_stamps_parallel<S, E>(
    issuer: &ShardedIssuer,
    signer: &S,
    addresses: &[SwarmAddress],
) -> Vec<StampResult>
where
    S: Fn(&B256) -> Result<Signature, E> + Sync,
    E: Into<SigningError>,
{
    use rayon::prelude::*;

    addresses
        .par_iter()
        .map(|address| {
            let result = sign_stamp_internal(issuer, signer, address);
            StampResult {
                address: *address,
                result,
            }
        })
        .collect()
}

#[cfg(feature = "parallel")]
fn sign_stamp_internal<S, E>(
    issuer: &ShardedIssuer,
    signer: &S,
    address: &SwarmAddress,
) -> Result<Stamp, SigningError>
where
    S: Fn(&B256) -> Result<Signature, E>,
    E: Into<SigningError>,
{
    let timestamp = current_timestamp();
    let digest = issuer.prepare_stamp(address, timestamp)?;
    let prehash = digest.to_prehash();
    let sig = signer(&prehash).map_err(|e| e.into())?;
    Ok(stamp_from_signature(&digest, sig))
}

/// Creates a stamp from a digest and signature.
#[cfg(feature = "parallel")]
#[inline]
fn stamp_from_signature(digest: &StampDigest, sig: Signature) -> Stamp {
    // Signature is now stored directly in Stamp
    Stamp::with_index(digest.batch_id, digest.index, digest.timestamp, sig)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;

    fn random_address() -> SwarmAddress {
        let mut bytes = [0u8; 32];
        for b in &mut bytes {
            *b = rand::random();
        }
        SwarmAddress::new(bytes)
    }

    #[test]
    fn test_sharded_issuer_basic() {
        let issuer = ShardedIssuer::new(B256::ZERO, 20, 16);

        assert_eq!(issuer.batch_id(), B256::ZERO);
        assert_eq!(issuer.batch_depth(), 20);
        assert_eq!(issuer.bucket_depth(), 16);
        assert_eq!(issuer.bucket_capacity(), 16); // 2^(20-16) = 16
        assert_eq!(issuer.shard_count(), DEFAULT_SHARD_COUNT);
    }

    #[test]
    fn test_sharded_issuer_prepare_stamp() {
        let issuer = ShardedIssuer::new(B256::ZERO, 20, 16);
        let address = random_address();

        let digest = issuer.prepare_stamp(&address, 12345).unwrap();

        assert_eq!(digest.batch_id, B256::ZERO);
        assert_eq!(digest.timestamp, 12345);
        assert_eq!(issuer.stamps_issued(), 1);
    }

    #[test]
    fn test_sharded_issuer_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let issuer = Arc::new(ShardedIssuer::new(B256::ZERO, 24, 16));
        let num_threads = 8;
        let stamps_per_thread = 1000;

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let issuer = Arc::clone(&issuer);
                thread::spawn(move || {
                    for _ in 0..stamps_per_thread {
                        let addr = random_address();
                        issuer.prepare_stamp(&addr, 0).unwrap();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(
            issuer.stamps_issued(),
            (num_threads * stamps_per_thread) as u64
        );
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn test_parallel_signing() {
        use crate::error::SigningError;

        let issuer = ShardedIssuer::new(B256::ZERO, 24, 16);
        let signer = PrivateKeySigner::random();

        let addresses: Vec<_> = (0..100).map(|_| random_address()).collect();

        let sign_fn = |prehash: &B256| -> Result<Signature, SigningError> {
            Ok(signer
                .sign_message_sync(prehash.as_slice())
                .map_err(alloy_signer::Error::other)?)
        };

        let results = sign_stamps_parallel(&issuer, &sign_fn, &addresses);

        assert_eq!(results.len(), 100);
        for result in &results {
            assert!(result.result.is_ok());
        }
        assert_eq!(issuer.stamps_issued(), 100);
    }
}

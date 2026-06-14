//! Stamp issuer trait for tracking bucket utilization.

use nectar_postage::{Batch, BatchId, StampDigest, StampError, StampIndex, calculate_bucket};
use nectar_primitives::SwarmAddress;

/// A trait for managing stamp issuance within a batch.
///
/// The stamp issuer tracks which bucket indices have been used and allocates
/// new indices for chunks as they are stamped. This is the stateful component
/// that ensures each stamp uses a unique index within its bucket.
///
/// # Separation of Concerns
///
/// The `StampIssuer` is responsible only for tracking and allocating indices.
/// Signing is handled separately by the `Stamper` trait, allowing the same
/// issuer state to be used with different signing mechanisms.
///
/// # Example
///
/// ```ignore
/// use nectar_postage_issuer::{StampIssuer, StampDigest, StampError};
/// use nectar_primitives::SwarmAddress;
///
/// struct MyIssuer { /* ... */ }
///
/// impl StampIssuer for MyIssuer {
///     fn prepare_stamp(&mut self, address: &SwarmAddress, timestamp: u64) -> Result<StampDigest, StampError> {
///         // Allocate index and return digest
///         todo!()
///     }
///
///     fn batch_id(&self) -> BatchId {
///         todo!()
///     }
///
///     // ... other methods
/// }
/// ```
pub trait StampIssuer {
    /// Prepares a stamp digest for the given chunk address.
    ///
    /// This method:
    /// 1. Calculates which bucket the chunk belongs to
    /// 2. Allocates the next available index within that bucket
    /// 3. Returns the digest that needs to be signed
    ///
    /// The caller is then responsible for signing the digest and creating
    /// the final stamp.
    ///
    /// # Arguments
    ///
    /// * `address` - The address of the chunk to stamp
    /// * `timestamp` - The timestamp to include in the stamp
    ///
    /// # Errors
    ///
    /// Returns `StampError::BucketFull` if the bucket has no remaining capacity.
    fn prepare_stamp(
        &mut self,
        address: &SwarmAddress,
        timestamp: u64,
    ) -> Result<StampDigest, StampError>;

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

    /// Returns the total number of stamps issued.
    fn stamps_issued(&self) -> u64;

    /// Returns the total capacity of the batch (2^depth).
    fn total_capacity(&self) -> u64 {
        1u64 << self.batch_depth()
    }

    /// Returns the bucket capacity (2^(depth - bucket_depth)).
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
        let max_util = self.max_bucket_utilization() as f64;
        let capacity = self.bucket_capacity() as f64;
        max_util / capacity >= threshold
    }
}

/// Selects how a [`MemoryIssuer`] allocates indices within a bucket.
///
/// The mode mirrors the on-chain mutability of the batch the issuer serves.
/// An immutable batch may never reuse a slot, so issuance fills each bucket
/// once and then refuses further stamps. A mutable batch may overwrite an
/// existing slot with a fresher chunk, so issuance walks the bucket as a ring
/// and wraps back to the first slot once every slot has been written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IssuanceMode {
    /// Fill each bucket once, refusing issuance with
    /// [`StampError::BucketFull`] when the bucket reaches capacity.
    ///
    /// This is the semantics of an immutable batch.
    Fill,
    /// Walk each bucket as a ring cursor, wrapping back to slot zero once
    /// every slot has been written so a later chunk overwrites an earlier one.
    ///
    /// This is the semantics of a mutable batch. Issuance never reports
    /// [`StampError::BucketFull`], but utilisation is still reported honestly:
    /// it counts the distinct slots that have been written and saturates at
    /// the bucket capacity rather than growing without bound.
    Ring,
}

/// An in-memory stamp issuer that tracks bucket utilization.
///
/// This implementation stores bucket indices in a vector and is suitable
/// for most use cases where the issuer state doesn't need to persist
/// across restarts.
///
/// The issuer operates in one of two [`IssuanceMode`]s. In [`IssuanceMode::Fill`]
/// every slot is written at most once and the bucket is refused once full. In
/// [`IssuanceMode::Ring`] the cursor wraps so a later chunk overwrites the
/// slot held by an earlier one, matching the overwrite semantics of a mutable
/// batch. Utilisation is reported honestly in both modes: it never exceeds the
/// bucket capacity, and a ring that has wrapped reports its bucket as full even
/// though issuance into it continues to succeed.
#[derive(Debug, Clone)]
pub struct MemoryIssuer {
    /// The batch ID.
    batch_id: BatchId,
    /// The batch depth.
    depth: u8,
    /// The bucket depth.
    bucket_depth: u8,
    /// How indices are allocated within a bucket.
    mode: IssuanceMode,
    /// Next slot to write for each bucket.
    ///
    /// In [`IssuanceMode::Ring`] this is the ring cursor and wraps modulo the
    /// bucket capacity. In [`IssuanceMode::Fill`] it is monotonic and never
    /// reaches the capacity.
    bucket_indices: alloc::vec::Vec<u32>,
    /// Whether each bucket has been written to capacity at least once.
    ///
    /// Only set in [`IssuanceMode::Ring`], where the cursor wraps and so can no
    /// longer be used to tell a saturated bucket apart from an empty one. Used
    /// to report utilisation honestly once a ring has wrapped.
    bucket_saturated: alloc::vec::Vec<bool>,
    /// Maximum utilization across all buckets.
    max_utilization: u32,
    /// Total stamps issued.
    stamps_issued: u64,
}

extern crate alloc;

impl MemoryIssuer {
    /// Creates a new memory issuer for the given batch in [`IssuanceMode::Fill`].
    pub fn new(batch_id: BatchId, depth: u8, bucket_depth: u8) -> Self {
        Self::with_mode(batch_id, depth, bucket_depth, IssuanceMode::Fill)
    }

    /// Creates a new memory issuer for the given batch in the requested mode.
    pub fn with_mode(batch_id: BatchId, depth: u8, bucket_depth: u8, mode: IssuanceMode) -> Self {
        let bucket_count = 1usize << bucket_depth;
        Self {
            batch_id,
            depth,
            bucket_depth,
            mode,
            bucket_indices: alloc::vec![0u32; bucket_count],
            bucket_saturated: alloc::vec![false; bucket_count],
            max_utilization: 0,
            stamps_issued: 0,
        }
    }

    /// Creates a memory issuer from a batch.
    ///
    /// The issuance mode is selected from the batch mutability: an immutable
    /// batch issues in [`IssuanceMode::Fill`] and a mutable batch issues in
    /// [`IssuanceMode::Ring`].
    pub fn from_batch(batch: &Batch) -> Self {
        let mode = if batch.immutable() {
            IssuanceMode::Fill
        } else {
            IssuanceMode::Ring
        };
        Self::with_mode(batch.id(), batch.depth(), batch.bucket_depth(), mode)
    }

    /// Returns the issuance mode.
    pub const fn mode(&self) -> IssuanceMode {
        self.mode
    }

    /// Returns the number of distinct slots written in a bucket.
    ///
    /// This saturates at the bucket capacity, so a wrapped ring reports the
    /// bucket as full rather than counting overwrites as fresh utilisation.
    fn bucket_fill(&self, bucket_idx: usize) -> u32 {
        if self.bucket_saturated[bucket_idx] {
            1u32 << (self.depth - self.bucket_depth)
        } else {
            self.bucket_indices[bucket_idx]
        }
    }
}

impl StampIssuer for MemoryIssuer {
    fn prepare_stamp(
        &mut self,
        address: &SwarmAddress,
        timestamp: u64,
    ) -> Result<StampDigest, StampError> {
        let bucket = calculate_bucket(address, self.bucket_depth);
        let bucket_idx = bucket as usize;

        let bucket_capacity = 1u32 << (self.depth - self.bucket_depth);

        // The slot this stamp is written into. In Fill mode this is the next
        // unused slot; in Ring mode it is the ring cursor, which may point at a
        // slot a previous chunk already wrote.
        let position = self.bucket_indices[bucket_idx];

        match self.mode {
            IssuanceMode::Fill => {
                if position >= bucket_capacity {
                    return Err(StampError::BucketFull {
                        bucket,
                        capacity: bucket_capacity,
                    });
                }
                self.bucket_indices[bucket_idx] = position + 1;
            }
            IssuanceMode::Ring => {
                // Advance the cursor and wrap at the bucket capacity. Once the
                // cursor wraps the bucket is saturated: every slot has been
                // written and further issuance overwrites the oldest chunk.
                let next = position + 1;
                if next >= bucket_capacity {
                    self.bucket_saturated[bucket_idx] = true;
                    self.bucket_indices[bucket_idx] = 0;
                } else {
                    self.bucket_indices[bucket_idx] = next;
                }
            }
        }

        self.stamps_issued += 1;

        // Update max utilization from the honest fill of this bucket, which
        // never exceeds the bucket capacity even when a ring has wrapped.
        let fill = self.bucket_fill(bucket_idx);
        if fill > self.max_utilization {
            self.max_utilization = fill;
        }

        let index = StampIndex::new(bucket, position);

        Ok(StampDigest::new(*address, self.batch_id, index, timestamp))
    }

    fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    fn batch_depth(&self) -> u8 {
        self.depth
    }

    fn bucket_depth(&self) -> u8 {
        self.bucket_depth
    }

    fn max_bucket_utilization(&self) -> u32 {
        self.max_utilization
    }

    fn bucket_utilization(&self, bucket: u32) -> u32 {
        let bucket_idx = bucket as usize;
        if bucket_idx >= self.bucket_indices.len() {
            return 0;
        }
        self.bucket_fill(bucket_idx)
    }

    fn bucket_has_capacity(&self, bucket: u32) -> bool {
        let bucket_idx = bucket as usize;
        if bucket_idx >= self.bucket_indices.len() {
            return false;
        }
        // Report honestly whether a fresh, never-written slot remains. A ring
        // that has wrapped reports no spare capacity even though further
        // issuance still succeeds by overwriting an earlier chunk.
        let bucket_capacity = 1u32 << (self.depth - self.bucket_depth);
        self.bucket_fill(bucket_idx) < bucket_capacity
    }

    fn stamps_issued(&self) -> u64 {
        self.stamps_issued
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;

    fn test_address(leading: u16) -> SwarmAddress {
        let mut bytes = [0u8; 32];
        bytes[0] = (leading >> 8) as u8;
        bytes[1] = leading as u8;
        SwarmAddress::new(bytes)
    }

    #[test]
    fn test_memory_issuer_basic() {
        let batch_id = B256::ZERO;
        let issuer = MemoryIssuer::new(batch_id, 20, 16);

        assert_eq!(issuer.batch_id(), batch_id);
        assert_eq!(issuer.batch_depth(), 20);
        assert_eq!(issuer.bucket_depth(), 16);
        assert_eq!(issuer.max_bucket_utilization(), 0);
        assert_eq!(issuer.stamps_issued(), 0);
        assert_eq!(issuer.bucket_count(), 65536);
        assert_eq!(issuer.bucket_capacity(), 16);
    }

    #[test]
    fn test_memory_issuer_prepare_stamp() {
        let mut issuer = MemoryIssuer::new(B256::ZERO, 20, 16);

        let address = test_address(0xCBE5);
        let digest = issuer.prepare_stamp(&address, 12345).unwrap();

        assert_eq!(digest.batch_id, B256::ZERO);
        assert_eq!(digest.index.bucket(), 0xCBE5);
        assert_eq!(digest.index.index(), 0);
        assert_eq!(digest.timestamp, 12345);
        assert_eq!(issuer.stamps_issued(), 1);
        assert_eq!(issuer.max_bucket_utilization(), 1);
    }

    #[test]
    fn test_memory_issuer_increments_index() {
        let mut issuer = MemoryIssuer::new(B256::ZERO, 20, 16);

        let address = test_address(0xCBE5);

        let d1 = issuer.prepare_stamp(&address, 1).unwrap();
        let d2 = issuer.prepare_stamp(&address, 2).unwrap();
        let d3 = issuer.prepare_stamp(&address, 3).unwrap();

        assert_eq!(d1.index.index(), 0);
        assert_eq!(d2.index.index(), 1);
        assert_eq!(d3.index.index(), 2);
        assert_eq!(issuer.stamps_issued(), 3);
    }

    #[test]
    fn test_memory_issuer_bucket_full() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket
        let mut issuer = MemoryIssuer::new(B256::ZERO, 17, 16);

        let address = test_address(0xABCD);

        // First two should succeed
        assert!(issuer.prepare_stamp(&address, 1).is_ok());
        assert!(issuer.prepare_stamp(&address, 2).is_ok());

        // Third should fail
        let result = issuer.prepare_stamp(&address, 3);
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
        let mut issuer = MemoryIssuer::new(B256::ZERO, 20, 16);

        let addr1 = test_address(0x1234);
        let addr2 = test_address(0x5678);

        issuer.prepare_stamp(&addr1, 1).unwrap();
        issuer.prepare_stamp(&addr1, 2).unwrap();
        issuer.prepare_stamp(&addr2, 3).unwrap();

        assert_eq!(issuer.bucket_utilization(0x1234), 2);
        assert_eq!(issuer.bucket_utilization(0x5678), 1);
        assert_eq!(issuer.bucket_utilization(0x9999), 0);
    }

    #[test]
    fn test_memory_issuer_capacity_check() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket
        let mut issuer = MemoryIssuer::new(B256::ZERO, 17, 16);

        let address = test_address(0x0001);

        assert!(issuer.bucket_has_capacity(0x0001));

        issuer.prepare_stamp(&address, 1).unwrap();
        assert!(issuer.bucket_has_capacity(0x0001));

        issuer.prepare_stamp(&address, 2).unwrap();
        assert!(!issuer.bucket_has_capacity(0x0001));
    }

    #[test]
    fn test_memory_issuer_near_capacity() {
        // depth=18, bucket_depth=16 gives 4 slots per bucket
        let mut issuer = MemoryIssuer::new(B256::ZERO, 18, 16);

        let address = test_address(0x0001);

        assert!(!issuer.is_near_capacity(0.5));

        issuer.prepare_stamp(&address, 1).unwrap();
        issuer.prepare_stamp(&address, 2).unwrap();

        // 2/4 = 0.5
        assert!(issuer.is_near_capacity(0.5));
        assert!(!issuer.is_near_capacity(0.75));

        issuer.prepare_stamp(&address, 3).unwrap();

        // 3/4 = 0.75
        assert!(issuer.is_near_capacity(0.75));
    }

    #[test]
    fn test_memory_issuer_ring_wraps_instead_of_full() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket.
        let mut issuer = MemoryIssuer::with_mode(B256::ZERO, 17, 16, IssuanceMode::Ring);

        let address = test_address(0xABCD);

        // Fill both slots in order.
        let d0 = issuer.prepare_stamp(&address, 1).unwrap();
        let d1 = issuer.prepare_stamp(&address, 2).unwrap();
        assert_eq!(d0.index.index(), 0);
        assert_eq!(d1.index.index(), 1);

        // A third issuance wraps back to slot zero rather than failing.
        let d2 = issuer.prepare_stamp(&address, 3).unwrap();
        assert_eq!(d2.index.index(), 0);

        // And it keeps walking the ring.
        let d3 = issuer.prepare_stamp(&address, 4).unwrap();
        assert_eq!(d3.index.index(), 1);

        assert_eq!(issuer.stamps_issued(), 4);
    }

    #[test]
    fn test_memory_issuer_ring_index_stays_within_capacity() {
        // depth=18, bucket_depth=16 gives 4 slots per bucket.
        let mut issuer = MemoryIssuer::with_mode(B256::ZERO, 18, 16, IssuanceMode::Ring);

        let address = test_address(0x0042);

        // Issue many more chunks than the bucket can hold; every wire index
        // must remain a valid slot in [0, capacity).
        for ts in 0..100u64 {
            let digest = issuer.prepare_stamp(&address, ts).unwrap();
            assert!(digest.index.index() < 4, "index escaped bucket capacity");
        }
        assert_eq!(issuer.stamps_issued(), 100);
    }

    #[test]
    fn test_memory_issuer_ring_utilization_is_honest() {
        // depth=18, bucket_depth=16 gives 4 slots per bucket.
        let mut issuer = MemoryIssuer::with_mode(B256::ZERO, 18, 16, IssuanceMode::Ring);

        let address = test_address(0x0001);

        // Utilisation climbs slot by slot as fresh slots are written.
        for (n, ts) in (1..=4u64).enumerate() {
            issuer.prepare_stamp(&address, ts).unwrap();
            assert_eq!(issuer.bucket_utilization(0x0001), (n as u32) + 1);
        }

        // Capacity is reached; a wrapped ring saturates at capacity and does
        // not count overwrites as fresh utilisation.
        assert_eq!(issuer.bucket_utilization(0x0001), 4);
        assert_eq!(issuer.max_bucket_utilization(), 4);

        for ts in 5..20u64 {
            issuer.prepare_stamp(&address, ts).unwrap();
            assert_eq!(issuer.bucket_utilization(0x0001), 4);
            assert_eq!(issuer.max_bucket_utilization(), 4);
        }
    }

    #[test]
    fn test_memory_issuer_ring_capacity_reported_honestly() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket.
        let mut issuer = MemoryIssuer::with_mode(B256::ZERO, 17, 16, IssuanceMode::Ring);

        let address = test_address(0x0001);

        // Spare capacity while fresh slots remain.
        assert!(issuer.bucket_has_capacity(0x0001));
        issuer.prepare_stamp(&address, 1).unwrap();
        assert!(issuer.bucket_has_capacity(0x0001));

        // Once every slot has been written the ring reports no spare capacity,
        // even though issuance into it still succeeds by overwriting.
        issuer.prepare_stamp(&address, 2).unwrap();
        assert!(!issuer.bucket_has_capacity(0x0001));

        // Issuance still succeeds despite the bucket reporting no capacity.
        assert!(issuer.prepare_stamp(&address, 3).is_ok());
        assert!(!issuer.bucket_has_capacity(0x0001));
    }

    #[test]
    fn test_memory_issuer_from_batch_selects_mode() {
        use nectar_postage::Batch;

        let mutable = Batch::new(B256::ZERO, 0, 0, Default::default(), 20, 16, false);
        let immutable = Batch::new(B256::ZERO, 0, 0, Default::default(), 20, 16, true);

        assert_eq!(
            MemoryIssuer::from_batch(&mutable).mode(),
            IssuanceMode::Ring
        );
        assert_eq!(
            MemoryIssuer::from_batch(&immutable).mode(),
            IssuanceMode::Fill
        );
    }

    #[test]
    fn test_memory_issuer_fill_mode_unchanged() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket. Fill mode is the
        // default and must still refuse a full bucket.
        let mut issuer = MemoryIssuer::new(B256::ZERO, 17, 16);
        assert_eq!(issuer.mode(), IssuanceMode::Fill);

        let address = test_address(0xABCD);
        assert!(issuer.prepare_stamp(&address, 1).is_ok());
        assert!(issuer.prepare_stamp(&address, 2).is_ok());

        let result = issuer.prepare_stamp(&address, 3);
        assert!(matches!(
            result,
            Err(StampError::BucketFull {
                bucket: 0xABCD,
                capacity: 2
            })
        ));
    }
}

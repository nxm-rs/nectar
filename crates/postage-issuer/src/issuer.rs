//! Stamp issuer trait for tracking bucket utilization.

use crate::counter::{CounterMode, CounterTable};
use crate::error::IssuerError;
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
        let max_util = self.max_bucket_utilization() as f64;
        let capacity = self.bucket_capacity() as f64;
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
#[derive(Debug, Clone)]
pub struct MemoryIssuer {
    /// The batch ID.
    batch_id: BatchId,
    /// The shared per-bucket fill watermarks. `counts[b]` is the next unused
    /// slot, monotone and never above the capacity.
    counters: CounterTable,
}

impl MemoryIssuer {
    /// Creates a new fill-only memory issuer for the given batch geometry.
    pub fn new(batch_id: BatchId, depth: u8, bucket_depth: u8) -> Self {
        Self {
            batch_id,
            counters: CounterTable::new(depth, bucket_depth, CounterMode::Fill),
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
    /// depth.
    pub const fn dilute(&mut self, new_depth: u8) -> Result<(), IssuerError> {
        let current = self.counters.depth();
        if new_depth < current {
            return Err(IssuerError::DepthDecrease {
                current,
                requested: new_depth,
            });
        }
        self.counters.set_depth(new_depth);
        Ok(())
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
    pub fn from_batch(batch: &Batch) -> Result<Self, IssuerError> {
        if batch.immutable() {
            Ok(Self::new(batch.id(), batch.depth(), batch.bucket_depth()))
        } else {
            Err(IssuerError::MutableNotSupported)
        }
    }
}

impl StampIssuer for MemoryIssuer {
    fn prepare_stamp(
        &mut self,
        address: &SwarmAddress,
        timestamp: u64,
    ) -> Result<StampDigest, StampError> {
        let bucket = calculate_bucket(address, self.counters.bucket_depth());
        // Fill mode ignores the predicate; a monotone watermark never lands on a
        // reserved slot.
        let position =
            self.counters
                .record(bucket, |_| false)
                .map_err(|err| StampError::BucketFull {
                    bucket,
                    capacity: match err {
                        crate::counter::CounterError::BucketFull { capacity, .. } => capacity,
                        _ => self.counters.bucket_capacity(),
                    },
                })?;

        let index = StampIndex::new(bucket, position);

        Ok(StampDigest::new(*address, self.batch_id, index, timestamp))
    }

    fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    fn batch_depth(&self) -> u8 {
        self.counters.depth()
    }

    fn bucket_depth(&self) -> u8 {
        self.counters.bucket_depth()
    }

    fn max_bucket_utilization(&self) -> u32 {
        // Fill watermarks are monotone, so the current maximum is the historical
        // maximum.
        self.counters.max_count()
    }

    fn bucket_utilization(&self, bucket: u32) -> u32 {
        self.counters.count(bucket).unwrap_or(0)
    }

    fn bucket_has_capacity(&self, bucket: u32) -> bool {
        self.counters.has_capacity(bucket).unwrap_or(false)
    }

    fn stamps_issued(&self) -> Option<u64> {
        // Fill issuance is monotone, so the counter sum is the lifetime count.
        Some(self.counters.total_issued())
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
        assert_eq!(issuer.stamps_issued(), Some(0));
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
        assert_eq!(issuer.stamps_issued(), Some(1));
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
        assert_eq!(issuer.stamps_issued(), Some(3));
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
    fn test_memory_issuer_from_batch_mutable_refused() {
        use nectar_postage::Batch;

        // A mutable batch must never yield an issuer: the obvious constructor
        // refuses it instead of handing back a reserved-blind ring that would
        // silently overwrite a self-hosted snapshot's own chunks.
        let mutable = Batch::new(B256::ZERO, 0, 0, Default::default(), 20, 16, false);

        assert!(matches!(
            MemoryIssuer::from_batch(&mutable),
            Err(IssuerError::MutableNotSupported)
        ));
    }

    #[test]
    fn test_memory_issuer_from_batch_immutable_parity_with_new() {
        use nectar_postage::Batch;

        // An immutable batch yields a fill-only issuer byte-for-byte identical
        // to `new` for the same geometry: same indices and the same digest.
        let batch_id = B256::from([0x11u8; 32]);
        let immutable = Batch::new(batch_id, 0, 0, Default::default(), 17, 16, true);

        let mut from_batch = MemoryIssuer::from_batch(&immutable).unwrap();
        let mut from_new = MemoryIssuer::new(batch_id, 17, 16);

        for ts in 0..2u64 {
            for leading in [0xCBE5u16, 0x0001, 0xABCD] {
                let address = test_address(leading);
                let a = from_batch.prepare_stamp(&address, ts).unwrap();
                let b = from_new.prepare_stamp(&address, ts).unwrap();
                assert_eq!(a.index.bucket(), b.index.bucket());
                assert_eq!(a.index.index(), b.index.index());
                assert_eq!(a.to_prehash(), b.to_prehash());
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
        let mut issuer = MemoryIssuer::new(B256::ZERO, 17, 16);
        let address = test_address(0xABCD);

        // Fill the bucket, then a dilution to depth 18 (4 slots) reopens it
        // without moving the existing watermark.
        issuer.prepare_stamp(&address, 1).unwrap();
        issuer.prepare_stamp(&address, 2).unwrap();
        assert!(issuer.prepare_stamp(&address, 3).is_err());

        issuer.dilute(18).unwrap();
        assert_eq!(issuer.bucket_capacity(), 4);
        // The watermark is unchanged, so the next slot is 2, not 0.
        let d = issuer.prepare_stamp(&address, 4).unwrap();
        assert_eq!(d.index.index(), 2);
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
}

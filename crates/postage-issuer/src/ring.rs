//! Type-state mutable (ring) stamp issuance.
//!
//! A mutable batch lets a fresh chunk overwrite the slot held by an older one,
//! so issuance walks each bucket as a ring cursor and wraps back to the first
//! slot once every slot has been written. This is the overwrite behaviour an
//! immutable batch must never have, which is why the fill-only [`MemoryIssuer`]
//! and [`ShardedIssuer`] refuse a mutable batch outright.
//!
//! A reserved-blind ring is dangerous in a self-hosting context: when the owner
//! keeps its own chunks in the same batch, an unconstrained ring would wrap
//! around and silently evict them. To make that impossible at compile time the
//! ring carries its reservation policy in a type parameter:
//!
//! - [`RingIssuer<Unreserved>`] protects nothing. It suits external tracking,
//!   where the caller keeps usage state elsewhere and nothing in the batch is
//!   protected.
//! - [`RingIssuer<Reserved>`] protects a supplied set of `(bucket, index)`
//!   slots and never re-emits one, even after the ring wraps. The protected
//!   slots come from `nectar-postage-usage` when the batch self-hosts.
//!
//! There is no public conversion from [`Unreserved`] to [`Reserved`], so a
//! function that demands a [`RingIssuer<Reserved>`] cannot be handed a
//! reserved-blind ring by mistake.
//!
//! [`MemoryIssuer`]: crate::MemoryIssuer
//! [`ShardedIssuer`]: crate::ShardedIssuer

extern crate alloc;

use alloc::collections::BTreeSet;
use alloc::vec::Vec;

use nectar_postage::{Batch, BatchId, StampDigest, StampError, StampIndex, calculate_bucket};
use nectar_primitives::SwarmAddress;

use crate::StampIssuer;
use crate::error::IssuerError;

mod sealed {
    /// Seals [`Reservation`](super::Reservation) so external crates cannot add
    /// reservation policies and break the self-hosting invariant.
    pub trait Sealed {}
}

/// A reservation policy: answers whether a given `(bucket, index)` slot is
/// protected and must never be emitted by a ring.
///
/// The trait is sealed. Only [`Unreserved`] and [`Reserved`] implement it, so
/// the set of policies a ring can carry is fixed by this crate and an external
/// crate cannot weaken the self-hosting guarantee.
pub trait Reservation: sealed::Sealed {
    /// Returns `true` if the slot at `index` in `bucket` is protected and must
    /// not be issued.
    fn is_protected(&self, bucket: u32, index: u32) -> bool;
}

/// A reservation policy that protects nothing.
///
/// A [`RingIssuer<Unreserved>`] wraps freely and may re-emit any slot. It suits
/// external tracking, where the caller holds usage state outside the batch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Unreserved;

impl sealed::Sealed for Unreserved {}

impl Reservation for Unreserved {
    #[inline]
    fn is_protected(&self, _bucket: u32, _index: u32) -> bool {
        false
    }
}

/// A reservation policy that protects a fixed set of `(bucket, index)` slots.
///
/// A [`RingIssuer<Reserved>`] never emits a protected slot, even after the ring
/// wraps. The protected slots are the chunks a self-hosting owner keeps in the
/// batch, supplied by `nectar-postage-usage`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Reserved {
    /// The protected slots the ring must never re-emit, keyed by bucket.
    slots: BTreeSet<(u32, u32)>,
}

impl Reserved {
    /// Builds a reservation from an iterator of protected `(bucket, index)`
    /// slots.
    pub fn new(slots: impl IntoIterator<Item = (u32, u32)>) -> Self {
        Self {
            slots: slots.into_iter().collect(),
        }
    }

    /// Returns the number of protected slots.
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Returns `true` if no slot is protected.
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}

impl FromIterator<(u32, u32)> for Reserved {
    fn from_iter<T: IntoIterator<Item = (u32, u32)>>(iter: T) -> Self {
        Self::new(iter)
    }
}

impl sealed::Sealed for Reserved {}

impl Reservation for Reserved {
    #[inline]
    fn is_protected(&self, bucket: u32, index: u32) -> bool {
        self.slots.contains(&(bucket, index))
    }
}

/// A mutable (ring) stamp issuer.
///
/// Issuance advances a per-bucket cursor and wraps at the bucket capacity
/// `2^(depth - bucket_depth)`, so a later chunk overwrites the slot held by an
/// earlier one. The reservation policy `R` decides which slots, if any, the
/// ring must skip:
///
/// - [`RingIssuer::external`] builds a [`RingIssuer<Unreserved>`] that protects
///   nothing.
/// - [`RingIssuer::reserved`] builds a [`RingIssuer<Reserved>`] that never
///   emits a protected slot.
///
/// Both constructors require a mutable batch. An immutable batch is refused
/// with [`IssuerError::ImmutableNotSupported`]: immutable batches are fill-only
/// and use [`MemoryIssuer`](crate::MemoryIssuer).
#[derive(Debug, Clone)]
pub struct RingIssuer<R = Unreserved> {
    /// The batch ID.
    batch_id: BatchId,
    /// The batch depth.
    depth: u8,
    /// The bucket depth.
    bucket_depth: u8,
    /// The bucket capacity, `2^(depth - bucket_depth)`.
    bucket_capacity: u32,
    /// Ring cursor for each bucket. Wraps modulo the bucket capacity.
    cursors: Vec<u32>,
    /// Whether each bucket has been written to capacity at least once.
    ///
    /// Once the cursor wraps it can no longer tell a saturated bucket apart
    /// from an empty one, so this tracks saturation to report utilization
    /// honestly.
    saturated: Vec<bool>,
    /// Maximum utilization observed across all buckets.
    max_utilization: u32,
    /// Total stamps issued.
    stamps_issued: u64,
    /// The reservation policy.
    reservation: R,
}

impl RingIssuer<Unreserved> {
    /// Builds an externally tracked ring for a mutable batch.
    ///
    /// The ring protects nothing: it wraps freely and may re-emit any slot. The
    /// caller is responsible for tracking usage outside the batch, which is the
    /// external-tracking model.
    ///
    /// # Errors
    ///
    /// Returns [`IssuerError::ImmutableNotSupported`] if the batch is immutable;
    /// immutable batches are fill-only and use
    /// [`MemoryIssuer`](crate::MemoryIssuer).
    pub fn external(batch: &Batch) -> Result<Self, IssuerError> {
        Self::for_mutable_batch(batch, Unreserved)
    }
}

impl RingIssuer<Reserved> {
    /// Builds a self-hosting ring for a mutable batch with a set of protected
    /// slots.
    ///
    /// The ring never emits a protected slot, even after it wraps. The
    /// protected slots are the chunks the owner keeps in the batch, supplied by
    /// `nectar-postage-usage`.
    ///
    /// # Errors
    ///
    /// Returns [`IssuerError::ImmutableNotSupported`] if the batch is immutable;
    /// immutable batches are fill-only and use
    /// [`MemoryIssuer`](crate::MemoryIssuer).
    pub fn reserved(
        batch: &Batch,
        slots: impl IntoIterator<Item = (u32, u32)>,
    ) -> Result<Self, IssuerError> {
        Self::for_mutable_batch(batch, Reserved::new(slots))
    }
}

impl<R: Reservation> RingIssuer<R> {
    /// Builds a ring for a mutable batch with the given reservation policy.
    fn for_mutable_batch(batch: &Batch, reservation: R) -> Result<Self, IssuerError> {
        if batch.immutable() {
            return Err(IssuerError::ImmutableNotSupported);
        }
        Ok(Self::with_reservation(
            batch.id(),
            batch.depth(),
            batch.bucket_depth(),
            reservation,
        ))
    }

    /// Builds a ring directly from geometry and a reservation policy.
    fn with_reservation(batch_id: BatchId, depth: u8, bucket_depth: u8, reservation: R) -> Self {
        let bucket_count = 1usize << bucket_depth;
        Self {
            batch_id,
            depth,
            bucket_depth,
            bucket_capacity: 1u32 << (depth - bucket_depth),
            cursors: alloc::vec![0u32; bucket_count],
            saturated: alloc::vec![false; bucket_count],
            max_utilization: 0,
            stamps_issued: 0,
            reservation,
        }
    }

    /// Returns the number of distinct slots written in a bucket.
    ///
    /// This saturates at the bucket capacity, so a wrapped ring reports the
    /// bucket as full rather than counting overwrites as fresh utilization.
    fn bucket_fill(&self, bucket_idx: usize) -> u32 {
        if self.saturated[bucket_idx] {
            self.bucket_capacity
        } else {
            self.cursors[bucket_idx]
        }
    }

    /// Advances the cursor for `bucket` to the next unprotected slot, returning
    /// the slot to emit.
    ///
    /// The cursor starts at its current position and walks forward, wrapping at
    /// the bucket capacity and marking the bucket saturated on each wrap. Up to
    /// `bucket_capacity` slots are inspected; if every slot is protected the
    /// bucket has no issuable slot and [`IssuerError::RingExhausted`] is
    /// returned rather than emitting a protected slot.
    fn next_slot(&mut self, bucket: u32) -> Result<u32, IssuerError> {
        let bucket_idx = bucket as usize;
        for _ in 0..self.bucket_capacity {
            let position = self.cursors[bucket_idx];

            // Advance the cursor, wrapping at the bucket capacity and marking
            // saturation on each wrap.
            let next = position + 1;
            if next >= self.bucket_capacity {
                self.saturated[bucket_idx] = true;
                self.cursors[bucket_idx] = 0;
            } else {
                self.cursors[bucket_idx] = next;
            }

            if !self.reservation.is_protected(bucket, position) {
                return Ok(position);
            }
        }
        Err(IssuerError::RingExhausted { bucket })
    }

    /// Prepares a stamp digest for the given chunk address.
    ///
    /// Advances the bucket ring to the next unprotected slot and returns the
    /// digest to sign.
    ///
    /// # Errors
    ///
    /// Returns [`StampError`] only via [`IssuerError::RingExhausted`] when every
    /// slot in the target bucket is protected, which is geometrically
    /// impossible at real batch depths.
    fn prepare_ring_stamp(
        &mut self,
        address: &SwarmAddress,
        timestamp: u64,
    ) -> Result<StampDigest, IssuerError> {
        let bucket = calculate_bucket(address, self.bucket_depth);
        let position = self.next_slot(bucket)?;

        self.stamps_issued += 1;

        let fill = self.bucket_fill(bucket as usize);
        if fill > self.max_utilization {
            self.max_utilization = fill;
        }

        let index = StampIndex::new(bucket, position);
        Ok(StampDigest::new(*address, self.batch_id, index, timestamp))
    }

    /// Returns the bucket capacity, `2^(depth - bucket_depth)`.
    pub const fn bucket_capacity(&self) -> u32 {
        self.bucket_capacity
    }

    /// Returns a reference to the reservation policy.
    pub const fn reservation(&self) -> &R {
        &self.reservation
    }
}

impl<R: Reservation> StampIssuer for RingIssuer<R> {
    fn prepare_stamp(
        &mut self,
        address: &SwarmAddress,
        timestamp: u64,
    ) -> Result<StampDigest, StampError> {
        // A ring never reports BucketFull; the only failure is a fully reserved
        // bucket, which is geometrically impossible at real depths. Surface it
        // through StampError::BucketFull so it flows through the StampIssuer and
        // Stamper contract without a new wire error.
        self.prepare_ring_stamp(address, timestamp)
            .map_err(|err| match err {
                IssuerError::RingExhausted { bucket } => StampError::BucketFull {
                    bucket,
                    capacity: self.bucket_capacity,
                },
                // `prepare_ring_stamp` only ever yields RingExhausted.
                _ => unreachable!("ring issuance only fails with RingExhausted"),
            })
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
        if bucket_idx >= self.cursors.len() {
            return 0;
        }
        self.bucket_fill(bucket_idx)
    }

    fn bucket_has_capacity(&self, bucket: u32) -> bool {
        let bucket_idx = bucket as usize;
        if bucket_idx >= self.cursors.len() {
            return false;
        }
        // Report honestly whether a fresh, never-written slot remains. A ring
        // that has wrapped reports no spare capacity even though issuance into
        // it still succeeds by overwriting an earlier chunk.
        self.bucket_fill(bucket_idx) < self.bucket_capacity
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

    fn mutable_batch(depth: u8, bucket_depth: u8) -> Batch {
        Batch::new(
            B256::ZERO,
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
            B256::ZERO,
            0,
            0,
            Default::default(),
            depth,
            bucket_depth,
            true,
        )
    }

    #[test]
    fn external_ring_wraps_and_reuses_slots() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket.
        let batch = mutable_batch(17, 16);
        let mut issuer = RingIssuer::external(&batch).unwrap();

        let address = test_address(0xABCD);

        let d0 = issuer.prepare_ring_stamp(&address, 1).unwrap();
        let d1 = issuer.prepare_ring_stamp(&address, 2).unwrap();
        assert_eq!(d0.index.index(), 0);
        assert_eq!(d1.index.index(), 1);

        // A third issuance wraps back to slot zero rather than failing.
        let d2 = issuer.prepare_ring_stamp(&address, 3).unwrap();
        assert_eq!(d2.index.index(), 0);

        let d3 = issuer.prepare_ring_stamp(&address, 4).unwrap();
        assert_eq!(d3.index.index(), 1);

        assert_eq!(issuer.stamps_issued(), 4);
    }

    #[test]
    fn external_ring_index_stays_within_capacity() {
        // depth=18, bucket_depth=16 gives 4 slots per bucket.
        let batch = mutable_batch(18, 16);
        let mut issuer = RingIssuer::external(&batch).unwrap();

        let address = test_address(0x0042);

        for ts in 0..100u64 {
            let digest = issuer.prepare_ring_stamp(&address, ts).unwrap();
            assert!(digest.index.index() < 4, "index escaped bucket capacity");
        }
        assert_eq!(issuer.stamps_issued(), 100);
    }

    #[test]
    fn reserved_ring_never_emits_a_protected_slot() {
        // depth=18, bucket_depth=16 gives 4 slots per bucket. Protect slots 1
        // and 3 in the target bucket; the ring may only ever emit 0 and 2.
        let batch = mutable_batch(18, 16);
        let bucket = calculate_bucket(&test_address(0x00AA), 16);
        let mut issuer = RingIssuer::reserved(&batch, [(bucket, 1), (bucket, 3)]).unwrap();

        let address = test_address(0x00AA);

        // Issue far past one wrap so every wrap is exercised.
        for ts in 0..50u64 {
            let digest = issuer.prepare_ring_stamp(&address, ts).unwrap();
            let index = digest.index.index();
            assert!(
                index == 0 || index == 2,
                "ring emitted protected or out-of-range slot {index}"
            );
            assert!(!issuer.reservation().is_protected(bucket, index));
        }
    }

    #[test]
    fn reserved_ring_exhausts_when_every_slot_is_protected() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket. Protect both, so
        // the bucket has no issuable slot.
        let batch = mutable_batch(17, 16);
        let bucket = calculate_bucket(&test_address(0x0001), 16);
        let mut issuer = RingIssuer::reserved(&batch, [(bucket, 0), (bucket, 1)]).unwrap();

        let address = test_address(0x0001);
        assert!(matches!(
            issuer.prepare_ring_stamp(&address, 1),
            Err(IssuerError::RingExhausted { bucket: b }) if b == bucket
        ));
    }

    #[test]
    fn external_refuses_immutable_batch() {
        let batch = immutable_batch(20, 16);
        assert!(matches!(
            RingIssuer::external(&batch),
            Err(IssuerError::ImmutableNotSupported)
        ));
    }

    #[test]
    fn reserved_refuses_immutable_batch() {
        let batch = immutable_batch(20, 16);
        assert!(matches!(
            RingIssuer::reserved(&batch, [(0u32, 0u32)]),
            Err(IssuerError::ImmutableNotSupported)
        ));
    }

    #[test]
    fn ring_reports_utilization_and_capacity_honestly() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket.
        let batch = mutable_batch(17, 16);
        let mut issuer = RingIssuer::external(&batch).unwrap();

        let address = test_address(0x0001);
        let bucket = calculate_bucket(&address, 16);

        assert!(issuer.bucket_has_capacity(bucket));
        issuer.prepare_ring_stamp(&address, 1).unwrap();
        assert!(issuer.bucket_has_capacity(bucket));
        assert_eq!(issuer.bucket_utilization(bucket), 1);

        issuer.prepare_ring_stamp(&address, 2).unwrap();
        assert!(!issuer.bucket_has_capacity(bucket));
        assert_eq!(issuer.bucket_utilization(bucket), 2);
        assert_eq!(issuer.max_bucket_utilization(), 2);

        // Issuance still succeeds despite the bucket reporting no capacity, and
        // utilization saturates rather than counting overwrites.
        issuer.prepare_ring_stamp(&address, 3).unwrap();
        assert_eq!(issuer.bucket_utilization(bucket), 2);
        assert_eq!(issuer.max_bucket_utilization(), 2);
    }

    #[test]
    fn ring_drops_into_batch_stamper() {
        use crate::{BatchStamper, Stamper};
        use alloy_signer_local::PrivateKeySigner;

        // depth=17, bucket_depth=16 gives 2 slots per bucket. A ring stamps
        // through BatchStamper exactly like a MemoryIssuer, wrapping past the
        // bucket capacity instead of refusing.
        let batch = mutable_batch(17, 16);
        let issuer = RingIssuer::external(&batch).unwrap();
        let signer = PrivateKeySigner::random();
        let mut stamper = BatchStamper::new(issuer, signer);

        let address = test_address(0xABCD);

        let s0 = stamper.stamp(&address).unwrap();
        let s1 = stamper.stamp(&address).unwrap();
        let s2 = stamper.stamp(&address).unwrap();

        assert_eq!(s0.index(), 0);
        assert_eq!(s1.index(), 1);
        // Wraps rather than failing, which a fill-only issuer would not.
        assert_eq!(s2.index(), 0);
        assert_eq!(s0.bucket(), s2.bucket());
    }

    #[test]
    fn ring_stamp_issuer_surfaces_exhaustion_as_bucket_full() {
        let batch = mutable_batch(17, 16);
        let bucket = calculate_bucket(&test_address(0x0001), 16);
        let mut issuer = RingIssuer::reserved(&batch, [(bucket, 0), (bucket, 1)]).unwrap();

        let address = test_address(0x0001);
        let result = StampIssuer::prepare_stamp(&mut issuer, &address, 1);
        assert!(matches!(
            result,
            Err(StampError::BucketFull { bucket: b, capacity: 2 }) if b == bucket
        ));
    }
}

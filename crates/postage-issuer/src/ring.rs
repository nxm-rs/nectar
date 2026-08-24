//! Type-state mutable (ring) stamp issuance.
//!
//! A mutable batch lets a fresh chunk overwrite the slot held by an older one,
//! so issuance walks each bucket as a ring cursor and wraps back to the first
//! slot once every slot has been written. This is the overwrite behaviour an
//! immutable batch must never have, which is why the fill-only [`MemoryIssuer`]
//! refuses a mutable batch outright.
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

use alloc::collections::BTreeSet;
use alloc::vec::Vec;
use core::cell::RefCell;

use nectar_postage::{
    Batch, BatchDepth, BatchId, Bucket, BucketDepth, StampError, calculate_bucket,
};
use nectar_primitives::{ChunkAddress, Mainnet, SwarmSpec, error::BoxedError};

use crate::StampIssuer;
use crate::counter::{CounterError, CounterMode, CounterTable};
use crate::error::{IssuerError, RingExhausted};
use crate::permit::Prepared;

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
///
/// The network reaches the ring through its [`BucketDepth`].
#[derive(Debug)]
pub struct RingIssuer<R = Unreserved, S: SwarmSpec = Mainnet> {
    /// The batch ID.
    batch_id: BatchId,
    /// A wrap scan reads more than one word, so it cannot compare-and-swap the
    /// way a fill watermark does; the cell serializes it and leaves it `!Sync`.
    state: RefCell<RingState<S>>,
    /// The reservation policy.
    reservation: R,
}

#[derive(Debug)]
struct RingState<S: SwarmSpec> {
    /// The shared per-bucket ring cursors, in the `[0, capacity]` deferred-wrap
    /// representation. `counts[b] == capacity` means "wrap on the next write".
    counters: CounterTable<S>,
    /// Whether each bucket has been written to capacity at least once.
    ///
    /// The wire representation defers each wrap, so once a bucket has wrapped its
    /// cursor falls back into `[0, capacity)` and no longer marks saturation on
    /// its own. This in-memory latch records it so utilization is reported
    /// honestly; it is never serialized.
    saturated: Vec<bool>,
    /// Maximum utilization observed across all buckets, latched so a wrapped ring
    /// reports its peak rather than the live cursor.
    max_utilization: u32,
    /// Lifetime stamps issued, a true monotone count of issuance calls (not the
    /// counter sum, which a ring would undercount on wrap).
    stamps_issued: u64,
}

// The spec is a type-level tag, so this carries no bound on `S` beyond
// `SwarmSpec`; deriving would demand `S: Clone` of a marker type that holds no
// data.
impl<S: SwarmSpec> Clone for RingState<S> {
    fn clone(&self) -> Self {
        Self {
            counters: self.counters.clone(),
            saturated: self.saturated.clone(),
            max_utilization: self.max_utilization,
            stamps_issued: self.stamps_issued,
        }
    }
}

impl<R: Clone, S: SwarmSpec> Clone for RingIssuer<R, S> {
    fn clone(&self) -> Self {
        Self {
            batch_id: self.batch_id,
            state: RefCell::new(self.state.borrow().clone()),
            reservation: self.reservation.clone(),
        }
    }
}

impl<S: SwarmSpec> RingIssuer<Unreserved, S> {
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
    /// [`MemoryIssuer`](crate::MemoryIssuer). Returns
    /// [`IssuerError::Geometry`] if the chain-decoded depth is one no counter
    /// table can hold.
    pub fn external(batch: &Batch<S>) -> Result<Self, IssuerError> {
        Self::for_mutable_batch(batch, Unreserved)
    }
}

impl<S: SwarmSpec> RingIssuer<Reserved, S> {
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
    /// [`MemoryIssuer`](crate::MemoryIssuer). Returns
    /// [`IssuerError::Geometry`] if the chain-decoded depth is one no counter
    /// table can hold.
    pub fn reserved(
        batch: &Batch<S>,
        slots: impl IntoIterator<Item = (u32, u32)>,
    ) -> Result<Self, IssuerError> {
        Self::for_mutable_batch(batch, Reserved::new(slots))
    }
}

impl<R: Reservation, S: SwarmSpec> RingIssuer<R, S> {
    /// Builds a ring for a mutable batch with the given reservation policy.
    fn for_mutable_batch(batch: &Batch<S>, reservation: R) -> Result<Self, IssuerError> {
        if batch.immutable() {
            return Err(IssuerError::ImmutableNotSupported);
        }
        batch.geometry()?;
        Ok(Self::with_reservation(
            batch.id(),
            batch.depth(),
            batch.bucket_depth(),
            reservation,
        ))
    }

    /// Builds a ring directly from geometry and a reservation policy.
    pub(crate) fn with_reservation(
        batch_id: BatchId,
        depth: u8,
        bucket_depth: BucketDepth<S>,
        reservation: R,
    ) -> Self {
        let bucket_count = 1usize << bucket_depth.get();
        Self {
            batch_id,
            state: RefCell::new(RingState {
                counters: CounterTable::new(depth, bucket_depth, CounterMode::Ring),
                saturated: alloc::vec![false; bucket_count],
                max_utilization: 0,
                stamps_issued: 0,
            }),
            reservation,
        }
    }

    /// Claims the next unprotected slot in the bucket `address` falls into,
    /// wrapping past the bucket capacity rather than refusing.
    ///
    /// # Errors
    ///
    /// [`IssuerError::RingExhausted`] when every slot in the target bucket is
    /// protected, which is geometrically impossible at real batch depths.
    pub fn reserve_slot(
        &self,
        address: &ChunkAddress,
        timestamp: u64,
    ) -> Result<Prepared<S>, IssuerError> {
        let state = &mut *self.state.borrow_mut();
        let bucket_depth = state.counters.bucket_depth();
        let bucket = calculate_bucket(address, bucket_depth);
        let position = state.claim(bucket, &self.reservation)?;
        let depth = BatchDepth::new(state.counters.depth(), bucket_depth)?;
        Ok(Prepared::new(
            *address,
            self.batch_id,
            bucket,
            depth,
            position,
            timestamp,
        ))
    }

    /// Returns the bucket capacity, `2^(depth - bucket_depth)`.
    pub fn bucket_capacity(&self) -> u32 {
        self.state.borrow().counters.bucket_capacity()
    }

    /// Returns a reference to the reservation policy.
    pub const fn reservation(&self) -> &R {
        &self.reservation
    }
}

impl<S: SwarmSpec> RingState<S> {
    /// Returns the number of distinct slots written in a bucket.
    ///
    /// This saturates at the bucket capacity, so a wrapped ring reports the
    /// bucket as full rather than counting overwrites as fresh utilization.
    // Every caller guarantees `bucket_idx` is within the bucket count (either by
    // an explicit bounds check or via a successful `record` on the counter
    // table), and `saturated` and `counts()` share that length by construction.
    #[allow(clippy::indexing_slicing)]
    fn bucket_fill(&self, bucket_idx: usize) -> u32 {
        if self.saturated[bucket_idx] {
            self.counters.bucket_capacity()
        } else {
            self.counters.counts()[bucket_idx]
        }
    }

    /// Advances the cursor for `bucket` to the next unprotected slot through the
    /// shared counter table, returning the slot to emit.
    ///
    /// The shared table holds the cursor in the `[0, capacity]` deferred-wrap
    /// representation and skips the reserved slots. Saturation is latched here
    /// from the post-advance cursor: a cursor that reaches the capacity has just
    /// written the bucket's last fresh slot. If every slot is protected the table
    /// returns [`CounterError::RingExhausted`], mapped to
    /// [`IssuerError::RingExhausted`].
    fn claim<R: Reservation>(
        &mut self,
        bucket: Bucket<S>,
        reservation: &R,
    ) -> Result<u32, IssuerError> {
        let value = bucket.value();
        let position = self
            .counters
            .record(bucket, |slot| reservation.is_protected(value, slot))
            .map_err(|err| match err {
                CounterError::RingExhausted(exhausted) => IssuerError::RingExhausted(exhausted),
                // `record` reports nothing else here: ring mode never fills, the
                // bucket comes from the address at the table's own depth so it is
                // in range, and construction errors cannot arise from an advance.
                _ => IssuerError::RingExhausted(RingExhausted::new(value)),
            })?;
        // A cursor sitting at the capacity has just filled the bucket's last
        // fresh slot, so the bucket is saturated from here on.
        if self.counters.count(value).unwrap_or(0) == self.counters.bucket_capacity() {
            // `record` above succeeded, so `bucket` is within the bucket count and
            // `saturated` has that same length by construction; `u32` always fits
            // `usize` on the >=32-bit targets this crate supports.
            #[allow(clippy::indexing_slicing, clippy::as_conversions)]
            {
                self.saturated[value as usize] = true;
            }
        }
        // Monotone u64 issuance counter; one increment per stamp cannot
        // realistically overflow 2^64.
        #[allow(clippy::arithmetic_side_effects)]
        {
            self.stamps_issued += 1;
        }
        // `u32` always fits `usize` on the >=32-bit targets this crate supports.
        #[allow(clippy::as_conversions)]
        let fill = self.bucket_fill(value as usize);
        if fill > self.max_utilization {
            self.max_utilization = fill;
        }
        Ok(position)
    }
}

impl<R: Reservation, S: SwarmSpec> StampIssuer for RingIssuer<R, S> {
    type Spec = S;

    fn reserve(&self, address: &ChunkAddress, timestamp: u64) -> Result<Prepared<S>, StampError> {
        // A ring never reports BucketFull; the only failure is a fully reserved
        // bucket, which is geometrically impossible at real depths. Surface it
        // through StampError::BucketFull so it flows through the StampIssuer and
        // Stamper contract without a new wire error.
        self.reserve_slot(address, timestamp)
            .map_err(|err| match err {
                IssuerError::RingExhausted(exhausted) => StampError::BucketFull {
                    bucket: exhausted.bucket,
                    capacity: self.bucket_capacity(),
                },
                IssuerError::Geometry(geometry) => geometry,
                // Ownership and batch-identity errors carry the same structured
                // fields the validator's own variants name.
                IssuerError::NotBatchOwner { owner, signer } => StampError::OwnerMismatch {
                    expected: owner,
                    actual: signer,
                },
                IssuerError::BatchMismatch { issuer, signer } => StampError::BatchMismatch {
                    expected: signer,
                    actual: issuer,
                },
                // `reserve_slot` yields nothing else: its slot source maps every
                // counter error to RingExhausted, and the construction and
                // geometry-shape conditions cannot reach the stamp loop. Carried
                // boxed, a surprise keeps its type and message instead of
                // panicking the caller.
                err => {
                    let boxed: BoxedError = Box::new(err);
                    StampError::External(boxed)
                }
            })
    }

    fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    fn batch_depth(&self) -> u8 {
        self.state.borrow().counters.depth()
    }

    fn bucket_depth(&self) -> u8 {
        self.state.borrow().counters.bucket_depth().get()
    }

    fn max_bucket_utilization(&self) -> u32 {
        self.state.borrow().max_utilization
    }

    fn bucket_utilization(&self, bucket: u32) -> u32 {
        let state = self.state.borrow();
        // `u32` always fits `usize` on the >=32-bit targets this crate supports.
        #[allow(clippy::as_conversions)]
        let bucket_idx = bucket as usize;
        if bucket_idx >= state.counters.counts().len() {
            return 0;
        }
        state.bucket_fill(bucket_idx)
    }

    fn bucket_has_capacity(&self, bucket: u32) -> bool {
        let state = self.state.borrow();
        // `u32` always fits `usize` on the >=32-bit targets this crate supports.
        #[allow(clippy::as_conversions)]
        let bucket_idx = bucket as usize;
        if bucket_idx >= state.counters.counts().len() {
            return false;
        }
        // Report honestly whether a fresh, never-written slot remains. A ring
        // that has wrapped reports no spare capacity even though issuance into
        // it still succeeds by overwriting an earlier chunk.
        state.bucket_fill(bucket_idx) < state.counters.bucket_capacity()
    }

    fn stamps_issued(&self) -> Option<u64> {
        // A standalone ring keeps a true monotone issuance count, so it can be
        // honest here even though its counter sum would undercount on wrap.
        Some(self.state.borrow().stamps_issued)
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
            BucketDepth::new(bucket_depth).unwrap(),
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
            BucketDepth::new(bucket_depth).unwrap(),
            true,
        )
    }

    fn bucket_depth() -> BucketDepth {
        BucketDepth::new(16).unwrap()
    }

    #[test]
    fn external_ring_wraps_and_reuses_slots() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket.
        let batch = mutable_batch(17, 16);
        let issuer = RingIssuer::external(&batch).unwrap();

        let address = test_address(0xABCD);

        let d0 = issuer.reserve_slot(&address, 1).unwrap();
        let d1 = issuer.reserve_slot(&address, 2).unwrap();
        assert_eq!(d0.index().index(), 0);
        assert_eq!(d1.index().index(), 1);

        // A third issuance wraps back to slot zero rather than failing.
        let d2 = issuer.reserve_slot(&address, 3).unwrap();
        assert_eq!(d2.index().index(), 0);

        let d3 = issuer.reserve_slot(&address, 4).unwrap();
        assert_eq!(d3.index().index(), 1);

        assert_eq!(issuer.stamps_issued(), Some(4));
    }

    #[test]
    fn a_ring_refuses_a_depth_no_counter_table_can_hold() {
        for depth in [8u8, 48, u8::MAX] {
            let batch = mutable_batch(depth, 16);
            assert!(matches!(
                RingIssuer::external(&batch),
                Err(IssuerError::Geometry(_))
            ));
            assert!(matches!(
                RingIssuer::reserved(&batch, []),
                Err(IssuerError::Geometry(_))
            ));
        }
    }

    #[test]
    fn external_ring_index_stays_within_capacity() {
        // depth=18, bucket_depth=16 gives 4 slots per bucket.
        let batch = mutable_batch(18, 16);
        let issuer = RingIssuer::external(&batch).unwrap();

        let address = test_address(0x0042);

        for ts in 0..100u64 {
            let permit = issuer.reserve_slot(&address, ts).unwrap();
            assert!(permit.index().index() < 4, "index escaped bucket capacity");
        }
        assert_eq!(issuer.stamps_issued(), Some(100));
    }

    #[test]
    fn reserved_ring_never_emits_a_protected_slot() {
        // depth=18, bucket_depth=16 gives 4 slots per bucket. Protect slots 1
        // and 3 in the target bucket; the ring may only ever emit 0 and 2.
        let batch = mutable_batch(18, 16);
        let bucket = calculate_bucket(&test_address(0x00AA), bucket_depth()).value();
        let issuer = RingIssuer::reserved(&batch, [(bucket, 1), (bucket, 3)]).unwrap();

        let address = test_address(0x00AA);

        // Issue far past one wrap so every wrap is exercised.
        for ts in 0..50u64 {
            let permit = issuer.reserve_slot(&address, ts).unwrap();
            let index = permit.index().index();
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
        let bucket = calculate_bucket(&test_address(0x0001), bucket_depth()).value();
        let issuer = RingIssuer::reserved(&batch, [(bucket, 0), (bucket, 1)]).unwrap();

        let address = test_address(0x0001);
        let err = issuer
            .reserve_slot(&address, 1)
            .expect_err("every slot is protected");
        assert!(matches!(
            err,
            IssuerError::RingExhausted(RingExhausted { bucket: b }) if b == bucket
        ));
        let cause = core::error::Error::source(&err)
            .expect("the shared condition is the source")
            .downcast_ref::<RingExhausted>()
            .expect("the source is the shared condition");
        assert_eq!(cause.bucket, bucket);
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
        let issuer = RingIssuer::external(&batch).unwrap();

        let address = test_address(0x0001);
        let bucket = calculate_bucket(&address, bucket_depth()).value();

        assert!(issuer.bucket_has_capacity(bucket));
        issuer.reserve_slot(&address, 1).unwrap();
        assert!(issuer.bucket_has_capacity(bucket));
        assert_eq!(issuer.bucket_utilization(bucket), 1);

        issuer.reserve_slot(&address, 2).unwrap();
        assert!(!issuer.bucket_has_capacity(bucket));
        assert_eq!(issuer.bucket_utilization(bucket), 2);
        assert_eq!(issuer.max_bucket_utilization(), 2);

        // Issuance still succeeds despite the bucket reporting no capacity, and
        // utilization saturates rather than counting overwrites.
        issuer.reserve_slot(&address, 3).unwrap();
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
        let bucket = calculate_bucket(&test_address(0x0001), bucket_depth()).value();
        let issuer = RingIssuer::reserved(&batch, [(bucket, 0), (bucket, 1)]).unwrap();

        let address = test_address(0x0001);
        let result = StampIssuer::reserve(&issuer, &address, 1);
        assert!(matches!(
            result,
            Err(StampError::BucketFull { bucket: b, capacity: 2 }) if b == bucket
        ));
    }

    mod proptests {
        use alloc::collections::BTreeMap;
        use proptest::prelude::*;

        use super::*;

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(64))]

            /// Ring issuance cycles each bucket's slots in order, utilization
            /// saturates at the capacity instead of counting overwrites,
            /// spare capacity is reported iff a fresh slot remains, and the
            /// lifetime count stays exact across wraps.
            #[test]
            fn ring_wraps_in_range_and_reports_saturation_honestly(
                bucket_depth in 16u8..=18,
                excess in 0u8..=4,
                leads in proptest::collection::vec(0u16..6, 1..160),
            ) {
                let batch = mutable_batch(bucket_depth + excess, bucket_depth);
                let issuer = RingIssuer::external(&batch).unwrap();
                let capacity = issuer.bucket_capacity();
                let mut writes = BTreeMap::<u16, u32>::new();
                let mut ts = 0u64;
                for &lead in &leads {
                    ts += 1;
                    let bucket = u32::from(lead) << (bucket_depth - 16);
                    let permit = issuer.reserve_slot(&test_address(lead), ts).unwrap();
                    let n = writes.entry(lead).or_insert(0);
                    prop_assert_eq!(permit.index().bucket(), bucket);
                    prop_assert_eq!(permit.index().index(), *n % capacity);
                    *n += 1;
                    prop_assert_eq!(issuer.bucket_utilization(bucket), (*n).min(capacity));
                    prop_assert_eq!(issuer.bucket_has_capacity(bucket), *n < capacity);
                }
                let peak = writes.values().map(|&n| n.min(capacity)).max().unwrap_or(0);
                prop_assert_eq!(issuer.max_bucket_utilization(), peak);
                prop_assert_eq!(
                    issuer.stamps_issued(),
                    Some(u64::try_from(leads.len()).unwrap())
                );
            }
        }
    }
}

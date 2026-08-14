//! Wiring [`BatchEvent::DepthIncrease`] through to issuer dilution.
//!
//! A node that issues stamps holds a live issuer per batch. When the postage
//! contract emits a depth increase for a batch, the matching issuer must grow
//! its per-bucket capacity so that previously full buckets can accept more
//! chunks. This module provides the small adapter that performs that wiring: a
//! registry of dilutable issuers keyed by [`BatchId`] that implements
//! [`BatchEventHandler`] and, on a [`BatchEvent::DepthIncrease`], calls
//! [`Dilutable::dilute`] on the matching issuer.
//!
//! The adapter lives in the issuer crate rather than in `nectar-postage`
//! because dilution is an issuer concern: `nectar-postage` is the lower crate
//! and knows nothing about issuers, so the dependency points up from the event
//! trait to the issuer.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::IssuerError;
use crate::issuer::MemoryIssuer;
use nectar_marker::{MaybeSend, MaybeSync};
use nectar_postage::{BatchEvent, BatchEventHandler, BatchId};
use nectar_primitives::SwarmSpec;

/// An issuer whose per-bucket capacity can be grown by an on-chain dilution.
///
/// Dilution takes `&self`, so the registry and a signing pipeline hold the
/// same issuer.
///
/// The trait is spec-agnostic: it only reads scalar geometry, so one registry
/// can hold issuers for different networks behind `dyn Dilutable`.
pub trait Dilutable: MaybeSend + MaybeSync {
    /// Returns the batch ID this issuer issues stamps for.
    fn batch_id(&self) -> BatchId;

    /// Returns the current batch depth.
    fn batch_depth(&self) -> u8;

    /// Returns the current per-bucket capacity (`2^(depth - bucket_depth)`).
    fn bucket_capacity(&self) -> u32;

    /// Applies an on-chain dilution, growing the per-bucket capacity to the
    /// geometry implied by `new_depth`.
    ///
    /// # Errors
    ///
    /// Returns [`IssuerError::DepthDecrease`] if `new_depth` is below the
    /// current depth.
    fn dilute(&self, new_depth: u8) -> Result<(), IssuerError>;
}

/// A registered issuer, shared between the registry and its pipelines.
pub type IssuerHandle = Arc<dyn Dilutable>;

impl<S: SwarmSpec> Dilutable for MemoryIssuer<S> {
    // The geometry accessors come from the StampIssuer trait, so they are named
    // explicitly to avoid resolving back into this Dilutable impl.
    fn batch_id(&self) -> BatchId {
        crate::StampIssuer::batch_id(self)
    }

    fn batch_depth(&self) -> u8 {
        crate::StampIssuer::batch_depth(self)
    }

    fn bucket_capacity(&self) -> u32 {
        crate::StampIssuer::bucket_capacity(self)
    }

    fn dilute(&self, new_depth: u8) -> Result<(), IssuerError> {
        Self::dilute(self, new_depth)
    }
}

/// A dilution observed on chain and awaiting its confirmations.
#[derive(Debug, Clone, Copy)]
struct Pending {
    batch_id: BatchId,
    depth: u8,
    block: u64,
}

impl Pending {
    const fn is_confirmed(&self, head: u64, confirmations: u64) -> bool {
        self.block.saturating_add(confirmations) <= head
    }
}

/// A registry of dilutable issuers keyed by [`BatchId`].
///
/// Register a shared issuer with [`register`](Self::register), then feed batch
/// events through [`BatchEventHandler`]. A [`BatchEvent::DepthIncrease`] for a
/// registered batch grows that issuer's capacity once the dilution block has
/// [`confirmations`](Self::confirmations) behind it; an event for an
/// unregistered batch is a no-op. All other event variants are ignored, since
/// dilution is the only state this adapter owns.
///
/// Widened capacity is gated because a stamp minted into it is invalid to any
/// peer that has not yet seen the event.
#[derive(Default)]
pub struct IssuerRegistry {
    issuers: HashMap<BatchId, IssuerHandle>,
    pending: Vec<Pending>,
    confirmations: u64,
    block: u64,
}

impl IssuerRegistry {
    /// Creates an empty registry that demands `confirmations` blocks behind a
    /// dilution before it issues into the widened capacity.
    pub fn new(confirmations: u64) -> Self {
        Self {
            confirmations,
            ..Self::default()
        }
    }

    /// Returns the confirmations demanded of a dilution before issuance.
    pub const fn confirmations(&self) -> u64 {
        self.confirmations
    }

    /// Returns the highest block the registry has observed.
    pub const fn block(&self) -> u64 {
        self.block
    }

    /// Registers a shared issuer under its own batch ID.
    ///
    /// A subsequent registration for the same batch ID replaces the previous
    /// handle and returns it.
    pub fn register(&mut self, issuer: IssuerHandle) -> Option<IssuerHandle> {
        self.issuers.insert(issuer.batch_id(), issuer)
    }

    /// Returns a handle to the issuer registered for `batch_id`.
    pub fn get(&self, batch_id: &BatchId) -> Option<IssuerHandle> {
        self.issuers.get(batch_id).map(Arc::clone)
    }

    /// Removes the issuer registered for `batch_id`, dropping any dilution it
    /// was still waiting on.
    pub fn remove(&mut self, batch_id: &BatchId) -> Option<IssuerHandle> {
        self.pending.retain(|pending| pending.batch_id != *batch_id);
        self.issuers.remove(batch_id)
    }

    /// Returns the number of registered issuers.
    pub fn len(&self) -> usize {
        self.issuers.len()
    }

    /// Returns `true` if no issuers are registered.
    pub fn is_empty(&self) -> bool {
        self.issuers.is_empty()
    }

    /// Returns the deepest dilution observed for `batch_id` that is not yet
    /// confirmed.
    pub fn pending_depth(&self, batch_id: &BatchId) -> Option<u8> {
        self.pending
            .iter()
            .filter(|pending| pending.batch_id == *batch_id)
            .map(|pending| pending.depth)
            .max()
    }

    /// Advances the observed chain head, applying every dilution that the head
    /// now confirms.
    ///
    /// # Errors
    ///
    /// The first [`Dilutable::dilute`] failure, after every other confirmed
    /// dilution has been applied.
    pub fn advance_to(&mut self, block: u64) -> Result<(), IssuerError> {
        self.block = self.block.max(block);
        self.settle()
    }

    /// Applies the confirmed dilutions and keeps the rest.
    fn settle(&mut self) -> Result<(), IssuerError> {
        let (due, held): (Vec<Pending>, Vec<Pending>) = std::mem::take(&mut self.pending)
            .into_iter()
            .partition(|pending| pending.is_confirmed(self.block, self.confirmations));
        self.pending = held;

        let mut outcome = Ok(());
        for pending in due {
            // Depth is raised by maximum, so a redelivered or reordered
            // dilution is a no-op and the order here does not matter.
            if let Some(issuer) = self.issuers.get(&pending.batch_id) {
                let applied = issuer.dilute(pending.depth);
                if outcome.is_ok() {
                    outcome = applied;
                }
            }
        }
        outcome
    }
}

impl std::fmt::Debug for IssuerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IssuerRegistry")
            .field("issuers", &self.issuers.keys().collect::<Vec<_>>())
            .field("pending", &self.pending)
            .field("confirmations", &self.confirmations)
            .field("block", &self.block)
            .finish()
    }
}

impl BatchEventHandler for IssuerRegistry {
    type Error = IssuerError;

    fn handle_event(&mut self, event: BatchEvent) -> Result<(), Self::Error> {
        match event {
            BatchEvent::DepthIncrease {
                batch_id,
                new_depth,
                block,
            } => {
                // A depth increase for a batch we do not track is a no-op, not
                // an error: another handler owns that batch's issuer.
                if !self.issuers.contains_key(&batch_id) {
                    return Ok(());
                }
                self.pending.push(Pending {
                    batch_id,
                    depth: new_depth,
                    block,
                });
                // The event proves the chain reached its own block.
                self.advance_to(block)
            }
            // Created, TopUp, and Expired carry no capacity change this adapter
            // is responsible for.
            BatchEvent::Created { .. } | BatchEvent::TopUp { .. } | BatchEvent::Expired { .. } => {
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryIssuer;
    use nectar_postage::{BucketDepth, StampError};
    use nectar_primitives::{ChunkAddress, Mainnet};

    const DILUTION_BLOCK: u64 = 1_000;
    const CONFIRMATIONS: u64 = 8;

    fn batch_id(byte: u8) -> BatchId {
        BatchId::new([byte; 32])
    }

    /// A two-slot issuer: depth 17 over bucket depth 16.
    fn issuer(id: BatchId) -> Arc<MemoryIssuer> {
        Arc::new(MemoryIssuer::<Mainnet>::new(
            id,
            17,
            BucketDepth::new(16).unwrap(),
        ))
    }

    fn address() -> ChunkAddress {
        ChunkAddress::new([0xAB; 32])
    }

    fn dilution(batch_id: BatchId, new_depth: u8) -> BatchEvent {
        BatchEvent::DepthIncrease {
            batch_id,
            new_depth,
            block: DILUTION_BLOCK,
        }
    }

    #[test]
    fn depth_increase_grows_registered_issuer_capacity() {
        let tracked = batch_id(0x11);
        let mut registry = IssuerRegistry::new(0);
        registry.register(issuer(tracked));
        assert_eq!(registry.get(&tracked).unwrap().bucket_capacity(), 2);

        registry.handle_event(dilution(tracked, 18)).unwrap();

        // The diluted issuer reflects the new depth: depth 18 over bucket_depth
        // 16 is 4 slots per bucket.
        let issuer = registry.get(&tracked).unwrap();
        assert_eq!(issuer.batch_depth(), 18);
        assert_eq!(issuer.bucket_capacity(), 4);
    }

    #[test]
    fn a_registered_handle_stays_usable_for_issuance() {
        // The registry holds a handle rather than the issuer, so the same
        // issuer signs while the registry dilutes it.
        let tracked = batch_id(0x22);
        let handle = issuer(tracked);
        let mut registry = IssuerRegistry::new(0);
        registry.register(handle.clone());

        handle.reserve(&address(), 1).unwrap();
        handle.reserve(&address(), 2).unwrap();
        assert!(matches!(
            handle.reserve(&address(), 3),
            Err(StampError::BucketFull { capacity: 2, .. })
        ));

        registry.handle_event(dilution(tracked, 18)).unwrap();

        // The watermark does not move, so the reopened bucket continues at 2.
        assert_eq!(handle.reserve(&address(), 4).unwrap().index().index(), 2);
    }

    #[test]
    fn issuance_waits_for_the_dilution_to_confirm() {
        let tracked = batch_id(0x77);
        let handle = issuer(tracked);
        let mut registry = IssuerRegistry::new(CONFIRMATIONS);
        registry.register(handle.clone());
        handle.reserve(&address(), 1).unwrap();
        handle.reserve(&address(), 2).unwrap();

        registry.handle_event(dilution(tracked, 18)).unwrap();

        // Observed, not yet issuable: the widened range is invalid to any peer
        // that has not seen the event.
        assert_eq!(registry.pending_depth(&tracked), Some(18));
        assert_eq!(handle.batch_depth(), 17);
        assert!(handle.reserve(&address(), 3).is_err());

        registry
            .advance_to(DILUTION_BLOCK + CONFIRMATIONS - 1)
            .unwrap();
        assert_eq!(handle.batch_depth(), 17);
        assert!(handle.reserve(&address(), 4).is_err());

        registry.advance_to(DILUTION_BLOCK + CONFIRMATIONS).unwrap();
        assert_eq!(registry.pending_depth(&tracked), None);
        assert_eq!(handle.batch_depth(), 18);
        assert_eq!(handle.reserve(&address(), 5).unwrap().index().index(), 2);
    }

    #[test]
    fn a_confirmed_backfill_applies_without_waiting() {
        // Replayed history is already confirmed, so it must not park the issuer
        // at the old depth.
        let tracked = batch_id(0x88);
        let handle = issuer(tracked);
        let mut registry = IssuerRegistry::new(CONFIRMATIONS);
        registry.register(handle.clone());

        registry.handle_event(dilution(tracked, 18)).unwrap();
        registry
            .advance_to(DILUTION_BLOCK + CONFIRMATIONS * 4)
            .unwrap();
        registry
            .handle_event(BatchEvent::DepthIncrease {
                batch_id: tracked,
                new_depth: 19,
                block: DILUTION_BLOCK + 1,
            })
            .unwrap();

        assert_eq!(handle.batch_depth(), 19);
        assert_eq!(registry.pending_depth(&tracked), None);
    }

    #[test]
    fn each_pending_dilution_confirms_on_its_own_block() {
        let tracked = batch_id(0x99);
        let handle = issuer(tracked);
        let mut registry = IssuerRegistry::new(CONFIRMATIONS);
        registry.register(handle.clone());

        registry.handle_event(dilution(tracked, 18)).unwrap();
        registry
            .handle_event(BatchEvent::DepthIncrease {
                batch_id: tracked,
                new_depth: 20,
                block: DILUTION_BLOCK + 10,
            })
            .unwrap();
        assert_eq!(registry.pending_depth(&tracked), Some(20));

        registry.advance_to(DILUTION_BLOCK + CONFIRMATIONS).unwrap();
        assert_eq!(handle.batch_depth(), 18);
        assert_eq!(registry.pending_depth(&tracked), Some(20));

        registry
            .advance_to(DILUTION_BLOCK + 10 + CONFIRMATIONS)
            .unwrap();
        assert_eq!(handle.batch_depth(), 20);
    }

    #[test]
    fn a_removed_issuer_leaves_no_dilution_behind() {
        let tracked = batch_id(0xAA);
        let handle = issuer(tracked);
        let mut registry = IssuerRegistry::new(CONFIRMATIONS);
        registry.register(handle.clone());
        registry.handle_event(dilution(tracked, 18)).unwrap();

        assert!(registry.remove(&tracked).is_some());
        registry.advance_to(DILUTION_BLOCK + CONFIRMATIONS).unwrap();

        assert_eq!(registry.pending_depth(&tracked), None);
        assert_eq!(handle.batch_depth(), 17);
        assert!(registry.is_empty());
    }

    #[test]
    fn depth_increase_for_unknown_batch_leaves_tracked_issuer_untouched() {
        let tracked = batch_id(0x33);
        let other = batch_id(0x44);

        let mut registry = IssuerRegistry::new(0);
        registry.register(issuer(tracked));

        // An event for a batch we do not track must not error and must leave
        // the tracked issuer untouched.
        registry.handle_event(dilution(other, 24)).unwrap();

        let issuer = registry.get(&tracked).unwrap();
        assert_eq!(issuer.batch_depth(), 17);
        assert_eq!(issuer.bucket_capacity(), 2);
        // The unrelated batch was never registered, so nothing was created for
        // it.
        assert!(registry.get(&other).is_none());
        assert_eq!(registry.pending_depth(&other), None);
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn non_depth_events_are_ignored() {
        let tracked = batch_id(0x55);
        let mut registry = IssuerRegistry::new(0);
        registry.register(issuer(tracked));

        registry
            .handle_event(BatchEvent::TopUp {
                batch_id: tracked,
                new_value: 1000,
            })
            .unwrap();
        registry
            .handle_event(BatchEvent::Expired { batch_id: tracked })
            .unwrap();

        let issuer = registry.get(&tracked).unwrap();
        assert_eq!(issuer.batch_depth(), 17);
        assert_eq!(issuer.bucket_capacity(), 2);
    }

    #[test]
    fn depth_decrease_event_surfaces_error_defensively() {
        // The contract never emits a decrease, but if a malformed event arrives
        // the adapter must not silently corrupt state: the issuer's own guard
        // refuses it and the error propagates.
        let tracked = batch_id(0x66);
        let handle = Arc::new(MemoryIssuer::<Mainnet>::new(
            tracked,
            18,
            BucketDepth::new(16).unwrap(),
        ));
        let mut registry = IssuerRegistry::new(0);
        registry.register(handle);

        let result = registry.handle_event(dilution(tracked, 17));
        assert!(matches!(
            result,
            Err(IssuerError::DepthDecrease {
                current: 18,
                requested: 17
            })
        ));
        assert_eq!(registry.pending_depth(&tracked), None);
    }
}

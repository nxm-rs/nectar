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

use crate::error::IssuerError;
use crate::issuer::MemoryIssuer;
use crate::sharded::ShardedIssuer;
use nectar_postage::{BatchEvent, BatchEventHandler, BatchId};

/// An issuer whose per-bucket capacity can be grown by an on-chain dilution.
///
/// This is the minimal surface the [`IssuerRegistry`] needs to drive a
/// [`BatchEvent::DepthIncrease`] through to the right issuer. It is implemented
/// for the fill-only issuers in this crate ([`MemoryIssuer`] and
/// [`ShardedIssuer`]); a self-hosting ring issuer dilutes through its snapshot
/// in `nectar-postage-usage` and is not registered here.
pub trait Dilutable {
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
    fn dilute(&mut self, new_depth: u8) -> Result<(), IssuerError>;
}

impl Dilutable for MemoryIssuer {
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

    fn dilute(&mut self, new_depth: u8) -> Result<(), IssuerError> {
        Self::dilute(self, new_depth)
    }
}

impl Dilutable for ShardedIssuer {
    fn batch_id(&self) -> BatchId {
        Self::batch_id(self)
    }

    fn batch_depth(&self) -> u8 {
        Self::batch_depth(self)
    }

    fn bucket_capacity(&self) -> u32 {
        Self::bucket_capacity(self)
    }

    fn dilute(&mut self, new_depth: u8) -> Result<(), IssuerError> {
        Self::dilute(self, new_depth)
    }
}

/// A registry of dilutable issuers keyed by [`BatchId`].
///
/// Register a live issuer with [`register`](Self::register), then feed batch
/// events through [`BatchEventHandler`]. A [`BatchEvent::DepthIncrease`] for a
/// registered batch grows that issuer's capacity; an event for an unregistered
/// batch is a no-op. All other event variants are ignored, since dilution is
/// the only state this adapter owns.
#[derive(Default)]
pub struct IssuerRegistry {
    issuers: HashMap<BatchId, Box<dyn Dilutable + Send>>,
}

impl IssuerRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers an issuer under its own batch ID.
    ///
    /// A subsequent registration for the same batch ID replaces the previous
    /// issuer and returns it.
    pub fn register<I>(&mut self, issuer: I) -> Option<Box<dyn Dilutable + Send>>
    where
        I: Dilutable + Send + 'static,
    {
        self.issuers
            .insert(Dilutable::batch_id(&issuer), Box::new(issuer))
    }

    /// Returns a shared reference to the issuer registered for `batch_id`.
    pub fn get(&self, batch_id: &BatchId) -> Option<&(dyn Dilutable + Send)> {
        self.issuers.get(batch_id).map(|boxed| &**boxed)
    }

    /// Removes the issuer registered for `batch_id`, if any.
    pub fn remove(&mut self, batch_id: &BatchId) -> Option<Box<dyn Dilutable + Send>> {
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
}

impl std::fmt::Debug for IssuerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IssuerRegistry")
            .field("issuers", &self.issuers.keys().collect::<Vec<_>>())
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
            } => self
                .issuers
                .get_mut(&batch_id)
                // A depth increase for a batch we do not track is a no-op, not
                // an error: another handler owns that batch's issuer.
                .map_or(Ok(()), |issuer| issuer.dilute(new_depth)),
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
    use nectar_postage::BucketDepth;

    fn batch_id(byte: u8) -> BatchId {
        BatchId::new([byte; 32])
    }

    #[test]
    fn depth_increase_grows_registered_issuer_capacity() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket.
        let tracked = batch_id(0x11);
        let mut registry = IssuerRegistry::new();
        registry.register(MemoryIssuer::new(
            tracked,
            17,
            BucketDepth::new(16).unwrap(),
        ));
        assert_eq!(registry.get(&tracked).unwrap().bucket_capacity(), 2);

        registry
            .handle_event(BatchEvent::DepthIncrease {
                batch_id: tracked,
                new_depth: 18,
            })
            .unwrap();

        // The diluted issuer reflects the new depth: depth 18 over bucket_depth
        // 16 is 4 slots per bucket.
        let issuer = registry.get(&tracked).unwrap();
        assert_eq!(issuer.batch_depth(), 18);
        assert_eq!(issuer.bucket_capacity(), 4);
    }

    #[test]
    fn depth_increase_grows_sharded_issuer_capacity() {
        let tracked = batch_id(0x22);
        let mut registry = IssuerRegistry::new();
        registry.register(ShardedIssuer::new(
            tracked,
            17,
            BucketDepth::new(16).unwrap(),
        ));
        assert_eq!(registry.get(&tracked).unwrap().bucket_capacity(), 2);

        registry
            .handle_event(BatchEvent::DepthIncrease {
                batch_id: tracked,
                new_depth: 20,
            })
            .unwrap();

        let issuer = registry.get(&tracked).unwrap();
        assert_eq!(issuer.batch_depth(), 20);
        assert_eq!(issuer.bucket_capacity(), 16);
    }

    #[test]
    fn depth_increase_for_unknown_batch_leaves_tracked_issuer_untouched() {
        let tracked = batch_id(0x33);
        let other = batch_id(0x44);

        let mut registry = IssuerRegistry::new();
        registry.register(MemoryIssuer::new(
            tracked,
            17,
            BucketDepth::new(16).unwrap(),
        ));

        // An event for a batch we do not track must not error and must leave
        // the tracked issuer untouched.
        registry
            .handle_event(BatchEvent::DepthIncrease {
                batch_id: other,
                new_depth: 24,
            })
            .unwrap();

        let issuer = registry.get(&tracked).unwrap();
        assert_eq!(issuer.batch_depth(), 17);
        assert_eq!(issuer.bucket_capacity(), 2);
        // The unrelated batch was never registered, so nothing was created for
        // it.
        assert!(registry.get(&other).is_none());
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn non_depth_events_are_ignored() {
        let tracked = batch_id(0x55);
        let mut registry = IssuerRegistry::new();
        registry.register(MemoryIssuer::new(
            tracked,
            17,
            BucketDepth::new(16).unwrap(),
        ));

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
        let mut registry = IssuerRegistry::new();
        registry.register(MemoryIssuer::new(
            tracked,
            18,
            BucketDepth::new(16).unwrap(),
        ));

        let result = registry.handle_event(BatchEvent::DepthIncrease {
            batch_id: tracked,
            new_depth: 17,
        });
        assert!(matches!(
            result,
            Err(IssuerError::DepthDecrease {
                current: 18,
                requested: 17
            })
        ));
    }
}

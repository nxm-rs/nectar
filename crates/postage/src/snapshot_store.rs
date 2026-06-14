//! A store for recovered issuer snapshot state, keyed by [`BatchId`].
//!
//! Issuing postage stamps needs per-bucket counters so every stamp claims a
//! fresh storage slot. That issuer state can always be rebuilt from the network:
//! it is published inside the batch it describes, as single-owner chunks at
//! addresses derived from the batch id and owner alone, so a user can recover it
//! on any machine from just their key and batch id. The network is therefore the
//! source of truth.
//!
//! A [`SnapshotStore`] is a *cache* in front of that recovery path, not an
//! authority. It lets an issuer avoid a network round trip on the warm path by
//! keeping the most recently observed state for each batch locally. A cold or
//! evicted entry is never an error: the caller falls back to network recovery
//! and may then [`persist`](SnapshotStore::persist) the rebuilt state to warm
//! the cache again. Because the trait is a cache, an implementation is free to
//! drop entries (bounded memory, eviction, a fresh process) without violating
//! any invariant, and a returned snapshot must still be validated against the
//! network before it is trusted for issuance.
//!
//! The trait is generic over the snapshot state type `S` so this crate stays
//! free of the issuer-side snapshot encoding: a consumer such as the
//! `nectar-postage-usage` crate supplies its own snapshot type. The store only
//! ever moves opaque values keyed by [`BatchId`].

use crate::BatchId;

/// A cache for recovered issuer snapshot state, keyed by [`BatchId`].
///
/// Implementations persist and load the snapshot state `S` for a batch. The
/// network is the source of truth for this state (see the module-level
/// docs); a store is only a warm-path cache, so a missing entry is
/// reported as `Ok(None)` rather than an error and the caller recovers from the
/// network instead.
///
/// # Async Design
///
/// The methods are async so an implementation may sit in front of a slow
/// backend (disk, a key-value database) without forcing callers to block.
///
/// # Example
///
/// ```ignore
/// use nectar_postage::{BatchId, SnapshotStore};
///
/// async fn warm<S, T: SnapshotStore<S>>(store: &T, id: &BatchId) -> Option<S> {
///     // Try the cache; on a miss the caller would recover from the network.
///     store.load(id).await.ok().flatten()
/// }
/// ```
pub trait SnapshotStore<S> {
    /// The error type returned by store operations.
    type Error: std::error::Error;

    /// Loads the snapshot state for `id`.
    ///
    /// Returns `Ok(None)` on a cache miss. A miss is expected on a cold store
    /// and is not an error: the caller recovers the state from the network and
    /// may [`persist`](Self::persist) it afterwards. A returned value is a
    /// cached hint and must still be validated against the network before it is
    /// trusted for issuance. When `S` is a `nectar-postage-usage` snapshot the
    /// loaded value is unvalidated and carries no persist capability; it must be
    /// admitted through that crate's network-floor check before any persist.
    fn load(
        &self,
        id: &BatchId,
    ) -> impl std::future::Future<Output = Result<Option<S>, Self::Error>> + Send;

    /// Persists the snapshot state for `id`, overwriting any cached entry.
    ///
    /// This only updates the local cache; it does not publish to the network
    /// and confers no authority on the stored value.
    fn persist(
        &self,
        id: &BatchId,
        snapshot: S,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;

    /// Removes any cached snapshot state for `id`.
    ///
    /// Returns `true` if an entry existed and was removed. Dropping an entry is
    /// always safe: the state can be recovered from the network.
    fn remove(
        &self,
        id: &BatchId,
    ) -> impl std::future::Future<Output = Result<bool, Self::Error>> + Send;

    /// Returns whether a snapshot state is cached for `id`.
    fn contains(
        &self,
        id: &BatchId,
    ) -> impl std::future::Future<Output = Result<bool, Self::Error>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::sync::Mutex;

    /// An in-memory [`SnapshotStore`] for tests.
    ///
    /// Backed by a plain map behind a mutex, it models the cache contract
    /// exactly: entries can be loaded, overwritten, and removed, and a miss is a
    /// plain `None`. It performs no network recovery of its own.
    #[derive(Debug, Default)]
    struct InMemorySnapshotStore<S> {
        entries: Mutex<HashMap<BatchId, S>>,
    }

    impl<S> InMemorySnapshotStore<S> {
        fn new() -> Self {
            Self {
                entries: Mutex::new(HashMap::new()),
            }
        }

        fn len(&self) -> usize {
            self.entries.lock().expect("poisoned").len()
        }
    }

    impl<S: Clone + Send + Sync> SnapshotStore<S> for InMemorySnapshotStore<S> {
        type Error = Infallible;

        async fn load(&self, id: &BatchId) -> Result<Option<S>, Self::Error> {
            Ok(self.entries.lock().expect("poisoned").get(id).cloned())
        }

        async fn persist(&self, id: &BatchId, snapshot: S) -> Result<(), Self::Error> {
            self.entries.lock().expect("poisoned").insert(*id, snapshot);
            Ok(())
        }

        async fn remove(&self, id: &BatchId) -> Result<bool, Self::Error> {
            Ok(self.entries.lock().expect("poisoned").remove(id).is_some())
        }

        async fn contains(&self, id: &BatchId) -> Result<bool, Self::Error> {
            Ok(self.entries.lock().expect("poisoned").contains_key(id))
        }
    }

    fn id(byte: u8) -> BatchId {
        B256::repeat_byte(byte)
    }

    #[tokio::test]
    async fn load_misses_on_cold_store() {
        let store: InMemorySnapshotStore<u64> = InMemorySnapshotStore::new();
        // A cold load is a miss, not an error: the caller recovers from the
        // network instead.
        assert_eq!(store.load(&id(1)).await.unwrap(), None);
        assert!(!store.contains(&id(1)).await.unwrap());
    }

    #[tokio::test]
    async fn persist_then_load_round_trips() {
        let store = InMemorySnapshotStore::new();
        store.persist(&id(2), 42u64).await.unwrap();

        assert!(store.contains(&id(2)).await.unwrap());
        assert_eq!(store.load(&id(2)).await.unwrap(), Some(42));
        // A different batch id is still a miss: entries are keyed by batch id.
        assert_eq!(store.load(&id(3)).await.unwrap(), None);
    }

    #[tokio::test]
    async fn persist_overwrites_existing_entry() {
        let store = InMemorySnapshotStore::new();
        store.persist(&id(4), 1u64).await.unwrap();
        store.persist(&id(4), 2u64).await.unwrap();

        // The later persist wins; the cache holds one entry per batch id.
        assert_eq!(store.load(&id(4)).await.unwrap(), Some(2));
        assert_eq!(store.len(), 1);
    }

    #[tokio::test]
    async fn remove_reports_prior_presence() {
        let store = InMemorySnapshotStore::new();
        store.persist(&id(5), 7u64).await.unwrap();

        // Removing a present entry reports true and clears it; the state can
        // still be recovered from the network, so this is always safe.
        assert!(store.remove(&id(5)).await.unwrap());
        assert_eq!(store.load(&id(5)).await.unwrap(), None);
        // Removing an absent entry reports false.
        assert!(!store.remove(&id(5)).await.unwrap());
    }

    #[tokio::test]
    async fn entries_are_isolated_by_batch_id() {
        let store = InMemorySnapshotStore::new();
        store.persist(&id(6), 60u64).await.unwrap();
        store.persist(&id(7), 70u64).await.unwrap();

        // Distinct batch ids do not alias one another.
        assert_eq!(store.load(&id(6)).await.unwrap(), Some(60));
        assert_eq!(store.load(&id(7)).await.unwrap(), Some(70));
        assert!(store.remove(&id(6)).await.unwrap());
        assert_eq!(store.load(&id(6)).await.unwrap(), None);
        assert_eq!(store.load(&id(7)).await.unwrap(), Some(70));
        assert_eq!(store.len(), 1);
    }
}

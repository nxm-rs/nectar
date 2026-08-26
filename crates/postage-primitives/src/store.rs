//! The synchronous stamp-keyed chunk store seam.
//!
//! A node's reserve runs stamp admission inside its own write transaction,
//! so it reads its store synchronously, keyed on the stamp as much as on
//! the chunk. [`StoreKey`] is that key: the chunk address, the batch, and
//! the hash of the stamp version. The stamp is half the primary key, not
//! an optional field beside it, so a stampless put is a type error.
//!
//! [`ChunkStore`] composes the postage-free synchronous retrieval seam
//! `nectar_primitives::store::ChunkGetSync` for the stampless body read, so
//! one store answers both asks. A miss answers a classified absence
//! through [`StoreError`] either way. Presence survives on this seam as
//! the fallible [`contains`](ChunkStore::contains). [`Lifted`] lifts a
//! store onto the asynchronous seams, so one store serves both.

use alloc::sync::Arc;
use alloc::vec::Vec;

use alloy_primitives::B256;
use nectar_primitives::marker::{MaybeSend, MaybeSync};
use nectar_primitives::store::{ChunkGet, ChunkGetSync, ChunkPut};
use nectar_primitives::{AnyChunkSet, Chunk, ChunkAddress, DEFAULT_BODY_SIZE, Verified};

use crate::{BatchId, Stamp, StampedChunk, Unvalidated, ValidationState};

/// The key of a stamped-store entry.
///
/// One address holds many entries, one per batch and stamp version
/// that covers it, so the stamp's facts are half the primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StoreKey {
    /// The content address of the stored chunk.
    address: ChunkAddress,
    /// The batch the stored stamp belongs to.
    batch: BatchId,
    /// The hash of the stored stamp's wire bytes, [`Stamp::hash`].
    stamp_hash: B256,
}

impl StoreKey {
    /// Create a key from its three parts.
    #[inline]
    #[must_use]
    pub const fn new(address: ChunkAddress, batch: BatchId, stamp_hash: B256) -> Self {
        Self {
            address,
            batch,
            stamp_hash,
        }
    }

    /// Create the key a stamped unit stores under.
    #[inline]
    #[must_use]
    pub fn for_chunk(address: ChunkAddress, stamp: &Stamp) -> Self {
        Self::new(address, stamp.batch(), stamp.hash())
    }

    /// The chunk content address.
    #[inline]
    #[must_use]
    pub const fn address(&self) -> ChunkAddress {
        self.address
    }

    /// The batch.
    #[inline]
    #[must_use]
    pub const fn batch(&self) -> BatchId {
        self.batch
    }

    /// The stamp hash.
    #[inline]
    #[must_use]
    pub const fn stamp_hash(&self) -> B256 {
        self.stamp_hash
    }
}

/// The synchronous stamp-keyed local chunk store.
///
/// This is the seam a node's reserve implements: entry storage keyed on
/// the chunk address, the batch, and the stamp hash. The store is storage,
/// not certification: the caller admits the stamp inside its own write
/// transaction and stores what it admitted.
///
/// Reads come in three forms. The body read inherited from the retrieval
/// seam names no stamp at all. The lookup reads name the whole key,
/// of a pull consumer that already holds a descriptor. The address-only
/// stamped read resolves one binding of several: the first in (batch,
/// stamp index) key order. The body is content addressed, so every covering
/// stamp names the same data; key order picks a representative without
/// comparing stamps. A miss answers a classified absence either way, never
/// an `Option`.
///
/// The put takes only a stamped unit; a stampless put is a type error:
///
/// ```compile_fail
/// use nectar_primitives::store::ChunkStoreError;
/// use nectar_primitives::{Chunk, Verified};
///
/// fn stampless_put(
///     store: &dyn nectar_postage_primitives::ChunkStore<Error = ChunkStoreError>,
///     chunk: &Chunk<Verified>,
/// ) {
///     store.put(chunk);
/// }
/// ```
pub trait ChunkStore<const B: usize = DEFAULT_BODY_SIZE>:
    MaybeSend + MaybeSync + ChunkGetSync<AnyChunkSet<B>, Trust = Verified>
{
    /// Store a stamped chunk under the key its own facts name.
    ///
    /// Storing again under a key replaces the entry. A store may turn down
    /// a re-stamp that is not strictly newer than the stored binding; the
    /// classification is the store's own error type.
    fn put(&self, unit: &StampedChunk<Verified, Unvalidated, B>) -> Result<(), Self::Error>;

    /// Read the entry stored under the exact key.
    fn lookup(&self, key: &StoreKey)
    -> Result<StampedChunk<Verified, Unvalidated, B>, Self::Error>;

    /// Read the stamped entry at an address alone.
    ///
    /// Several stamps may cover the address. The binding first in
    /// (batch, stamp index) key order is the one returned; the returned
    /// stamp hands back the batch and stamp hash the exact key wants.
    fn at(
        &self,
        address: &ChunkAddress,
    ) -> Result<StampedChunk<Verified, Unvalidated, B>, Self::Error>;

    /// Whether the entry is stored under the exact key.
    ///
    /// An absence answers `Ok(false)`; only a medium failure is an error.
    fn contains(&self, key: &StoreKey) -> Result<bool, Self::Error>;

    /// The keys of every entry stamped under the batch.
    ///
    /// Grouping is a batch verb. A store never exposes a transaction.
    fn group(&self, batch: &BatchId) -> Result<Vec<StoreKey>, Self::Error>;
}

impl<const B: usize, T: ChunkStore<B> + ?Sized> ChunkStore<B> for &T {
    fn put(&self, unit: &StampedChunk<Verified, Unvalidated, B>) -> Result<(), Self::Error> {
        (**self).put(unit)
    }

    fn lookup(
        &self,
        key: &StoreKey,
    ) -> Result<StampedChunk<Verified, Unvalidated, B>, Self::Error> {
        <T as ChunkStore<B>>::lookup(&**self, key)
    }

    fn at(
        &self,
        address: &ChunkAddress,
    ) -> Result<StampedChunk<Verified, Unvalidated, B>, Self::Error> {
        (**self).at(address)
    }

    fn contains(&self, key: &StoreKey) -> Result<bool, Self::Error> {
        (**self).contains(key)
    }

    fn group(&self, batch: &BatchId) -> Result<Vec<StoreKey>, Self::Error> {
        (**self).group(batch)
    }
}

impl<const B: usize, T: ChunkStore<B> + ?Sized> ChunkStore<B> for Arc<T> {
    fn put(&self, unit: &StampedChunk<Verified, Unvalidated, B>) -> Result<(), Self::Error> {
        (**self).put(unit)
    }

    fn lookup(
        &self,
        key: &StoreKey,
    ) -> Result<StampedChunk<Verified, Unvalidated, B>, Self::Error> {
        <T as ChunkStore<B>>::lookup(&**self, key)
    }

    fn at(
        &self,
        address: &ChunkAddress,
    ) -> Result<StampedChunk<Verified, Unvalidated, B>, Self::Error> {
        (**self).at(address)
    }

    fn contains(&self, key: &StoreKey) -> Result<bool, Self::Error> {
        (**self).contains(key)
    }

    fn group(&self, batch: &BatchId) -> Result<Vec<StoreKey>, Self::Error> {
        (**self).group(batch)
    }
}

/// The adapter that lifts the synchronous seams onto the asynchronous
/// seams.
///
/// The get leg bounds on the stampless retrieval seam alone and the put
/// leg on the stamp-keyed store, so a cache that answers bodies lifts for
/// `ChunkGet` and a full store lifts for both. The `&T` and `Arc<T>`
/// blanket impls make the adapter usable behind a pointer.
#[derive(Debug, Clone)]
pub struct Lifted<S> {
    store: S,
}

impl<S> Lifted<S> {
    /// Wrap a store.
    #[inline]
    #[must_use]
    pub const fn new(store: S) -> Self {
        Self { store }
    }

    /// The wrapped store.
    #[inline]
    #[must_use]
    pub const fn store(&self) -> &S {
        &self.store
    }

    /// Unwrap into the store.
    #[inline]
    #[must_use]
    pub fn into_store(self) -> S {
        self.store
    }
}

impl<S: ChunkGetSync<AnyChunkSet<DEFAULT_BODY_SIZE>>> ChunkGet<AnyChunkSet<DEFAULT_BODY_SIZE>>
    for Lifted<S>
{
    type Trust = S::Trust;
    type Error = S::Error;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<Self::Trust, AnyChunkSet<DEFAULT_BODY_SIZE>>, Self::Error> {
        self.store.get(address)
    }
}

impl<V: ValidationState, S: ChunkStore> ChunkPut<StampedChunk<Verified, V, DEFAULT_BODY_SIZE>>
    for Lifted<S>
{
    type Error = S::Error;

    async fn put(
        &self,
        unit: StampedChunk<Verified, V, DEFAULT_BODY_SIZE>,
    ) -> Result<(), Self::Error> {
        let (chunk, stamp) = unit.into_parts();
        self.store.put(&StampedChunk::new(chunk, stamp))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, RwLock};

    use alloy_primitives::{Signature, keccak256};
    use arbitrary::Unstructured;
    use nectar_primitives::store::ChunkStoreError;
    use nectar_primitives::{ChunkOps, ContentChunk};
    use nectar_testing::run;

    use super::*;
    use crate::{StampIndex, Validated, generators};

    type Entry = (Chunk<Verified, AnyChunkSet>, Stamp);

    /// In-memory double: a map from exact keys to (chunk, stamp) pairs,
    /// serving the stampless body read and the stamped verbs.
    #[derive(Default)]
    struct MemStore {
        entries: RwLock<BTreeMap<StoreKey, Entry>>,
    }

    impl MemStore {
        fn new() -> Self {
            Self::default()
        }
    }

    impl ChunkGetSync<AnyChunkSet> for MemStore {
        type Trust = Verified;
        type Error = ChunkStoreError;

        fn get(&self, address: &ChunkAddress) -> Result<Chunk<Verified, AnyChunkSet>, Self::Error> {
            let entries = self.entries.read().unwrap();
            entries
                .iter()
                .find(|(key, _)| key.address() == *address)
                .map(|(_, (chunk, _))| chunk.clone())
                .ok_or_else(|| ChunkStoreError::not_found(address))
        }
    }

    impl ChunkStore for MemStore {
        fn put(&self, unit: &StampedChunk<Verified, Unvalidated>) -> Result<(), Self::Error> {
            let key = StoreKey::for_chunk(*unit.address(), unit.stamp());
            let (chunk, stamp) = unit.clone().into_parts();
            self.entries.write().unwrap().insert(key, (chunk, stamp));
            Ok(())
        }

        fn lookup(
            &self,
            key: &StoreKey,
        ) -> Result<StampedChunk<Verified, Unvalidated>, Self::Error> {
            let entries = self.entries.read().unwrap();
            entries
                .get(key)
                .map(|(chunk, stamp)| StampedChunk::new(chunk.clone(), stamp.clone()))
                .ok_or_else(|| ChunkStoreError::not_found(&key.address()))
        }

        fn at(
            &self,
            address: &ChunkAddress,
        ) -> Result<StampedChunk<Verified, Unvalidated>, Self::Error> {
            let entries = self.entries.read().unwrap();
            let first = entries
                .iter()
                .filter(|(key, _)| key.address() == *address)
                .min_by_key(|(_, (_, stamp))| (stamp.batch(), stamp.stamp_index().to_be_bytes()));
            match first {
                Some((_, (chunk, stamp))) => Ok(StampedChunk::new(chunk.clone(), stamp.clone())),
                None => Err(ChunkStoreError::not_found(address)),
            }
        }

        fn contains(&self, key: &StoreKey) -> Result<bool, Self::Error> {
            Ok(self.entries.read().unwrap().contains_key(key))
        }

        fn group(&self, batch: &BatchId) -> Result<Vec<StoreKey>, Self::Error> {
            let entries = self.entries.read().unwrap();
            Ok(entries
                .keys()
                .filter(|key| key.batch() == *batch)
                .copied()
                .collect())
        }
    }

    /// In-memory double with only the retrieval seam: a body cache a `ChunkStore` is not.
    #[derive(Default)]
    struct CacheStore {
        bodies: RwLock<BTreeMap<ChunkAddress, Chunk<Verified, AnyChunkSet>>>,
    }

    impl ChunkGetSync<AnyChunkSet> for CacheStore {
        type Trust = Verified;
        type Error = ChunkStoreError;

        fn get(&self, address: &ChunkAddress) -> Result<Chunk<Verified, AnyChunkSet>, Self::Error> {
            self.bodies
                .read()
                .unwrap()
                .get(address)
                .cloned()
                .ok_or_else(|| ChunkStoreError::not_found(address))
        }
    }

    fn content_chunk() -> Chunk<Verified, AnyChunkSet> {
        Chunk::from_envelope(ContentChunk::new(&b"stamped store"[..]).unwrap().into()).unwrap()
    }

    fn coherently_stamped(
        chunk: Chunk<Verified, AnyChunkSet>,
    ) -> StampedChunk<Verified, Unvalidated> {
        let mut u = Unstructured::new(&[7u8; 128]);
        let (_, stamp) = generators::batch_and_stamp(&mut u, chunk.address()).unwrap();
        StampedChunk::new(chunk, stamp)
    }

    fn coherently_validated(
        chunk: Chunk<Verified, AnyChunkSet>,
    ) -> StampedChunk<Verified, Validated> {
        let mut u = Unstructured::new(&[7u8; 128]);
        let (batch, stamp) = generators::batch_and_stamp(&mut u, chunk.address()).unwrap();
        StampedChunk::new(chunk, stamp).validate(&batch).unwrap()
    }

    #[test]
    fn put_lookup_contains_round_trips_the_exact_key() {
        let store = MemStore::new();
        let unit = coherently_stamped(content_chunk());
        let key = StoreKey::for_chunk(*unit.address(), unit.stamp());

        store.put(&unit).unwrap();
        assert_eq!(store.lookup(&key).unwrap(), unit);
        assert!(store.contains(&key).unwrap());
    }

    #[test]
    fn a_miss_answers_the_classified_absence() {
        let store = MemStore::new();
        let unit = coherently_stamped(content_chunk());
        let key = StoreKey::for_chunk(*unit.address(), unit.stamp());

        let absent = store.lookup(&key).unwrap_err();
        assert!(absent.is_definitely_absent());
        assert!(!absent.is_transient());
        assert!(!store.contains(&key).unwrap());
        let absent = store.at(unit.address()).unwrap_err();
        assert!(absent.is_definitely_absent());
        assert!(
            store
                .get(unit.address())
                .unwrap_err()
                .is_definitely_absent()
        );
    }

    #[test]
    fn the_body_read_is_stampless_and_the_exact_read_round_trips() {
        let store = MemStore::new();
        let unit = coherently_stamped(content_chunk());
        let key = StoreKey::for_chunk(*unit.address(), unit.stamp());

        store.put(&unit).unwrap();
        let body = store.get(unit.address()).unwrap();
        let stamped = store.lookup(&key).unwrap();
        assert_eq!(stamped, unit);
        assert_eq!(body.envelope().data(), unit.chunk().envelope().data());
    }

    #[test]
    fn at_picks_the_first_batch_in_key_order() {
        let store = MemStore::new();
        let chunk = content_chunk();
        let address = *chunk.address();
        let sig = Signature::from_raw_array(&[0u8; 65]).unwrap();

        // The lexicographically smaller batch wins even though the
        // larger batch's binding holds the smaller index.
        let in_small_batch =
            Stamp::with_index(BatchId::new([0x22u8; 32]), StampIndex::new(5, 5), 1, sig);
        let in_large_batch =
            Stamp::with_index(BatchId::new([0xffu8; 32]), StampIndex::new(0, 0), 2, sig);
        let first = StampedChunk::new(chunk.clone(), in_large_batch);
        let second = StampedChunk::new(chunk, in_small_batch.clone());
        store.put(&first).unwrap();
        store.put(&second).unwrap();

        let resolved = store.at(&address).unwrap();
        assert_eq!(*resolved.stamp(), in_small_batch);
        let key = StoreKey::for_chunk(address, resolved.stamp());
        assert_eq!(store.lookup(&key).unwrap(), second);
    }

    #[test]
    fn at_picks_the_first_index_in_key_order_within_a_batch() {
        let store = MemStore::new();
        let chunk = content_chunk();
        let address = *chunk.address();
        let sig = Signature::from_raw_array(&[0u8; 65]).unwrap();

        // Same batch, so the 8-byte index order decides: (0, 9) before (1, 1).
        let earlier = Stamp::with_index(BatchId::ZERO, StampIndex::new(0, 9), 100, sig);
        let later = Stamp::with_index(BatchId::ZERO, StampIndex::new(1, 1), 500, sig);
        let first = StampedChunk::new(chunk.clone(), earlier.clone());
        let second = StampedChunk::new(chunk, later);
        store.put(&first).unwrap();
        store.put(&second).unwrap();

        let resolved = store.at(&address).unwrap();
        assert_eq!(*resolved.stamp(), earlier);
        assert_eq!(
            StoreKey::for_chunk(address, resolved.stamp()),
            StoreKey::for_chunk(address, &earlier)
        );
    }

    #[test]
    fn group_lists_the_batchs_keys() {
        let store = MemStore::new();
        let unit = coherently_stamped(content_chunk());
        let batch = unit.stamp().batch();

        store.put(&unit).unwrap();
        let keys = store.group(&batch).unwrap();
        assert_eq!(keys, [StoreKey::for_chunk(*unit.address(), unit.stamp())]);
        assert!(store.group(&BatchId::ZERO).unwrap().is_empty());
    }

    fn object_safe(_store: &dyn ChunkStore<Error = ChunkStoreError>) {}

    fn object_safe_retrieval(
        _store: &dyn ChunkGetSync<AnyChunkSet, Trust = Verified, Error = ChunkStoreError>,
    ) {
    }

    #[test]
    fn the_seams_stay_object_safe() {
        let store = MemStore::new();
        object_safe(&store);
        object_safe_retrieval(&store);
    }

    #[test]
    fn a_zero_fielded_stamp_hashes_its_wire_bytes() {
        let sig = Signature::from_raw_array(&[0u8; 65]).unwrap();
        let stamp = Stamp::with_index(BatchId::ZERO, StampIndex::new(0, 0), 0, sig);

        assert_eq!(stamp.hash(), keccak256(stamp.to_bytes()));
        assert_ne!(
            stamp.hash(),
            Stamp::with_index(BatchId::ZERO, StampIndex::new(0, 0), 1, sig).hash()
        );
    }

    #[test]
    fn lifted_serves_the_async_seams() {
        run(async {
            let store = MemStore::new();
            let lifted = Lifted::new(&store);
            let sealed = coherently_validated(content_chunk());
            let address = *sealed.address();

            lifted.put(sealed).await.unwrap();

            let chunk = lifted.get(&address).await.unwrap();
            assert_eq!(*chunk.address(), address);
        });
    }

    #[test]
    fn the_lifted_get_leg_bounds_only_on_retrieval() {
        run(async {
            let cache = CacheStore::default();
            let chunk = content_chunk();
            cache
                .bodies
                .write()
                .unwrap()
                .insert(*chunk.address(), chunk.clone());
            let lifted = Lifted::new(&cache);

            let read = lifted.get(chunk.address()).await.unwrap();
            assert_eq!(*read.address(), *chunk.address());
        });
    }

    #[test]
    fn lifted_answers_the_classified_absence() {
        run(async {
            let store = MemStore::new();
            let lifted = Lifted::new(&store);
            let address: ChunkAddress = [0u8; 32].into();

            let absent = lifted.get(&address).await.unwrap_err();
            assert!(absent.is_definitely_absent());
            assert!(!absent.is_transient());
        });
    }

    #[test]
    fn lifted_accepts_an_unvalidated_pair() {
        run(async {
            let store = MemStore::new();
            let lifted = Lifted::new(&store);
            let unit = coherently_stamped(content_chunk());
            let address = *unit.address();

            lifted.put(unit).await.unwrap();
            let chunk = lifted.get(&address).await.unwrap();
            assert_eq!(*chunk.address(), address);
        });
    }

    #[test]
    fn lifted_delegates_through_a_pointer() {
        run(async {
            let dyn_store: Arc<dyn ChunkStore<Error = ChunkStoreError>> = Arc::new(MemStore::new());
            let lifted = Lifted::new(Arc::clone(&dyn_store));
            let unit = coherently_stamped(content_chunk());
            let address = *unit.address();

            lifted.put(unit.clone()).await.unwrap();
            let chunk = lifted.get(&address).await.unwrap();
            assert_eq!(*chunk.address(), address);
            assert!(
                dyn_store
                    .contains(&StoreKey::for_chunk(address, unit.stamp()))
                    .unwrap()
            );
        });
    }
}

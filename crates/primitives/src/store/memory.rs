//! In-memory chunk storage.

use alloc::collections::BTreeMap;

#[cfg(feature = "std")]
use parking_lot::RwLock;

use crate::chunk::{Chunk, ChunkAddress, ChunkRegistry, StandardChunkSet, Verified};

use super::ChunkStoreError;
use super::typed::{ChunkGet, ChunkHas, ChunkPut};

/// Single-threaded stand-in for `RwLock` on the `no_std` side: guests execute
/// single-threaded, so a `RefCell` provides the same interior mutability.
#[cfg(not(feature = "std"))]
#[derive(Debug)]
struct RwLock<T>(core::cell::RefCell<T>);

#[cfg(not(feature = "std"))]
impl<T> RwLock<T> {
    const fn new(value: T) -> Self {
        Self(core::cell::RefCell::new(value))
    }

    fn read(&self) -> core::cell::Ref<'_, T> {
        self.0.borrow()
    }

    fn write(&self) -> core::cell::RefMut<'_, T> {
        self.0.borrow_mut()
    }

    fn into_inner(self) -> T {
        self.0.into_inner()
    }
}

/// In-memory chunk storage over an address-keyed map.
///
/// Holds only sealed chunks and is process-private, so reads are `Verified`:
/// nothing can alter a chunk between put and get.
///
/// Uses interior mutability so `ChunkPut::put(&self)` works without
/// external synchronization: `parking_lot::RwLock` under `std`, an unsync
/// cell on the single-threaded `no_std` side.
#[derive(Debug)]
pub struct MemoryStore<R: ChunkRegistry = StandardChunkSet> {
    chunks: RwLock<BTreeMap<ChunkAddress, Chunk<Verified, R>>>,
}

impl<R: ChunkRegistry> Clone for MemoryStore<R> {
    fn clone(&self) -> Self {
        Self {
            chunks: RwLock::new(self.chunks.read().clone()),
        }
    }
}

impl<R: ChunkRegistry> Default for MemoryStore<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R: ChunkRegistry> MemoryStore<R> {
    /// Create an empty memory store.
    pub const fn new() -> Self {
        Self {
            chunks: RwLock::new(BTreeMap::new()),
        }
    }

    /// Build a store from a collection of sealed chunks, keyed by address.
    pub fn from_chunks(chunks: impl IntoIterator<Item = Chunk<Verified, R>>) -> Self {
        Self {
            chunks: RwLock::new(chunks.into_iter().map(|c| (*c.address(), c)).collect()),
        }
    }

    /// Get a cloned chunk by address.
    pub fn get(&self, address: &ChunkAddress) -> Option<Chunk<Verified, R>> {
        self.chunks.read().get(address).cloned()
    }

    /// Number of stored chunks.
    pub fn len(&self) -> usize {
        self.chunks.read().len()
    }

    /// Whether the store is empty.
    pub fn is_empty(&self) -> bool {
        self.chunks.read().is_empty()
    }

    /// Consume the store and return all chunks.
    pub fn into_chunks(self) -> BTreeMap<ChunkAddress, Chunk<Verified, R>> {
        self.chunks.into_inner()
    }
}

impl<R: ChunkRegistry> ChunkPut<R> for MemoryStore<R> {
    type Error = core::convert::Infallible;

    async fn put(&self, chunk: Chunk<Verified, R>) -> Result<(), Self::Error> {
        self.chunks.write().insert(*chunk.address(), chunk);
        Ok(())
    }
}

impl<R: ChunkRegistry> ChunkGet<R> for MemoryStore<R> {
    type Trust = Verified;
    type Error = ChunkStoreError;

    async fn get(&self, address: &ChunkAddress) -> Result<Chunk<Verified, R>, Self::Error> {
        self.chunks
            .read()
            .get(address)
            .cloned()
            .ok_or_else(|| ChunkStoreError::not_found(address))
    }
}

impl<R: ChunkRegistry> ChunkHas for MemoryStore<R> {
    async fn has(&self, address: &ChunkAddress) -> bool {
        self.chunks.read().contains_key(address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::{ChunkOps, ContentChunk};
    use nectar_testing::run;

    #[test]
    fn test_memory_store() {
        let store = MemoryStore::<StandardChunkSet>::new();
        assert!(store.is_empty());

        let chunk = ContentChunk::new(b"hello".as_slice()).unwrap();
        let addr = *chunk.address();
        let sealed: Chunk = Chunk::from_envelope(chunk.into()).unwrap();

        run(ChunkPut::put(&store, sealed)).unwrap();
        assert_eq!(store.len(), 1);
        assert!(run(ChunkHas::has(&store, &addr)));
        assert_eq!(store.get(&addr).map(|c| *c.address()), Some(addr));
    }
}

//! In-memory chunk storage.

use std::collections::HashMap;

use parking_lot::RwLock;

use crate::chunk::{Chunk, ChunkAddress, ChunkRegistry, StandardChunkSet, Verified};

use super::ChunkStoreError;
use super::typed::{ChunkGet, ChunkHas, ChunkPut};

/// In-memory chunk storage using a `RwLock<HashMap>`.
///
/// Holds only sealed chunks and is process-private, so reads are `Verified`:
/// nothing can alter a chunk between put and get.
///
/// Uses interior mutability so `ChunkPut::put(&self)` works without
/// external synchronization.
#[derive(Debug)]
pub struct MemoryStore<R: ChunkRegistry = StandardChunkSet> {
    chunks: RwLock<HashMap<ChunkAddress, Chunk<Verified, R>>>,
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
    pub fn new() -> Self {
        Self {
            chunks: RwLock::new(HashMap::new()),
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
    pub fn into_chunks(self) -> HashMap<ChunkAddress, Chunk<Verified, R>> {
        self.chunks.into_inner()
    }
}

impl<R: ChunkRegistry> ChunkPut<R> for MemoryStore<R> {
    type Error = std::convert::Infallible;

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

impl<R: ChunkRegistry> ChunkGet<R> for HashMap<ChunkAddress, Chunk<Verified, R>> {
    type Trust = Verified;
    type Error = ChunkStoreError;

    async fn get(&self, address: &ChunkAddress) -> Result<Chunk<Verified, R>, Self::Error> {
        self.get(address)
            .cloned()
            .ok_or_else(|| ChunkStoreError::not_found(address))
    }
}

impl<R: ChunkRegistry> ChunkHas for HashMap<ChunkAddress, Chunk<Verified, R>> {
    async fn has(&self, address: &ChunkAddress) -> bool {
        self.contains_key(address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::{ChunkOps, ContentChunk};
    use nectar_testing::run;

    #[test]
    fn test_memory_store() {
        run(async {
            let store = MemoryStore::<StandardChunkSet>::new();
            assert!(store.is_empty());

            let chunk = ContentChunk::new(b"hello".as_slice()).unwrap();
            let addr = *chunk.address();
            let sealed: Chunk = Chunk::from_envelope(chunk.into()).unwrap();

            ChunkPut::put(&store, sealed).await.unwrap();
            assert_eq!(store.len(), 1);
            assert!(ChunkHas::has(&store, &addr).await);
            assert_eq!(store.get(&addr).map(|c| *c.address()), Some(addr));
        })
    }
}

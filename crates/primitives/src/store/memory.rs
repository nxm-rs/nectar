//! In-memory chunk storage.

use std::collections::HashMap;

use parking_lot::RwLock;

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::{AnyChunk, ChunkAddress};

use super::ChunkStoreError;
use super::typed::{SyncChunkGet, SyncChunkHas, SyncChunkPut};

/// In-memory chunk storage using a `RwLock<HashMap>`.
///
/// Uses interior mutability so `SyncChunkPut::put(&self)` works without
/// external synchronization.
#[derive(Debug)]
pub struct MemoryStore<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    chunks: RwLock<HashMap<ChunkAddress, AnyChunk<BODY_SIZE>>>,
}

impl<const BODY_SIZE: usize> Clone for MemoryStore<BODY_SIZE> {
    fn clone(&self) -> Self {
        Self {
            chunks: RwLock::new(self.chunks.read().clone()),
        }
    }
}

impl<const BODY_SIZE: usize> Default for MemoryStore<BODY_SIZE> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const BODY_SIZE: usize> MemoryStore<BODY_SIZE> {
    /// Create an empty memory store.
    pub fn new() -> Self {
        Self {
            chunks: RwLock::new(HashMap::new()),
        }
    }

    /// Get a cloned chunk by address.
    pub fn get(&self, address: &ChunkAddress) -> Option<AnyChunk<BODY_SIZE>> {
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
    pub fn into_chunks(self) -> HashMap<ChunkAddress, AnyChunk<BODY_SIZE>> {
        self.chunks.into_inner()
    }
}

impl<const BODY_SIZE: usize> SyncChunkPut<BODY_SIZE> for MemoryStore<BODY_SIZE> {
    type Error = std::convert::Infallible;

    fn put(&self, chunk: AnyChunk<BODY_SIZE>) -> Result<(), Self::Error> {
        self.chunks.write().insert(*chunk.address(), chunk);
        Ok(())
    }
}

impl<const BODY_SIZE: usize> SyncChunkGet<BODY_SIZE> for MemoryStore<BODY_SIZE> {
    type Error = ChunkStoreError;

    fn get(&self, address: &ChunkAddress) -> Result<AnyChunk<BODY_SIZE>, Self::Error> {
        self.chunks
            .read()
            .get(address)
            .cloned()
            .ok_or_else(|| ChunkStoreError::not_found(address))
    }

    fn is_not_found(&self, error: &Self::Error) -> bool {
        error.is_not_found()
    }
}

impl<const BODY_SIZE: usize> SyncChunkHas<BODY_SIZE> for MemoryStore<BODY_SIZE> {
    fn has(&self, address: &ChunkAddress) -> bool {
        self.chunks.read().contains_key(address)
    }
}

impl<const BODY_SIZE: usize> SyncChunkGet<BODY_SIZE>
    for HashMap<ChunkAddress, AnyChunk<BODY_SIZE>>
{
    type Error = ChunkStoreError;

    fn get(&self, address: &ChunkAddress) -> Result<AnyChunk<BODY_SIZE>, Self::Error> {
        self.get(address)
            .cloned()
            .ok_or_else(|| ChunkStoreError::not_found(address))
    }

    fn is_not_found(&self, error: &Self::Error) -> bool {
        error.is_not_found()
    }
}

impl<const BODY_SIZE: usize> SyncChunkHas<BODY_SIZE>
    for HashMap<ChunkAddress, AnyChunk<BODY_SIZE>>
{
    fn has(&self, address: &ChunkAddress) -> bool {
        self.contains_key(address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::{Chunk, ContentChunk};

    #[test]
    fn test_memory_store() {
        let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
        assert!(store.is_empty());

        let chunk = ContentChunk::new(b"hello".as_slice()).unwrap();
        let addr = *chunk.address();
        let any: AnyChunk = chunk.into();

        SyncChunkPut::put(&store, any.clone()).unwrap();
        assert_eq!(store.len(), 1);
        assert!(SyncChunkHas::has(&store, &addr));
        assert_eq!(store.get(&addr), Some(any));
    }
}

//! In-memory chunk storage.

use std::collections::HashMap;

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::{AnyChunk, ChunkAddress};

use super::ChunkStoreError;
use super::typed::{ChunkGet, ChunkHas, ChunkPut};

/// In-memory chunk storage using a HashMap.
#[derive(Debug, Clone)]
pub struct MemoryStore<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    chunks: HashMap<ChunkAddress, AnyChunk<BODY_SIZE>>,
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
            chunks: HashMap::new(),
        }
    }

    /// Get a chunk by address.
    pub fn get(&self, address: &ChunkAddress) -> Option<&AnyChunk<BODY_SIZE>> {
        self.chunks.get(address)
    }

    /// Number of stored chunks.
    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    /// Whether the store is empty.
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Access all stored chunks.
    pub const fn chunks(&self) -> &HashMap<ChunkAddress, AnyChunk<BODY_SIZE>> {
        &self.chunks
    }

    /// Consume the store and return all chunks.
    pub fn into_chunks(self) -> HashMap<ChunkAddress, AnyChunk<BODY_SIZE>> {
        self.chunks
    }
}

impl<const BODY_SIZE: usize> ChunkPut<BODY_SIZE> for MemoryStore<BODY_SIZE> {
    type Error = std::convert::Infallible;

    fn put(&mut self, chunk: AnyChunk<BODY_SIZE>) -> Result<(), Self::Error> {
        self.chunks.insert(*chunk.address(), chunk);
        Ok(())
    }
}

impl<const BODY_SIZE: usize> ChunkGet<BODY_SIZE> for MemoryStore<BODY_SIZE> {
    type Error = ChunkStoreError;

    fn get(&self, address: &ChunkAddress) -> Result<AnyChunk<BODY_SIZE>, Self::Error> {
        self.chunks
            .get(address)
            .cloned()
            .ok_or_else(|| ChunkStoreError::not_found(address))
    }
}

impl<const BODY_SIZE: usize> ChunkHas<BODY_SIZE> for MemoryStore<BODY_SIZE> {
    fn has(&self, address: &ChunkAddress) -> bool {
        self.chunks.contains_key(address)
    }
}

impl<const BODY_SIZE: usize> ChunkGet<BODY_SIZE> for HashMap<ChunkAddress, AnyChunk<BODY_SIZE>> {
    type Error = ChunkStoreError;

    fn get(&self, address: &ChunkAddress) -> Result<AnyChunk<BODY_SIZE>, Self::Error> {
        self.get(address)
            .cloned()
            .ok_or_else(|| ChunkStoreError::not_found(address))
    }
}

impl<const BODY_SIZE: usize> ChunkHas<BODY_SIZE> for HashMap<ChunkAddress, AnyChunk<BODY_SIZE>> {
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
        let mut store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
        assert!(store.is_empty());

        let chunk = ContentChunk::new(b"hello".as_slice()).unwrap();
        let addr = *chunk.address();
        let any: AnyChunk = chunk.into();

        ChunkPut::put(&mut store, any.clone()).unwrap();
        assert_eq!(store.len(), 1);
        assert!(ChunkHas::has(&store, &addr));
        assert_eq!(store.get(&addr), Some(&any));
    }
}

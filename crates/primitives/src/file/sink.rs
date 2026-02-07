//! Chunk storage implementations.

use std::collections::HashMap;

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::{Chunk, ChunkAddress, ContentChunk};

use super::traits::{ChunkGet, ChunkHas, ChunkPut};
use super::error::FileError;

/// In-memory chunk storage using a HashMap.
#[derive(Debug, Clone)]
pub struct MemorySink<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    chunks: HashMap<ChunkAddress, ContentChunk<BODY_SIZE>>,
}

impl<const BODY_SIZE: usize> Default for MemorySink<BODY_SIZE> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const BODY_SIZE: usize> MemorySink<BODY_SIZE> {
    /// Create an empty memory sink.
    pub fn new() -> Self {
        Self {
            chunks: HashMap::new(),
        }
    }

    /// Get a chunk by address.
    pub fn get(&self, address: &ChunkAddress) -> Option<&ContentChunk<BODY_SIZE>> {
        self.chunks.get(address)
    }

    /// Number of stored chunks.
    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    /// Whether the sink is empty.
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Access all stored chunks.
    pub fn chunks(&self) -> &HashMap<ChunkAddress, ContentChunk<BODY_SIZE>> {
        &self.chunks
    }

    /// Consume the sink and return all chunks.
    pub fn into_chunks(self) -> HashMap<ChunkAddress, ContentChunk<BODY_SIZE>> {
        self.chunks
    }
}

impl<const BODY_SIZE: usize> ChunkPut<BODY_SIZE> for MemorySink<BODY_SIZE> {
    type Error = std::convert::Infallible;

    fn put(&mut self, chunk: ContentChunk<BODY_SIZE>) -> Result<(), Self::Error> {
        self.chunks.insert(*chunk.address(), chunk);
        Ok(())
    }
}

impl<const BODY_SIZE: usize> ChunkGet<BODY_SIZE> for MemorySink<BODY_SIZE> {
    type Error = FileError;

    fn get(&self, address: &ChunkAddress) -> Result<ContentChunk<BODY_SIZE>, Self::Error> {
        self.chunks
            .get(address)
            .cloned()
            .ok_or_else(|| FileError::ChunkNotFound(*address))
    }
}

impl<const BODY_SIZE: usize> ChunkGet<BODY_SIZE> for &MemorySink<BODY_SIZE> {
    type Error = FileError;

    fn get(&self, address: &ChunkAddress) -> Result<ContentChunk<BODY_SIZE>, Self::Error> {
        self.chunks
            .get(address)
            .cloned()
            .ok_or_else(|| FileError::ChunkNotFound(*address))
    }
}

impl<const BODY_SIZE: usize> ChunkHas<BODY_SIZE> for MemorySink<BODY_SIZE> {
    fn has(&self, address: &ChunkAddress) -> bool {
        self.chunks.contains_key(address)
    }
}

/// Chunk storage that collects chunks into a Vec in insertion order.
#[derive(Debug, Clone)]
pub struct VecSink<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    chunks: Vec<ContentChunk<BODY_SIZE>>,
}

impl<const BODY_SIZE: usize> Default for VecSink<BODY_SIZE> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const BODY_SIZE: usize> VecSink<BODY_SIZE> {
    /// Create an empty vec sink.
    pub fn new() -> Self {
        Self { chunks: Vec::new() }
    }

    /// Create a vec sink with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            chunks: Vec::with_capacity(capacity),
        }
    }

    /// Number of stored chunks.
    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    /// Whether the sink is empty.
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Access stored chunks in insertion order.
    pub fn chunks(&self) -> &[ContentChunk<BODY_SIZE>] {
        &self.chunks
    }

    /// Consume the sink and return all chunks.
    pub fn into_chunks(self) -> Vec<ContentChunk<BODY_SIZE>> {
        self.chunks
    }
}

impl<const BODY_SIZE: usize> ChunkPut<BODY_SIZE> for VecSink<BODY_SIZE> {
    type Error = std::convert::Infallible;

    fn put(&mut self, chunk: ContentChunk<BODY_SIZE>) -> Result<(), Self::Error> {
        self.chunks.push(chunk);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_sink() {
        let mut sink = MemorySink::<DEFAULT_BODY_SIZE>::new();
        assert!(sink.is_empty());

        let chunk = ContentChunk::new(b"hello".as_slice()).unwrap();
        let addr = *chunk.address();

        ChunkPut::put(&mut sink, chunk.clone()).unwrap();
        assert_eq!(sink.len(), 1);
        assert!(ChunkHas::has(&sink, &addr));
        assert_eq!(sink.get(&addr), Some(&chunk));
    }

    #[test]
    fn test_vec_sink() {
        let mut sink = VecSink::<DEFAULT_BODY_SIZE>::new();
        assert!(sink.is_empty());

        let chunk1 = ContentChunk::new(b"first".as_slice()).unwrap();
        let chunk2 = ContentChunk::new(b"second".as_slice()).unwrap();

        ChunkPut::put(&mut sink, chunk1.clone()).unwrap();
        ChunkPut::put(&mut sink, chunk2.clone()).unwrap();

        assert_eq!(sink.len(), 2);
        assert_eq!(sink.chunks()[0], chunk1);
        assert_eq!(sink.chunks()[1], chunk2);
    }
}

//! Chunk sink trait and implementations for storing produced chunks.

use std::collections::HashMap;

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::{Chunk, ChunkAddress, ContentChunk};

/// Receives chunks produced by the splitter.
pub trait ChunkSink<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    /// Error type for sink operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Store a chunk.
    fn put(&mut self, chunk: ContentChunk<BODY_SIZE>) -> Result<(), Self::Error>;
}

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
    /// Create a new empty memory sink.
    pub fn new() -> Self {
        Self {
            chunks: HashMap::new(),
        }
    }

    /// Get a chunk by address.
    pub fn get(&self, address: &ChunkAddress) -> Option<&ContentChunk<BODY_SIZE>> {
        self.chunks.get(address)
    }

    /// Check if a chunk exists.
    pub fn contains(&self, address: &ChunkAddress) -> bool {
        self.chunks.contains_key(address)
    }

    /// Get the number of stored chunks.
    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    /// Check if the sink is empty.
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Get all stored chunks.
    pub fn chunks(&self) -> &HashMap<ChunkAddress, ContentChunk<BODY_SIZE>> {
        &self.chunks
    }

    /// Consume the sink and return the chunks.
    pub fn into_chunks(self) -> HashMap<ChunkAddress, ContentChunk<BODY_SIZE>> {
        self.chunks
    }
}

impl<const BODY_SIZE: usize> ChunkSink<BODY_SIZE> for MemorySink<BODY_SIZE> {
    type Error = std::convert::Infallible;

    fn put(&mut self, chunk: ContentChunk<BODY_SIZE>) -> Result<(), Self::Error> {
        self.chunks.insert(*chunk.address(), chunk);
        Ok(())
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
    /// Create a new empty vec sink.
    pub fn new() -> Self {
        Self { chunks: Vec::new() }
    }

    /// Create a new vec sink with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            chunks: Vec::with_capacity(capacity),
        }
    }

    /// Get the number of stored chunks.
    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    /// Check if the sink is empty.
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Get all stored chunks.
    pub fn chunks(&self) -> &[ContentChunk<BODY_SIZE>] {
        &self.chunks
    }

    /// Consume the sink and return the chunks.
    pub fn into_chunks(self) -> Vec<ContentChunk<BODY_SIZE>> {
        self.chunks
    }
}

impl<const BODY_SIZE: usize> ChunkSink<BODY_SIZE> for VecSink<BODY_SIZE> {
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

        sink.put(chunk.clone()).unwrap();
        assert_eq!(sink.len(), 1);
        assert!(sink.contains(&addr));
        assert_eq!(sink.get(&addr), Some(&chunk));
    }

    #[test]
    fn test_vec_sink() {
        let mut sink = VecSink::<DEFAULT_BODY_SIZE>::new();
        assert!(sink.is_empty());

        let chunk1 = ContentChunk::new(b"first".as_slice()).unwrap();
        let chunk2 = ContentChunk::new(b"second".as_slice()).unwrap();

        sink.put(chunk1.clone()).unwrap();
        sink.put(chunk2.clone()).unwrap();

        assert_eq!(sink.len(), 2);
        assert_eq!(sink.chunks()[0], chunk1);
        assert_eq!(sink.chunks()[1], chunk2);
    }
}

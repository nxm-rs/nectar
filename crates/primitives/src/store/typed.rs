//! Typed chunk storage traits.

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::{ChunkAddress, ContentChunk};

/// Stores chunks.
pub trait ChunkPut<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    /// Error type for put operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Store a chunk.
    fn put(&mut self, chunk: ContentChunk<BODY_SIZE>) -> Result<(), Self::Error>;
}

/// Retrieves chunks by address.
pub trait ChunkGet<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    /// Error type for get operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Get a chunk by address.
    fn get(&self, address: &ChunkAddress) -> Result<ContentChunk<BODY_SIZE>, Self::Error>;
}

/// Checks chunk existence.
pub trait ChunkHas<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    /// Check if a chunk exists.
    fn has(&self, address: &ChunkAddress) -> bool;
}

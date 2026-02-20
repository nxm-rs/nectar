//! Async typed chunk storage traits.

use std::future::Future;

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::{ChunkAddress, ContentChunk};

/// Async chunk retrieval.
pub trait AsyncChunkGet<const BODY_SIZE: usize = DEFAULT_BODY_SIZE>: Send + Sync {
    /// Error type for get operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Get a chunk by address.
    fn get(
        &self,
        address: &ChunkAddress,
    ) -> impl Future<Output = Result<ContentChunk<BODY_SIZE>, Self::Error>> + Send;
}

/// Async chunk storage.
pub trait AsyncChunkPut<const BODY_SIZE: usize = DEFAULT_BODY_SIZE>: Send + Sync {
    /// Error type for put operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Store a chunk.
    fn put(
        &self,
        chunk: ContentChunk<BODY_SIZE>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

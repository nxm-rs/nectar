//! Async storage traits for chunk operations.

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

/// Async random-access read trait.
pub trait AsyncReadAt: Send + Sync {
    /// Read data at offset into buffer.
    fn read_at(
        &self,
        offset: u64,
        buf: &mut [u8],
    ) -> impl Future<Output = std::io::Result<usize>> + Send;

    /// Total size of the data source.
    fn len(&self) -> u64;

    /// Whether the data source is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

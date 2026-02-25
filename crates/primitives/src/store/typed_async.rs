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

/// Blanket impl: any sync `ChunkGet` type that is `Send + Sync` automatically
/// gets `AsyncChunkGet` by wrapping the sync call. Types needing genuinely async
/// retrieval (e.g. network fetch) should implement `AsyncChunkGet` directly
/// without implementing `ChunkGet`.
impl<T, const BODY_SIZE: usize> AsyncChunkGet<BODY_SIZE> for T
where
    T: super::typed::ChunkGet<BODY_SIZE> + Send + Sync,
{
    type Error = T::Error;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<ContentChunk<BODY_SIZE>, Self::Error> {
        <Self as super::typed::ChunkGet<BODY_SIZE>>::get(self, address)
    }
}

/// Async chunk existence check.
pub trait AsyncChunkHas<const BODY_SIZE: usize = DEFAULT_BODY_SIZE>: Send + Sync {
    /// Check if a chunk exists.
    fn has(
        &self,
        address: &ChunkAddress,
    ) -> impl Future<Output = bool> + Send;
}

/// Blanket impl: any sync `ChunkHas + Send + Sync` gets `AsyncChunkHas`.
impl<T, const BODY_SIZE: usize> AsyncChunkHas<BODY_SIZE> for T
where
    T: super::typed::ChunkHas<BODY_SIZE> + Send + Sync,
{
    async fn has(&self, address: &ChunkAddress) -> bool {
        <Self as super::typed::ChunkHas<BODY_SIZE>>::has(self, address)
    }
}

/// Async chunk storage (takes `&self`, requiring interior mutability).
///
/// Unlike sync [`ChunkPut`](super::typed::ChunkPut) which takes `&mut self`,
/// this trait uses `&self` because async contexts typically need shared ownership.
/// Implementors should use interior mutability (e.g. `Mutex`, `RwLock`).
pub trait AsyncChunkPut<const BODY_SIZE: usize = DEFAULT_BODY_SIZE>: Send + Sync {
    /// Error type for put operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Store a chunk.
    fn put(
        &self,
        chunk: ContentChunk<BODY_SIZE>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Adapter that wraps a sync `ChunkPut` in a `Mutex` to implement `AsyncChunkPut`.
#[derive(Debug)]
pub struct AsyncChunkPutAdapter<T>(std::sync::Mutex<T>);

impl<T> AsyncChunkPutAdapter<T> {
    /// Wrap a sync `ChunkPut` for use in async contexts.
    pub fn new(inner: T) -> Self {
        Self(std::sync::Mutex::new(inner))
    }

    /// Consume the adapter and return the inner store.
    pub fn into_inner(self) -> T {
        self.0.into_inner().expect("mutex not poisoned")
    }
}

impl<T: Send, const BODY_SIZE: usize> AsyncChunkPut<BODY_SIZE> for AsyncChunkPutAdapter<T>
where
    T: super::typed::ChunkPut<BODY_SIZE> + Send,
    T::Error: Send + Sync,
{
    type Error = T::Error;

    async fn put(&self, chunk: ContentChunk<BODY_SIZE>) -> Result<(), Self::Error> {
        self.0.lock().expect("mutex not poisoned").put(chunk)
    }
}

// Cannot provide a blanket impl `AsyncChunkPut for T where T: ChunkPut` because
// `ChunkPut::put` takes `&mut self` while `AsyncChunkPut::put` takes `&self`.
// Use `AsyncChunkPutAdapter` to bridge the two.

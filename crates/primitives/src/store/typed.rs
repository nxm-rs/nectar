//! Typed chunk storage traits.
//!
//! Async traits (`ChunkGet`, `ChunkPut`, `ChunkHas`) are the primary API.
//! Sync traits (`SyncChunkGet`, `SyncChunkPut`, `SyncChunkHas`) exist for
//! CPU-bound paths (splitter, mantaray). Blanket impls bridge sync → async
//! automatically for any `Send + Sync` type.

use std::future::Future;

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::{AnyChunk, ChunkAddress};
use crate::store::{MaybeSend, MaybeSync};

/// Stores chunks (synchronous, `&self`; implementors use interior mutability).
pub trait SyncChunkPut<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    /// Error type for put operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Store a chunk.
    fn put(&self, chunk: AnyChunk<BODY_SIZE>) -> Result<(), Self::Error>;
}

/// Retrieves chunks by address (synchronous).
pub trait SyncChunkGet<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    /// Error type for get operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Get a chunk by address.
    fn get(&self, address: &ChunkAddress) -> Result<AnyChunk<BODY_SIZE>, Self::Error>;

    /// Whether `error` signals a genuine absence rather than a backend failure.
    ///
    /// Callers that treat a miss as "not present yet" (feed finders walking an
    /// index, mantaray traversal probing for a node) use this to separate a
    /// clean miss from an error they must propagate. The default returns
    /// `false`, so an implementor that never distinguishes the two keeps every
    /// error opaque.
    fn is_not_found(&self, error: &Self::Error) -> bool {
        let _ = error;
        false
    }
}

/// Checks chunk existence (synchronous).
pub trait SyncChunkHas<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    /// Check if a chunk exists.
    fn has(&self, address: &ChunkAddress) -> bool;
}

impl<T: SyncChunkPut<BS>, const BS: usize> SyncChunkPut<BS> for &T {
    type Error = T::Error;
    fn put(&self, chunk: AnyChunk<BS>) -> Result<(), Self::Error> {
        (**self).put(chunk)
    }
}

impl<T: SyncChunkPut<BS>, const BS: usize> SyncChunkPut<BS> for &mut T {
    type Error = T::Error;
    fn put(&self, chunk: AnyChunk<BS>) -> Result<(), Self::Error> {
        (**self).put(chunk)
    }
}

impl<T: SyncChunkGet<BS>, const BS: usize> SyncChunkGet<BS> for &T {
    type Error = T::Error;
    fn get(&self, address: &ChunkAddress) -> Result<AnyChunk<BS>, Self::Error> {
        (**self).get(address)
    }
    fn is_not_found(&self, error: &Self::Error) -> bool {
        (**self).is_not_found(error)
    }
}

impl<T: SyncChunkGet<BS>, const BS: usize> SyncChunkGet<BS> for &mut T {
    type Error = T::Error;
    fn get(&self, address: &ChunkAddress) -> Result<AnyChunk<BS>, Self::Error> {
        (**self).get(address)
    }
    fn is_not_found(&self, error: &Self::Error) -> bool {
        (**self).is_not_found(error)
    }
}

impl<T: SyncChunkHas<BS>, const BS: usize> SyncChunkHas<BS> for &T {
    fn has(&self, address: &ChunkAddress) -> bool {
        (**self).has(address)
    }
}

impl<T: SyncChunkHas<BS>, const BS: usize> SyncChunkHas<BS> for &mut T {
    fn has(&self, address: &ChunkAddress) -> bool {
        (**self).has(address)
    }
}

/// Async chunk retrieval (primary API).
///
/// Types implementing `SyncChunkGet + Send + Sync` get this automatically via
/// a blanket impl. Types needing genuinely async retrieval (e.g. network
/// fetch) should implement `ChunkGet` directly without implementing
/// `SyncChunkGet`.
pub trait ChunkGet<const BODY_SIZE: usize = DEFAULT_BODY_SIZE>: MaybeSend + MaybeSync {
    /// Error type for get operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Get a chunk by address.
    fn get(
        &self,
        address: &ChunkAddress,
    ) -> impl Future<Output = Result<AnyChunk<BODY_SIZE>, Self::Error>> + MaybeSend;

    /// Whether `error` signals a genuine absence rather than a backend failure.
    ///
    /// A finder that walks an address space (feed lookup, mantaray traversal)
    /// uses this to treat a miss as absence and propagate every other error.
    /// The default returns `false`; a network getter that knows its own
    /// not-found variant should override it.
    fn is_not_found(&self, error: &Self::Error) -> bool {
        let _ = error;
        false
    }
}

impl<T, const BS: usize> ChunkGet<BS> for T
where
    T: SyncChunkGet<BS> + Send + Sync,
{
    type Error = T::Error;

    async fn get(&self, address: &ChunkAddress) -> Result<AnyChunk<BS>, Self::Error> {
        SyncChunkGet::get(self, address)
    }

    fn is_not_found(&self, error: &Self::Error) -> bool {
        SyncChunkGet::is_not_found(self, error)
    }
}

/// Async chunk existence check (primary API).
pub trait ChunkHas<const BODY_SIZE: usize = DEFAULT_BODY_SIZE>: MaybeSend + MaybeSync {
    /// Check if a chunk exists.
    fn has(&self, address: &ChunkAddress) -> impl Future<Output = bool> + MaybeSend;
}

impl<T, const BS: usize> ChunkHas<BS> for T
where
    T: SyncChunkHas<BS> + Send + Sync,
{
    async fn has(&self, address: &ChunkAddress) -> bool {
        SyncChunkHas::has(self, address)
    }
}

/// Async chunk storage (primary API, `&self`).
///
/// Implementors should use interior mutability (e.g. `Mutex`, `RwLock`).
/// Types implementing `SyncChunkPut + Send + Sync` get this automatically.
pub trait ChunkPut<const BODY_SIZE: usize = DEFAULT_BODY_SIZE>: MaybeSend + MaybeSync {
    /// Error type for put operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Store a chunk.
    fn put(
        &self,
        chunk: AnyChunk<BODY_SIZE>,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend;
}

impl<T, const BS: usize> ChunkPut<BS> for T
where
    T: SyncChunkPut<BS> + Send + Sync,
{
    type Error = T::Error;

    async fn put(&self, chunk: AnyChunk<BS>) -> Result<(), Self::Error> {
        SyncChunkPut::put(self, chunk)
    }
}

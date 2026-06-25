//! Typed chunk storage traits.
//!
//! Async traits (`ChunkGet`, `ChunkPut`, `ChunkHas`) are the primary API.
//! Sync traits (`SyncChunkGet`, `SyncChunkPut`, `SyncChunkHas`) exist for
//! CPU-bound paths (splitter, mantaray). Blanket impls bridge sync → async
//! automatically for any `MaybeSend + MaybeSync` type.

use std::future::Future;

use super::maybe_send::{MaybeSend, MaybeSync};
use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::{AnyChunk, ChunkAddress};

/// Stores chunks (synchronous, `&self` — implementors use interior mutability).
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
}

impl<T: SyncChunkGet<BS>, const BS: usize> SyncChunkGet<BS> for &mut T {
    type Error = T::Error;
    fn get(&self, address: &ChunkAddress) -> Result<AnyChunk<BS>, Self::Error> {
        (**self).get(address)
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
}

impl<T, const BS: usize> ChunkGet<BS> for T
where
    T: SyncChunkGet<BS> + MaybeSend + MaybeSync,
{
    type Error = T::Error;

    async fn get(&self, address: &ChunkAddress) -> Result<AnyChunk<BS>, Self::Error> {
        SyncChunkGet::get(self, address)
    }
}

/// Async chunk existence check (primary API).
pub trait ChunkHas<const BODY_SIZE: usize = DEFAULT_BODY_SIZE>: MaybeSend + MaybeSync {
    /// Check if a chunk exists.
    fn has(&self, address: &ChunkAddress) -> impl Future<Output = bool> + MaybeSend;
}

impl<T, const BS: usize> ChunkHas<BS> for T
where
    T: SyncChunkHas<BS> + MaybeSend + MaybeSync,
{
    async fn has(&self, address: &ChunkAddress) -> bool {
        SyncChunkHas::has(self, address)
    }
}

/// Async chunk storage (primary API, `&self`).
///
/// Implementors should use interior mutability (e.g. `Mutex`, `RwLock`).
/// Types implementing `SyncChunkPut + MaybeSend + MaybeSync` get this automatically.
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
    T: SyncChunkPut<BS> + MaybeSend + MaybeSync,
{
    type Error = T::Error;

    async fn put(&self, chunk: AnyChunk<BS>) -> Result<(), Self::Error> {
        SyncChunkPut::put(self, chunk)
    }
}

#[cfg(target_arch = "wasm32")]
mod wasm_send_sync_proof {
    // A store that is neither Send nor Sync (raw pointer marker) must still
    // satisfy ChunkGet on wasm32, proving the MaybeSend + MaybeSync relaxation.
    use super::*;

    struct NotSendSync(core::marker::PhantomData<*const ()>);

    impl<const BS: usize> ChunkGet<BS> for NotSendSync {
        type Error = std::io::Error;
        async fn get(&self, _addr: &ChunkAddress) -> Result<AnyChunk<BS>, Self::Error> {
            unreachable!()
        }
    }

    fn _assert<const BS: usize, S: ChunkGet<BS>>() {}

    #[allow(dead_code)]
    fn _proof() {
        _assert::<{ DEFAULT_BODY_SIZE }, NotSendSync>()
    }
}

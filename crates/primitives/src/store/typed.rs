//! Typed chunk storage traits.
//!
//! `ChunkGet`, `ChunkPut`, and `ChunkHas` are async and carry `MaybeSend`/
//! `MaybeSync` bounds so a store may be `!Send` on wasm.

use std::future::Future;

use super::maybe_send::{MaybeSend, MaybeSync};
use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::{AnyChunk, ChunkAddress};

/// Async chunk retrieval (primary API).
pub trait ChunkGet<const BODY_SIZE: usize = DEFAULT_BODY_SIZE>: MaybeSend + MaybeSync {
    /// Error type for get operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Get a chunk by address.
    fn get(
        &self,
        address: &ChunkAddress,
    ) -> impl Future<Output = Result<AnyChunk<BODY_SIZE>, Self::Error>> + MaybeSend;
}

impl<T: ChunkGet<BS> + ?Sized, const BS: usize> ChunkGet<BS> for &T {
    type Error = T::Error;

    fn get(
        &self,
        address: &ChunkAddress,
    ) -> impl Future<Output = Result<AnyChunk<BS>, Self::Error>> + MaybeSend {
        (**self).get(address)
    }
}

/// Async chunk existence check (primary API).
pub trait ChunkHas<const BODY_SIZE: usize = DEFAULT_BODY_SIZE>: MaybeSend + MaybeSync {
    /// Check if a chunk exists.
    fn has(&self, address: &ChunkAddress) -> impl Future<Output = bool> + MaybeSend;
}

impl<T: ChunkHas<BS> + ?Sized, const BS: usize> ChunkHas<BS> for &T {
    fn has(&self, address: &ChunkAddress) -> impl Future<Output = bool> + MaybeSend {
        (**self).has(address)
    }
}

/// Async chunk storage (primary API, `&self`).
///
/// Implementors should use interior mutability (e.g. `Mutex`, `RwLock`).
pub trait ChunkPut<const BODY_SIZE: usize = DEFAULT_BODY_SIZE>: MaybeSend + MaybeSync {
    /// Error type for put operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Store a chunk.
    fn put(
        &self,
        chunk: AnyChunk<BODY_SIZE>,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend;
}

impl<T: ChunkPut<BS> + ?Sized, const BS: usize> ChunkPut<BS> for &T {
    type Error = T::Error;

    fn put(
        &self,
        chunk: AnyChunk<BS>,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend {
        (**self).put(chunk)
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

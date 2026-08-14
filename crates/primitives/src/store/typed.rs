//! Typed chunk storage traits.
//!
//! `ChunkGet`, `ChunkPut`, and `ChunkHas` are async and carry `MaybeSend`/
//! `MaybeSync` bounds (on the traits and their error types) so a store may be
//! `!Send` on single-threaded targets. Writes are uniformly sealed (a
//! [`PutUnit`] carries a certified address); trust is a property of the read
//! medium, declared once per backend through [`ChunkGet::Trust`].

use core::future::Future;

use crate::chunk::{Chunk, ChunkAddress, ChunkRegistry, StandardChunkSet, TrustState, Verified};
use crate::marker::{MaybeSend, MaybeSync};

/// Async chunk retrieval (primary API).
///
/// [`Trust`](Self::Trust) states what the medium may have done to a sealed
/// chunk since it was written: an exclusively held file reads back
/// [`Verified`], a medium other parties can script reads back `Unverified`.
pub trait ChunkGet<R: ChunkRegistry = StandardChunkSet>: MaybeSend + MaybeSync {
    /// Trust level of chunks read back from this medium.
    type Trust: TrustState;

    /// Error type for get operations.
    type Error: core::error::Error + MaybeSend + MaybeSync + 'static;

    /// Get a chunk by address.
    fn get(
        &self,
        address: &ChunkAddress,
    ) -> impl Future<Output = Result<Chunk<Self::Trust, R>, Self::Error>> + MaybeSend;
}

impl<R: ChunkRegistry, T: ChunkGet<R> + ?Sized> ChunkGet<R> for &T {
    type Trust = T::Trust;
    type Error = T::Error;

    fn get(
        &self,
        address: &ChunkAddress,
    ) -> impl Future<Output = Result<Chunk<Self::Trust, R>, Self::Error>> + MaybeSend {
        (**self).get(address)
    }
}

impl<R: ChunkRegistry, T: ChunkGet<R> + ?Sized> ChunkGet<R> for alloc::sync::Arc<T> {
    type Trust = T::Trust;
    type Error = T::Error;

    fn get(
        &self,
        address: &ChunkAddress,
    ) -> impl Future<Output = Result<Chunk<Self::Trust, R>, Self::Error>> + MaybeSend {
        (**self).get(address)
    }
}

/// Async chunk existence check (primary API).
pub trait ChunkHas: MaybeSend + MaybeSync {
    /// Check if a chunk exists.
    fn has(&self, address: &ChunkAddress) -> impl Future<Output = bool> + MaybeSend;
}

impl<T: ChunkHas + ?Sized> ChunkHas for &T {
    fn has(&self, address: &ChunkAddress) -> impl Future<Output = bool> + MaybeSend {
        (**self).has(address)
    }
}

impl<T: ChunkHas + ?Sized> ChunkHas for alloc::sync::Arc<T> {
    fn has(&self, address: &ChunkAddress) -> impl Future<Output = bool> + MaybeSend {
        (**self).has(address)
    }
}

/// What a [`ChunkPut`] moves: a sealed chunk, or a richer unit some other
/// crate defines over one.
pub trait PutUnit: MaybeSend + 'static {
    /// The certified address the unit is stored under.
    fn address(&self) -> &ChunkAddress;
}

impl<R: ChunkRegistry> PutUnit for Chunk<Verified, R> {
    #[inline]
    fn address(&self) -> &ChunkAddress {
        Self::address(self)
    }
}

/// Async chunk storage (primary API, `&self`).
///
/// Only accepts proof: there is no trust parameter to widen, so an
/// uncertified chunk cannot enter any store. `U` is the unit the sink
/// demands, so a sink wanting more than a sealed chunk names a richer unit
/// instead of gaining a second verb. Implementors should use interior
/// mutability (e.g. `Mutex`, `RwLock`).
pub trait ChunkPut<U: PutUnit = Chunk<Verified>>: MaybeSend + MaybeSync {
    /// Error type for put operations.
    type Error: core::error::Error + MaybeSend + MaybeSync + 'static;

    /// Store one unit.
    fn put(&self, unit: U) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend;
}

impl<U: PutUnit, T: ChunkPut<U> + ?Sized> ChunkPut<U> for &T {
    type Error = T::Error;

    fn put(&self, unit: U) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend {
        (**self).put(unit)
    }
}

impl<U: PutUnit, T: ChunkPut<U> + ?Sized> ChunkPut<U> for alloc::sync::Arc<T> {
    type Error = T::Error;

    fn put(&self, unit: U) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend {
        (**self).put(unit)
    }
}

/// Marker for getters whose read medium hands back exactly what was sealed:
/// [`ChunkGet`] with `Trust = Verified`.
///
/// Blanket-implemented. Consensus consumers bound on this, so feeding them
/// from an untrusted medium is a type error, not a runtime concern.
pub trait TrustedGet<R: ChunkRegistry = StandardChunkSet>: ChunkGet<R, Trust = Verified> {}

impl<R: ChunkRegistry, T: ChunkGet<R, Trust = Verified> + ?Sized> TrustedGet<R> for T {}

#[cfg(not(multi_thread))]
mod send_sync_relaxation_proof {
    // A store that is neither Send nor Sync (raw pointer marker) must still
    // satisfy ChunkGet wherever the relaxation applies (wasm32, or the unsync
    // feature), proving the MaybeSend + MaybeSync relaxation for the store
    // and for its error type alike.
    use super::*;
    use crate::chunk::Unverified;

    struct NotSendSync(core::marker::PhantomData<*const ()>);

    #[derive(Debug, thiserror::Error)]
    #[error("not send")]
    struct NotSendError(core::marker::PhantomData<*const ()>);

    impl ChunkGet<StandardChunkSet> for NotSendSync {
        type Trust = Unverified;
        type Error = NotSendError;
        async fn get(
            &self,
            _addr: &ChunkAddress,
        ) -> Result<Chunk<Unverified, StandardChunkSet>, Self::Error> {
            Err(NotSendError(core::marker::PhantomData))
        }
    }

    const fn _assert<S: ChunkGet<StandardChunkSet>>() {}

    #[allow(dead_code)]
    const fn _proof() {
        _assert::<NotSendSync>()
    }
}

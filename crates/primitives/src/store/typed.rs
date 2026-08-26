//! Typed chunk storage traits.
//!
//! `ChunkGet` and `ChunkPut` are async and carry `MaybeSend`/`MaybeSync`
//! bounds (on the traits and their error types) so a store may be `!Send` on
//! single-threaded targets. `ChunkGetSync` is the synchronous counterpart
//! of `ChunkGet`: the same stampless address read, without a future, for a
//! store that reads its entries inside its own write transaction. The seam
//! error is classified through [`StoreError`], so a generic consumer
//! separates a definite miss from a failure. Trust is a property of the
//! read medium, declared once per backend through [`ChunkGet::Trust`].
//! Validation is a property of the put unit, declared once per unit through
//! [`PutUnit::Validation`].

use core::future::Future;

use crate::chunk::{Chunk, ChunkAddress, ChunkRegistry, StandardChunkSet, TrustState, Verified};
use crate::error::StoreError;
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
    type Error: StoreError;

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

/// Validation a unit that a local or test store takes as-is carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoValidation;

/// What a [`ChunkPut`] moves. Not sealed, so whether a unit wraps a verified
/// chunk is a property of the chosen `U`, not of the trait.
pub trait PutUnit: MaybeSend + 'static {
    /// The validation the store applies to the unit. A local or test store
    /// applies none. A unit the network accounts for carries its postage
    /// stamp.
    type Validation;

    /// The address the unit is stored under.
    fn address(&self) -> &ChunkAddress;
}

impl<R: ChunkRegistry> PutUnit for Chunk<Verified, R> {
    type Validation = NoValidation;

    #[inline]
    fn address(&self) -> &ChunkAddress {
        Self::address(self)
    }
}

const _VALIDATION_IS_NONE: fn(
    <Chunk<Verified, StandardChunkSet> as PutUnit>::Validation,
) -> NoValidation = {
    const fn unit(validation: NoValidation) -> NoValidation {
        validation
    }
    unit
};

/// Async chunk storage (primary API, `&self`).
///
/// There is no default put unit: the caller names the unit it stores, so an
/// unstamped put is never the choice by default.
///
/// Implementors should use interior mutability (e.g. `Mutex`, `RwLock`).
pub trait ChunkPut<U: PutUnit>: MaybeSend + MaybeSync {
    /// Error type for put operations.
    type Error: StoreError;

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

/// Synchronous chunk retrieval by address: the stampless counterpart of
/// [`ChunkGet`] without a future.
///
/// The value is stampless by design: the body is addressed, and the stamps
/// that cover it are the concern of the store that keys them. A store that
/// holds one body under several stamps answers the same body here.
pub trait ChunkGetSync<R: ChunkRegistry = StandardChunkSet>: MaybeSend + MaybeSync {
    /// Trust level of chunks read back from this medium.
    type Trust: TrustState;

    /// Error type for get operations.
    type Error: StoreError;

    /// Get a chunk by address.
    fn get(&self, address: &ChunkAddress) -> Result<Chunk<Self::Trust, R>, Self::Error>;
}

impl<R: ChunkRegistry, T: ChunkGetSync<R> + ?Sized> ChunkGetSync<R> for &T {
    type Trust = T::Trust;
    type Error = T::Error;

    fn get(&self, address: &ChunkAddress) -> Result<Chunk<Self::Trust, R>, Self::Error> {
        (**self).get(address)
    }
}

impl<R: ChunkRegistry, T: ChunkGetSync<R> + ?Sized> ChunkGetSync<R> for alloc::sync::Arc<T> {
    type Trust = T::Trust;
    type Error = T::Error;

    fn get(&self, address: &ChunkAddress) -> Result<Chunk<Self::Trust, R>, Self::Error> {
        (**self).get(address)
    }
}

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

    impl StoreError for NotSendError {
        fn is_definitely_absent(&self) -> bool {
            false
        }

        fn is_transient(&self) -> bool {
            false
        }
    }

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

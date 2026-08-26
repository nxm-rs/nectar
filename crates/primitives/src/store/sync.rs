//! Synchronous chunk retrieval.
//!
//! [`SyncChunkGet`] is the address read of the asynchronous [`ChunkGet`]
//! without a future: the same stampless body, the same classified absence
//! through [`StoreError`]. A store that admits stamps inside its own write
//! transaction reads its store synchronously, and this trait is the
//! retrieval half of that store. The stamp-keyed composition over it lives
//! in the postage primitives crate.

use crate::chunk::{Chunk, ChunkAddress, ChunkRegistry, StandardChunkSet, TrustState};
use crate::error::StoreError;
use crate::marker::{MaybeSend, MaybeSync};

/// Synchronous chunk retrieval by address.
///
/// The value is stampless by design: the body is addressed, and the stamps
/// that cover it are the concern of the store that keys them. A store that
/// holds one body under several stamps answers the same body here.
pub trait SyncChunkGet<R: ChunkRegistry = StandardChunkSet>: MaybeSend + MaybeSync {
    /// Trust level of chunks read back from this medium.
    type Trust: TrustState;

    /// Error type for get operations.
    type Error: StoreError;

    /// Get a chunk by address.
    fn get(&self, address: &ChunkAddress) -> Result<Chunk<Self::Trust, R>, Self::Error>;
}

impl<R: ChunkRegistry, T: SyncChunkGet<R> + ?Sized> SyncChunkGet<R> for &T {
    type Trust = T::Trust;
    type Error = T::Error;

    fn get(&self, address: &ChunkAddress) -> Result<Chunk<Self::Trust, R>, Self::Error> {
        (**self).get(address)
    }
}

impl<R: ChunkRegistry, T: SyncChunkGet<R> + ?Sized> SyncChunkGet<R> for alloc::sync::Arc<T> {
    type Trust = T::Trust;
    type Error = T::Error;

    fn get(&self, address: &ChunkAddress) -> Result<Chunk<Self::Trust, R>, Self::Error> {
        (**self).get(address)
    }
}

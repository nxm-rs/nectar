//! Erased chunk store and the nameable file aliases built on it.
//!
//! [`BoxedStore`] hides the concrete store type behind one cheap-to-clone
//! handle, so the [`DynFile`] family fits struct fields without a store
//! parameter.

use alloc::boxed::Box;
use alloc::sync::Arc;
use core::fmt;
use core::future::Future;
use core::pin::Pin;

use nectar_marker::{MaybeSend, MaybeSync};
use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::{AnyChunkSet, Chunk, ChunkAddress, Verified};
use nectar_primitives::store::{BoxedError, ChunkGet, TrustedGet};

use crate::read::{AnyFile, File, FileReader, FileStream};
use crate::walk::Plain;

/// Fetch failure behind an erased store; the concrete error survives as the
/// source.
#[derive(Debug, thiserror::Error)]
#[error("erased store fetch failed")]
pub struct BoxedStoreError(#[source] BoxedError);

/// Boxed erased fetch future: `Send` on multi-threaded targets, unbounded on
/// wasm32 and under the `unsync` feature.
#[cfg(multi_thread)]
type BoxGet<const B: usize> =
    Pin<Box<dyn Future<Output = Result<Chunk<Verified, AnyChunkSet<B>>, BoxedStoreError>> + Send>>;
/// Boxed erased fetch future: `Send` on multi-threaded targets, unbounded on
/// wasm32 and under the `unsync` feature.
#[cfg(not(multi_thread))]
type BoxGet<const B: usize> =
    Pin<Box<dyn Future<Output = Result<Chunk<Verified, AnyChunkSet<B>>, BoxedStoreError>>>>;

/// Object-safe fetch surface the adapter erases stores through.
trait ErasedGet<const B: usize>: MaybeSend + MaybeSync {
    fn get(&self, address: ChunkAddress) -> BoxGet<B>;
}

impl<S, const B: usize> ErasedGet<B> for S
where
    S: TrustedGet<AnyChunkSet<B>> + Clone + 'static,
{
    fn get(&self, address: ChunkAddress) -> BoxGet<B> {
        let store = self.clone();
        Box::pin(async move {
            store
                .get(&address)
                .await
                .map_err(|source| BoxedStoreError(Box::new(source)))
        })
    }
}

/// A trusted store with its type erased; cloning shares the same backend.
pub struct BoxedStore<const B: usize = DEFAULT_BODY_SIZE>(Arc<dyn ErasedGet<B>>);

impl<const B: usize> BoxedStore<B> {
    /// Erase a trusted store behind one nameable handle.
    pub fn new<S>(store: S) -> Self
    where
        S: TrustedGet<AnyChunkSet<B>> + Clone + 'static,
    {
        Self(Arc::new(store))
    }
}

impl<const B: usize> Clone for BoxedStore<B> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<const B: usize> fmt::Debug for BoxedStore<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoxedStore").finish_non_exhaustive()
    }
}

impl<const B: usize> ChunkGet<AnyChunkSet<B>> for BoxedStore<B> {
    type Trust = Verified;
    type Error = BoxedStoreError;

    fn get(
        &self,
        address: &ChunkAddress,
    ) -> impl Future<Output = Result<Chunk<Verified, AnyChunkSet<B>>, Self::Error>> + MaybeSend
    {
        self.0.get(*address)
    }
}

/// Erased-store file; nameable as a struct field.
pub type DynFile<M = Plain, const B: usize = DEFAULT_BODY_SIZE> = File<BoxedStore<B>, M, B>;

/// Erased-store file of either reference width.
pub type DynAnyFile<const B: usize = DEFAULT_BODY_SIZE> = AnyFile<BoxedStore<B>, B>;

/// Erased-store ordered reader.
pub type DynFileReader<M = Plain, const B: usize = DEFAULT_BODY_SIZE> =
    FileReader<BoxedStore<B>, M, B>;

/// Erased-store ordered stream.
pub type DynFileStream<M = Plain, const B: usize = DEFAULT_BODY_SIZE> =
    FileStream<BoxedStore<B>, M, B>;

//! Shared test fixtures: whole-buffer splits into a fresh memory store, and a
//! closure-driven fault store for the split fault battery.

use core::sync::atomic::{AtomicUsize, Ordering};
use std::boxed::Box;
use std::string::ToString;
use std::sync::Arc;

use futures::executor::block_on;
#[cfg(feature = "encryption")]
use nectar_primitives::chunk::encryption::EncryptedChunkRef;
use nectar_primitives::chunk::{AnyChunkSet, Chunk, ChunkAddress, Verified};
use nectar_primitives::store::{ChunkPut, ChunkStoreError, MaybeSend, MaybeSync, MemoryStore};

#[cfg(feature = "encryption")]
use crate::split::RandomKeys;
use crate::split::{Split, SplitMode};
#[cfg(feature = "encryption")]
use crate::walk::Encrypted;
use crate::walk::Plain;

/// A fault-injection store: forwards each put to `inner` when the policy
/// admits it, else returns the policy's error. The policy is consulted once
/// per put against a shared, monotonic 0-based put index, so it fires the same
/// way across the clones the split makes per dispatched put.
pub(crate) struct FaultStore<S, P, const B: usize> {
    inner: S,
    policy: P,
    seen: Arc<AtomicUsize>,
}

impl<S: Clone, P: Clone, const B: usize> Clone for FaultStore<S, P, B> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            policy: self.policy.clone(),
            seen: Arc::clone(&self.seen),
        }
    }
}

impl<S, P, const B: usize> FaultStore<S, P, B>
where
    S: ChunkPut<AnyChunkSet<B>>,
    P: Fn(usize) -> Result<(), ChunkStoreError>,
{
    /// Wrap `inner`, consulting `policy` against the running put index.
    pub(crate) fn new(inner: S, policy: P) -> Self {
        Self {
            inner,
            policy,
            seen: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl<S, P, const B: usize> ChunkPut<AnyChunkSet<B>> for FaultStore<S, P, B>
where
    S: ChunkPut<AnyChunkSet<B>>,
    P: Fn(usize) -> Result<(), ChunkStoreError> + MaybeSend + MaybeSync,
{
    type Error = ChunkStoreError;

    async fn put(&self, chunk: Chunk<Verified, AnyChunkSet<B>>) -> Result<(), ChunkStoreError> {
        let index = self.seen.fetch_add(1, Ordering::SeqCst);
        (self.policy)(index)?;
        self.inner
            .put(chunk)
            .await
            .map_err(|source| ChunkStoreError::Other(Box::new(source)))
    }
}

/// A store rejecting the put at index `n` and every put after it; the puts
/// before it forward to `inner`.
pub(crate) fn failing_at<S, const B: usize>(
    inner: S,
    n: usize,
) -> FaultStore<S, impl Fn(usize) -> Result<(), ChunkStoreError> + Clone, B>
where
    S: ChunkPut<AnyChunkSet<B>>,
{
    FaultStore::new(inner, move |index| {
        if index >= n {
            Err(ChunkStoreError::Other("put refused".to_string().into()))
        } else {
            Ok(())
        }
    })
}

/// A store rejecting every put; `inner` is never reached.
pub(crate) fn reject_all<S, const B: usize>(
    inner: S,
) -> FaultStore<S, impl Fn(usize) -> Result<(), ChunkStoreError> + Clone, B>
where
    S: ChunkPut<AnyChunkSet<B>>,
{
    failing_at(inner, 0)
}

/// Drive `data` whole through a fresh split, returning the root and the
/// filled store. `MemoryStore` clones deeply, so the split writes through a
/// shared `Arc` handle that unwraps once the puts have drained.
fn split_into<M, const B: usize>(data: &[u8]) -> (M::Root, MemoryStore<AnyChunkSet<B>>)
where
    M: SplitMode + Default,
{
    let store = Arc::new(MemoryStore::new());
    let root = block_on(Split::<Arc<MemoryStore<AnyChunkSet<B>>>, M, B>::collect(
        Arc::clone(&store),
        data,
    ))
    .unwrap();
    (root, Arc::into_inner(store).unwrap())
}

/// Split `data` into a fresh memory store, returning root and store.
pub(crate) fn split_fixture<const B: usize>(
    data: &[u8],
) -> (ChunkAddress, MemoryStore<AnyChunkSet<B>>) {
    split_into::<Plain, B>(data)
}

/// Split `data` into encrypted chunks in a fresh memory store.
#[cfg(feature = "encryption")]
pub(crate) fn split_encrypted_fixture<const B: usize>(
    data: &[u8],
) -> (EncryptedChunkRef, MemoryStore<AnyChunkSet<B>>) {
    split_into::<Encrypted<RandomKeys>, B>(data)
}

//! Shared test fixtures: whole-buffer splits into a fresh memory store.

use std::sync::Arc;

use futures::executor::block_on;
#[cfg(feature = "encryption")]
use nectar_primitives::chunk::encryption::EncryptedChunkRef;
use nectar_primitives::chunk::{AnyChunkSet, ChunkAddress};
use nectar_primitives::store::MemoryStore;

#[cfg(feature = "encryption")]
use crate::split::RandomKeys;
use crate::split::{Split, SplitMode};
#[cfg(feature = "encryption")]
use crate::walk::Encrypted;
use crate::walk::Plain;

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

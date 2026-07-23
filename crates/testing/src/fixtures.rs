//! Shared fixtures: whole-buffer splits into a fresh memory store, and spec
//! doubles that move the collision-bucket floor off mainnet's 16.

use core::num::NonZeroU8;
use std::error::Error;
use std::sync::Arc;

use nectar_file::{Plain, Split, SplitMode};
use nectar_postage::BucketDepth;
use nectar_primitives::chunk::{AnyChunkSet, ChunkAddress};
use nectar_primitives::store::MemoryStore;
use nectar_primitives::{DEFAULT_BODY_SIZE, NetworkId, SwarmSpec};

use crate::run;

/// Splits `data` whole through the streaming engine into a fresh store,
/// returning the root and the filled store. `MemoryStore` clones deeply, so
/// the split writes through a shared `Arc` handle that unwraps once the puts
/// have drained.
pub async fn try_split_into<M, const B: usize>(
    data: &[u8],
) -> Result<(M::Root, MemoryStore<AnyChunkSet<B>>), Box<dyn Error>>
where
    M: SplitMode + Default,
{
    let store = Arc::new(MemoryStore::new());
    let root =
        Split::<Arc<MemoryStore<AnyChunkSet<B>>>, M, B>::collect(Arc::clone(&store), data).await?;
    let store = Arc::into_inner(store).ok_or("split still holds the store")?;
    Ok((root, store))
}

/// [`try_split_into`] driven to completion on the calling thread; panics on
/// a split failure.
pub fn split_into<M, const B: usize>(data: &[u8]) -> (M::Root, MemoryStore<AnyChunkSet<B>>)
where
    M: SplitMode + Default,
{
    run(try_split_into::<M, B>(data)).expect("split over a memory store succeeds")
}

/// Plain split of `data` at the default profile, returning root and store.
pub async fn split_whole(data: &[u8]) -> Result<(ChunkAddress, MemoryStore), Box<dyn Error>> {
    try_split_into::<Plain, DEFAULT_BODY_SIZE>(data).await
}

/// Plain split of `data` into a fresh memory store, returning root and store.
pub fn split_fixture<const B: usize>(data: &[u8]) -> (ChunkAddress, MemoryStore<AnyChunkSet<B>>) {
    split_into::<Plain, B>(data)
}

/// A deployment whose collision-bucket floor is the format minimum, for the
/// geometries mainnet's floor of 16 forbids.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LowFloor;

impl SwarmSpec for LowFloor {
    const NETWORK_ID: NetworkId = NetworkId::TESTNET;
    const MIN_BUCKET_DEPTH: NonZeroU8 = NonZeroU8::new(1).unwrap();
}

/// A deployment whose collision-bucket floor is above mainnet's 16.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HighFloor;

impl SwarmSpec for HighFloor {
    const NETWORK_ID: NetworkId = NetworkId::TESTNET;
    const MIN_BUCKET_DEPTH: NonZeroU8 = NonZeroU8::new(20).unwrap();
}

/// A bucket depth [`LowFloor`] accepts.
pub fn low_floor(depth: u8) -> BucketDepth<LowFloor> {
    BucketDepth::new(depth).unwrap()
}

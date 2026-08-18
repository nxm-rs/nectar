//! Borrowed-store write driver: one [`Source`] drained into a bounded
//! [`Split`] over a store the caller only lends.
//!
//! The split's put window borrows the store for the length of the save, so
//! one [`File`](crate::File) handle writes through a store it does not own
//! and cannot cheaply clone, with no queue between the ascent and the puts.

use core::future::poll_fn;

use alloc::vec;

use nectar_primitives::chunk::{AnyChunkSet, Chunk, Verified};
use nectar_primitives::store::ChunkPut;

use super::SplitStats;
use super::engine::Split;
use super::error::SaveError;
#[cfg(test)]
use super::error::SplitError;
use super::mode::SplitMode;
#[cfg(test)]
use crate::config::PutWindow;
use crate::handle::Policy;
use crate::source::Source;

/// Drain `src` into a chunk tree over the borrowed `store`, returning the
/// root plus the write's witnesses.
///
/// The memory bound is the split's own: puts in flight stay within the
/// policy's put window, sealed chunks awaiting a slot within the spine
/// height, and one leaf body is buffered for the pull. The root is
/// delivered only after every put has settled. A pull that over-reports its
/// count is clamped to the buffer, so a broken source cannot stall the
/// drain.
pub(crate) async fn save_source<'a, T, M, Src, const B: usize>(
    store: &'a T,
    policy: Policy,
    mode: M,
    mut src: Src,
) -> Result<(M::Root, SplitStats), SaveError<T::Error, Src::Error>>
where
    T: ChunkPut<Chunk<Verified, AnyChunkSet<B>>>,
    M: SplitMode,
    Src: Source,
{
    let mut split: Split<'a, &'a T, M, B> = Split::with_mode(store, mode, policy.put_window());
    #[cfg(feature = "rayon")]
    if let Some(hash) = policy.hash_window() {
        split = split.with_hash_window(hash);
    }
    let mut buf = vec![0u8; B];
    loop {
        let filled = poll_fn(|cx| src.poll_fill(cx, &mut buf))
            .await
            .map_err(|source| SaveError::Source { source })?;
        if filled == 0 {
            break;
        }
        let mut rest = buf.get(..filled.min(buf.len())).unwrap_or_default();
        while !rest.is_empty() {
            let taken = poll_fn(|cx| split.poll_write(cx, rest)).await?;
            rest = rest.get(taken..).unwrap_or_default();
        }
    }
    let root = poll_fn(|cx| split.poll_finish(cx)).await?;
    Ok((root, split.stats()))
}

/// Test-only borrowed-store split of a whole buffer, over the same driver a
/// save rides.
#[cfg(test)]
pub(crate) async fn collect_into<T, M, const B: usize>(
    store: &T,
    window: PutWindow,
    data: &[u8],
) -> Result<M::Root, SplitError<T::Error>>
where
    T: ChunkPut<Chunk<Verified, AnyChunkSet<B>>>,
    M: SplitMode + Default,
{
    let policy = Policy::DEFAULT.with_put_window(window);
    match save_source::<T, M, &[u8], B>(store, policy, M::default(), data).await {
        Ok((root, _)) => Ok(root),
        Err(SaveError::Split(error)) => Err(error),
        Err(SaveError::Source { source }) => match source {},
    }
}

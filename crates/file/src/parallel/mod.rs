//! Thread-pool ingest: batch leaf hashing over a random-access source.
//!
//! [`split_read_at`] reads and seals leaves in pool-wide batches on the
//! rayon pool and threads their references through the same bounded ascent
//! as the streaming split, so roots and chunk sets are identical. Each
//! batch is one queued pool job, so submission never blocks, and at most
//! one batch hashes while the previous one drains through the put window:
//! retained memory stays at two batches of leaf bodies plus the window.

mod error;
mod handoff;
mod source;
#[cfg(test)]
mod tests;

use alloc::vec;
use alloc::vec::Vec;
use core::future::poll_fn;
use core::task::Poll;

use std::sync::Arc;

use bytes::Bytes;
use nectar_primitives::bmt::SPAN_SIZE;
use nectar_primitives::chunk::{AnyChunkSet, Chunk, Verified};
use nectar_primitives::store::ChunkPut;
use rayon::prelude::*;

pub use error::ReadAtError;
pub use source::ReadAt;

use self::error::LeafError;
use self::handoff::Handoff;
use crate::config::PutWindow;
use crate::num::u64_from_usize;
use crate::split::{Split, SplitMode};

/// One leaf sealed on a pool worker, awaiting admission to the ascent.
struct SealedLeaf<M: SplitMode, const B: usize> {
    chunk: Chunk<Verified, AnyChunkSet<B>>,
    reference: M::Ref,
    span: u64,
}

/// Split `source` into the tree under `store`, hashing leaves in pool-wide
/// batches on the rayon pool; the mode `M` picks the reference grammar.
///
/// The root and chunk set equal the streaming split of the same bytes. The
/// mode is default-constructed once and cloned across the ascent and pool
/// workers, so a key-sourcing mode draws every key from one stream.
/// Dropping the future abandons in-flight puts; a batch already queued on
/// the pool finishes and is discarded.
///
/// ```
/// use nectar_file::{Plain, PutWindow, split_read_at};
/// use nectar_primitives::chunk::AnyChunkSet;
/// use nectar_primitives::store::MemoryStore;
///
/// # futures::executor::block_on(async {
/// let data = vec![7u8; 10_000];
/// let store = MemoryStore::<AnyChunkSet<4096>>::new();
/// let root = split_read_at::<_, _, Plain, 4096>(data, store, PutWindow::DEFAULT)
///     .await
///     .unwrap();
/// assert_eq!(root.as_bytes().len(), 32);
/// # });
/// ```
pub async fn split_read_at<R, S, M, const B: usize>(
    source: R,
    store: S,
    window: PutWindow,
) -> Result<M::Root, ReadAtError<S::Error>>
where
    R: ReadAt + Send + Sync + 'static,
    S: ChunkPut<AnyChunkSet<B>> + Clone + 'static,
    M: SplitMode + Default + Clone,
    M::Ref: Send,
{
    let source = Arc::new(source);
    let size = source
        .len()
        .map_err(|source| ReadAtError::Length { source })?;
    let mode = M::default();
    let mut split = Split::<S, M, B>::with_mode(store, mode.clone(), window);
    let leaves = size.div_ceil(u64_from_usize(B));
    let batch = rayon::current_num_threads().max(1);
    let mut next = 0u64;
    let mut inflight = None;
    if next < leaves {
        let count = batch_len(leaves, next, batch);
        inflight = Some(submit_batch::<R, M, B>(
            &source,
            mode.clone(),
            next,
            count,
            size,
        ));
        next = next.saturating_add(u64_from_usize(count));
    }
    while let Some(mut handoff) = inflight.take() {
        // Collect the hashed batch while the put window keeps draining.
        let outcome = poll_fn(|cx| {
            if let Err(error) = split.pump(cx) {
                return Poll::Ready(Err(error));
            }
            handoff.poll_recv(cx).map(Ok)
        })
        .await;
        let sealed = match outcome {
            Ok(Some(sealed)) => sealed?,
            Ok(None) => return Err(ReadAtError::PoolDropped),
            Err(error) => return Err(error.into()),
        };
        // Queue the next batch first, so it hashes while this one drains.
        if next < leaves {
            let count = batch_len(leaves, next, batch);
            inflight = Some(submit_batch::<R, M, B>(
                &source,
                mode.clone(),
                next,
                count,
                size,
            ));
            next = next.saturating_add(u64_from_usize(count));
        }
        for leaf in sealed {
            poll_fn(|cx| split.poll_admit(cx)).await?;
            split.push_sealed(leaf.chunk, leaf.reference, leaf.span)?;
        }
    }
    poll_fn(|cx| split.poll_finish(cx))
        .await
        .map_err(ReadAtError::from)
}

/// Leaves in the batch starting at `next`; capped by `batch`, so the
/// narrowing is lossless.
fn batch_len(leaves: u64, next: u64, batch: usize) -> usize {
    let remaining = leaves.saturating_sub(next);
    usize::try_from(remaining.min(u64_from_usize(batch))).unwrap_or(batch)
}

/// Queue one batch of leaf reads and seals on the pool.
fn submit_batch<R, M, const B: usize>(
    source: &Arc<R>,
    mode: M,
    start: u64,
    count: usize,
    size: u64,
) -> Handoff<Result<Vec<SealedLeaf<M, B>>, LeafError>>
where
    R: ReadAt + Send + Sync + 'static,
    M: SplitMode,
    M::Ref: Send,
{
    let source = Arc::clone(source);
    handoff::submit(move || {
        (0..count)
            .into_par_iter()
            .map(|item| {
                let index = start.saturating_add(u64_from_usize(item));
                seal_leaf::<R, M, B>(source.as_ref(), &mode, index, size)
            })
            .collect()
    })
}

/// Read and seal the leaf at `index`; its span is its byte length.
fn seal_leaf<R, M, const B: usize>(
    source: &R,
    mode: &M,
    index: u64,
    size: u64,
) -> Result<SealedLeaf<M, B>, LeafError>
where
    R: ReadAt + ?Sized,
    M: SplitMode,
{
    let body = u64_from_usize(B);
    let offset = index.saturating_mul(body);
    let span = size.saturating_sub(offset).min(body);
    // The span is capped by the body size, so the narrowing is lossless.
    let take = usize::try_from(span).unwrap_or(B);
    let mut data = vec![0u8; take];
    read_full(source, offset, &mut data)?;
    let mut payload = Vec::with_capacity(SPAN_SIZE.saturating_add(take));
    payload.extend_from_slice(&span.to_le_bytes());
    payload.extend_from_slice(&data);
    let (chunk, reference) = mode
        .seal::<B>(Bytes::from(payload))
        .map_err(LeafError::Seal)?;
    Ok(SealedLeaf {
        chunk,
        reference,
        span,
    })
}

/// Fill `buf` from `offset`, looping over short reads; running out of
/// source is an error, never a silent truncation.
fn read_full<R: ReadAt + ?Sized>(source: &R, offset: u64, buf: &mut [u8]) -> Result<(), LeafError> {
    let mut filled = 0usize;
    while filled < buf.len() {
        let at = offset.saturating_add(u64_from_usize(filled));
        let Some(rest) = buf.get_mut(filled..) else {
            return Ok(());
        };
        let capacity = rest.len();
        let count = source
            .read_at(at, rest)
            .map_err(|source| LeafError::Read { offset: at, source })?;
        if count == 0 {
            return Err(LeafError::Short {
                offset: at,
                remaining: capacity,
            });
        }
        if count > capacity {
            return Err(LeafError::Overrun {
                offset: at,
                count,
                capacity,
            });
        }
        filled = filled.saturating_add(count);
    }
    Ok(())
}

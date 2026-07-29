//! Borrowed-store split entry point: an internal relay queue drained into a
//! bounded put window over the borrowed store.
//!
//! The borrowed store is what lets one [`File`](crate::File) handle write
//! through a store it does not own and cannot cheaply clone; the relay keeps
//! the split's own put concurrency on that borrowed store.

use core::convert::Infallible;
use core::future::poll_fn;
use core::num::NonZeroU16;

use alloc::boxed::Box;
use alloc::vec;

#[cfg(not(feature = "std"))]
use alloc::collections::VecDeque;
#[cfg(not(feature = "std"))]
use alloc::rc::Rc;
#[cfg(not(feature = "std"))]
use core::cell::RefCell;
#[cfg(feature = "std")]
use std::collections::VecDeque;
#[cfg(feature = "std")]
use std::sync::{Arc, Mutex, PoisonError};

use nectar_governor::{PutSink, Window};
use nectar_marker::MaybeSync;
use nectar_primitives::chunk::{AnyChunkSet, Chunk, Verified};
use nectar_primitives::store::ChunkPut;

use super::engine::{PutDone, Split};
use super::error::{SaveError, SplitError};
use super::SplitStats;
use super::mode::SplitMode;
#[cfg(test)]
use crate::config::PutWindow;
use crate::handle::Policy;
use crate::source::Source;

/// Drain `src` into a chunk tree, storing every chunk in the borrowed
/// `store`, and return the root plus the write's witnesses.
///
/// The split runs against an internal relay and each sealed chunk is
/// forwarded into a bounded put window borrowing `store`, so up to the
/// policy's put window are concurrently in flight. The memory bound is the
/// split's own: puts in flight stay within the window, buffered chunks
/// within the spine height, and one leaf body is buffered for the pull. The
/// root is delivered only after every put has settled.
///
/// The reported [`SplitStats::peak_put_in_flight`] is the outer window's
/// peak, not the relay's, so it witnesses the real store's concurrency.
pub(crate) async fn save_source<T, M, Src, const B: usize>(
    store: &T,
    policy: Policy,
    mut src: Src,
) -> Result<(M::Root, SplitStats), SaveError<T::Error, Src::Error>>
where
    T: ChunkPut<AnyChunkSet<B>> + MaybeSync,
    M: SplitMode + Default,
    Src: Source,
{
    let window = policy.put_window();
    let relay = Relay::<B>::default();
    #[allow(unused_mut, reason = "the pool fan-out is the only mutator")]
    let mut split: Split<Relay<B>, M, B> = Split::new(relay.clone(), window);
    #[cfg(feature = "rayon")]
    if let Some(hash) = policy.hash_window() {
        split = split.with_hash_window(hash);
    }
    let mut sink: PutSink<'_, PutDone<T::Error>> =
        PutSink::new(Window::from(NonZeroU16::from(window)));
    // Map each settled put's carried address and result to the typed error.
    let fold = |(address, result): PutDone<T::Error>| {
        result.map_err(|source| SplitError::Put { address, source })
    };
    let mut peak = 0usize;
    let mut buf = vec![0u8; B];
    loop {
        let filled = poll_fn(|cx| src.poll_fill(cx, &mut buf))
            .await
            .map_err(|source| SaveError::Source { source })?;
        if filled == 0 {
            break;
        }
        let mut rest = buf.get(..filled).unwrap_or_default();
        while !rest.is_empty() {
            let taken = poll_fn(|cx| split.poll_write(cx, rest))
                .await
                .map_err(widen::<T::Error>)?;
            rest = rest.get(taken..).unwrap_or(&[]);
            // Forward every chunk sealed this round before more bytes enter,
            // so the relay never holds more than one round's seals.
            forward(&relay, store, &mut sink, fold, &mut peak).await?;
        }
    }
    let root = poll_fn(|cx| split.poll_finish(cx))
        .await
        .map_err(widen::<T::Error>)?;
    forward(&relay, store, &mut sink, fold, &mut peak).await?;
    sink.settle(fold).await?;
    let mut stats = split.stats();
    stats.peak_put_in_flight = peak;
    Ok((root, stats))
}

/// Widen the relay-backed split's error to the borrowed store's error. The
/// relay is infallible, so the `Put` arm is unreachable.
fn widen<E>(error: SplitError<Infallible>) -> SplitError<E> {
    match error {
        SplitError::Put { source, .. } => match source {},
        SplitError::Seal(seal) => SplitError::Seal(seal),
        SplitError::SpanOverflow { span, add } => SplitError::SpanOverflow { span, add },
        SplitError::Finished => SplitError::Finished,
        SplitError::Poisoned => SplitError::Poisoned,
        SplitError::PoolDropped => SplitError::PoolDropped,
        SplitError::SpineDepleted => SplitError::SpineDepleted,
    }
}

/// Forward every queued chunk into the bounded window in seal order: admit a
/// slot (parking when full), open the put, then sweep the ready completions so
/// freshly admitted puts start before more bytes enter. `fold` maps each
/// settled put to the typed error, and `peak` records the window's high-water
/// occupancy.
async fn forward<'a, T, F, const B: usize>(
    relay: &Relay<B>,
    store: &'a T,
    sink: &mut PutSink<'a, PutDone<T::Error>>,
    mut fold: F,
    peak: &mut usize,
) -> Result<(), SplitError<T::Error>>
where
    T: ChunkPut<AnyChunkSet<B>> + MaybeSync,
    F: FnMut(PutDone<T::Error>) -> Result<(), SplitError<T::Error>>,
{
    while let Some(chunk) = relay.pop() {
        if let Some(completion) = sink.admit().await {
            fold(completion)?;
        }
        if let Some(completion) = sink.push(Box::pin(async move {
            let address = *chunk.address();
            (address, store.put(chunk).await)
        })) {
            fold(completion)?;
        }
        *peak = (*peak).max(sink.len());
    }
    sink.sweep(fold).await
}

/// Shared put queue bridging a borrowed store to the owned-handle store the
/// split clones per put: relay puts land here in seal order and [`forward`]
/// moves them into the bounded window borrowing the real store, so the
/// split never parks and its put concurrency lands on the borrowed store.
#[derive(Clone, Default)]
struct Relay<const B: usize> {
    #[cfg(feature = "std")]
    queue: Arc<Mutex<VecDeque<Chunk<Verified, AnyChunkSet<B>>>>>,
    /// Single-thread dual: pop and put each borrow within one call, and no
    /// borrow spans an await, so the cell is never held across a suspension.
    #[cfg(not(feature = "std"))]
    queue: Rc<RefCell<VecDeque<Chunk<Verified, AnyChunkSet<B>>>>>,
}

impl<const B: usize> Relay<B> {
    /// The oldest queued chunk; a poisoned lock hands back its inner queue,
    /// which a single push or pop cannot leave inconsistent.
    #[cfg(feature = "std")]
    fn pop(&self) -> Option<Chunk<Verified, AnyChunkSet<B>>> {
        self.queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front()
    }

    /// The oldest queued chunk.
    #[cfg(not(feature = "std"))]
    fn pop(&self) -> Option<Chunk<Verified, AnyChunkSet<B>>> {
        self.queue.borrow_mut().pop_front()
    }
}

impl<const B: usize> ChunkPut<AnyChunkSet<B>> for Relay<B> {
    type Error = Infallible;

    async fn put(&self, chunk: Chunk<Verified, AnyChunkSet<B>>) -> Result<(), Infallible> {
        #[cfg(feature = "std")]
        self.queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back(chunk);
        #[cfg(not(feature = "std"))]
        self.queue.borrow_mut().push_back(chunk);
        Ok(())
    }
}

/// Test-only borrowed-store split of a whole buffer, over the same relay a
/// save drives.
#[cfg(test)]
pub(crate) async fn collect_into<T, M, const B: usize>(
    store: &T,
    window: PutWindow,
    data: &[u8],
) -> Result<M::Root, SplitError<T::Error>>
where
    T: ChunkPut<AnyChunkSet<B>> + MaybeSync,
    M: SplitMode + Default,
{
    match save_source::<T, M, &[u8], B>(store, Policy::DEFAULT.with_put_window(window), data).await {
        Ok((root, _)) => Ok(root),
        Err(SaveError::Split(error)) => Err(error),
        Err(SaveError::Source { source }) => match source {},
    }
}

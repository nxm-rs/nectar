//! Borrowed-store split entry point: an internal relay queue drained into a
//! bounded put window over the borrowed store.

use core::convert::Infallible;
use core::future::poll_fn;
use core::pin::Pin;
use core::task::Poll;

use alloc::boxed::Box;

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

use futures_util::stream::Stream;
use nectar_kernel::{BoxFuture, FuturesUnordered};
use nectar_marker::MaybeSync;
use nectar_primitives::chunk::{AnyChunkSet, Chunk, Verified};
use nectar_primitives::store::ChunkPut;

use super::engine::{PutDone, Split};
use super::error::SplitError;
use super::mode::SplitMode;
use crate::config::PutWindow;

/// Split `data` under put `window` into the tree, storing every chunk in the
/// borrowed `store`, and return the root.
///
/// The borrowed-store companion to [`Split::collect`]: where `collect` owns
/// its store, this drives the split through an internal relay and forwards
/// each sealed chunk into a bounded put window borrowing `store`, so up to
/// `window` puts are concurrently in flight. The memory bound is the split's
/// own: puts in flight stay within `window` and buffered chunks within the
/// spine height. The root is delivered only after every put has settled.
///
/// ```
/// # nectar_testing::run(async {
/// use nectar_file::split::collect_into;
/// use nectar_file::{Plain, PutWindow};
/// use nectar_primitives::chunk::AnyChunkSet;
/// use nectar_primitives::store::MemoryStore;
///
/// let store = MemoryStore::<AnyChunkSet<4096>>::new();
/// let window = PutWindow::new(4).unwrap();
/// let root = collect_into::<_, Plain, 4096>(&store, window, b"hello swarm")
///     .await
///     .unwrap();
/// # let _ = root;
/// # });
/// ```
pub async fn collect_into<T, M, const B: usize>(
    store: &T,
    window: PutWindow,
    data: &[u8],
) -> Result<M::Root, SplitError<T::Error>>
where
    T: ChunkPut<AnyChunkSet<B>> + MaybeSync,
    M: SplitMode + Default,
{
    let relay = Relay::<B>::default();
    let mut split: Split<Relay<B>, M, B> = Split::new(relay.clone(), window);
    let mut puts: FuturesUnordered<BoxFuture<'_, PutDone<T::Error>>> = FuturesUnordered::new();
    let limit = usize::from(window.get());
    let mut rest = data;
    while !rest.is_empty() {
        let taken = poll_fn(|cx| split.poll_write(cx, rest))
            .await
            .map_err(widen::<T::Error>)?;
        rest = rest.get(taken..).unwrap_or(&[]);
        // Forward every chunk sealed this round before more bytes enter, so
        // the relay never holds more than one round's seals.
        drain(&relay, store, &mut puts, limit).await?;
    }
    let root = poll_fn(|cx| split.poll_finish(cx))
        .await
        .map_err(widen::<T::Error>)?;
    drain(&relay, store, &mut puts, limit).await?;
    settle(&mut puts).await?;
    Ok(root)
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

/// Forward every queued chunk into the put window in seal order, parking on a
/// completion whenever the window is full, then start the new puts without
/// parking.
async fn drain<'a, T, const B: usize>(
    relay: &Relay<B>,
    store: &'a T,
    puts: &mut FuturesUnordered<BoxFuture<'a, PutDone<T::Error>>>,
    limit: usize,
) -> Result<(), SplitError<T::Error>>
where
    T: ChunkPut<AnyChunkSet<B>> + MaybeSync,
{
    while let Some(chunk) = relay.pop() {
        while puts.len() >= limit {
            settle_one(puts).await?;
        }
        puts.push(Box::pin(async move {
            let address = *chunk.address();
            (address, store.put(chunk).await)
        }));
    }
    sweep(puts).await
}

/// Await one completion; an empty window is a no-op.
async fn settle_one<E>(
    puts: &mut FuturesUnordered<BoxFuture<'_, PutDone<E>>>,
) -> Result<(), SplitError<E>> {
    match poll_fn(|cx| Pin::new(&mut *puts).poll_next(cx)).await {
        Some((address, result)) => result.map_err(|source| SplitError::Put { address, source }),
        None => Ok(()),
    }
}

/// Await every outstanding put, so the root covers a fully stored tree.
async fn settle<E>(
    puts: &mut FuturesUnordered<BoxFuture<'_, PutDone<E>>>,
) -> Result<(), SplitError<E>> {
    while !puts.is_empty() {
        settle_one(puts).await?;
    }
    Ok(())
}

/// Fold settled puts without parking: every live put is polled once with the
/// task's waker, so freshly admitted puts start before more bytes enter.
async fn sweep<E>(
    puts: &mut FuturesUnordered<BoxFuture<'_, PutDone<E>>>,
) -> Result<(), SplitError<E>> {
    poll_fn(|cx| {
        loop {
            match Pin::new(&mut *puts).poll_next(cx) {
                Poll::Ready(Some((address, result))) => {
                    if let Err(source) = result {
                        return Poll::Ready(Err(SplitError::Put { address, source }));
                    }
                }
                Poll::Ready(None) | Poll::Pending => return Poll::Ready(Ok(())),
            }
        }
    })
    .await
}

/// Shared put queue bridging a borrowed store to the owned-handle store the
/// split clones per put: relay puts land here in seal order and [`drain`]
/// forwards them into the bounded window borrowing the real store, so the
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

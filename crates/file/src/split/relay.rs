//! Borrowed-store split entry point: an internal relay queue drained into a
//! bounded put window over the borrowed store.

use core::convert::Infallible;
use core::future::poll_fn;
use core::num::NonZeroU16;

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

use nectar_kernel::{PutSink, Window};
use nectar_marker::MaybeSync;
use nectar_primitives::chunk::{AnyChunkSet, Chunk, Verified};
use nectar_primitives::store::ChunkPut;

use super::engine::{PutDone, Split};
use super::error::SplitError;
use super::mode::SplitMode;
use crate::config::PutWindow;
#[cfg(feature = "std")]
use crate::num::u64_from_usize;
#[cfg(feature = "std")]
use crate::read_at::{ReadAt, ReadAtError, read_full};

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
    let mut sink: PutSink<'_, PutDone<T::Error>> =
        PutSink::new(Window::from(NonZeroU16::from(window)));
    // Map each settled put's carried address and result to the typed error.
    let fold = |(address, result): PutDone<T::Error>| {
        result.map_err(|source| SplitError::Put { address, source })
    };
    let mut rest = data;
    while !rest.is_empty() {
        let taken = poll_fn(|cx| split.poll_write(cx, rest))
            .await
            .map_err(widen::<T::Error>)?;
        rest = rest.get(taken..).unwrap_or(&[]);
        // Forward every chunk sealed this round before more bytes enter, so
        // the relay never holds more than one round's seals.
        forward(&relay, store, &mut sink, fold).await?;
    }
    let root = poll_fn(|cx| split.poll_finish(cx))
        .await
        .map_err(widen::<T::Error>)?;
    forward(&relay, store, &mut sink, fold).await?;
    sink.settle(fold).await?;
    Ok(root)
}

/// Split `source` under put `window` into the tree, storing every chunk in
/// the borrowed `store`, and return the root.
///
/// The reader companion to [`collect_into`]: leaf bodies are pulled from the
/// random-access `source` one body at a time and fed through the same relay
/// and bounded put window, so the source never becomes fully resident and the
/// memory bound is the split's own plus one leaf body. The root is delivered
/// only after every put has settled; for deterministic modes it equals the
/// slice split of the same bytes.
///
/// ```
/// # nectar_testing::run(async {
/// use nectar_file::split::collect_read_at_into;
/// use nectar_file::{Plain, PutWindow};
/// use nectar_primitives::chunk::AnyChunkSet;
/// use nectar_primitives::store::MemoryStore;
///
/// let store = MemoryStore::<AnyChunkSet<4096>>::new();
/// let window = PutWindow::new(4).unwrap();
/// let root = collect_read_at_into::<_, _, Plain, 4096>(&store, window, &b"hello swarm"[..])
///     .await
///     .unwrap();
/// # let _ = root;
/// # });
/// ```
#[cfg(feature = "std")]
pub async fn collect_read_at_into<R, T, M, const B: usize>(
    store: &T,
    window: PutWindow,
    source: R,
) -> Result<M::Root, ReadAtError<T::Error>>
where
    R: ReadAt,
    T: ChunkPut<AnyChunkSet<B>> + MaybeSync,
    M: SplitMode + Default,
{
    let size = source
        .len()
        .map_err(|source| ReadAtError::Length { source })?;
    let relay = Relay::<B>::default();
    let mut split: Split<Relay<B>, M, B> = Split::new(relay.clone(), window);
    let mut sink: PutSink<'_, PutDone<T::Error>> =
        PutSink::new(Window::from(NonZeroU16::from(window)));
    let fold = |(address, result): PutDone<T::Error>| {
        result.map_err(|source| SplitError::Put { address, source })
    };
    let mut buf = alloc::vec![0u8; B];
    let mut offset = 0u64;
    while offset < size {
        // The remainder is capped by the body size, so the narrowing is
        // lossless and `take` never exceeds the buffer length.
        let take = usize::try_from(size.saturating_sub(offset).min(u64_from_usize(B))).unwrap_or(B);
        let Some((body, _)) = buf.split_at_mut_checked(take) else {
            break;
        };
        read_full(&source, offset, body)?;
        let mut piece: &[u8] = body;
        while !piece.is_empty() {
            let taken = poll_fn(|cx| split.poll_write(cx, piece))
                .await
                .map_err(widen::<T::Error>)?;
            piece = piece.get(taken..).unwrap_or(&[]);
            // Forward every chunk sealed this round before more bytes enter, so
            // the relay never holds more than one round's seals.
            forward(&relay, store, &mut sink, fold).await?;
        }
        offset = offset.saturating_add(u64_from_usize(take));
    }
    let root = poll_fn(|cx| split.poll_finish(cx))
        .await
        .map_err(widen::<T::Error>)?;
    forward(&relay, store, &mut sink, fold).await?;
    sink.settle(fold).await?;
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

/// Forward every queued chunk into the bounded window in seal order: admit a
/// slot (parking when full), open the put, then sweep the ready completions so
/// freshly admitted puts start before more bytes enter. `fold` maps each
/// settled put to the typed error.
async fn forward<'a, T, F, const B: usize>(
    relay: &Relay<B>,
    store: &'a T,
    sink: &mut PutSink<'a, PutDone<T::Error>>,
    mut fold: F,
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

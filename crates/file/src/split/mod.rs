//! Poll-native split engine: the one bounded ascent building a chunk tree.
//!
//! Every write mode feeds this engine, and only its ascent seals
//! intermediates; the optional hash window fans leaf seals onto the rayon
//! pool and admits them in leaf order through this same ascent. The engine
//! is push-driven and io-free (no spawns, channels or timers, beyond the
//! opt-in pool handoff), all state lives in the [`Split`], and sealed chunks
//! flow to the store through a bounded put window.
//!
//! Normative invariants, each pinned by a test:
//!
//! 1. Root identity: the sealed chunk set and root over any byte stream
//!    equal a whole-buffer split of the same bytes, including the lone
//!    trailing reference that carries up unwrapped.
//! 2. Bounded put window: puts in flight never exceed the
//!    [`PutWindow`](crate::PutWindow); sealed chunks awaiting a slot are
//!    bounded by the spine height, and no further bytes are consumed while
//!    any remain.
//! 3. Cancel-safe write: a put slot is secured before any byte is consumed,
//!    so an abandoned `poll_write` consumes nothing.
//! 4. Poisoned fuse: every error is terminal; after one, every poll returns
//!    [`Poisoned`](SplitError::Poisoned). Retry policy composes beneath the
//!    store seam.
//! 5. Fused finish: `poll_finish` is cancel-safe and re-callable; after the
//!    root is delivered every later call returns the same root.
//! 6. Bounded hash window: pool leaf seals in flight never exceed the
//!    [`HashWindow`](crate::HashWindow), and sealed leaves are admitted in
//!    leaf order, so a deterministic mode's chunk stream matches the serial
//!    engine.

#[cfg(feature = "encryption")]
mod encrypted;
mod engine;
mod error;
#[cfg(all(
    feature = "rayon",
    not(target_arch = "wasm32"),
    not(feature = "unsync")
))]
mod handoff;
mod mode;
#[cfg(test)]
mod tests;

use core::convert::Infallible;
use core::future::poll_fn;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};

use nectar_primitives::chunk::{AnyChunkSet, Chunk, Verified};
use nectar_primitives::store::{ChunkPut, MaybeSync};

use crate::config::PutWindow;

#[cfg(feature = "encryption")]
#[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
pub use encrypted::{KeyError, KeySource, RandomKeys};
pub use engine::Split;
pub use error::{SealError, SplitError};
pub use mode::{Sealed, SplitMode};

/// Split `data` under put `window` into the tree, storing every chunk in the
/// borrowed `store`, and return the root.
///
/// The borrowed-store companion to [`Split::collect`]: where `collect` owns
/// its store, this drives the split through an internal relay and forwards
/// each sealed chunk to `store` between poll rounds. At most one put window of
/// chunks is retained, so the memory bound is the split's own: puts in flight
/// stay within `window` and buffered chunks within the spine height.
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
    let mut rest = data;
    while !rest.is_empty() {
        let taken = poll_fn(|cx| split.poll_write(cx, rest))
            .await
            .map_err(widen::<T::Error>)?;
        rest = rest.get(taken..).unwrap_or(&[]);
        // Forward every chunk sealed this round before more bytes enter, so
        // the relay never holds more than the window.
        drain(&relay, store).await?;
    }
    let root = poll_fn(|cx| split.poll_finish(cx))
        .await
        .map_err(widen::<T::Error>)?;
    drain(&relay, store).await?;
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

/// Forward every queued chunk to the borrowed store in seal order, capturing
/// each address before the put consumes the chunk.
async fn drain<T, const B: usize>(relay: &Relay<B>, store: &T) -> Result<(), SplitError<T::Error>>
where
    T: ChunkPut<AnyChunkSet<B>> + MaybeSync,
{
    while let Some(chunk) = relay.pop() {
        let address = *chunk.address();
        store
            .put(chunk)
            .await
            .map_err(|source| SplitError::Put { address, source })?;
    }
    Ok(())
}

/// Shared put queue bridging a borrowed store to the owned-handle store the
/// split clones per put: relay puts land here in seal order and [`drain`]
/// forwards them, so the split never parks and its memory bound carries over.
#[derive(Clone, Default)]
struct Relay<const B: usize> {
    queue: Arc<Mutex<VecDeque<Chunk<Verified, AnyChunkSet<B>>>>>,
}

impl<const B: usize> Relay<B> {
    /// The oldest queued chunk; a poisoned lock hands back its inner queue,
    /// which a single push or pop cannot leave inconsistent.
    fn pop(&self) -> Option<Chunk<Verified, AnyChunkSet<B>>> {
        self.queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front()
    }
}

impl<const B: usize> ChunkPut<AnyChunkSet<B>> for Relay<B> {
    type Error = Infallible;

    async fn put(&self, chunk: Chunk<Verified, AnyChunkSet<B>>) -> Result<(), Infallible> {
        self.queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back(chunk);
        Ok(())
    }
}

/// Occupancy witnesses of one split.
///
/// The peaks pin the engine's memory bounds in tests: puts in flight never
/// exceed the window, and sealed chunks awaiting a slot stay within the
/// spine height.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SplitStats {
    /// File bytes consumed.
    pub bytes: u64,
    /// Leaf chunks sealed.
    pub leaves: u64,
    /// Intermediate chunks sealed.
    pub intermediates: u64,
    /// Store puts dispatched.
    pub puts: u64,
    /// Peak puts in flight.
    pub peak_put_in_flight: usize,
    /// Peak leaf seals in flight on the hash pool; zero on the serial
    /// engine.
    pub peak_hash_in_flight: usize,
    /// Peak sealed chunks awaiting a put slot.
    pub peak_pending: usize,
    /// Spine levels touched.
    pub peak_spine: usize,
}

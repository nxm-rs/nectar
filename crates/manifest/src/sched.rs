//! Read-ahead scheduling shared by the ordered cursor and the dependency
//! traversal: a bounded sliding window of tagged node fetches, generic over
//! the completion payload.
//!
//! The walker owns the walk; the scheduler owns the window. Each walker
//! plans one step at a time through [`Scheduler::fill`], so scheduling
//! mirrors the walk's own termination and launches exactly the nodes the
//! walk fetches, in the order the walk needs them.

use alloc::boxed::Box;
use alloc::vec;
use alloc::vec::Vec;
use core::future::Future;
use core::pin::Pin;

use bytes::Bytes;
use futures_util::stream::{FuturesUnordered, StreamExt};

use crate::format::Format;
use crate::reader::ReaderError;
use crate::scan::Step;

/// A completed fetch tagged with the sequence id it was launched under, so
/// out-of-order completions route back to the descent that awaits them.
type Completion<T> = (usize, Result<T, ReaderError>);

/// An in-flight fetch. Boxed to hold heterogeneous fetch futures in one
/// queue; `Send` on multi-threaded targets, unbounded on wasm32 and under
/// the `unsync` feature.
#[cfg(multi_thread)]
type Fetch<'a, T> = Pin<Box<dyn Future<Output = Completion<T>> + Send + 'a>>;
#[cfg(not(multi_thread))]
type Fetch<'a, T> = Pin<Box<dyn Future<Output = Completion<T>> + 'a>>;

/// Bound on a schedulable fetch: `Send` on multi-threaded targets, unbounded
/// on wasm32 and under the `unsync` feature.
#[cfg(multi_thread)]
pub(crate) trait FetchFuture<'a, T>:
    Future<Output = Result<T, ReaderError>> + Send + 'a
{
}
#[cfg(multi_thread)]
impl<'a, T, Fut> FetchFuture<'a, T> for Fut where
    Fut: Future<Output = Result<T, ReaderError>> + Send + 'a
{
}
#[cfg(not(multi_thread))]
pub(crate) trait FetchFuture<'a, T>: Future<Output = Result<T, ReaderError>> + 'a {}
#[cfg(not(multi_thread))]
impl<'a, T, Fut> FetchFuture<'a, T> for Fut where Fut: Future<Output = Result<T, ReaderError>> + 'a {}

/// One chunk's ordered contents plus the walk position within them.
#[derive(Clone, Debug)]
pub(crate) struct Frame<F: Format> {
    /// Key bytes consumed to reach this chunk's root; empty for a walker
    /// that does not track keys.
    pub(crate) base: Bytes,
    /// The chunk's steps in ascending key order.
    pub(crate) steps: Vec<Step<F>>,
    /// The next step to visit.
    pub(crate) index: usize,
    /// Per-step prefetch tag, parallel to `steps`: the sequence id a
    /// referenced child was launched under, once the window scheduled it.
    sched: Vec<Option<usize>>,
}

impl<F: Format> Frame<F> {
    /// A frame over `steps`, resuming at `index`, with an empty prefetch tag.
    pub(crate) fn new(base: Bytes, steps: Vec<Step<F>>, index: usize) -> Self {
        let sched = vec![None; steps.len()];
        Self {
            base,
            steps,
            index,
            sched,
        }
    }

    /// The sequence id the step at `index` was launched under, if scheduled.
    pub(crate) fn tag(&self, index: usize) -> Option<usize> {
        self.sched.get(index).copied().flatten()
    }
}

/// What the walker plans for one step ahead of the walk.
pub(crate) enum Plan<Fut> {
    /// Nothing to fetch at this step.
    Skip,
    /// The walk stops at this step; nothing at or beyond it is fetched.
    Stop,
    /// Fetch the referenced child at this step.
    Fetch(Fut),
}

/// A sliding read-ahead window of tagged node fetches, generic over the
/// completion payload.
///
/// In-flight fetches plus buffered completions never exceed the cap passed
/// to [`fill`](Self::fill). All state lives in `self`, so a cancelled await
/// on [`take`](Self::take) loses no completions.
#[derive(Debug)]
pub(crate) struct Scheduler<'a, T> {
    /// Fetches launched by the window, awaiting completion.
    inflight: FuturesUnordered<Fetch<'a, T>>,
    /// Completions that arrived before the descent awaiting them; drained by
    /// sequence id. Bounded with `inflight` by the window.
    ready: Vec<Completion<T>>,
    /// The next fetch sequence id to hand out.
    next_seq: usize,
}

impl<'a, T> Scheduler<'a, T> {
    /// An empty window.
    pub(crate) fn new() -> Self {
        Self {
            inflight: FuturesUnordered::new(),
            ready: Vec::new(),
            next_seq: 0,
        }
    }

    /// Fill the window: walk the unvisited steps of `frames` and launch each
    /// planned fetch, until at most `cap` fetches are in the window.
    ///
    /// Deepest frame first is ascending-key order from the walk, so the
    /// child needed soonest is always launched first and never starved.
    /// [`Plan::Stop`] ends scheduling where the walk itself stops, and a
    /// step already tagged is never relaunched.
    pub(crate) fn fill<F, Fut>(
        &mut self,
        cap: usize,
        frames: &mut [Frame<F>],
        mut plan: impl FnMut(&Bytes, &Step<F>) -> Plan<Fut>,
    ) where
        F: Format,
        Fut: FetchFuture<'a, T>,
    {
        'outer: for frame in frames.iter_mut().rev() {
            let mut index = frame.index;
            while let Some(step) = frame.steps.get(index) {
                if self.inflight.len().saturating_add(self.ready.len()) >= cap {
                    break 'outer;
                }
                match plan(&frame.base, step) {
                    Plan::Stop => break 'outer,
                    Plan::Skip => {}
                    Plan::Fetch(fetch) => {
                        if !matches!(frame.sched.get(index), Some(Some(_))) {
                            let seq = self.next_seq;
                            self.next_seq = self.next_seq.saturating_add(1);
                            if let Some(slot) = frame.sched.get_mut(index) {
                                *slot = Some(seq);
                            }
                            self.inflight.push(box_fetch(seq, fetch));
                        }
                    }
                }
                index = index.saturating_add(1);
            }
        }
    }

    /// The completion launched under `seq`: drive in-flight fetches until it
    /// arrives, buffering earlier-arriving completions. `None` when the
    /// launch is unaccounted for and the caller must fetch directly.
    pub(crate) async fn take(&mut self, seq: usize) -> Option<Result<T, ReaderError>> {
        if let Some(pos) = self.ready.iter().position(|(other, _)| *other == seq) {
            return Some(self.ready.swap_remove(pos).1);
        }
        loop {
            match self.inflight.next().await {
                Some((other, result)) if other == seq => return Some(result),
                Some(pair) => self.ready.push(pair),
                None => return None,
            }
        }
    }
}

/// Tag `fetch` with `seq` and box it into the window's queue shape.
fn box_fetch<'a, T, Fut>(seq: usize, fetch: Fut) -> Fetch<'a, T>
where
    Fut: FetchFuture<'a, T>,
{
    Box::pin(async move { (seq, fetch.await) })
}

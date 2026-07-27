//! Frontier bookkeeping shared by the manifest walk policies: per-step
//! prefetch tags and the bounded fill that admits fetches into the kernel
//! in-flight set.
//!
//! The walker owns the walk; the fill owns the window. Each policy plans one
//! step at a time, so admission mirrors the walk's own termination and
//! launches exactly the nodes the walk fetches, in the order the walk needs
//! them.

use alloc::boxed::Box;
use alloc::vec;
use alloc::vec::Vec;
use core::future::Future;

use bytes::Bytes;
use nectar_kernel::{BoxFuture, InFlight};

use crate::format::Format;
use crate::reader::ReaderError;
use crate::scan::Step;

/// A completed fetch tagged with the sequence id it was launched under, so
/// out-of-order completions route back to the descent that awaits them.
pub(crate) type Completion<T> = (usize, Result<T, ReaderError>);

/// Bound on an admissible fetch: `Send` on multi-threaded targets, unbounded
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
    /// referenced child was launched under, once the window admitted it.
    tags: Vec<Option<usize>>,
}

impl<F: Format> Frame<F> {
    /// A frame over `steps`, resuming at `index`, with no prefetch tags.
    pub(crate) fn new(base: Bytes, steps: Vec<Step<F>>, index: usize) -> Self {
        let tags = vec![None; steps.len()];
        Self {
            base,
            steps,
            index,
            tags,
        }
    }

    /// The sequence id the step at `index` was launched under, if admitted.
    pub(crate) fn tag(&self, index: usize) -> Option<usize> {
        self.tags.get(index).copied().flatten()
    }

    /// Clear the tag at `index`: its launch is spent, so the next fill
    /// relaunches the step.
    pub(crate) fn clear_tag(&mut self, index: usize) {
        if let Some(slot) = self.tags.get_mut(index) {
            *slot = None;
        }
    }

    /// Record the sequence id the step at `index` was launched under.
    fn set_tag(&mut self, index: usize, seq: usize) {
        if let Some(slot) = self.tags.get_mut(index) {
            *slot = Some(seq);
        }
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

/// Fill the window: walk the unvisited steps of `frames` and launch each
/// planned fetch into `in_flight`, until at most `window` fetches are
/// outstanding, in flight plus `buffered`.
///
/// Deepest frame first is ascending-key order from the walk, so the child
/// needed soonest is always launched first and never starved. The walk's
/// next needed fetch always launches, window full or not, mirroring the
/// serial walk's own fetch; only the lookahead behind it is capped.
/// [`Plan::Stop`] ends admission where the walk itself stops, and a step
/// already tagged is never relaunched.
pub(crate) fn fill<'a, F, T, Fut>(
    window: usize,
    buffered: usize,
    next_seq: &mut usize,
    frames: &mut [Frame<F>],
    in_flight: &mut InFlight<'a, Completion<T>>,
    mut plan: impl FnMut(&Bytes, &Step<F>) -> Plan<Fut>,
) where
    F: Format,
    Fut: FetchFuture<'a, T>,
{
    let mut head_served = false;
    'outer: for frame in frames.iter_mut().rev() {
        let mut index = frame.index;
        while let Some(step) = frame.steps.get(index) {
            match plan(&frame.base, step) {
                Plan::Stop => break 'outer,
                Plan::Skip => {}
                Plan::Fetch(fetch) => {
                    if frame.tag(index).is_some() {
                        head_served = true;
                    } else {
                        let occupancy = in_flight.len().saturating_add(buffered);
                        if head_served && occupancy >= window {
                            break 'outer;
                        }
                        let seq = *next_seq;
                        *next_seq = next_seq.saturating_add(1);
                        frame.set_tag(index, seq);
                        in_flight.push(box_fetch(seq, fetch));
                        head_served = true;
                    }
                }
            }
            index = index.saturating_add(1);
        }
    }
}

/// The completion launched under `seq`, once it has landed in `ready`.
pub(crate) fn claim<T>(
    ready: &mut Vec<Completion<T>>,
    seq: usize,
) -> Option<Result<T, ReaderError>> {
    let position = ready.iter().position(|(tag, _)| *tag == seq)?;
    Some(ready.swap_remove(position).1)
}

/// Tag `fetch` with `seq` and box it into the in-flight queue shape.
fn box_fetch<'a, T, Fut>(seq: usize, fetch: Fut) -> BoxFuture<'a, Completion<T>>
where
    Fut: FetchFuture<'a, T>,
{
    Box::pin(async move { (seq, fetch.await) })
}

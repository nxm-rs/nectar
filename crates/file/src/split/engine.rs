//! The bounded ascent state machine.

use alloc::boxed::Box;
use alloc::collections::VecDeque;
use alloc::vec::Vec;
use core::fmt;
use core::future::Future;
use core::mem;
use core::pin::Pin;
use core::task::{Context, Poll};

use bytes::Bytes;
use futures_util::stream::{FuturesUnordered, Stream};
use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::bmt::SPAN_SIZE;
use nectar_primitives::chunk::{AnyChunkSet, Chunk, ChunkAddress, Verified};
use nectar_primitives::store::ChunkPut;

use super::SplitStats;
use super::error::SplitError;
use super::mode::SplitMode;
use crate::config::PutWindow;
use crate::num::{fan_out, u64_from_u32, u64_from_usize};

/// Completion payload; the future carries the chunk's address back for the
/// error context.
type PutDone<E> = (ChunkAddress, Result<(), E>);

/// Boxed put future: `Send` on multi-threaded targets, unbounded on wasm32
/// and under the `unsync` feature.
#[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
type BoxPut<E> = Pin<Box<dyn Future<Output = PutDone<E>> + Send>>;
/// Boxed put future: `Send` on multi-threaded targets, unbounded on wasm32
/// and under the `unsync` feature.
#[cfg(any(target_arch = "wasm32", feature = "unsync"))]
type BoxPut<E> = Pin<Box<dyn Future<Output = PutDone<E>>>>;

/// One spine level: references awaiting a close and the bytes they span.
struct Level<M: SplitMode> {
    refs: Vec<M::Ref>,
    span: u64,
}

impl<M: SplitMode> Level<M> {
    const fn new() -> Self {
        Self {
            refs: Vec::new(),
            span: 0,
        }
    }
}

/// Lifecycle of a split; the finished and poisoned phases are fuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Accepting writes.
    Writing,
    /// Tail leaf sealed; closing the spine bottom up.
    Closing,
    /// Root chosen; draining the put window.
    Draining,
    /// Root delivered; every later finish returns it again.
    Finished,
    /// A terminal failure; every later poll returns `Poisoned`.
    Poisoned,
}

/// The one poll-native split: a bounded, spill-on-close ascent building a
/// chunk tree over a byte stream.
///
/// All state lives here, so every poll is cancel-safe and dropping the
/// split loses only in-flight puts. The module docs state the normative
/// admission invariants.
pub struct Split<S, M, const B: usize = DEFAULT_BODY_SIZE>
where
    S: ChunkPut<AnyChunkSet<B>>,
    M: SplitMode,
{
    store: S,
    /// Data-carrying reference slots per intermediate, from the mode.
    slots: u64,
    window: usize,
    /// Partial tail leaf, always shorter than the body.
    leaf: Vec<u8>,
    /// Spine frontier: per-level references awaiting a close, bottom up.
    spine: Vec<Level<M>>,
    /// Sealed chunks awaiting a put slot; bounded by the spine height.
    pending: VecDeque<Chunk<Verified, AnyChunkSet<B>>>,
    in_flight: FuturesUnordered<BoxPut<S::Error>>,
    phase: Phase,
    root: Option<M::Root>,
    stats: SplitStats,
}

impl<S, M, const B: usize> Split<S, M, B>
where
    S: ChunkPut<AnyChunkSet<B>> + Clone + 'static,
    M: SplitMode,
{
    /// Compile-time profile guard for the split's span arithmetic.
    const PROFILE: () = {
        assert!(B.is_power_of_two(), "body size must be a power of two");
        assert!(
            u64_from_usize(B) <= u64_from_u32(u32::MAX),
            "body size must fit the u32 geometry"
        );
        let fan = fan_out(u64_from_usize(B), u64_from_u32(M::MODE.ref_size()));
        assert!(fan >= 2, "fan-out must be at least two");
    };

    /// Split a byte stream into the tree under `store`, holding at most
    /// `window` puts in flight.
    pub fn new(store: S, window: PutWindow) -> Self {
        const { Self::PROFILE };
        let branches = fan_out(u64_from_usize(B), u64_from_u32(M::MODE.ref_size()));
        // A close must shrink its level, so the seam floors at two slots.
        let slots = M::data_slots(branches).max(2);
        Self {
            store,
            slots,
            window: usize::from(window.get()),
            leaf: Vec::new(),
            spine: Vec::new(),
            pending: VecDeque::new(),
            in_flight: FuturesUnordered::new(),
            phase: Phase::Writing,
            root: None,
            stats: SplitStats::default(),
        }
    }

    /// Occupancy witnesses accumulated so far.
    pub const fn stats(&self) -> SplitStats {
        self.stats
    }

    /// Whether the split has delivered its root or failed.
    pub const fn is_finished(&self) -> bool {
        matches!(self.phase, Phase::Finished | Phase::Poisoned)
    }

    /// Consume bytes from `buf`, sealing and spilling as leaves fill; at
    /// most one leaf body is consumed per call.
    ///
    /// Cancel-safe: a put slot is secured before any byte is consumed, so a
    /// poll that returns `Pending` has consumed nothing.
    pub fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, SplitError<S::Error>>> {
        match self.phase {
            Phase::Writing => {}
            Phase::Poisoned => return Poll::Ready(Err(SplitError::Poisoned)),
            Phase::Closing | Phase::Draining | Phase::Finished => {
                return Poll::Ready(Err(SplitError::Finished));
            }
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if let Err(error) = self.step_puts(cx) {
            return Poll::Ready(Err(self.poison(error)));
        }
        if !self.pending.is_empty() || self.in_flight.len() >= self.window {
            return Poll::Pending;
        }
        let take = buf.len().min(B.saturating_sub(self.leaf.len()));
        let Some((bytes, _)) = buf.split_at_checked(take) else {
            return Poll::Ready(Ok(0));
        };
        self.leaf.extend_from_slice(bytes);
        self.stats.bytes = self.stats.bytes.saturating_add(u64_from_usize(take));
        if self.leaf.len() == B {
            let data = mem::take(&mut self.leaf);
            if let Err(error) = self.spill_leaf(data) {
                return Poll::Ready(Err(self.poison(error)));
            }
        }
        Poll::Ready(Ok(take))
    }

    /// Drive the split to its root: seal the tail, close the spine bottom
    /// up, drain the put window, then deliver the root.
    ///
    /// Cancel-safe and fused: all progress lives in `self`, an abandoned
    /// call loses nothing, and every call after the first delivery returns
    /// the same root again.
    pub fn poll_finish(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<M::Root, SplitError<S::Error>>> {
        loop {
            match self.phase {
                Phase::Poisoned => return Poll::Ready(Err(SplitError::Poisoned)),
                Phase::Finished => return Poll::Ready(self.delivered()),
                Phase::Writing | Phase::Closing => {
                    if let Err(error) = self.step_puts(cx) {
                        return Poll::Ready(Err(self.poison(error)));
                    }
                    if !self.pending.is_empty() || self.in_flight.len() >= self.window {
                        return Poll::Pending;
                    }
                    let step = if let Phase::Writing = self.phase {
                        self.flush_tail()
                    } else {
                        self.close_step()
                    };
                    if let Err(error) = step {
                        return Poll::Ready(Err(self.poison(error)));
                    }
                }
                Phase::Draining => {
                    if let Err(error) = self.step_puts(cx) {
                        return Poll::Ready(Err(self.poison(error)));
                    }
                    if self.pending.is_empty() && self.in_flight.is_empty() {
                        self.phase = Phase::Finished;
                        continue;
                    }
                    return Poll::Pending;
                }
            }
        }
    }

    /// The fused root; its absence past the finish is a broken invariant.
    fn delivered(&mut self) -> Result<M::Root, SplitError<S::Error>> {
        match &self.root {
            Some(root) => Ok(root.clone()),
            None => Err(self.poison(SplitError::SpineDepleted)),
        }
    }

    /// Fuse the split shut, handing the terminal error back.
    const fn poison(&mut self, error: SplitError<S::Error>) -> SplitError<S::Error> {
        self.phase = Phase::Poisoned;
        error
    }

    /// Seal the tail leaf (or the empty stream's empty chunk) and enter the
    /// closing phase.
    fn flush_tail(&mut self) -> Result<(), SplitError<S::Error>> {
        if !self.leaf.is_empty() || self.stats.bytes == 0 {
            let data = mem::take(&mut self.leaf);
            self.spill_leaf(data)?;
        }
        self.phase = Phase::Closing;
        Ok(())
    }

    /// Close the lowest occupied level: carry a lone reference up for free,
    /// seal a filled group, or crown the root at the top.
    fn close_step(&mut self) -> Result<(), SplitError<S::Error>> {
        loop {
            let Some(at) = self.spine.iter().position(|level| !level.refs.is_empty()) else {
                return Err(SplitError::SpineDepleted);
            };
            let above = at.saturating_add(1);
            let top = self
                .spine
                .iter()
                .skip(above)
                .all(|level| level.refs.is_empty());
            let Some(level) = self.spine.get_mut(at) else {
                return Err(SplitError::SpineDepleted);
            };
            if top && level.refs.len() == 1 {
                let refs = mem::take(&mut level.refs);
                level.span = 0;
                let Some(only) = refs.into_iter().next() else {
                    return Err(SplitError::SpineDepleted);
                };
                self.root = Some(M::into_root(only));
                self.phase = Phase::Draining;
                return Ok(());
            }
            let refs = mem::take(&mut level.refs);
            let span = mem::replace(&mut level.span, 0);
            if let [only] = refs.as_slice() {
                // A lone reference carries up unwrapped: wrapping it would
                // declare a span its single child already covers.
                self.push_carry(above, only.clone(), span)?;
                continue;
            }
            let sealed = self.seal_intermediate(span, &refs)?;
            self.push_carry(above, sealed, span)?;
            // One seal per step; the caller re-secures put capacity.
            return Ok(());
        }
    }

    /// Seal one leaf payload and thread its reference into the spine.
    fn spill_leaf(&mut self, data: Vec<u8>) -> Result<(), SplitError<S::Error>> {
        let span = u64_from_usize(data.len());
        let mut payload = Vec::with_capacity(SPAN_SIZE.saturating_add(data.len()));
        payload.extend_from_slice(&span.to_le_bytes());
        payload.extend_from_slice(&data);
        let (chunk, reference) = M::seal::<B>(Bytes::from(payload))?;
        self.stats.leaves = self.stats.leaves.saturating_add(1);
        self.enqueue(chunk);
        self.push_ref(0, reference, span)
    }

    /// Thread a reference into the spine at `at`, closing and spilling
    /// every level it fills.
    fn push_ref(
        &mut self,
        at: usize,
        reference: M::Ref,
        span: u64,
    ) -> Result<(), SplitError<S::Error>> {
        let mut at = at;
        let mut reference = reference;
        let mut span = span;
        loop {
            self.push_carry(at, reference, span)?;
            let Some(level) = self.spine.get_mut(at) else {
                return Err(SplitError::SpineDepleted);
            };
            if u64_from_usize(level.refs.len()) < self.slots {
                return Ok(());
            }
            let refs = mem::take(&mut level.refs);
            let closed = mem::replace(&mut level.span, 0);
            reference = self.seal_intermediate(closed, &refs)?;
            span = closed;
            at = at.saturating_add(1);
        }
    }

    /// Append a reference at `at` without the close-on-full check; the
    /// closing phase wraps each level as it reaches it.
    fn push_carry(
        &mut self,
        at: usize,
        reference: M::Ref,
        span: u64,
    ) -> Result<(), SplitError<S::Error>> {
        while self.spine.len() <= at {
            self.spine.push(Level::new());
        }
        self.stats.peak_spine = self.stats.peak_spine.max(self.spine.len());
        let Some(level) = self.spine.get_mut(at) else {
            return Err(SplitError::SpineDepleted);
        };
        let sum = level
            .span
            .checked_add(span)
            .ok_or(SplitError::SpanOverflow {
                span: level.span,
                add: span,
            })?;
        level.refs.push(reference);
        level.span = sum;
        Ok(())
    }

    /// Seal one intermediate payload from its children's references.
    fn seal_intermediate(
        &mut self,
        span: u64,
        refs: &[M::Ref],
    ) -> Result<M::Ref, SplitError<S::Error>> {
        let ref_size = usize::try_from(M::MODE.ref_size()).unwrap_or(usize::MAX);
        let capacity = SPAN_SIZE.saturating_add(refs.len().saturating_mul(ref_size));
        let mut payload = Vec::with_capacity(capacity);
        payload.extend_from_slice(&span.to_le_bytes());
        for reference in refs {
            M::write_ref(reference, &mut payload);
        }
        let (chunk, reference) = M::seal::<B>(Bytes::from(payload))?;
        self.stats.intermediates = self.stats.intermediates.saturating_add(1);
        self.enqueue(chunk);
        Ok(reference)
    }

    /// Queue a sealed chunk for the put window, admitting what fits.
    fn enqueue(&mut self, chunk: Chunk<Verified, AnyChunkSet<B>>) {
        self.pending.push_back(chunk);
        self.admit();
        self.stats.peak_pending = self.stats.peak_pending.max(self.pending.len());
    }

    /// Move pending chunks into the put window while slots are free.
    fn admit(&mut self) {
        while self.in_flight.len() < self.window {
            let Some(chunk) = self.pending.pop_front() else {
                return;
            };
            self.dispatch(chunk);
        }
    }

    /// Start one put, moving the chunk into its future; the completion
    /// carries the address back.
    fn dispatch(&mut self, chunk: Chunk<Verified, AnyChunkSet<B>>) {
        let store = self.store.clone();
        let put: BoxPut<S::Error> = Box::pin(async move {
            let address = *chunk.address();
            (address, store.put(chunk).await)
        });
        self.in_flight.push(put);
        self.stats.puts = self.stats.puts.saturating_add(1);
        self.stats.peak_put_in_flight = self.stats.peak_put_in_flight.max(self.in_flight.len());
    }

    /// Fold completed puts back in and admit pending chunks; a failed put
    /// is terminal.
    fn step_puts(&mut self, cx: &mut Context<'_>) -> Result<(), SplitError<S::Error>> {
        loop {
            self.admit();
            match Pin::new(&mut self.in_flight).poll_next(cx) {
                Poll::Ready(Some((address, result))) => {
                    result.map_err(|source| SplitError::Put { address, source })?;
                }
                Poll::Ready(None) | Poll::Pending => return Ok(()),
            }
        }
    }
}

impl<S, M, const B: usize> fmt::Debug for Split<S, M, B>
where
    S: ChunkPut<AnyChunkSet<B>>,
    M: SplitMode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Split")
            .field("phase", &self.phase)
            .field("window", &self.window)
            .field("slots", &self.slots)
            .field("leaf_len", &self.leaf.len())
            .field("spine_levels", &self.spine.len())
            .field("pending", &self.pending.len())
            .field("in_flight", &self.in_flight.len())
            .finish_non_exhaustive()
    }
}

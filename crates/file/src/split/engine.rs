//! The bounded ascent state machine.

use alloc::boxed::Box;
use alloc::collections::VecDeque;
use alloc::vec::Vec;
use core::fmt;
#[cfg(test)]
use core::future::poll_fn;
use core::mem;
use core::num::NonZeroU16;
use core::task::{Context, Poll};

use bytes::Bytes;
#[cfg(feature = "rayon")]
use futures_util::stream::{FuturesOrdered, StreamExt};
use nectar_governor::{PutSink, Window};
use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::bmt::SPAN_SIZE;
use nectar_primitives::chunk::{AnyChunkSet, Chunk, ChunkAddress, Verified};
use nectar_primitives::store::ChunkPut;
use nectar_tasks::BoxFuture;

use super::SplitStats;
use super::error::{SealError, SplitError};
#[cfg(feature = "rayon")]
use super::handoff::{self, Handoff};
use super::mode::{Sealed, SplitMode};
#[cfg(feature = "rayon")]
use crate::config::HashWindow;
use crate::config::PutWindow;
use crate::num::{fan_out, u64_from_u32, u64_from_usize};

/// Completion payload; the future carries the chunk's address back for the
/// error context.
pub(super) type PutDone<E> = (ChunkAddress, Result<(), E>);

/// Boxed put future; the alias relaxes `Send` off the multi-threaded
/// targets.
type BoxPut<'a, E> = BoxFuture<'a, PutDone<E>>;

/// Pool leaf seal reply: its span rides back alongside the sealed chunk so
/// the ordered stream needs no side channel.
#[cfg(feature = "rayon")]
type SealOut<M, const B: usize> = (u64, Result<Sealed<M, B>, SealError>);

/// Handoff carrying one pool leaf seal back to the engine.
#[cfg(feature = "rayon")]
type SealHandoff<M, const B: usize> = Handoff<SealOut<M, B>>;

/// Submitter queueing one leaf payload on the pool under its drawn state,
/// tagging the reply with the leaf's span.
#[cfg(feature = "rayon")]
type SealSubmit<M, const B: usize> =
    Box<dyn Fn(<M as SplitMode>::Draw, Bytes, u64) -> SealHandoff<M, B> + Send + Sync>;

/// Pool fan-out for leaf seals: an ordered stream of in-flight jobs and the
/// submitter that queues one payload.
///
/// `FuturesOrdered` yields in submission order and parks out-of-order
/// completions, so admission stays in leaf order while wakers register for
/// every in-flight seal, not just the head.
#[cfg(feature = "rayon")]
struct HashFan<M: SplitMode, const B: usize> {
    window: usize,
    submit: SealSubmit<M, B>,
    seals: FuturesOrdered<SealHandoff<M, B>>,
    /// Draws stashed at submission for the ascent seals the queued leaves
    /// trigger on admission; spent front-first.
    draws: VecDeque<M::Draw>,
    /// Leaves submitted so far; positions the ascent draws.
    submitted: u64,
}

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
pub struct Split<'a, S, M, const B: usize = DEFAULT_BODY_SIZE>
where
    S: ChunkPut<AnyChunkSet<B>>,
    M: SplitMode,
{
    store: S,
    mode: M,
    /// Data-carrying reference slots per intermediate, from the mode.
    slots: u64,
    /// Tail leaf payload under build: the span placeholder, then content
    /// shorter than one body; empty between leaves.
    leaf: Vec<u8>,
    /// Spine frontier: per-level references awaiting a close, bottom up.
    spine: Vec<Level<M>>,
    /// Sealed chunks awaiting a put slot; bounded by the spine height.
    pending: VecDeque<Chunk<Verified, AnyChunkSet<B>>>,
    /// Bounded put window over sealed chunks; `pending` feeds it as slots free.
    puts: PutSink<BoxPut<'a, S::Error>>,
    /// Pool fan-out for leaf seals; `None` keeps sealing inline.
    #[cfg(feature = "rayon")]
    hash: Option<HashFan<M, B>>,
    phase: Phase,
    root: Option<M::Root>,
    stats: SplitStats,
}

impl<'a, S, M, const B: usize> Split<'a, S, M, B>
where
    S: ChunkPut<AnyChunkSet<B>> + Clone + 'a,
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
    #[cfg(test)]
    pub fn new(store: S, window: PutWindow) -> Self
    where
        M: Default,
    {
        Self::with_mode(store, M::default(), window)
    }

    /// Split through an explicitly constructed `mode` (a keyed encrypted
    /// mode); otherwise identical to [`new`](Self::new).
    pub fn with_mode(store: S, mode: M, window: PutWindow) -> Self {
        const { Self::PROFILE };
        let branches = fan_out(u64_from_usize(B), u64_from_u32(M::MODE.ref_size()));
        // A close must shrink its level, so the seam floors at two slots.
        let slots = M::data_slots(branches).max(2);
        Self {
            store,
            mode,
            slots,
            leaf: Vec::new(),
            spine: Vec::new(),
            pending: VecDeque::new(),
            puts: PutSink::new(Window::from(NonZeroU16::from(window))),
            #[cfg(feature = "rayon")]
            hash: None,
            phase: Phase::Writing,
            root: None,
            stats: SplitStats::default(),
        }
    }

    /// Fan leaf sealing onto the rayon pool, holding at most `window` seals
    /// in flight; sealed leaves are admitted in leaf order and every draw
    /// lands at submission, so a deterministic mode's chunk stream is
    /// byte-identical to the serial engine's. Configure before the first
    /// write. [`Policy::with_hash_window`](crate::Policy::with_hash_window)
    /// is the public knob.
    #[cfg(feature = "rayon")]
    #[cfg_attr(docsrs, doc(cfg(feature = "rayon")))]
    #[must_use]
    pub fn with_hash_window(mut self, window: HashWindow) -> Self
    where
        M::Ref: Send,
    {
        self.hash = Some(HashFan {
            window: usize::from(window.get()),
            submit: Box::new(|draw, payload, span| {
                handoff::submit(move || (span, M::seal_with::<B>(draw, payload)))
            }),
            seals: FuturesOrdered::new(),
            draws: VecDeque::new(),
            submitted: 0,
        });
        self
    }

    /// Occupancy witnesses accumulated so far.
    pub const fn stats(&self) -> SplitStats {
        self.stats
    }

    /// Whether the split has delivered its root or failed.
    #[cfg(test)]
    pub const fn is_finished(&self) -> bool {
        matches!(self.phase, Phase::Finished | Phase::Poisoned)
    }

    /// Consume bytes from `buf`, sealing and spilling as leaves fill; at
    /// most one leaf body is consumed per call.
    ///
    /// Cancel-safe: a put slot (or a hash slot when the pool fan-out is on)
    /// is secured before any byte is consumed, so a poll that returns
    /// `Pending` has consumed nothing.
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
        #[cfg(feature = "rayon")]
        if let Err(error) = self.drain_seals(cx) {
            return Poll::Ready(Err(self.poison(error)));
        }
        if self.write_gate_blocked() {
            return Poll::Pending;
        }
        let take = buf.len().min(B.saturating_sub(self.leaf_len()));
        let Some((bytes, _)) = buf.split_at_checked(take) else {
            return Poll::Ready(Ok(0));
        };
        self.begin_leaf();
        self.leaf.extend_from_slice(bytes);
        self.stats.bytes = self.stats.bytes.saturating_add(u64_from_usize(take));
        if self.leaf_len() == B {
            let payload = mem::take(&mut self.leaf);
            if let Err(error) = self.spill_leaf(payload) {
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
                    // Every pool leaf seal must land before the tail seals
                    // and the spine closes, so the ascent stays in leaf
                    // order.
                    #[cfg(feature = "rayon")]
                    {
                        if let Err(error) = self.drain_seals(cx) {
                            return Poll::Ready(Err(self.poison(error)));
                        }
                        if self.seals_queued() > 0 {
                            return Poll::Pending;
                        }
                    }
                    if !self.pending.is_empty() || !self.puts.admits() {
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
                    if self.pending.is_empty() && self.puts.is_empty() {
                        self.phase = Phase::Finished;
                        continue;
                    }
                    return Poll::Pending;
                }
            }
        }
    }

    /// Split all of `data`, store its chunks, and return the root under an
    /// explicit put `window`.
    ///
    /// A test-only one-shot over [`poll_write`](Self::poll_write) and
    /// [`poll_finish`](Self::poll_finish); production writes ride
    /// [`File::save`](crate::File::save), which drives the same poll surface
    /// through a borrowed store.
    #[cfg(test)]
    pub async fn collect_with(
        store: S,
        window: PutWindow,
        data: &[u8],
    ) -> Result<M::Root, SplitError<S::Error>>
    where
        M: Default,
    {
        let mut split = Self::new(store, window);
        let mut rest = data;
        while !rest.is_empty() {
            let taken = poll_fn(|cx| split.poll_write(cx, rest)).await?;
            rest = rest.get(taken..).unwrap_or(&[]);
        }
        poll_fn(|cx| split.poll_finish(cx)).await
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
            self.begin_leaf();
            let mut payload = mem::take(&mut self.leaf);
            // Give the reserved slack back before the payload is pinned in
            // the sealed chunk.
            payload.shrink_to_fit();
            self.spill_leaf(payload)?;
        }
        self.phase = Phase::Closing;
        Ok(())
    }

    /// Content bytes in the tail leaf, behind its span placeholder.
    const fn leaf_len(&self) -> usize {
        self.leaf.len().saturating_sub(SPAN_SIZE)
    }

    /// Reserve one payload and lay down the span placeholder; a no-op once
    /// the leaf is started.
    fn begin_leaf(&mut self) {
        if self.leaf.is_empty() {
            self.leaf.reserve_exact(SPAN_SIZE.saturating_add(B));
            self.leaf.extend_from_slice(&[0u8; SPAN_SIZE]);
        }
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

    /// Patch the span into a finished leaf payload, seal it (inline or on
    /// the pool) and thread its reference into the spine.
    fn spill_leaf(&mut self, mut payload: Vec<u8>) -> Result<(), SplitError<S::Error>> {
        let span = u64_from_usize(payload.len().saturating_sub(SPAN_SIZE));
        if let Some((head, _)) = payload.split_first_chunk_mut::<SPAN_SIZE>() {
            *head = span.to_le_bytes();
        }
        #[cfg(feature = "rayon")]
        if let Some(fan) = &mut self.hash {
            // The leaf's draw, then one per ascent seal its admission
            // triggers, so the draw order equals the serial engine's.
            let draw = self.mode.draw()?;
            fan.submitted = fan.submitted.saturating_add(1);
            let mut filled = fan.submitted;
            while filled.checked_rem(self.slots) == Some(0) {
                fan.draws.push_back(self.mode.draw()?);
                let Some(next) = filled.checked_div(self.slots) else {
                    break;
                };
                filled = next;
            }
            fan.seals
                .push_back((fan.submit)(draw, Bytes::from(payload), span));
            self.stats.peak_hash_in_flight = self.stats.peak_hash_in_flight.max(fan.seals.len());
            return Ok(());
        }
        let (chunk, reference) = self.mode.seal::<B>(Bytes::from(payload))?;
        self.stats.leaves = self.stats.leaves.saturating_add(1);
        self.enqueue(chunk)?;
        self.push_ref(0, reference, span)
    }

    /// Whether the write gate refuses bytes this poll: a full hash window
    /// when the fan-out is on, otherwise the serial put gate.
    fn write_gate_blocked(&self) -> bool {
        #[cfg(feature = "rayon")]
        if let Some(fan) = &self.hash {
            return fan.seals.len() >= fan.window;
        }
        !self.pending.is_empty() || !self.puts.admits()
    }

    /// Admit pool-sealed leaves in leaf order while put capacity allows.
    ///
    /// The ordered stream yields the next leaf only once its predecessors
    /// have, so ascent, intermediate sealing and put dispatch order match the
    /// serial engine; later completions park until their turn. Pulling one
    /// leaf per put slot keeps admission behind the put gate, and the stream
    /// registers a waker for every in-flight seal before it parks.
    #[cfg(feature = "rayon")]
    fn drain_seals(&mut self, cx: &mut Context<'_>) -> Result<(), SplitError<S::Error>> {
        loop {
            self.step_puts(cx)?;
            if !self.pending.is_empty() || !self.puts.admits() {
                return Ok(());
            }
            let Some(fan) = self.hash.as_mut() else {
                return Ok(());
            };
            let (span, sealed) = match fan.seals.poll_next_unpin(cx) {
                Poll::Pending | Poll::Ready(None) => return Ok(()),
                Poll::Ready(Some(None)) => return Err(SplitError::PoolDropped),
                Poll::Ready(Some(Some((span, result)))) => (span, result?),
            };
            let (chunk, reference) = sealed;
            self.stats.leaves = self.stats.leaves.saturating_add(1);
            self.enqueue(chunk)?;
            self.push_ref(0, reference, span)?;
        }
    }

    /// Leaf seals still on the pool; zero when the fan-out is off.
    #[cfg(feature = "rayon")]
    fn seals_queued(&self) -> usize {
        self.hash.as_ref().map_or(0, |fan| fan.seals.len())
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
    ///
    /// The payload is built uniquely owned: an encrypted mode's sealer
    /// relies on that to wipe it, since the body carries every child key.
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
        let (chunk, reference) = self.seal_inline(Bytes::from(payload))?;
        self.stats.intermediates = self.stats.intermediates.saturating_add(1);
        self.enqueue(chunk)?;
        Ok(reference)
    }

    /// Seal one payload on the engine thread; under the pool fan-out a
    /// streaming ascent seal spends its submission-time draw.
    fn seal_inline(&mut self, payload: Bytes) -> Result<Sealed<M, B>, SealError> {
        #[cfg(feature = "rayon")]
        if let Some(draw) = self.hash.as_mut().and_then(|fan| fan.draws.pop_front()) {
            return M::seal_with::<B>(draw, payload);
        }
        self.mode.seal::<B>(payload)
    }

    /// Queue a sealed chunk for the put window, admitting what fits.
    fn enqueue(
        &mut self,
        chunk: Chunk<Verified, AnyChunkSet<B>>,
    ) -> Result<(), SplitError<S::Error>> {
        self.pending.push_back(chunk);
        self.admit()?;
        self.stats.peak_pending = self.stats.peak_pending.max(self.pending.len());
        Ok(())
    }

    /// Move pending chunks into the put window while slots are free; a put
    /// that fails on its opening poll is terminal.
    fn admit(&mut self) -> Result<(), SplitError<S::Error>> {
        while self.puts.admits() {
            let Some(chunk) = self.pending.pop_front() else {
                return Ok(());
            };
            self.dispatch(chunk)?;
        }
        Ok(())
    }

    /// Start one put, moving the chunk into its future; the completion
    /// carries the address back.
    ///
    /// The future is polled once on the spot: a put that finishes
    /// synchronously never occupies the window, and a pending one parks
    /// there to be driven with the caller's waker.
    fn dispatch(
        &mut self,
        chunk: Chunk<Verified, AnyChunkSet<B>>,
    ) -> Result<(), SplitError<S::Error>> {
        let store = self.store.clone();
        let put: BoxPut<'a, S::Error> = Box::pin(async move {
            let address = *chunk.address();
            (address, store.put(chunk).await)
        });
        self.stats.puts = self.stats.puts.saturating_add(1);
        match self.puts.push(put) {
            Some((address, result)) => result.map_err(|source| SplitError::Put { address, source }),
            None => {
                self.stats.peak_put_in_flight = self.stats.peak_put_in_flight.max(self.puts.len());
                Ok(())
            }
        }
    }

    /// Fold completed puts back in and admit pending chunks; a failed put
    /// is terminal. An empty window skips the poll machinery entirely.
    fn step_puts(&mut self, cx: &mut Context<'_>) -> Result<(), SplitError<S::Error>> {
        loop {
            self.admit()?;
            if self.puts.is_empty() {
                return Ok(());
            }
            match self.puts.poll_step(cx) {
                Poll::Ready(Some((address, result))) => {
                    result.map_err(|source| SplitError::Put { address, source })?;
                }
                Poll::Ready(None) | Poll::Pending => return Ok(()),
            }
        }
    }
}

impl<S, M, const B: usize> fmt::Debug for Split<'_, S, M, B>
where
    S: ChunkPut<AnyChunkSet<B>>,
    M: SplitMode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Split")
            .field("phase", &self.phase)
            .field("window", &self.puts.window().get())
            .field("slots", &self.slots)
            .field("leaf_len", &self.leaf.len().saturating_sub(SPAN_SIZE))
            .field("spine_levels", &self.spine.len())
            .field("pending", &self.pending.len())
            .field("in_flight", &self.puts.len())
            .finish_non_exhaustive()
    }
}

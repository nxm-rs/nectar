//! The bounded descent state machine.

use alloc::boxed::Box;
use alloc::collections::{BTreeMap, VecDeque};
use core::fmt;
use core::future::Future;
use core::ops::Range;
use core::pin::Pin;
use core::task::{Context, Poll};

use bytes::Bytes;
use futures_util::stream::{FuturesUnordered, Stream};
use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::{AnyChunkSet, Chunk, ChunkAddress, ChunkOps, Verified};
use nectar_primitives::store::TrustedGet;

use super::error::{ShapeError, WalkError};
use super::mode::WalkMode;
use super::{Frame, WalkStats};
use crate::config::{BranchBudget, Window};

/// One pending tree node: where its bytes live and what fetches it.
struct Node<M: WalkMode> {
    address: ChunkAddress,
    /// Reference context routed with the node; the encrypted mode's body
    /// decoder consumes it.
    #[allow(dead_code, reason = "consumed once the encrypted decoder lands")]
    context: M::Context,
    /// Absolute offset of the subtree's first byte.
    start: u64,
    /// Bytes the subtree covers.
    span: u64,
}

impl<M: WalkMode> Node<M> {
    /// Sequence key: the node's first in-range byte.
    fn key(&self, range_start: u64) -> u64 {
        self.start.max(range_start)
    }
}

/// Completion payload; the future carries its node back, which is the whole
/// of sequence routing.
type Fetched<M, E, const B: usize> = (Node<M>, Result<Chunk<Verified, AnyChunkSet<B>>, E>);

/// Boxed fetch future: `Send` on multi-threaded targets, unbounded on wasm32
/// and under the `unsync` feature.
#[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
type BoxFetch<M, E, const B: usize> = Pin<Box<dyn Future<Output = Fetched<M, E, B>> + Send>>;
/// Boxed fetch future: `Send` on multi-threaded targets, unbounded on wasm32
/// and under the `unsync` feature.
#[cfg(any(target_arch = "wasm32", feature = "unsync"))]
type BoxFetch<M, E, const B: usize> = Pin<Box<dyn Future<Output = Fetched<M, E, B>>>>;

/// Which frame a drain takes from the ready set.
#[derive(Clone, Copy)]
enum Drain {
    /// Only the head frame, in file order.
    Ordered,
    /// The lowest ready frame, regardless of order.
    Any,
}

/// The one poll-native walk: a bounded, sequence-routed descent of a chunk
/// tree over a byte range.
///
/// All state lives here, so every poll is cancel-safe and dropping the walk
/// loses only in-flight round trips. The module docs state the normative
/// admission invariants.
pub struct Walk<S, M, const B: usize = DEFAULT_BODY_SIZE>
where
    S: TrustedGet<AnyChunkSet<B>>,
    M: WalkMode,
{
    store: S,
    range_start: u64,
    range_end: u64,
    body: u64,
    branches: u64,
    window: usize,
    branch_budget: usize,
    /// Discovered leaves awaiting a window slot, ascending by key.
    leaf_frontier: VecDeque<Node<M>>,
    /// Discovered intermediates awaiting descent, ascending by key; the
    /// flattened frame stack of the serial walk.
    branch_frontier: VecDeque<Node<M>>,
    in_flight: FuturesUnordered<BoxFetch<M, S::Error, B>>,
    /// Keys of in-flight leaf fetches, counted per key.
    leaf_keys: BTreeMap<u64, usize>,
    /// Keys of in-flight branch fetches, counted per key.
    branch_keys: BTreeMap<u64, usize>,
    leaf_in_flight: usize,
    branch_in_flight: usize,
    /// Resolved leaf bodies, clipped to the range, keyed by offset.
    ready: BTreeMap<u64, Bytes>,
    done: bool,
    stats: WalkStats,
}

impl<S, M, const B: usize> Walk<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>> + Clone + 'static,
    M: WalkMode,
{
    /// Compile-time profile guard for the walk's span arithmetic.
    const PROFILE: () = {
        assert!(B.is_power_of_two(), "body size must be a power of two");
        assert!(
            u64_from_usize(B) <= u64_from_u32(u32::MAX),
            "body size must fit the u32 geometry"
        );
        let fan_out = fan_out(u64_from_usize(B), u64_from_u32(M::MODE.ref_size()));
        assert!(fan_out >= 2, "fan-out must be at least two");
    };

    /// Walk `range` of the tree under `root`, whose total `span` the caller
    /// read from the root chunk. The engine re-fetches the root as its first
    /// node, so the fetch set stays identical to a cold serial walk.
    pub fn new(
        store: S,
        root: ChunkAddress,
        context: M::Context,
        span: u64,
        range: Range<u64>,
        window: Window,
    ) -> Self {
        const { Self::PROFILE };
        let body = u64_from_usize(B);
        let branches = fan_out(body, u64_from_u32(M::MODE.ref_size()));
        let range_end = range.end.min(span);
        let range_start = range.start.min(range_end);
        let budget = BranchBudget::derive(window, u32::try_from(branches).unwrap_or(u32::MAX));
        let mut walk = Self {
            store,
            range_start,
            range_end,
            body,
            branches,
            window: usize::from(window.get()),
            branch_budget: usize::try_from(budget.get()).unwrap_or(usize::MAX),
            leaf_frontier: VecDeque::new(),
            branch_frontier: VecDeque::new(),
            in_flight: FuturesUnordered::new(),
            leaf_keys: BTreeMap::new(),
            branch_keys: BTreeMap::new(),
            leaf_in_flight: 0,
            branch_in_flight: 0,
            ready: BTreeMap::new(),
            done: false,
            stats: WalkStats::default(),
        };
        walk.enqueue(Node {
            address: root,
            context,
            start: 0,
            span,
        });
        walk
    }

    /// Clipped absolute byte range this walk delivers.
    pub const fn range(&self) -> Range<u64> {
        self.range_start..self.range_end
    }

    /// Occupancy witnesses accumulated so far.
    pub const fn stats(&self) -> WalkStats {
        self.stats
    }

    /// Whether the walk has delivered its last frame or failed.
    pub const fn is_finished(&self) -> bool {
        self.done
    }

    /// Deliver the next frame in file order: consecutive frames tile the
    /// range gaplessly.
    ///
    /// Cancel-safe: all progress lives in `self`, so an abandoned call loses
    /// nothing. `Ready(None)` after the last in-range byte or after a
    /// terminal error.
    pub fn poll_next_ordered(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame, WalkError<S::Error>>>> {
        self.poll_frame(cx, Drain::Ordered)
    }

    /// Deliver the next frame in completion order, lowest ready offset
    /// first. Same contract as [`poll_next_ordered`](Self::poll_next_ordered)
    /// without the ordering guarantee.
    pub fn poll_next_any(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame, WalkError<S::Error>>>> {
        self.poll_frame(cx, Drain::Any)
    }

    fn poll_frame(
        &mut self,
        cx: &mut Context<'_>,
        drain: Drain,
    ) -> Poll<Option<Result<Frame, WalkError<S::Error>>>> {
        if self.done {
            return Poll::Ready(None);
        }
        loop {
            self.admit();
            if let Some(frame) = self.take_ready(drain) {
                return Poll::Ready(Some(Ok(frame)));
            }
            match Pin::new(&mut self.in_flight).poll_next(cx) {
                Poll::Ready(Some((node, fetched))) => {
                    if let Err(error) = self.absorb(node, fetched) {
                        self.done = true;
                        return Poll::Ready(Some(Err(error)));
                    }
                }
                Poll::Ready(None) => {
                    self.done = true;
                    let pending = self
                        .leaf_frontier
                        .len()
                        .saturating_add(self.branch_frontier.len());
                    if pending == 0 && self.ready.is_empty() {
                        return Poll::Ready(None);
                    }
                    return Poll::Ready(Some(Err(WalkError::Stalled {
                        pending,
                        occupancy: self.occupancy(),
                    })));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    /// Admit queued nodes until neither lane may proceed.
    fn admit(&mut self) {
        loop {
            let Some(head) = self.head_key() else { return };
            let branch = self.try_admit_branch(head);
            let leaf = self.try_admit_leaf(head);
            if !branch && !leaf {
                return;
            }
        }
    }

    /// The head: the lowest key still owed to the consumer. Every byte below
    /// it has been yielded, because a node only leaves the walk by being
    /// yielded or expanded into its children.
    fn head_key(&self) -> Option<u64> {
        let candidates = [
            self.leaf_frontier
                .front()
                .map(|node| node.key(self.range_start)),
            self.branch_frontier
                .front()
                .map(|node| node.key(self.range_start)),
            self.leaf_keys.first_key_value().map(|(&key, _)| key),
            self.branch_keys.first_key_value().map(|(&key, _)| key),
            self.ready.first_key_value().map(|(&key, _)| key),
        ];
        candidates.into_iter().flatten().min()
    }

    /// Whether the head already occupies a window slot (an in-flight leaf
    /// fetch or a buffered frame); the reservation stands only while it does
    /// not.
    fn head_holds_slot(&self, head: u64) -> bool {
        self.leaf_keys.contains_key(&head)
            || self
                .ready
                .first_key_value()
                .is_some_and(|(&key, _)| key == head)
    }

    /// Leaf bodies held: in flight plus buffered.
    fn occupancy(&self) -> usize {
        self.leaf_in_flight.saturating_add(self.ready.len())
    }

    /// Admit the lowest queued branch. The head branch only needs a budget
    /// slot (liveness over the reference cap); any other branch also needs
    /// absorption room.
    fn try_admit_branch(&mut self, head: u64) -> bool {
        if self.branch_in_flight >= self.branch_budget {
            return false;
        }
        let Some(front) = self.branch_frontier.front() else {
            return false;
        };
        if front.key(self.range_start) != head && !self.expansion_room() {
            return false;
        }
        let Some(node) = self.branch_frontier.pop_front() else {
            return false;
        };
        self.dispatch(node);
        true
    }

    /// Admit the lowest queued leaf. The head leaf may take the last window
    /// slot; any other leaf must leave it free until the head holds one.
    fn try_admit_leaf(&mut self, head: u64) -> bool {
        let Some(front) = self.leaf_frontier.front() else {
            return false;
        };
        let cap = if front.key(self.range_start) == head || self.head_holds_slot(head) {
            self.window
        } else {
            self.window.saturating_sub(1)
        };
        if self.occupancy() >= cap {
            return false;
        }
        let Some(node) = self.leaf_frontier.pop_front() else {
            return false;
        };
        self.dispatch(node);
        true
    }

    /// Whether the leaf frontier can absorb every outstanding expansion plus
    /// one more, keeping buffered leaf references within `window + branches`
    /// outside the head exemption.
    fn expansion_room(&self) -> bool {
        let pending = u64_from_usize(self.leaf_frontier.len());
        let reserved = u64_from_usize(self.branch_in_flight)
            .saturating_add(1)
            .saturating_mul(self.branches);
        pending.saturating_add(reserved)
            <= u64_from_usize(self.window).saturating_add(self.branches)
    }

    /// Start one fetch, moving the node into its future; the completion
    /// carries it back.
    fn dispatch(&mut self, node: Node<M>) {
        let key = node.key(self.range_start);
        if node.span <= self.body {
            let slot = self.leaf_keys.entry(key).or_insert(0);
            *slot = slot.saturating_add(1);
            self.leaf_in_flight = self.leaf_in_flight.saturating_add(1);
            self.stats.peak_occupancy = self.stats.peak_occupancy.max(self.occupancy());
        } else {
            let slot = self.branch_keys.entry(key).or_insert(0);
            *slot = slot.saturating_add(1);
            self.branch_in_flight = self.branch_in_flight.saturating_add(1);
            self.stats.peak_branch_in_flight =
                self.stats.peak_branch_in_flight.max(self.branch_in_flight);
        }
        self.stats.fetches = self.stats.fetches.saturating_add(1);
        let store = self.store.clone();
        let fetch: BoxFetch<M, S::Error, B> = Box::pin(async move {
            let address = node.address;
            let fetched = store.get(&address).await;
            (node, fetched)
        });
        self.in_flight.push(fetch);
    }

    /// Retire a completed fetch from the in-flight accounting.
    fn retire(&mut self, key: u64, leaf: bool) {
        let keys = if leaf {
            &mut self.leaf_keys
        } else {
            &mut self.branch_keys
        };
        if let Some(count) = keys.get_mut(&key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                keys.remove(&key);
            }
        }
        if leaf {
            self.leaf_in_flight = self.leaf_in_flight.saturating_sub(1);
        } else {
            self.branch_in_flight = self.branch_in_flight.saturating_sub(1);
        }
    }

    /// Fold one completion back into the walk: buffer a leaf body or expand
    /// an intermediate. Every completion must return the requested address.
    fn absorb(
        &mut self,
        node: Node<M>,
        fetched: Result<Chunk<Verified, AnyChunkSet<B>>, S::Error>,
    ) -> Result<(), WalkError<S::Error>> {
        let leaf = node.span <= self.body;
        let key = node.key(self.range_start);
        self.retire(key, leaf);
        let chunk = fetched.map_err(|source| WalkError::Fetch {
            address: node.address,
            source,
        })?;
        let returned = *chunk.address();
        if returned != node.address {
            return Err(WalkError::AddressMismatch {
                requested: node.address,
                returned,
            });
        }
        let data = chunk.into_envelope().data().clone();
        if leaf {
            let len = u64_from_usize(data.len());
            if len != node.span {
                return Err(ShapeError::LeafLength {
                    offset: node.start,
                    span: node.span,
                    len,
                }
                .into());
            }
            self.ready.insert(key, self.clip(&node, data));
            Ok(())
        } else {
            self.expand(&node, &data).map_err(WalkError::from)
        }
    }

    /// Expand an intermediate body into its overlapping children.
    fn expand(&mut self, node: &Node<M>, body: &Bytes) -> Result<(), ShapeError> {
        let sub = child_subspan(node.span, self.body, self.branches);
        let expected = node.span.div_ceil(sub);
        let mut input: &[u8] = body;
        for index in 0..expected {
            let Some((address, context)) = M::take_ref(&mut input) else {
                return Err(ShapeError::Arity {
                    offset: node.start,
                    expected,
                    have: index,
                });
            };
            let overflow = ShapeError::Offset {
                offset: node.start,
                span: node.span,
            };
            let delta = index.checked_mul(sub).ok_or(overflow)?;
            let start = node.start.checked_add(delta).ok_or(overflow)?;
            let span = sub.min(node.span.saturating_sub(delta));
            self.enqueue(Node {
                address,
                context,
                start,
                span,
            });
        }
        Ok(())
    }

    /// Queue a node in key order, pruning subtrees outside the range; an
    /// empty range prunes everything.
    fn enqueue(&mut self, node: Node<M>) {
        let end = node.start.saturating_add(node.span);
        if self.range_start >= self.range_end
            || node.start >= self.range_end
            || end <= self.range_start
        {
            return;
        }
        let range_start = self.range_start;
        let key = node.key(range_start);
        let queue = if node.span <= self.body {
            &mut self.leaf_frontier
        } else {
            &mut self.branch_frontier
        };
        let at = queue.partition_point(|queued| queued.key(range_start) <= key);
        queue.insert(at, node);
        self.stats.peak_leaf_frontier = self.stats.peak_leaf_frontier.max(self.leaf_frontier.len());
    }

    /// Clip a leaf body to the in-range window; the bounds are clamped into
    /// the body by construction, so the slice cannot be out of range.
    fn clip(&self, node: &Node<M>, data: Bytes) -> Bytes {
        let len = data.len();
        let low = clamp_index(self.range_start.saturating_sub(node.start), len);
        let high = clamp_index(self.range_end.saturating_sub(node.start), len).max(low);
        data.slice(low..high)
    }

    /// Take the next deliverable frame, if the drain permits one.
    fn take_ready(&mut self, drain: Drain) -> Option<Frame> {
        if let Drain::Ordered = drain {
            let head = self.head_key()?;
            let (&key, _) = self.ready.first_key_value()?;
            if key != head {
                return None;
            }
        }
        self.ready
            .pop_first()
            .map(|(offset, data)| Frame { offset, data })
    }
}

impl<S, M, const B: usize> fmt::Debug for Walk<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>>,
    M: WalkMode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Walk")
            .field("range_start", &self.range_start)
            .field("range_end", &self.range_end)
            .field("window", &self.window)
            .field("branch_budget", &self.branch_budget)
            .field("leaf_in_flight", &self.leaf_in_flight)
            .field("branch_in_flight", &self.branch_in_flight)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

/// Lossless widening; `From` is not const-callable here.
#[cfg(target_pointer_width = "64")]
const fn u64_from_usize(value: usize) -> u64 {
    u64::from_le_bytes(value.to_le_bytes())
}

/// Lossless widening; `From` is not const-callable here.
#[cfg(target_pointer_width = "32")]
const fn u64_from_usize(value: usize) -> u64 {
    let [a, b, c, d] = value.to_le_bytes();
    u64::from_le_bytes([a, b, c, d, 0, 0, 0, 0])
}

/// Lossless widening; `From` is not const-callable.
const fn u64_from_u32(value: u32) -> u64 {
    let [a, b, c, d] = value.to_le_bytes();
    u64::from_le_bytes([a, b, c, d, 0, 0, 0, 0])
}

/// References per intermediate body; zero only for a degenerate profile the
/// compile-time guard rejects.
const fn fan_out(body: u64, ref_size: u64) -> u64 {
    match body.checked_div(ref_size) {
        Some(fan) => fan,
        None => 0,
    }
}

/// Child span under a parent covering `span` bytes: the smallest
/// `body * branches^k` whose full fan-out reaches the parent span.
const fn child_subspan(span: u64, body: u64, branches: u64) -> u64 {
    let mut sub = body;
    loop {
        match sub.checked_mul(branches) {
            Some(covered) if covered < span => sub = covered,
            _ => return sub,
        }
    }
}

/// Clamp a body-relative offset into an index of a `len`-byte body.
fn clamp_index(value: u64, len: usize) -> usize {
    usize::try_from(value).unwrap_or(len).min(len)
}

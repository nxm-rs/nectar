//! The bounded descent state machine.

use alloc::boxed::Box;
use alloc::collections::{BTreeMap, VecDeque};
use core::fmt;
use core::ops::Range;
use core::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use nectar_governor::{
    Admission, AdmitPolicy, BoxFuture, FromFn, FuturesUnordered, Observations, StaticDriver,
    WalkPolicy, from_fn, get_verified,
};
use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::{Chunk, ChunkAddress, ChunkOps, ContentOnlyChunkSet, Verified};
use nectar_primitives::store::TrustedGet;

use super::error::{ShapeError, WalkError};
use super::mode::WalkMode;
use super::{Frame, WalkStats, WindowPolicyFn};
use crate::config::{BranchBudget, Window};
use crate::num::{fan_out, u64_from_u32, u64_from_usize};

/// One pending tree node: where its bytes live and what fetches it.
struct Node<M: WalkMode> {
    address: ChunkAddress,
    /// Reference context routed with the node; the mode's body decoder
    /// consumes it.
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
type Fetched<M, E, const B: usize> = (Node<M>, Result<Chunk<Verified, ContentOnlyChunkSet<B>>, E>);

/// Boxed fetch future; the kernel alias relaxes `Send` off the
/// multi-threaded targets.
type BoxFetch<M, E, const B: usize> = BoxFuture<'static, Fetched<M, E, B>>;

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
    S: TrustedGet<ContentOnlyChunkSet<B>>,
    M: WalkMode,
{
    driver: StaticDriver<FileWalkPolicy<S, M, B>, Fetched<M, S::Error, B>>,
}

/// The file walk's [`WalkPolicy`]: the two-lane branch/leaf frontier, its
/// head-reserved admission, byte-offset ordering, eager-terminal error, and
/// clip/expand completion fold. The kernel driver owns only the loop and the
/// in-flight set.
struct FileWalkPolicy<S, M, const B: usize>
where
    S: TrustedGet<ContentOnlyChunkSet<B>>,
    M: WalkMode,
{
    store: S,
    range_start: u64,
    range_end: u64,
    body: u64,
    branches: u64,
    admission: Admission,
    branch_budget: usize,
    /// Adaptive cap: retunes the window between admission rounds.
    policy: Option<FromFn<WindowPolicyFn>>,
    /// Leaf completions since the last policy call.
    completed: usize,
    /// Discovered leaves awaiting a window slot, ascending by key.
    leaf_frontier: VecDeque<Node<M>>,
    /// Discovered intermediates awaiting descent, ascending by key; the
    /// flattened frame stack of the serial walk.
    branch_frontier: VecDeque<Node<M>>,
    /// Keys of in-flight leaf fetches, counted per key.
    leaf_keys: BTreeMap<u64, usize>,
    /// Keys of in-flight branch fetches, counted per key.
    branch_keys: BTreeMap<u64, usize>,
    leaf_in_flight: usize,
    branch_in_flight: usize,
    /// Resolved leaf bodies, clipped to the range, keyed by offset.
    ready: BTreeMap<u64, Bytes>,
    /// Staging buffer the mode's body decoder reuses across nodes.
    scratch: BytesMut,
    stats: WalkStats,
}

impl<S, M, const B: usize> Walk<S, M, B>
where
    S: TrustedGet<ContentOnlyChunkSet<B>> + Clone + 'static,
    M: WalkMode,
{
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
        const { FileWalkPolicy::<S, M, B>::PROFILE };
        let body = u64_from_usize(B);
        let branches = fan_out(body, u64_from_u32(M::MODE.ref_size()));
        let range_end = range.end.min(span);
        let range_start = range.start.min(range_end);
        let mut policy = FileWalkPolicy {
            store,
            range_start,
            range_end,
            body,
            branches,
            admission: Admission::new(window),
            branch_budget: branch_budget(window, branches),
            policy: None,
            completed: 0,
            leaf_frontier: VecDeque::new(),
            branch_frontier: VecDeque::new(),
            leaf_keys: BTreeMap::new(),
            branch_keys: BTreeMap::new(),
            leaf_in_flight: 0,
            branch_in_flight: 0,
            ready: BTreeMap::new(),
            scratch: BytesMut::new(),
            stats: WalkStats::default(),
        };
        policy.enqueue(Node {
            address: root,
            context,
            start: 0,
            span,
        });
        Self {
            driver: StaticDriver::new(policy),
        }
    }

    /// Adaptive cap: `policy` recomputes the window between admission
    /// rounds, seeded with the built window. Occupancy above a shrunk cap
    /// drains by attrition; the head stays admissible at any depth.
    #[must_use]
    pub fn with_policy(mut self, policy: WindowPolicyFn) -> Self {
        self.driver.policy_mut().policy = Some(from_fn(policy));
        self
    }

    /// Detach the policy with its accumulated state; a successor walk
    /// re-arms it.
    pub(crate) fn take_policy(&mut self) -> Option<WindowPolicyFn> {
        self.driver
            .policy_mut()
            .policy
            .take()
            .map(FromFn::into_inner)
    }

    /// Clipped absolute byte range this walk delivers.
    pub const fn range(&self) -> Range<u64> {
        let policy = self.driver.policy();
        policy.range_start..policy.range_end
    }

    /// Occupancy witnesses accumulated so far.
    pub const fn stats(&self) -> WalkStats {
        self.driver.policy().stats
    }

    /// Whether the walk has delivered its last frame or failed.
    pub const fn is_finished(&self) -> bool {
        self.driver.is_finished()
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
        self.driver.poll(cx, Drain::Ordered)
    }

    /// Deliver the next frame in completion order, lowest ready offset
    /// first. Same contract as [`poll_next_ordered`](Self::poll_next_ordered)
    /// without the ordering guarantee.
    pub fn poll_next_any(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame, WalkError<S::Error>>>> {
        self.driver.poll(cx, Drain::Any)
    }
}

impl<S, M, const B: usize> WalkPolicy<'static> for FileWalkPolicy<S, M, B>
where
    S: TrustedGet<ContentOnlyChunkSet<B>> + Clone + 'static,
    M: WalkMode,
{
    type Fetched = Fetched<M, S::Error, B>;
    type Frame = Frame;
    type Error = WalkError<S::Error>;
    type Drain = Drain;

    #[inline]
    fn admit(&mut self, in_flight: &mut FuturesUnordered<BoxFuture<'static, Self::Fetched>>) {
        self.retune();
        loop {
            let Some(head) = self.head_key() else { return };
            let branch = self.try_admit_branch(head, in_flight);
            let leaf = self.try_admit_leaf(head, in_flight);
            if !branch && !leaf {
                return;
            }
        }
    }

    #[inline]
    fn take_ready(&mut self, drain: Drain) -> Option<Result<Frame, Self::Error>> {
        if let Drain::Ordered = drain {
            let head = self.head_key()?;
            let (&key, _) = self.ready.first_key_value()?;
            if key != head {
                return None;
            }
        }
        self.ready
            .pop_first()
            .map(|(offset, data)| Ok(Frame { offset, data }))
    }

    #[inline]
    fn absorb(&mut self, (node, fetched): Self::Fetched) -> Result<(), Self::Error> {
        let leaf = node.span <= self.body;
        let key = node.key(self.range_start);
        self.retire(key, leaf);
        let chunk = fetched.map_err(|source| WalkError::Fetch {
            address: node.address,
            source,
        })?;
        let data = chunk.into_envelope().data().clone();
        let take = self.plaintext_len(&node, leaf);
        let data =
            M::decode_body(&node.context, B, take, data, &mut self.scratch).map_err(|source| {
                WalkError::Decode {
                    offset: node.start,
                    source,
                }
            })?;
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

    #[inline]
    fn drained(&self) -> Result<(), Self::Error> {
        let pending = self
            .leaf_frontier
            .len()
            .saturating_add(self.branch_frontier.len());
        if pending == 0 && self.ready.is_empty() {
            Ok(())
        } else {
            Err(WalkError::Stalled {
                pending,
                occupancy: self.occupancy(),
            })
        }
    }
}

impl<S, M, const B: usize> FileWalkPolicy<S, M, B>
where
    S: TrustedGet<ContentOnlyChunkSet<B>> + Clone + 'static,
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

    /// Let the policy recompute the window; a change re-derives the branch
    /// budget.
    fn retune(&mut self) {
        let occupancy = self.occupancy();
        let Some(policy) = self.policy.as_mut() else {
            return;
        };
        let observations = Observations {
            completions: self.completed,
            occupancy,
            in_flight: self.leaf_in_flight,
        };
        self.completed = 0;
        let window = policy.window(&observations);
        if window != self.admission.window() {
            self.admission = Admission::new(window);
            self.branch_budget = branch_budget(window, self.branches);
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
    fn try_admit_branch(
        &mut self,
        head: u64,
        in_flight: &mut FuturesUnordered<BoxFuture<'static, Fetched<M, S::Error, B>>>,
    ) -> bool {
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
        self.dispatch(node, in_flight);
        true
    }

    /// Admit the lowest queued leaf. The head leaf may take the last window
    /// slot; any other leaf must leave it free until the head holds one.
    fn try_admit_leaf(
        &mut self,
        head: u64,
        in_flight: &mut FuturesUnordered<BoxFuture<'static, Fetched<M, S::Error, B>>>,
    ) -> bool {
        let Some(front) = self.leaf_frontier.front() else {
            return false;
        };
        let head_candidate = front.key(self.range_start) == head;
        let head_holds = self.head_holds_slot(head);
        // An unserved head is admitted at any occupancy: a shrunk cap may
        // sit below the buffered frames, and only the head drains them.
        // With a fixed window an unserved head implies a free slot, so this
        // never lifts the fixed bound.
        let admitted = (head_candidate && !head_holds)
            || self
                .admission
                .admits(self.occupancy(), head_candidate || head_holds);
        if !admitted {
            return false;
        }
        let Some(node) = self.leaf_frontier.pop_front() else {
            return false;
        };
        self.dispatch(node, in_flight);
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
            <= u64::from(self.admission.window().get()).saturating_add(self.branches)
    }

    /// Start one fetch, moving the node into its future; the completion
    /// carries it back.
    fn dispatch(
        &mut self,
        node: Node<M>,
        in_flight: &mut FuturesUnordered<BoxFuture<'static, Fetched<M, S::Error, B>>>,
    ) {
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
        let fetch: BoxFetch<M, S::Error, B> = Box::pin(get_verified(store, node.address, node));
        in_flight.push(fetch);
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
            self.completed = self.completed.saturating_add(1);
        } else {
            self.branch_in_flight = self.branch_in_flight.saturating_sub(1);
        }
    }

    /// Plaintext bytes a node's body carries: a leaf's span, or an
    /// intermediate's packed references, never past the profile's body.
    fn plaintext_len(&self, node: &Node<M>, leaf: bool) -> usize {
        let take = if leaf {
            node.span
        } else {
            let sub = child_subspan(node.span, self.body, self.branches);
            let ref_size = u64_from_u32(M::MODE.ref_size());
            node.span.div_ceil(sub).saturating_mul(ref_size)
        };
        // take is capped at the body size, which is the usize B.
        usize::try_from(take.min(self.body)).unwrap_or(B)
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
}

impl<S, M, const B: usize> fmt::Debug for Walk<S, M, B>
where
    S: TrustedGet<ContentOnlyChunkSet<B>>,
    M: WalkMode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let policy = self.driver.policy();
        f.debug_struct("Walk")
            .field("range_start", &policy.range_start)
            .field("range_end", &policy.range_end)
            .field("window", &policy.admission.window())
            .field("branch_budget", &policy.branch_budget)
            .field("policy", &policy.policy.is_some())
            .field("leaf_in_flight", &policy.leaf_in_flight)
            .field("branch_in_flight", &policy.branch_in_flight)
            .field("done", &self.driver.is_finished())
            .finish_non_exhaustive()
    }
}

/// Branch budget for `window`, widened into the engine's counter type.
fn branch_budget(window: Window, branches: u64) -> usize {
    let budget = BranchBudget::derive(window, u32::try_from(branches).unwrap_or(u32::MAX));
    usize::try_from(budget.get()).unwrap_or(usize::MAX)
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

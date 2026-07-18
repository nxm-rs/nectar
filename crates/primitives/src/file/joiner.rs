//! Async joiner with BFS expansion and concurrent chunk fetching.

use std::io::SeekFrom;
use std::marker::PhantomData;
use std::sync::Arc;

/// Default number of concurrent chunk fetches for async operations.
const DEFAULT_ASYNC_CONCURRENCY: usize = 8;

/// Default number of times a failed leaf fetch is re-enqueued before the
/// chunk-granular offset stream surfaces the error.
const DEFAULT_LEAF_RETRIES: u32 = 4;

/// Maximum intermediate (non-leaf) chunk fetches kept in flight by the
/// chunk-granular streaming walks.
///
/// Intermediate nodes are rare relative to leaves (one per branching-factor
/// leaves) and each resolves to a whole chunk of child addresses, so a small
/// handful keeps leaf discovery far ahead of leaf consumption. Capping them is
/// what stops a wide subtree frontier from being fetched in full before the
/// first data leaf: the cap leaves the rest of the configured width free for
/// leaves, so leaf bytes begin flowing after only a short descent rather than
/// after the whole frontier is drained. The cap shapes the walk for
/// time-to-first-byte, sized to keep roughly a subtree descent's worth of
/// structure fetches concurrent; any reduction in the cold-start fetch burst is
/// a side effect, not the purpose.
pub(crate) const MAX_INTERMEDIATE_IN_FLIGHT: usize = 4;

#[cfg(feature = "tokio")]
use bytes::Buf;
use bytes::Bytes;
use futures::stream::{self, FuturesUnordered, Stream, StreamExt};

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::{AnyChunkSet, ChunkAddress};

use super::error::{FileError, Result};
use super::frontier::{
    SubtreeNode, expand_frontier, frontier_seed, overlapping_children, read_subtree_bodies,
};
use super::mode::{JoinMode, PlainMode};
use super::tree::{ChunkRange, TreeParams};
use crate::store::{MaybeSend, TrustedStore};

#[cfg(feature = "encryption")]
use super::mode::EncryptedMode;

/// Generic async joiner parameterized by chunk mode.
pub struct GenericJoiner<G, M: JoinMode, const BODY_SIZE: usize = DEFAULT_BODY_SIZE>
where
    G: TrustedStore<AnyChunkSet<BODY_SIZE>>,
{
    getter: Arc<G>,
    root: ChunkAddress,
    context: M::JoinerContext,
    span: u64,
    tree: TreeParams<BODY_SIZE>,
    /// Pre-expanded frontier for parallel work distribution (computed once at construction).
    subtrees: Vec<SubtreeNode<M>>,
    position: u64,
    concurrency: usize,
    _mode: PhantomData<M>,
}

/// Plain (unencrypted) async joiner.
pub type Joiner<G, const BODY_SIZE: usize = DEFAULT_BODY_SIZE> =
    GenericJoiner<G, PlainMode, BODY_SIZE>;

/// Encrypted async joiner.
#[cfg(feature = "encryption")]
pub type EncryptedJoiner<G, const BODY_SIZE: usize = DEFAULT_BODY_SIZE> =
    GenericJoiner<G, EncryptedMode, BODY_SIZE>;

impl<G, M, const BODY_SIZE: usize> std::fmt::Debug for GenericJoiner<G, M, BODY_SIZE>
where
    G: TrustedStore<AnyChunkSet<BODY_SIZE>>,
    M: JoinMode,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericJoiner")
            .field("root", &self.root)
            .field("span", &self.span)
            .field("position", &self.position)
            .field("concurrency", &self.concurrency)
            .finish_non_exhaustive()
    }
}

/// Collect leaf bodies for a set of subtrees with concurrent fetching.
async fn collect_subtree_bodies<G, M, const BODY_SIZE: usize>(
    getter: &Arc<G>,
    subtrees: Vec<SubtreeNode<M>>,
    chunk_range: ChunkRange,
    concurrency: usize,
) -> Result<Vec<Bytes>>
where
    G: TrustedStore<AnyChunkSet<BODY_SIZE>>,
    M: JoinMode + MaybeSend + Sync,
{
    let bodies: Vec<Bytes> = stream::iter(subtrees)
        .map(|st| {
            let getter = Arc::clone(getter);
            async move { read_subtree_bodies::<G, M, BODY_SIZE>(&*getter, &st, &chunk_range).await }
        })
        .buffered(concurrency)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<Vec<Bytes>>>>()?
        .into_iter()
        .flatten()
        .collect();
    Ok(bodies)
}

impl<G, M, const BODY_SIZE: usize> GenericJoiner<G, M, BODY_SIZE>
where
    G: TrustedStore<AnyChunkSet<BODY_SIZE>>,
    M: JoinMode + MaybeSend + Sync,
{
    /// Create an async joiner from a root reference.
    pub async fn new(getter: G, input: M::RootRef) -> Result<Self> {
        const { super::constants::assert_valid_body_size::<BODY_SIZE>() };

        let (root, span, context) =
            super::mode::joiner_init::<M, G, BODY_SIZE>(&getter, input).await?;
        let tree = TreeParams::<BODY_SIZE>::new(span);

        let target = DEFAULT_ASYNC_CONCURRENCY * 2;
        let full_range = tree.chunks_for_range(0, span);
        let subtrees =
            expand_frontier::<G, M, BODY_SIZE>(&getter, &root, &context, span, &full_range, target)
                .await?;

        Ok(Self {
            getter: Arc::new(getter),
            root,
            context,
            span,
            tree,
            subtrees,
            position: 0,
            concurrency: DEFAULT_ASYNC_CONCURRENCY,
            _mode: PhantomData,
        })
    }

    /// Open for a forward streaming download, skipping the eager frontier
    /// expansion.
    ///
    /// [`new`](Self::new) pre-expands a balanced subtree frontier with a
    /// level-synchronous BFS, so one slow intermediate stalls every byte until
    /// its whole level resolves. The chunk-granular offset stream descends the
    /// tree concurrently from any seed, so a forward download needs no
    /// pre-expansion: seed it with the root alone and let the bounded pool walk
    /// the rest with no per-level barrier, so a slow intermediate lags only its
    /// own subtree rather than the whole download. Use this for
    /// [`download_into`](Self::download_into) and
    /// [`into_offset_stream_chunked`](Self::into_offset_stream_chunked); the
    /// random-access reads ([`read_all`](Self::read_all),
    /// [`into_windowed_reader`](Self::into_windowed_reader)) want the balanced
    /// frontier and should open with [`new`](Self::new). The root is re-fetched
    /// once as the stream descends it (a single chunk).
    pub async fn open_streaming(getter: G, input: M::RootRef) -> Result<Self> {
        const { super::constants::assert_valid_body_size::<BODY_SIZE>() };

        let (root, span, context) =
            super::mode::joiner_init::<M, G, BODY_SIZE>(&getter, input).await?;
        let tree = TreeParams::<BODY_SIZE>::new(span);
        let subtrees = vec![frontier_seed::<M>(&root, &context, span)];

        Ok(Self {
            getter: Arc::new(getter),
            root,
            context,
            span,
            tree,
            subtrees,
            position: 0,
            concurrency: DEFAULT_ASYNC_CONCURRENCY,
            _mode: PhantomData,
        })
    }

    /// Set concurrency level for prefetching.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    /// Total file size.
    #[inline]
    pub const fn size(&self) -> u64 {
        self.span
    }

    /// Current read position.
    #[inline]
    pub const fn position(&self) -> u64 {
        self.position
    }

    /// Root address.
    #[inline]
    pub const fn root(&self) -> &ChunkAddress {
        &self.root
    }

    // crate-private accessors for sibling-module joiner extensions
    #[allow(dead_code, reason = "consumed by sibling-module joiner extensions")]
    pub(crate) const fn getter(&self) -> &Arc<G> {
        &self.getter
    }
    #[allow(dead_code, reason = "consumed by sibling-module joiner extensions")]
    pub(crate) fn subtrees(&self) -> &[SubtreeNode<M>] {
        &self.subtrees
    }
    #[allow(dead_code, reason = "consumed by sibling-module joiner extensions")]
    pub(crate) const fn tree(&self) -> TreeParams<BODY_SIZE> {
        self.tree
    }
    #[allow(dead_code, reason = "consumed by sibling-module joiner extensions")]
    pub(crate) const fn concurrency(&self) -> usize {
        self.concurrency
    }
    #[allow(dead_code, reason = "consumed by sibling-module joiner extensions")]
    pub(crate) const fn context(&self) -> &M::JoinerContext {
        &self.context
    }

    /// Read a range of bytes with concurrent fetching using the cached frontier.
    pub async fn read_range(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        Self::read_range_with(
            &self.getter,
            &self.subtrees,
            &self.root,
            &self.context,
            self.span,
            self.tree,
            self.concurrency,
            offset,
            len,
        )
        .await
    }

    /// Read entire file into memory.
    pub async fn read_all(&self) -> Result<Vec<u8>> {
        self.read_range(0, crate::cast::usize_from_u64(self.span))
            .await
    }

    /// Shared read-range implementation used by both `read_range` and `poll_read`.
    #[allow(
        clippy::too_many_arguments,
        reason = "internal helper threading already-decomposed reader state from two call sites"
    )]
    #[allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)] // validate_read_range clamps offset + actual_len within the single chunk body; chunk indices and subtree byte offsets are bounded by the file span, so the u64 products/sums cannot overflow
    async fn read_range_with(
        getter: &Arc<G>,
        subtrees: &[SubtreeNode<M>],
        root: &ChunkAddress,
        context: &M::JoinerContext,
        span: u64,
        tree: TreeParams<BODY_SIZE>,
        concurrency: usize,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>> {
        use super::helpers::{ReadRangeCheck, validate_read_range};

        let (offset, actual_len) = match validate_read_range::<BODY_SIZE>(offset, len, span) {
            ReadRangeCheck::Empty => return Ok(Vec::new()),
            ReadRangeCheck::SingleChunk { offset, actual_len } => {
                let chunk = getter.get(root).await.map_err(FileError::getter)?;
                let body = M::decode_body::<_, BODY_SIZE>(chunk.into_envelope(), context, span)?;
                // offset < span <= BODY_SIZE in the single-chunk case.
                let start = crate::cast::usize_from_u64(offset);
                let end = start + actual_len;
                return Ok(body[start..end].to_vec());
            }
            ReadRangeCheck::MultiChunk { offset, actual_len } => (offset, actual_len),
        };

        let chunk_range = tree.chunks_for_range(offset, crate::cast::u64_from_usize(actual_len));
        let range_start_byte = chunk_range.start * crate::cast::u64_from_usize(BODY_SIZE);
        let range_end_byte = chunk_range.end * crate::cast::u64_from_usize(BODY_SIZE);

        let relevant: Vec<_> = subtrees
            .iter()
            .filter(|st| {
                st.byte_offset < range_end_byte && st.byte_offset + st.span > range_start_byte
            })
            .cloned()
            .collect();

        let bodies =
            collect_subtree_bodies::<G, M, BODY_SIZE>(getter, relevant, chunk_range, concurrency)
                .await?;

        Ok(super::tree::assemble_range(
            &tree,
            offset,
            actual_len,
            &chunk_range,
            &bodies,
        ))
    }

    /// Update read position (synchronous — just updates internal state).
    pub fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.position = super::resolve_seek_position(pos, self.position, self.span)?;
        Ok(self.position)
    }

    /// Convert into a stream of leaf chunk bodies.
    pub fn into_stream(self) -> impl Stream<Item = Result<Bytes>> {
        let getter = self.getter;
        let chunk_range = self.tree.chunks_for_range(0, self.span);

        struct State<M: JoinMode> {
            subtrees: std::vec::IntoIter<SubtreeNode<M>>,
            pending: std::vec::IntoIter<Bytes>,
        }

        let state = State {
            subtrees: self.subtrees.into_iter(),
            pending: Vec::new().into_iter(),
        };

        stream::unfold(state, move |mut state| {
            let getter = Arc::clone(&getter);
            async move {
                // Drain pending leaf bodies from the last subtree.
                if let Some(body) = state.pending.next() {
                    return Some((Ok(body), state));
                }

                // Fetch the next subtree's leaf bodies.
                let st = state.subtrees.next()?;
                match read_subtree_bodies::<G, M, BODY_SIZE>(&*getter, &st, &chunk_range).await {
                    Ok(bodies) => {
                        let mut iter = bodies.into_iter();
                        match iter.next() {
                            Some(first) => {
                                state.pending = iter;
                                Some((Ok(first), state))
                            }
                            None => Some((Ok(Bytes::new()), state)),
                        }
                    }
                    Err(e) => Some((Err(e), state)),
                }
            }
        })
    }

    /// Convert into a stream of `(byte_offset, leaf_body)` pairs fetched with
    /// bounded concurrency and yielded out of order as each leaf lands.
    ///
    /// Unlike [`into_stream`](Self::into_stream), which walks subtrees one at a
    /// time and emits bytes in file order, this keeps up to `concurrency`
    /// subtree fetches in flight and yields each leaf the moment its subtree
    /// resolves, tagged with its absolute byte offset in the file. Reassembling
    /// the pairs by offset reproduces the file; nothing is buffered into a
    /// single contiguous result, so peak memory is bounded by the in-flight
    /// width, not the file size. Set the width with
    /// [`with_concurrency`](Self::with_concurrency).
    #[allow(clippy::arithmetic_side_effects)] // base + leaf_index * BODY_SIZE addresses a leaf inside the file, so it is bounded by the file span (u64)
    pub fn into_offset_stream(self) -> impl Stream<Item = Result<(u64, Bytes)>> {
        let getter = self.getter;
        let concurrency = self.concurrency;
        let chunk_range = self.tree.chunks_for_range(0, self.span);

        // Each subtree fetch resolves to its base offset plus its leaves in tree
        // order; `buffer_unordered` yields whichever subtree finishes first.
        let subtrees = stream::iter(self.subtrees)
            .map(move |st| {
                let getter = Arc::clone(&getter);
                async move {
                    let base = st.byte_offset;
                    let bodies =
                        read_subtree_bodies::<G, M, BODY_SIZE>(&*getter, &st, &chunk_range).await?;
                    Ok::<(u64, Vec<Bytes>), FileError>((base, bodies))
                }
            })
            .buffer_unordered(concurrency.max(1));

        // Flatten each resolved subtree into per-leaf `(offset, body)` pairs,
        // draining one subtree's leaves before polling the next ready subtree.
        struct State<S> {
            subtrees: S,
            base: u64,
            leaf_index: usize,
            pending: std::vec::IntoIter<Bytes>,
        }

        let state = State {
            subtrees: Box::pin(subtrees),
            base: 0,
            leaf_index: 0,
            pending: Vec::new().into_iter(),
        };

        stream::unfold(state, move |mut state| async move {
            loop {
                // Drain leaves already in hand, offsetting each leaf from its
                // subtree base by a whole body per preceding leaf.
                if let Some(body) = state.pending.next() {
                    let offset = state.base
                        + crate::cast::u64_from_usize(state.leaf_index)
                            * crate::cast::u64_from_usize(BODY_SIZE);
                    state.leaf_index += 1;
                    return Some((Ok((offset, body)), state));
                }

                // Pull the next resolved subtree (out of order).
                match state.subtrees.next().await? {
                    Ok((base, bodies)) => {
                        state.base = base;
                        state.leaf_index = 0;
                        state.pending = bodies.into_iter();
                        // Loop back to emit this subtree's first leaf.
                    }
                    Err(e) => return Some((Err(e), state)),
                }
            }
        })
    }

    /// Convert into a stream of `(byte_offset, leaf_body)` pairs fetched with
    /// per-chunk bounded concurrency and yielded out of order as each leaf lands.
    ///
    /// Where [`into_offset_stream`](Self::into_offset_stream) keeps up to
    /// `concurrency` *subtree* fetches in flight and walks each subtree as a
    /// sequential descent, this walks the tree at *chunk* granularity: every
    /// intermediate and leaf fetch competes for the same bounded in-flight pool,
    /// so up to `concurrency` individual chunks are in flight regardless of how
    /// few top-level subtrees the tree has. Effective leaf concurrency is
    /// `min(concurrency, leaf_count)` even for a tree that fans into one wide
    /// subtree, which the subtree-granular variant cannot reach.
    ///
    /// The walk is a bounded producer/worker model expressed without a spawned
    /// task: a queue of pending tree nodes feeds a [`FuturesUnordered`] pool
    /// refilled to the width each step. An intermediate node resolves to its
    /// overlapping children (re-queued); a leaf resolves to its
    /// `(byte_offset, body)` pair. A leaf fetch that fails is re-enqueued (up to
    /// a bounded retry budget) and the worker pulls the next node, so a slow or
    /// flaky chunk retries without holding its slot or gating ready leaves.
    ///
    /// Peak memory is bounded by the pool width plus the pending queue, not the
    /// file size. Set the width with [`with_concurrency`](Self::with_concurrency).
    /// Reassembling the pairs by offset reproduces the file, byte-for-byte equal
    /// to [`read_all`](Self::read_all).
    #[allow(
        clippy::arithmetic_side_effects,
        clippy::expect_used,
        clippy::missing_panics_doc
    )] // retries - 1 is guarded by retries > 0; intermediate_in_flight moves in lockstep with admissions (bounded by MAX_INTERMEDIATE_IN_FLIGHT) so +1/-1 cannot wrap; the pop_front().expect is guarded by the !is_empty() admission check just above, so no panic is reachable to document
    pub fn into_offset_stream_chunked(self) -> impl Stream<Item = Result<(u64, Bytes)>>
    where
        G: 'static,
    {
        let getter = self.getter;
        let width = self.concurrency.max(1);
        let chunk_range = self.tree.chunks_for_range(0, self.span);

        // One unit of pending work: a tree node plus its remaining retry budget.
        // The budget only decrements on a failed leaf fetch.
        struct Pending<M: JoinMode> {
            node: SubtreeNode<M>,
            retries: u32,
        }

        // What a worker future resolves to once its chunk lands.
        enum Resolved<M: JoinMode> {
            /// A leaf: its absolute byte offset and decoded body.
            Leaf(u64, Bytes),
            /// An intermediate: its overlapping children to re-queue.
            Children(Vec<SubtreeNode<M>>),
            /// A leaf fetch failed with retries left: re-queue this node.
            Retry(Pending<M>),
            /// A fetch failed terminally (retries exhausted or intermediate error).
            Failed(FileError),
        }

        #[cfg(not(target_arch = "wasm32"))]
        type BoxResolvedFuture<M> =
            std::pin::Pin<Box<dyn std::future::Future<Output = Resolved<M>> + Send>>;
        #[cfg(target_arch = "wasm32")]
        type BoxResolvedFuture<M> =
            std::pin::Pin<Box<dyn std::future::Future<Output = Resolved<M>>>>;

        // Fetch one node: a leaf yields its body, an intermediate yields its
        // children. A leaf error consumes one retry, then re-queues or fails.
        async fn fetch_one<G, M, const BS: usize>(
            getter: &G,
            chunk_range: &ChunkRange,
            pending: Pending<M>,
        ) -> Resolved<M>
        where
            G: TrustedStore<AnyChunkSet<BS>>,
            M: JoinMode + MaybeSend + Sync,
        {
            let node = &pending.node;
            let body = match super::mode::read_chunk_body::<M, G, BS>(
                getter,
                &node.addr,
                &node.context,
                node.span,
            )
            .await
            {
                Ok(body) => body,
                Err(e) => {
                    if node.span <= crate::cast::u64_from_usize(BS) && pending.retries > 0 {
                        return Resolved::Retry(Pending {
                            node: pending.node,
                            retries: pending.retries - 1,
                        });
                    }
                    return Resolved::Failed(e);
                }
            };

            if node.span <= crate::cast::u64_from_usize(BS) {
                return Resolved::Leaf(node.byte_offset, body);
            }
            match overlapping_children::<M, BS>(&body, node, chunk_range) {
                Ok(children) => Resolved::Children(children),
                Err(e) => Resolved::Failed(e),
            }
        }

        // Intermediates and leaves run on separate budgets out of `width`: at
        // most `MAX_INTERMEDIATE_IN_FLIGHT` intermediate fetches resolve at once
        // (each exposes a chunk of leaf addresses), and the rest of the width
        // fetches leaf data. Leaves are emitted out of order the moment they
        // land, so no reorder buffer is needed here.
        struct State<G, M: JoinMode, const BS: usize> {
            getter: Arc<G>,
            chunk_range: ChunkRange,
            width: usize,
            node_queue: std::collections::VecDeque<Pending<M>>,
            leaf_queue: std::collections::VecDeque<Pending<M>>,
            intermediate_in_flight: usize,
            in_flight: FuturesUnordered<BoxResolvedFuture<M>>,
        }

        let mut node_queue = std::collections::VecDeque::new();
        let mut leaf_queue = std::collections::VecDeque::new();
        for st in self.subtrees {
            let pending = Pending {
                node: st,
                retries: DEFAULT_LEAF_RETRIES,
            };
            if pending.node.span <= crate::cast::u64_from_usize(BODY_SIZE) {
                leaf_queue.push_back(pending);
            } else {
                node_queue.push_back(pending);
            }
        }

        let state = State::<G, M, BODY_SIZE> {
            getter,
            chunk_range,
            width,
            node_queue,
            leaf_queue,
            intermediate_in_flight: 0,
            in_flight: FuturesUnordered::new(),
        };

        stream::unfold(state, move |mut state| async move {
            loop {
                // Refill the in-flight pool. An intermediate is admitted up to
                // the intermediate cap, reserving at least one slot for a ready
                // leaf at tiny widths; otherwise the slot fetches leaf data. With
                // no leaf ready, intermediates may use the slot to drive the
                // descent. So at most `width` chunks fetch at once and at most
                // `MAX_INTERMEDIATE_IN_FLIGHT` of them are intermediates.
                while state.in_flight.len() < state.width {
                    let leaf_ready = !state.leaf_queue.is_empty();
                    let can_admit_intermediate = state.intermediate_in_flight
                        < MAX_INTERMEDIATE_IN_FLIGHT
                        && !state.node_queue.is_empty();
                    let admit_intermediate = can_admit_intermediate
                        && (!leaf_ready || state.intermediate_in_flight + 1 < state.width);

                    let pending = if admit_intermediate {
                        state.intermediate_in_flight += 1;
                        state.node_queue.pop_front().expect("node queue non-empty")
                    } else if let Some(leaf) = state.leaf_queue.pop_front() {
                        leaf
                    } else {
                        break;
                    };

                    let getter = Arc::clone(&state.getter);
                    let range = state.chunk_range;
                    let fut: BoxResolvedFuture<M> = Box::pin(async move {
                        fetch_one::<G, M, BODY_SIZE>(&*getter, &range, pending).await
                    });
                    state.in_flight.push(fut);
                }

                // Nothing in flight and nothing queued: the tree is drained.
                let resolved = state.in_flight.next().await?;
                match resolved {
                    Resolved::Leaf(offset, body) => {
                        return Some((Ok((offset, body)), state));
                    }
                    Resolved::Children(children) => {
                        state.intermediate_in_flight -= 1;
                        for child in children {
                            let pending = Pending {
                                node: child,
                                retries: DEFAULT_LEAF_RETRIES,
                            };
                            if pending.node.span <= crate::cast::u64_from_usize(BODY_SIZE) {
                                state.leaf_queue.push_back(pending);
                            } else {
                                state.node_queue.push_back(pending);
                            }
                        }
                    }
                    Resolved::Retry(pending) => {
                        // Re-enqueue at the back so the worker pulls fresh work
                        // first; the failed chunk retries without holding a slot.
                        state.leaf_queue.push_back(pending);
                    }
                    Resolved::Failed(e) => return Some((Err(e), state)),
                }
            }
        })
    }

    /// Like [`into_offset_stream_chunked`](Self::into_offset_stream_chunked) but
    /// restricted to the byte range `[start, start + len)`.
    ///
    /// Only subtrees and intermediate children overlapping the range are walked,
    /// and the first and last partial leaves are clipped so each emitted
    /// `(offset, body)` lies inside the range. Offsets stay absolute in the file,
    /// so reassembling the pairs over `[start, start + len)` reproduces
    /// [`read_range`](Self::read_range) byte-for-byte. A whole-file range equals
    /// [`into_offset_stream_chunked`](Self::into_offset_stream_chunked); an empty
    /// or out-of-bounds range yields an empty stream.
    pub fn into_offset_stream_chunked_range(
        self,
        start: u64,
        len: u64,
    ) -> impl Stream<Item = Result<(u64, Bytes)>>
    where
        G: 'static,
    {
        chunked_range_stream_from::<G, M, BODY_SIZE>(
            self.getter,
            self.subtrees,
            self.tree,
            self.span,
            self.concurrency,
            start,
            len,
        )
    }

    /// Convert into an `AsyncRead` reader.
    #[cfg(feature = "tokio")]
    pub fn into_reader(self) -> JoinerReader<G, M, BODY_SIZE> {
        JoinerReader {
            joiner: self,
            buffer: Bytes::new(),
            future: None,
        }
    }
}

/// Build the chunk-granular range stream from already-decomposed joiner parts.
///
/// The single home of the chunk-granular walk: both
/// [`GenericJoiner::into_offset_stream_chunked_range`] (which moves its parts
/// in) and consumers that retain reusable joiner state (which clone their parts
/// in) drive the same implementation. Walks only the subtrees and intermediate
/// children overlapping `[start, start + len)`, clips boundary leaves to the
/// range, and keeps absolute offsets, so the returned stream is identical to the
/// inherent method.
#[allow(clippy::arithmetic_side_effects, clippy::expect_used)] // range_end >= range_start by the min/saturating clamps above; retries - 1 is guarded by retries > 0; in-flight counters move in lockstep with admissions; leaf/chunk offsets are bounded by the file span; range_end - leaf_start is guarded by the leaf_start >= range_end continue; the pop_front().expect is guarded by the !is_empty() admission check just above
pub(crate) fn chunked_range_stream_from<G, M, const BODY_SIZE: usize>(
    getter: Arc<G>,
    subtrees: Vec<SubtreeNode<M>>,
    tree: TreeParams<BODY_SIZE>,
    span: u64,
    concurrency: usize,
    start: u64,
    len: u64,
) -> impl Stream<Item = Result<(u64, Bytes)>> + 'static
where
    G: TrustedStore<AnyChunkSet<BODY_SIZE>> + 'static,
    M: JoinMode + MaybeSend + Sync,
{
    let width = concurrency.max(1);

    // Clamp the requested window to the file and derive the chunk range that
    // seeds the queue and prunes intermediate children, exactly as the full
    // walk uses the whole-file chunk range.
    let range_start = start.min(span);
    let range_end = (start.saturating_add(len)).min(span);
    let chunk_range = tree.chunks_for_range(range_start, range_end - range_start);

    // One unit of pending work: a tree node plus its remaining retry budget.
    // The budget only decrements on a failed leaf fetch.
    struct Pending<M: JoinMode> {
        node: SubtreeNode<M>,
        retries: u32,
    }

    // What a worker future resolves to once its chunk lands.
    enum Resolved<M: JoinMode> {
        /// A leaf: its absolute byte offset and decoded body.
        Leaf(u64, Bytes),
        /// An intermediate: its overlapping children to re-queue.
        Children(Vec<SubtreeNode<M>>),
        /// A leaf fetch failed with retries left: re-queue this node.
        Retry(Pending<M>),
        /// A fetch failed terminally (retries exhausted or intermediate error).
        Failed(FileError),
    }

    #[cfg(not(target_arch = "wasm32"))]
    type BoxResolvedFuture<M> =
        std::pin::Pin<Box<dyn std::future::Future<Output = Resolved<M>> + Send>>;
    #[cfg(target_arch = "wasm32")]
    type BoxResolvedFuture<M> = std::pin::Pin<Box<dyn std::future::Future<Output = Resolved<M>>>>;

    // Fetch one node: a leaf yields its body, an intermediate yields its
    // overlapping children. A leaf error consumes one retry, then re-queues
    // or fails.
    async fn fetch_one<G, M, const BS: usize>(
        getter: &G,
        chunk_range: &ChunkRange,
        pending: Pending<M>,
    ) -> Resolved<M>
    where
        G: TrustedStore<AnyChunkSet<BS>>,
        M: JoinMode + MaybeSend + Sync,
    {
        let node = &pending.node;
        let body = match super::mode::read_chunk_body::<M, G, BS>(
            getter,
            &node.addr,
            &node.context,
            node.span,
        )
        .await
        {
            Ok(body) => body,
            Err(e) => {
                if node.span <= crate::cast::u64_from_usize(BS) && pending.retries > 0 {
                    return Resolved::Retry(Pending {
                        node: pending.node,
                        retries: pending.retries - 1,
                    });
                }
                return Resolved::Failed(e);
            }
        };

        if node.span <= crate::cast::u64_from_usize(BS) {
            return Resolved::Leaf(node.byte_offset, body);
        }
        match overlapping_children::<M, BS>(&body, node, chunk_range) {
            Ok(children) => Resolved::Children(children),
            Err(e) => Resolved::Failed(e),
        }
    }

    // Intermediates and leaves run on separate budgets out of `width`, exactly
    // as the whole-file walk: at most `MAX_INTERMEDIATE_IN_FLIGHT` intermediate
    // fetches resolve at once and the rest of the width fetches leaf data, so a
    // wide frontier no longer front-loads ahead of the first in-range leaf.
    struct State<G, M: JoinMode> {
        getter: Arc<G>,
        chunk_range: ChunkRange,
        range_start: u64,
        range_end: u64,
        width: usize,
        node_queue: std::collections::VecDeque<Pending<M>>,
        leaf_queue: std::collections::VecDeque<Pending<M>>,
        intermediate_in_flight: usize,
        in_flight: FuturesUnordered<BoxResolvedFuture<M>>,
    }

    // Seed the queues with only the subtrees overlapping the range, splitting
    // leaves from intermediates; an empty range leaves both empty and the
    // stream finishes at once.
    let mut node_queue = std::collections::VecDeque::new();
    let mut leaf_queue = std::collections::VecDeque::new();
    if range_end > range_start {
        let range_start_byte = chunk_range.start * crate::cast::u64_from_usize(BODY_SIZE);
        let range_end_byte = chunk_range.end * crate::cast::u64_from_usize(BODY_SIZE);
        for st in subtrees {
            if st.byte_offset < range_end_byte && st.byte_offset + st.span > range_start_byte {
                let pending = Pending {
                    node: st,
                    retries: DEFAULT_LEAF_RETRIES,
                };
                if pending.node.span <= crate::cast::u64_from_usize(BODY_SIZE) {
                    leaf_queue.push_back(pending);
                } else {
                    node_queue.push_back(pending);
                }
            }
        }
    }

    let state = State::<G, M> {
        getter,
        chunk_range,
        range_start,
        range_end,
        width,
        node_queue,
        leaf_queue,
        intermediate_in_flight: 0,
        in_flight: FuturesUnordered::new(),
    };

    stream::unfold(state, move |mut state| async move {
        loop {
            // Refill the in-flight pool: intermediates up to the cap (reserving
            // a leaf slot at tiny widths), the rest of the width for leaf data.
            while state.in_flight.len() < state.width {
                let leaf_ready = !state.leaf_queue.is_empty();
                let can_admit_intermediate = state.intermediate_in_flight
                    < MAX_INTERMEDIATE_IN_FLIGHT
                    && !state.node_queue.is_empty();
                let admit_intermediate = can_admit_intermediate
                    && (!leaf_ready || state.intermediate_in_flight + 1 < state.width);

                let pending = if admit_intermediate {
                    state.intermediate_in_flight += 1;
                    state.node_queue.pop_front().expect("node queue non-empty")
                } else if let Some(leaf) = state.leaf_queue.pop_front() {
                    leaf
                } else {
                    break;
                };

                let getter = Arc::clone(&state.getter);
                let range = state.chunk_range;
                let fut: BoxResolvedFuture<M> = Box::pin(async move {
                    fetch_one::<G, M, BODY_SIZE>(&*getter, &range, pending).await
                });
                state.in_flight.push(fut);
            }

            // Nothing in flight and nothing queued: the range is drained.
            let resolved = state.in_flight.next().await?;
            match resolved {
                Resolved::Leaf(leaf_start, body) => {
                    // Clip the leaf to the range. Offsets stay absolute, so a
                    // boundary leaf emits only its in-range slice, and the
                    // emitted offset is the later of the leaf start and the
                    // range start.
                    let leaf_end = leaf_start + crate::cast::u64_from_usize(body.len());
                    if leaf_end <= state.range_start || leaf_start >= state.range_end {
                        continue;
                    }
                    // Both clip bounds are <= body.len(), so they fit usize.
                    let clip_lo =
                        crate::cast::usize_from_u64(state.range_start.saturating_sub(leaf_start));
                    let clip_hi = crate::cast::usize_from_u64(
                        (state.range_end - leaf_start).min(crate::cast::u64_from_usize(body.len())),
                    );
                    let offset = leaf_start.max(state.range_start);
                    return Some((Ok((offset, body.slice(clip_lo..clip_hi))), state));
                }
                Resolved::Children(children) => {
                    state.intermediate_in_flight -= 1;
                    for child in children {
                        let pending = Pending {
                            node: child,
                            retries: DEFAULT_LEAF_RETRIES,
                        };
                        if pending.node.span <= crate::cast::u64_from_usize(BODY_SIZE) {
                            state.leaf_queue.push_back(pending);
                        } else {
                            state.node_queue.push_back(pending);
                        }
                    }
                }
                Resolved::Retry(pending) => {
                    // Re-enqueue at the back so the worker pulls fresh work
                    // first; the failed chunk retries without holding a slot.
                    state.leaf_queue.push_back(pending);
                }
                Resolved::Failed(e) => return Some((Err(e), state)),
            }
        }
    })
}

/// Wrapper providing `tokio::io::AsyncRead` over a [`GenericJoiner`].
///
/// Created via [`GenericJoiner::into_reader`].
#[cfg(feature = "tokio")]
pub struct JoinerReader<G, M: JoinMode, const BODY_SIZE: usize = DEFAULT_BODY_SIZE>
where
    G: TrustedStore<AnyChunkSet<BODY_SIZE>>,
{
    joiner: GenericJoiner<G, M, BODY_SIZE>,
    buffer: Bytes,
    #[allow(clippy::type_complexity)]
    future: Option<std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>>> + Send>>>,
}

#[cfg(feature = "tokio")]
impl<G, M, const BODY_SIZE: usize> std::fmt::Debug for JoinerReader<G, M, BODY_SIZE>
where
    G: TrustedStore<AnyChunkSet<BODY_SIZE>>,
    M: JoinMode,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinerReader")
            .field("joiner", &self.joiner)
            .field("buffer_len", &self.buffer.len())
            .field("has_pending_future", &self.future.is_some())
            .finish()
    }
}

// Safety: JoinerReader contains no self-referential data.
// The boxed future is heap-allocated and all other fields are plain data.
#[cfg(feature = "tokio")]
impl<G: TrustedStore<AnyChunkSet<BODY_SIZE>>, M: JoinMode, const BODY_SIZE: usize> Unpin
    for JoinerReader<G, M, BODY_SIZE>
{
}

#[cfg(feature = "tokio")]
impl<G, M, const BODY_SIZE: usize> tokio::io::AsyncRead for JoinerReader<G, M, BODY_SIZE>
where
    G: TrustedStore<AnyChunkSet<BODY_SIZE>> + 'static,
    M: JoinMode + Send + Sync + 'static,
{
    #[allow(
        clippy::arithmetic_side_effects,
        clippy::indexing_slicing,
        clippy::unwrap_used
    )] // to_copy = min(len, remaining) bounds both slices; span - position is guarded by the EOF return above; position advances by read lengths bounded by the file span; the unwrap follows the is_none() branch that just set the future
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::task::Poll;

        let this = self.get_mut();

        // Drain any leftover buffer first
        if !this.buffer.is_empty() {
            let to_copy = this.buffer.len().min(buf.remaining());
            buf.put_slice(&this.buffer[..to_copy]);
            this.buffer.advance(to_copy);
            return Poll::Ready(Ok(()));
        }

        // EOF check
        if this.joiner.position >= this.joiner.span {
            return Poll::Ready(Ok(()));
        }

        // Create a future for the next read if we don't have one
        if this.future.is_none() {
            let position = this.joiner.position;
            let remaining = crate::cast::usize_from_u64(this.joiner.span - position);
            let read_len = remaining.min(BODY_SIZE);
            let getter = Arc::clone(&this.joiner.getter);
            let root = this.joiner.root;
            let context = this.joiner.context.clone();
            let span = this.joiner.span;
            let tree = this.joiner.tree;
            let concurrency = this.joiner.concurrency;
            let subtrees: Vec<SubtreeNode<M>> = this.joiner.subtrees.clone();

            let fut = async move {
                GenericJoiner::<G, M, BODY_SIZE>::read_range_with(
                    &getter,
                    &subtrees,
                    &root,
                    &context,
                    span,
                    tree,
                    concurrency,
                    position,
                    read_len,
                )
                .await
            };
            this.future = Some(Box::pin(fut));
        }

        // Poll the future
        let fut = this.future.as_mut().unwrap();
        match fut.as_mut().poll(cx) {
            Poll::Ready(Ok(data)) => {
                this.future = None;
                let bytes = Bytes::from(data);
                this.joiner.position += crate::cast::u64_from_usize(bytes.len());
                let to_copy = bytes.len().min(buf.remaining());
                buf.put_slice(&bytes[..to_copy]);
                if to_copy < bytes.len() {
                    this.buffer = bytes.slice(to_copy..);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => {
                this.future = None;
                Poll::Ready(Err(std::io::Error::other(e)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(feature = "tokio")]
impl<G, M, const BODY_SIZE: usize> tokio::io::AsyncSeek for JoinerReader<G, M, BODY_SIZE>
where
    G: TrustedStore<AnyChunkSet<BODY_SIZE>> + 'static,
    M: JoinMode + Send + Sync + 'static,
{
    fn start_seek(self: std::pin::Pin<&mut Self>, pos: SeekFrom) -> std::io::Result<()> {
        let this = self.get_mut();
        this.joiner.position =
            super::resolve_seek_position(pos, this.joiner.position, this.joiner.span)?;
        this.buffer = Bytes::new();
        this.future = None;
        Ok(())
    }

    fn poll_complete(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        std::task::Poll::Ready(Ok(self.get_mut().joiner.position))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::{Chunk, ChunkOps, StandardChunkSet, Verified};
    use crate::file::split;
    use crate::store::ChunkGet;
    use nectar_testing::{run, yield_now};
    use std::collections::HashMap;

    pub(super) fn split_and_store(data: &[u8]) -> (ChunkAddress, HashMap<ChunkAddress, Chunk>) {
        let (root, store) = split::<DEFAULT_BODY_SIZE>(data).unwrap();
        (root, store.into_chunks())
    }

    generate_plain_joiner_tests!(Joiner);

    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A store that counts `get` calls and can park one chosen address until a
    /// gate is released, so a test can prove the lazy open neither pre-expands
    /// the frontier nor lets a slow intermediate stall other subtrees.
    #[derive(Clone)]
    struct ProbeStore {
        inner: Arc<HashMap<ChunkAddress, Chunk>>,
        gets: Arc<AtomicUsize>,
    }

    impl ChunkGet<StandardChunkSet> for ProbeStore {
        type Trust = Verified;
        type Error = crate::store::ChunkStoreError;
        async fn get(&self, address: &ChunkAddress) -> std::result::Result<Chunk, Self::Error> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            self.inner
                .get(address)
                .cloned()
                .ok_or_else(|| crate::store::ChunkStoreError::not_found(address))
        }
    }

    async fn drain_chunked_to_buf<G>(joiner: GenericJoiner<G, PlainMode>, total: usize) -> Vec<u8>
    where
        G: TrustedStore + 'static,
    {
        let stream = joiner.into_offset_stream_chunked();
        futures::pin_mut!(stream);
        let mut buf = vec![0u8; total];
        while let Some(item) = stream.next().await {
            let (offset, body) = item.unwrap();
            let start = offset as usize;
            buf[start..start + body.len()].copy_from_slice(&body);
        }
        buf
    }

    /// The lazy open seeds the chunked stream from the root and reproduces a
    /// multi-level file byte-for-byte.
    #[test]
    fn streaming_open_assembles_byte_exact() {
        run(async {
            // ~600 leaves -> root over an intermediate level, so the lazy seed must
            // descend intermediates rather than emit a single leaf.
            let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 600 + 123)
                .map(|i| (i % 256) as u8)
                .collect();
            let (root, store) = split_and_store(&data);

            let joiner = Joiner::open_streaming(store, root).await.unwrap();
            let total = joiner.size() as usize;
            let got = drain_chunked_to_buf(joiner, total).await;
            assert_eq!(got, data);
        })
    }

    /// The lazy open fetches only the root before returning: no level-synchronous
    /// frontier expansion, so no intermediate can stall the open. The eager
    /// [`new`](Joiner::new) over the same tree fetches strictly more.
    #[test]
    fn streaming_open_fetches_only_the_root() {
        run(async {
            let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 600)
                .map(|i| (i % 256) as u8)
                .collect();
            let (root, store) = split_and_store(&data);
            let store = Arc::new(store);

            let lazy_gets = Arc::new(AtomicUsize::new(0));
            let lazy = ProbeStore {
                inner: Arc::clone(&store),
                gets: Arc::clone(&lazy_gets),
            };
            let _joiner = Joiner::open_streaming(lazy, root).await.unwrap();
            assert_eq!(
                lazy_gets.load(Ordering::SeqCst),
                1,
                "lazy open fetches only the root"
            );

            let eager_gets = Arc::new(AtomicUsize::new(0));
            let eager = ProbeStore {
                inner: store,
                gets: Arc::clone(&eager_gets),
            };
            let _joiner = Joiner::new(eager, root).await.unwrap();
            assert!(
                eager_gets.load(Ordering::SeqCst) > 1,
                "eager open pre-expands the frontier (root + intermediates)"
            );
        })
    }

    /// A lazily-opened stream reassembles byte-exact even when every fetch is
    /// delayed, proving the descent is correct under latency and reordering.
    #[test]
    fn streaming_open_is_byte_exact_under_latency() {
        run(async {
            let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 300 + 7)
                .map(|i| (i % 256) as u8)
                .collect();
            let (root, store) = split_and_store(&data);
            let joiner = Joiner::open_streaming(store, root)
                .await
                .unwrap()
                .with_concurrency(4);
            let total = joiner.size() as usize;
            let got = drain_chunked_to_buf(joiner, total).await;
            assert_eq!(got, data);
        })
    }

    #[test]
    fn test_joiner_stream() {
        run(async {
            let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3)
                .map(|i| (i % 256) as u8)
                .collect();
            let (root, store) = split_and_store(&data);

            let joiner = Joiner::new(store, root).await.unwrap();
            let chunks: Vec<Result<Bytes>> = joiner.into_stream().collect().await;

            let mut recovered = Vec::new();
            for chunk in chunks {
                recovered.extend_from_slice(&chunk.unwrap());
            }
            assert_eq!(recovered, data);
        })
    }

    /// Drain `into_offset_stream` into an offset-keyed map, asserting every leaf
    /// arrives exactly once, then reassemble by offset and compare to `read_all`.
    async fn assert_offset_stream_matches(data: &[u8]) {
        let (root, store) = split_and_store(data);

        let expected = Joiner::new(store.clone(), root)
            .await
            .unwrap()
            .read_all()
            .await
            .unwrap();

        let joiner = Joiner::new(store, root).await.unwrap();
        let total = joiner.size();
        let pairs: Vec<Result<(u64, Bytes)>> = joiner.into_offset_stream().collect().await;

        let mut reassembled = vec![0u8; total as usize];
        let mut covered = 0u64;
        let mut seen_offsets = std::collections::HashSet::new();
        for pair in pairs {
            let (offset, body) = pair.unwrap();
            assert!(
                seen_offsets.insert(offset),
                "offset {offset} yielded more than once"
            );
            let start = offset as usize;
            let end = start + body.len();
            reassembled[start..end].copy_from_slice(&body);
            covered += body.len() as u64;
        }

        assert_eq!(covered, total, "every byte covered exactly once");
        assert_eq!(reassembled, expected, "offset reassembly equals read_all");
        assert_eq!(reassembled, data, "offset reassembly equals input");
    }

    #[test]
    fn test_offset_stream_small() {
        run(async {
            assert_offset_stream_matches(b"hello world").await;
        })
    }

    #[test]
    fn test_offset_stream_exact_chunk() {
        run(async {
            assert_offset_stream_matches(&vec![0xAB; DEFAULT_BODY_SIZE]).await;
        })
    }

    #[test]
    fn test_offset_stream_multi_chunk() {
        run(async {
            let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3 + 123)
                .map(|i| (i % 256) as u8)
                .collect();
            assert_offset_stream_matches(&data).await;
        })
    }

    #[test]
    fn test_offset_stream_129_chunks() {
        run(async {
            let refs_per_chunk = DEFAULT_BODY_SIZE / super::super::constants::REF_SIZE;
            let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * (refs_per_chunk + 1))
                .map(|i| (i % 256) as u8)
                .collect();
            assert_offset_stream_matches(&data).await;
        })
    }

    #[test]
    fn test_offset_stream_concurrency_one() {
        run(async {
            // Width 1 still yields every leaf with the right offset (degenerate
            // concurrent path), so the reassembly invariant holds independent of fan.
            let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 5 + 7)
                .map(|i| (i % 256) as u8)
                .collect();
            let (root, store) = split_and_store(&data);
            let joiner = Joiner::new(store, root).await.unwrap().with_concurrency(1);
            let total = joiner.size();
            let mut reassembled = vec![0u8; total as usize];
            let stream = joiner.into_offset_stream();
            futures::pin_mut!(stream);
            while let Some(pair) = stream.next().await {
                let (offset, body) = pair.unwrap();
                let start = offset as usize;
                reassembled[start..start + body.len()].copy_from_slice(&body);
            }
            assert_eq!(reassembled, data);
        })
    }

    /// Drain `into_offset_stream_chunked` into an offset-keyed buffer, asserting
    /// every leaf arrives exactly once, then reassemble and compare to `read_all`.
    async fn assert_offset_stream_chunked_matches(data: &[u8]) {
        let (root, store) = split_and_store(data);

        let expected = Joiner::new(store.clone(), root)
            .await
            .unwrap()
            .read_all()
            .await
            .unwrap();

        let joiner = Joiner::new(store, root).await.unwrap();
        let total = joiner.size();
        let pairs: Vec<Result<(u64, Bytes)>> = joiner.into_offset_stream_chunked().collect().await;

        let mut reassembled = vec![0u8; total as usize];
        let mut covered = 0u64;
        let mut seen_offsets = std::collections::HashSet::new();
        for pair in pairs {
            let (offset, body) = pair.unwrap();
            assert!(
                seen_offsets.insert(offset),
                "offset {offset} yielded more than once"
            );
            let start = offset as usize;
            let end = start + body.len();
            reassembled[start..end].copy_from_slice(&body);
            covered += body.len() as u64;
        }

        assert_eq!(covered, total, "every byte covered exactly once");
        assert_eq!(reassembled, expected, "chunked reassembly equals read_all");
        assert_eq!(reassembled, data, "chunked reassembly equals input");
    }

    #[test]
    fn test_offset_stream_chunked_small() {
        run(async {
            assert_offset_stream_chunked_matches(b"hello world").await;
        })
    }

    #[test]
    fn test_offset_stream_chunked_exact_chunk() {
        run(async {
            assert_offset_stream_chunked_matches(&vec![0xAB; DEFAULT_BODY_SIZE]).await;
        })
    }

    #[test]
    fn test_offset_stream_chunked_multi_chunk() {
        run(async {
            let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3 + 123)
                .map(|i| (i % 256) as u8)
                .collect();
            assert_offset_stream_chunked_matches(&data).await;
        })
    }

    #[test]
    fn test_offset_stream_chunked_three_level_tree() {
        run(async {
            // 129 leaves needs a three-level tree, exercising intermediate-node
            // re-queueing in the chunk-granular walk.
            let refs_per_chunk = DEFAULT_BODY_SIZE / super::super::constants::REF_SIZE;
            let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * (refs_per_chunk + 1))
                .map(|i| (i % 256) as u8)
                .collect();
            assert_offset_stream_chunked_matches(&data).await;
        })
    }

    #[test]
    fn test_offset_stream_chunked_concurrency_one() {
        run(async {
            // Width 1 still yields every leaf with the right offset.
            let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 5 + 7)
                .map(|i| (i % 256) as u8)
                .collect();
            let (root, store) = split_and_store(&data);
            let joiner = Joiner::new(store, root).await.unwrap().with_concurrency(1);
            let total = joiner.size();
            let mut reassembled = vec![0u8; total as usize];
            let stream = joiner.into_offset_stream_chunked();
            futures::pin_mut!(stream);
            while let Some(pair) = stream.next().await {
                let (offset, body) = pair.unwrap();
                let start = offset as usize;
                reassembled[start..start + body.len()].copy_from_slice(&body);
            }
            assert_eq!(reassembled, data);
        })
    }

    /// A getter that holds each fetch open across several executor yields so the
    /// test can observe how many fetches the consumer admits at once, proving the
    /// chunk-granular stream reaches per-chunk (not per-subtree) concurrency.
    #[derive(Clone)]
    struct ConcurrencyProbe {
        chunks: Arc<HashMap<ChunkAddress, Chunk>>,
        in_flight: Arc<std::sync::atomic::AtomicUsize>,
        max_in_flight: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl crate::store::ChunkGet<StandardChunkSet> for ConcurrencyProbe {
        type Trust = Verified;
        type Error = crate::store::ChunkStoreError;

        async fn get(&self, address: &ChunkAddress) -> std::result::Result<Chunk, Self::Error> {
            use std::sync::atomic::Ordering;
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            // Yield several times so concurrently-admitted fetches overlap here
            // before any resolves; the peak counter then reflects pool width.
            for _ in 0..8 {
                yield_now().await;
            }
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            self.chunks
                .get(address)
                .cloned()
                .ok_or_else(|| crate::store::ChunkStoreError::not_found(address))
        }
    }

    #[test]
    fn test_offset_stream_chunked_per_chunk_concurrency() {
        run(async {
            // A flat two-level tree: one intermediate over many leaves. The
            // subtree-granular stream would walk it as a single sequential descent
            // (in-flight = 1 leaf at a time); the chunk-granular stream fans the
            // leaves across the width.
            let leaves = 40usize;
            let width = 16usize;
            let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * leaves)
                .map(|i| (i % 256) as u8)
                .collect();
            let (root, store) = split_and_store(&data);

            let probe = ConcurrencyProbe {
                chunks: Arc::new(store),
                in_flight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                max_in_flight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            };
            let max_seen = Arc::clone(&probe.max_in_flight);

            let joiner = Joiner::new(probe, root)
                .await
                .unwrap()
                .with_concurrency(width);

            let total = joiner.size();
            let mut reassembled = vec![0u8; total as usize];
            let stream = joiner.into_offset_stream_chunked();
            futures::pin_mut!(stream);
            while let Some(pair) = stream.next().await {
                let (offset, body) = pair.unwrap();
                let start = offset as usize;
                reassembled[start..start + body.len()].copy_from_slice(&body);
            }
            assert_eq!(reassembled, data, "concurrency probe still reassembles");

            let peak = max_seen.load(std::sync::atomic::Ordering::SeqCst);
            assert!(
                peak >= width,
                "chunk-granular stream should reach width {width} in-flight, saw {peak}"
            );
        })
    }

    /// Branching for a 256-byte body is `256 / REF_SIZE` = 8, so a few hundred
    /// leaves build a wide intermediate frontier with little data. This keeps the
    /// front-load guards cheap while still exercising a frontier far larger than
    /// the intermediate cap.
    const TINY_BODY: usize = 256;

    /// Sealed chunk currency at the tiny body size.
    type TinyChunk = Chunk<Verified, AnyChunkSet<TINY_BODY>>;

    /// Content addresses of every data leaf for `data` under `TINY_BODY`, so a
    /// probe getter can tell a leaf fetch from an intermediate fetch.
    fn tiny_leaf_addresses(data: &[u8]) -> std::collections::HashSet<ChunkAddress> {
        let mut set = std::collections::HashSet::new();
        for block in data.chunks(TINY_BODY) {
            let chunk = crate::chunk::ContentChunk::<TINY_BODY>::new(block.to_vec()).unwrap();
            set.insert(*chunk.address());
        }
        set
    }

    /// A probe getter that records the leaf/intermediate kind of every fetch in
    /// start order, tracks peak concurrent intermediate fetches, and can park one
    /// chosen intermediate until the consumer has delivered `slow_gate` leaves.
    #[derive(Clone)]
    struct OrderProbe {
        chunks: Arc<HashMap<ChunkAddress, TinyChunk>>,
        leaves: Arc<std::collections::HashSet<ChunkAddress>>,
        /// One entry per fetch in start order: `true` for a leaf fetch.
        kinds: Arc<std::sync::Mutex<Vec<bool>>>,
        intermediate_in_flight: Arc<std::sync::atomic::AtomicUsize>,
        peak_intermediate_in_flight: Arc<std::sync::atomic::AtomicUsize>,
        delivered_leaves: Arc<std::sync::atomic::AtomicUsize>,
        slow_addr: Option<ChunkAddress>,
        slow_gate: usize,
    }

    impl OrderProbe {
        fn new(store: HashMap<ChunkAddress, TinyChunk>, data: &[u8]) -> Self {
            Self {
                chunks: Arc::new(store),
                leaves: Arc::new(tiny_leaf_addresses(data)),
                kinds: Arc::new(std::sync::Mutex::new(Vec::new())),
                intermediate_in_flight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                peak_intermediate_in_flight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                delivered_leaves: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                slow_addr: None,
                slow_gate: 0,
            }
        }

        /// Drop the recorded order and the peak counter, e.g. after construction
        /// so only the streaming walk's fetches are measured.
        fn reset(&self) {
            use std::sync::atomic::Ordering;
            self.kinds.lock().unwrap().clear();
            self.peak_intermediate_in_flight.store(0, Ordering::SeqCst);
        }

        /// Number of intermediate fetches before the first leaf fetch in the
        /// recorded order.
        fn intermediates_before_first_leaf(&self) -> usize {
            let kinds = self.kinds.lock().unwrap();
            kinds.iter().take_while(|is_leaf| !**is_leaf).count()
        }

        fn intermediate_fetches(&self) -> usize {
            self.kinds.lock().unwrap().iter().filter(|l| !**l).count()
        }
    }

    impl crate::store::ChunkGet<AnyChunkSet<TINY_BODY>> for OrderProbe {
        type Trust = Verified;
        type Error = crate::store::ChunkStoreError;

        async fn get(&self, address: &ChunkAddress) -> std::result::Result<TinyChunk, Self::Error> {
            use std::sync::atomic::Ordering;
            let is_leaf = self.leaves.contains(address);
            self.kinds.lock().unwrap().push(is_leaf);
            if !is_leaf {
                let now = self.intermediate_in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak_intermediate_in_flight
                    .fetch_max(now, Ordering::SeqCst);
            }
            if self.slow_addr == Some(*address) {
                while self.delivered_leaves.load(Ordering::SeqCst) < self.slow_gate {
                    yield_now().await;
                }
            }
            for _ in 0..4 {
                yield_now().await;
            }
            if !is_leaf {
                self.intermediate_in_flight.fetch_sub(1, Ordering::SeqCst);
            }
            self.chunks
                .get(address)
                .cloned()
                .ok_or_else(|| crate::store::ChunkStoreError::not_found(address))
        }
    }

    fn tiny_deep_data(leaves: usize) -> Vec<u8> {
        (0..TINY_BODY * leaves).map(|i| (i % 251) as u8).collect()
    }

    /// The regression guard: on a wide frontier the first data leaf must be
    /// fetched after only a short descent (a few intermediates), never after the
    /// whole frontier is drained. The breadth-first walk fetched every
    /// intermediate first, so the first leaf landed ~frontier-size fetches in.
    #[test]
    fn test_offset_stream_chunked_first_leaf_before_frontier() {
        run(async {
            let data = tiny_deep_data(900);
            let (root, store) = split::<TINY_BODY>(&data).unwrap();
            let probe = OrderProbe::new(store.into_chunks(), &data);

            let joiner = Joiner::<_, TINY_BODY>::new(probe.clone(), root)
                .await
                .unwrap();
            // Measure only the streaming walk, not the upfront frontier expansion.
            probe.reset();

            let total = joiner.size();
            let mut reassembled = vec![0u8; total as usize];
            let stream = joiner.into_offset_stream_chunked();
            futures::pin_mut!(stream);
            while let Some(pair) = stream.next().await {
                let (offset, body) = pair.unwrap();
                reassembled[offset as usize..offset as usize + body.len()].copy_from_slice(&body);
            }
            assert_eq!(reassembled, data, "deep-tree reassembly is byte-exact");

            let frontier = probe.intermediate_fetches();
            assert!(
                frontier >= 40,
                "test needs a frontier far larger than the cap, saw {frontier}"
            );
            let before = probe.intermediates_before_first_leaf();
            assert!(
                before <= 4 * MAX_INTERMEDIATE_IN_FLIGHT,
                "first leaf fetched after {before} intermediates (frontier {frontier}); \
             expected a short descent, not the whole frontier"
            );
        })
    }

    /// Intermediate fetches in flight never exceed the cap.
    #[test]
    fn test_offset_stream_chunked_intermediate_cap() {
        run(async {
            let data = tiny_deep_data(900);
            let (root, store) = split::<TINY_BODY>(&data).unwrap();
            let probe = OrderProbe::new(store.into_chunks(), &data);

            let joiner = Joiner::<_, TINY_BODY>::new(probe.clone(), root)
                .await
                .unwrap()
                .with_concurrency(16);
            probe.reset();

            let stream = joiner.into_offset_stream_chunked();
            futures::pin_mut!(stream);
            while let Some(pair) = stream.next().await {
                pair.unwrap();
            }

            let peak = probe
                .peak_intermediate_in_flight
                .load(std::sync::atomic::Ordering::SeqCst);
            assert!(
                peak <= MAX_INTERMEDIATE_IN_FLIGHT,
                "intermediate in-flight peak {peak} exceeds cap {MAX_INTERMEDIATE_IN_FLIGHT}"
            );
        })
    }

    /// A single slow intermediate must not stall the rest of the stream: leaves
    /// from other subtrees keep flowing while it is parked. The slow node parks
    /// until the consumer has delivered a batch of leaves, so completion proves
    /// those leaves were delivered without it.
    #[test]
    fn test_offset_stream_chunked_slow_intermediate_does_not_stall() {
        run(async {
            let data = tiny_deep_data(900);
            let (root, store) = split::<TINY_BODY>(&data).unwrap();
            let store = store.into_chunks();

            // Park an intermediate that only the streaming walk fetches: a leaf
            // parent, every child a data leaf. An upper intermediate is read by
            // the upfront frontier expansion in `new`, before any leaf can be
            // delivered, so parking one there deadlocks the gate below.
            let leaves = tiny_leaf_addresses(&data);
            let slow = *store
                .iter()
                .find(|(addr, chunk)| {
                    let body = chunk.envelope().data();
                    !leaves.contains(*addr)
                        && **addr != root
                        && body.len() % super::super::constants::REF_SIZE == 0
                        && body.chunks(super::super::constants::REF_SIZE).all(|child| {
                            ChunkAddress::from_slice(child).is_ok_and(|a| leaves.contains(&a))
                        })
                })
                .expect("a leaf-parent intermediate exists")
                .0;

            let mut probe = OrderProbe::new(store, &data);
            probe.slow_addr = Some(slow);
            probe.slow_gate = 100;
            let delivered = Arc::clone(&probe.delivered_leaves);

            let joiner = Joiner::<_, TINY_BODY>::new(probe.clone(), root)
                .await
                .unwrap();

            let total = joiner.size();
            let mut reassembled = vec![0u8; total as usize];
            let stream = joiner.into_offset_stream_chunked();
            futures::pin_mut!(stream);
            while let Some(pair) = stream.next().await {
                let (offset, body) = pair.unwrap();
                reassembled[offset as usize..offset as usize + body.len()].copy_from_slice(&body);
                delivered.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            assert_eq!(
                reassembled, data,
                "stream completes past the slow intermediate"
            );
        })
    }

    /// Drain `into_offset_stream_chunked_range(start, len)`, reassemble the
    /// clipped `(offset, bytes)` pairs over `[start, start + len)`, and assert it
    /// equals `read_range(start, len)` byte-for-byte. Runs at the default width
    /// and at width 1.
    async fn assert_offset_stream_chunked_range_matches(data: &[u8], start: u64, len: u64) {
        for width in [DEFAULT_ASYNC_CONCURRENCY, 1] {
            let (root, store) = split_and_store(data);

            let expected = Joiner::new(store.clone(), root)
                .await
                .unwrap()
                .read_range(start, len as usize)
                .await
                .unwrap();

            let joiner = Joiner::new(store, root)
                .await
                .unwrap()
                .with_concurrency(width);
            let pairs: Vec<Result<(u64, Bytes)>> = joiner
                .into_offset_stream_chunked_range(start, len)
                .collect()
                .await;

            let mut reassembled = vec![0u8; expected.len()];
            let mut seen_offsets = std::collections::HashSet::new();
            for pair in pairs {
                let (offset, body) = pair.unwrap();
                assert!(
                    offset >= start && offset + body.len() as u64 <= start + len,
                    "offset {offset} (+{}) outside [{start}, {})",
                    body.len(),
                    start + len
                );
                assert!(
                    seen_offsets.insert(offset),
                    "offset {offset} yielded more than once (width {width})"
                );
                let rel = (offset - start) as usize;
                reassembled[rel..rel + body.len()].copy_from_slice(&body);
            }

            assert_eq!(
                reassembled, expected,
                "range reassembly equals read_range (width {width}, start {start}, len {len})"
            );
        }
    }

    #[test]
    fn test_offset_stream_chunked_range_windows() {
        run(async {
            let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 5 + 321)
                .map(|i| (i % 256) as u8)
                .collect();
            let bs = DEFAULT_BODY_SIZE as u64;
            let total = data.len() as u64;

            // sub-leaf: start and len both inside one leaf
            assert_offset_stream_chunked_range_matches(&data, bs + 10, 50).await;
            // leaf-aligned single leaf
            assert_offset_stream_chunked_range_matches(&data, bs, bs).await;
            // spans several leaves, partial at both ends
            assert_offset_stream_chunked_range_matches(&data, bs / 2, bs * 3 + 7).await;
            // last partial leaf
            assert_offset_stream_chunked_range_matches(&data, bs * 5, total - bs * 5).await;
            // whole file (must equal read_all)
            assert_offset_stream_chunked_range_matches(&data, 0, total).await;
            // zero-len (empty)
            assert_offset_stream_chunked_range_matches(&data, bs, 0).await;
        })
    }

    #[test]
    fn test_offset_stream_chunked_range_whole_equals_chunked() {
        run(async {
            // A whole-file range must reproduce `into_offset_stream_chunked` exactly:
            // same leaves, same absolute offsets, same bodies.
            let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3 + 99)
                .map(|i| (i % 256) as u8)
                .collect();
            let (root, store) = split_and_store(&data);

            let full = Joiner::new(store.clone(), root).await.unwrap();
            let total = full.size();
            let mut from_full: Vec<(u64, Vec<u8>)> = full
                .into_offset_stream_chunked()
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .map(|p| {
                    let (o, b) = p.unwrap();
                    (o, b.to_vec())
                })
                .collect();
            from_full.sort_by_key(|(o, _)| *o);

            let ranged = Joiner::new(store, root).await.unwrap();
            let mut from_range: Vec<(u64, Vec<u8>)> = ranged
                .into_offset_stream_chunked_range(0, total)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .map(|p| {
                    let (o, b) = p.unwrap();
                    (o, b.to_vec())
                })
                .collect();
            from_range.sort_by_key(|(o, _)| *o);

            assert_eq!(
                from_range, from_full,
                "whole-file range equals chunked walk"
            );

            // And the reassembly equals read_all.
            let expected = Joiner::new(split_and_store(&data).1, root)
                .await
                .unwrap()
                .read_all()
                .await
                .unwrap();
            let mut reassembled = vec![0u8; total as usize];
            for (o, b) in &from_range {
                reassembled[*o as usize..*o as usize + b.len()].copy_from_slice(b);
            }
            assert_eq!(reassembled, expected);
        })
    }

    #[cfg(feature = "encryption")]
    mod encrypted {
        use super::*;
        use crate::file::split_encrypted;

        fn encrypted_split_and_store(
            data: &[u8],
        ) -> (
            crate::chunk::encryption::EncryptedChunkRef,
            HashMap<ChunkAddress, Chunk>,
        ) {
            let (root_ref, store) = split_encrypted::<DEFAULT_BODY_SIZE>(data).unwrap();
            (root_ref, store.into_chunks())
        }

        generate_encrypted_joiner_tests!(EncryptedJoiner);

        #[test]
        fn test_encrypted_joiner_stream() {
            run(async {
                let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3)
                    .map(|i| (i % 256) as u8)
                    .collect();
                let (root_ref, store) = encrypted_split_and_store(&data);

                let joiner = EncryptedJoiner::new(store, root_ref).await.unwrap();
                let chunks: Vec<Result<Bytes>> = joiner.into_stream().collect().await;

                let mut recovered = Vec::new();
                for chunk in chunks {
                    recovered.extend_from_slice(&chunk.unwrap());
                }
                assert_eq!(recovered, data);
            })
        }

        /// Encrypted analogue of `assert_offset_stream_chunked_range_matches`.
        /// Encrypted leaf bodies are shorter than `BODY_SIZE` (the span is
        /// stripped), so clipping must key off `body.len()`, never a stride.
        async fn assert_encrypted_range_matches(data: &[u8], start: u64, len: u64) {
            for width in [DEFAULT_ASYNC_CONCURRENCY, 1] {
                let (root_ref, store) = encrypted_split_and_store(data);

                let expected = EncryptedJoiner::new(store.clone(), root_ref.clone())
                    .await
                    .unwrap()
                    .read_range(start, len as usize)
                    .await
                    .unwrap();

                let joiner = EncryptedJoiner::new(store, root_ref)
                    .await
                    .unwrap()
                    .with_concurrency(width);
                let pairs: Vec<Result<(u64, Bytes)>> = joiner
                    .into_offset_stream_chunked_range(start, len)
                    .collect()
                    .await;

                let mut reassembled = vec![0u8; expected.len()];
                let mut seen_offsets = std::collections::HashSet::new();
                for pair in pairs {
                    let (offset, body) = pair.unwrap();
                    assert!(
                        offset >= start && offset + body.len() as u64 <= start + len,
                        "offset {offset} (+{}) outside [{start}, {})",
                        body.len(),
                        start + len
                    );
                    assert!(
                        seen_offsets.insert(offset),
                        "offset {offset} yielded more than once (width {width})"
                    );
                    let rel = (offset - start) as usize;
                    reassembled[rel..rel + body.len()].copy_from_slice(&body);
                }

                assert_eq!(
                    reassembled, expected,
                    "encrypted range equals read_range (width {width}, start {start}, len {len})"
                );
            }
        }

        #[test]
        fn test_encrypted_offset_stream_chunked_range_windows() {
            run(async {
                let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 5 + 321)
                    .map(|i| (i % 256) as u8)
                    .collect();
                let bs = DEFAULT_BODY_SIZE as u64;
                let total = data.len() as u64;

                // sub-leaf
                assert_encrypted_range_matches(&data, bs + 10, 50).await;
                // leaf-aligned single leaf
                assert_encrypted_range_matches(&data, bs, bs).await;
                // spans several leaves
                assert_encrypted_range_matches(&data, bs / 2, bs * 3 + 7).await;
                // last partial leaf
                assert_encrypted_range_matches(&data, bs * 5, total - bs * 5).await;
                // whole file
                assert_encrypted_range_matches(&data, 0, total).await;
                // zero-len
                assert_encrypted_range_matches(&data, bs, 0).await;
            })
        }
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tokio_tests {
    use super::tests::split_and_store;
    use super::*;

    #[tokio::test]
    async fn test_reader_small() {
        use tokio::io::AsyncReadExt;

        let data = b"hello world";
        let (root, store) = split_and_store(data);

        let joiner = Joiner::new(store, root).await.unwrap();
        let mut reader = joiner.into_reader();
        let mut result = Vec::new();
        reader.read_to_end(&mut result).await.unwrap();
        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_reader_multi_chunk() {
        use tokio::io::AsyncReadExt;

        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3 + 123)
            .map(|i| (i % 256) as u8)
            .collect();
        let (root, store) = split_and_store(&data);

        let joiner = Joiner::new(store, root).await.unwrap();
        let mut reader = joiner.into_reader();
        let mut result = Vec::new();
        reader.read_to_end(&mut result).await.unwrap();
        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_reader_seek() {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        let data = b"hello world";
        let (root, store) = split_and_store(data);

        let joiner = Joiner::new(store, root).await.unwrap();
        let mut reader = joiner.into_reader();

        reader.seek(SeekFrom::Start(6)).await.unwrap();
        let mut buf = vec![0u8; 5];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"world");
    }

    #[tokio::test]
    async fn test_reader_seek_back_and_forth() {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3)
            .map(|i| (i % 256) as u8)
            .collect();
        let (root, store) = split_and_store(&data);

        let joiner = Joiner::new(store, root).await.unwrap();
        let mut reader = joiner.into_reader();

        // Read from middle
        reader
            .seek(SeekFrom::Start(DEFAULT_BODY_SIZE as u64))
            .await
            .unwrap();
        let mut buf1 = vec![0u8; 100];
        reader.read_exact(&mut buf1).await.unwrap();
        assert_eq!(&buf1, &data[DEFAULT_BODY_SIZE..DEFAULT_BODY_SIZE + 100]);

        // Seek back to start
        reader.seek(SeekFrom::Start(0)).await.unwrap();
        let mut buf2 = vec![0u8; 100];
        reader.read_exact(&mut buf2).await.unwrap();
        assert_eq!(&buf2, &data[..100]);

        // Seek to near-end
        reader.seek(SeekFrom::End(-50)).await.unwrap();
        let mut buf3 = vec![0u8; 50];
        reader.read_exact(&mut buf3).await.unwrap();
        assert_eq!(&buf3, &data[data.len() - 50..]);
    }
}

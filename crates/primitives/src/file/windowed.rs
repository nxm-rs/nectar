//! Bounded seek-and-play reader over the async joiner.
//!
//! [`WindowedReader`] seeks to a byte offset then streams forward in file order
//! with peak memory bounded by `window` leaf bodies rather than the file size.
//! The window slides with the read cursor: at most `window` leaf fetches plus
//! buffered leaves are held at once, and the lowest-offset (head) leaf is always
//! fetched first, so emission makes progress and the bound holds regardless of
//! resolve order. Reseeking drops the window and repositions against the
//! retained frontier, so no intermediate chunk is re-fetched.

use std::collections::{BTreeMap, VecDeque};
use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::{self, FuturesUnordered, Stream, StreamExt};

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::{AnyChunkSet, ChunkAddress};

use super::error::{FileError, Result};
use super::frontier::{SubtreeNode, overlapping_children};
use super::joiner::{GenericJoiner, MAX_INTERMEDIATE_IN_FLIGHT};
use super::mode::JoinMode;
use super::tree::{ChunkRange, TreeParams};
use crate::store::{MaybeSend, TrustedStore};

/// Number of times a failed leaf fetch is re-enqueued before the walk surfaces
/// the error.
const DEFAULT_LEAF_RETRIES: u32 = 4;

#[cfg(test)]
thread_local! {
    /// Test hook: peak observed `leaf_in_flight + buffered.len()` across a walk
    /// on this thread, so a test can assert the true window bound on leaf bodies
    /// held at once. Thread-local because `block_on` runs each test's walk on its
    /// own thread, so the parallel test runner never interleaves the counter.
    static PEAK_LEAF_OCCUPANCY: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Record a sampled occupancy, keeping the running peak for this thread.
#[cfg(test)]
fn record_occupancy(occupancy: usize) {
    PEAK_LEAF_OCCUPANCY.with(|p| p.set(p.get().max(occupancy)));
}

/// Seek-and-play reader: forward in-order delivery over a window that slides
/// with the read cursor, bounded to `window` in-flight plus buffered leaves.
///
/// Retains the joiner's reusable state (getter, pre-expanded frontier, tree
/// params) so each [`stream`](Self::stream) rebuilds a fresh window-bounded walk
/// from `[position, size)` without re-expanding the frontier. [`seek`](Self::seek)
/// only repositions; it never re-fetches intermediates.
pub struct WindowedReader<G, M: JoinMode, const BODY_SIZE: usize = DEFAULT_BODY_SIZE>
where
    G: TrustedStore<AnyChunkSet<BODY_SIZE>>,
{
    getter: Arc<G>,
    subtrees: Vec<SubtreeNode<M>>,
    tree: TreeParams<BODY_SIZE>,
    span: u64,
    concurrency: usize,
    position: u64,
    window: usize,
    #[allow(
        dead_code,
        reason = "retained reusable joiner state for re-stream-on-seek"
    )]
    root: ChunkAddress,
}

impl<G, M, const BODY_SIZE: usize> std::fmt::Debug for WindowedReader<G, M, BODY_SIZE>
where
    G: TrustedStore<AnyChunkSet<BODY_SIZE>>,
    M: JoinMode,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WindowedReader")
            .field("span", &self.span)
            .field("position", &self.position)
            .field("concurrency", &self.concurrency)
            .finish_non_exhaustive()
    }
}

impl<G, M, const BODY_SIZE: usize> GenericJoiner<G, M, BODY_SIZE>
where
    G: TrustedStore<AnyChunkSet<BODY_SIZE>> + 'static,
    M: JoinMode + MaybeSend + Sync,
{
    /// Seek-and-play reader over a window that slides with the read cursor. Peak
    /// memory is `window` leaf bodies: in-flight plus buffered leaves never
    /// exceed `window`. `window` is clamped to at least 1; a value of at least
    /// `2 * concurrency` keeps the pool full even while the head leaf is the
    /// straggler.
    pub fn into_windowed_reader(self, window: usize) -> WindowedReader<G, M, BODY_SIZE> {
        WindowedReader {
            getter: Arc::clone(self.getter()),
            subtrees: self.subtrees().to_vec(),
            tree: self.tree(),
            span: self.size(),
            concurrency: self.concurrency(),
            position: self.position(),
            root: *self.root(),
            window: window.max(1),
        }
    }
}

impl<G, M, const BODY_SIZE: usize> WindowedReader<G, M, BODY_SIZE>
where
    G: TrustedStore<AnyChunkSet<BODY_SIZE>> + 'static,
    M: JoinMode + MaybeSend + Sync,
{
    /// In-order leaf bodies from the current position to EOF. Each item is one
    /// contiguous run; reassembly is pure concatenation. Peak memory is `window`
    /// leaf bodies: the window slides with the emit cursor, so only leaves within
    /// `window` spans ahead of it are ever fetched or held.
    pub fn stream(&mut self) -> impl Stream<Item = Result<Bytes>> + '_ {
        windowed_walk::<G, M, BODY_SIZE>(
            Arc::clone(&self.getter),
            self.subtrees.clone(),
            self.tree,
            self.span,
            self.concurrency,
            self.position,
            self.window,
        )
    }

    /// Reposition: drop the window and restart the walk from `pos`. Cheap; reuses
    /// the frontier, no intermediate re-fetch.
    pub fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.position = super::resolve_seek_position(pos, self.position, self.span)?;
        Ok(self.position)
    }

    /// Current read position.
    #[inline]
    pub const fn position(&self) -> u64 {
        self.position
    }

    /// Total file size.
    #[inline]
    pub const fn size(&self) -> u64 {
        self.span
    }
}

/// Cfg-gated boxed resolved-future alias: `+ Send` on native, unbounded on wasm.
#[cfg(not(target_arch = "wasm32"))]
type BoxResolvedFuture<M> = Pin<Box<dyn std::future::Future<Output = Resolved<M>> + Send>>;
#[cfg(target_arch = "wasm32")]
type BoxResolvedFuture<M> = Pin<Box<dyn std::future::Future<Output = Resolved<M>>>>;

/// One unit of pending work: a tree node plus its remaining retry budget. The
/// budget only decrements on a failed leaf fetch.
struct Pending<M: JoinMode> {
    node: SubtreeNode<M>,
    retries: u32,
}

/// What a worker future resolves to once its chunk lands.
enum Resolved<M: JoinMode> {
    /// A leaf: its absolute byte offset and decoded body.
    Leaf(u64, Bytes),
    /// An intermediate: the parent byte offset (to retire from the undescended
    /// set) plus its overlapping children to re-queue.
    Children(u64, Vec<SubtreeNode<M>>),
    /// A leaf fetch failed with retries left: re-queue this node.
    Retry(Pending<M>),
    /// A fetch failed terminally (retries exhausted or intermediate error).
    Failed(FileError),
}

/// Is this node a leaf?
#[inline]
fn is_leaf<M: JoinMode, const BS: usize>(node: &SubtreeNode<M>) -> bool {
    node.span <= crate::cast::u64_from_usize(BS)
}

/// Fetch one node: a leaf yields its body, an intermediate yields its children.
/// A leaf error consumes one retry, then re-queues or fails.
#[allow(clippy::arithmetic_side_effects)] // retries - 1 is guarded by retries > 0
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
            if is_leaf::<M, BS>(node) && pending.retries > 0 {
                return Resolved::Retry(Pending {
                    node: pending.node,
                    retries: pending.retries - 1,
                });
            }
            return Resolved::Failed(e);
        }
    };

    if is_leaf::<M, BS>(node) {
        return Resolved::Leaf(node.byte_offset, body);
    }
    let parent_offset = node.byte_offset;
    match overlapping_children::<M, BS>(&body, node, chunk_range) {
        Ok(children) => Resolved::Children(parent_offset, children),
        Err(e) => Resolved::Failed(e),
    }
}

/// Insert into an offset-ordered pending queue, keeping it ascending by byte
/// offset so the front is always the lowest-offset node.
fn insert_by_offset<M: JoinMode>(queue: &mut VecDeque<Pending<M>>, pending: Pending<M>) {
    let pos = queue.partition_point(|p| p.node.byte_offset < pending.node.byte_offset);
    queue.insert(pos, pending);
}

/// Window-bounded in-order walk from `start` to EOF.
///
/// A single walk integrating fetch and reorder. Intermediates and leaves are
/// descended lowest-offset first and run on separate budgets out of `width`: at
/// most `MAX_INTERMEDIATE_IN_FLIGHT` intermediate fetches resolve at once
/// (enough to keep the next region's leaves discovered ahead of the cursor
/// without fetching the whole frontier first), and a leaf is admitted only while
/// in-flight plus buffered leaves are below `window`. Resolved leaves land in a
/// `BTreeMap` reorder buffer keyed by absolute offset; the head is emitted when
/// its offset equals `next_emit_offset`, which then advances by `body.len()`
/// (never a fixed stride, so shorter encrypted leaves stay aligned) and frees a
/// slot, sliding the window forward.
///
/// In-order and deadlock freedom rest on one rule: a queued leaf is admitted
/// only when its offset is below every undescended intermediate's offset (a leaf
/// at or above one could be preceded by a lower leaf that intermediate has yet
/// to yield). Safe leaves admitted lowest-first are therefore a contiguous
/// emittable prefix, so the window can never fill with un-emittable leaves while
/// the head is still hidden behind an intermediate, and descending the lowest
/// intermediate always exposes the head's region next. The count gate keeps
/// in-flight plus buffered leaves at `window`, so peak memory is `window` leaf
/// bodies regardless of resolve order, leaf span, tree depth, or file size.
#[allow(clippy::arithmetic_side_effects, clippy::expect_used)] // span - range_start is guarded by range_start = start.min(span); leaf/chunk offsets are bounded by the file span; the in-flight and pending-intermediate counters move in lockstep with admissions/retirements so +=/-= cannot wrap; each expect follows the emptiness/head check observed just above it
fn windowed_walk<G, M, const BODY_SIZE: usize>(
    getter: Arc<G>,
    subtrees: Vec<SubtreeNode<M>>,
    tree: TreeParams<BODY_SIZE>,
    span: u64,
    concurrency: usize,
    start: u64,
    window: usize,
) -> impl Stream<Item = Result<Bytes>>
where
    G: TrustedStore<AnyChunkSet<BODY_SIZE>> + 'static,
    M: JoinMode + MaybeSend + Sync,
{
    let width = concurrency.max(1);
    let window = window.max(1);

    let range_start = start.min(span);
    let chunk_range = tree.chunks_for_range(range_start, span - range_start);

    struct State<G, M: JoinMode> {
        getter: Arc<G>,
        chunk_range: ChunkRange,
        range_start: u64,
        width: usize,
        window: usize,
        next_emit_offset: u64,
        /// Leaves not yet admitted to the pool, kept in offset order so the
        /// lowest-offset (head) leaf is always admitted first.
        leaf_queue: VecDeque<Pending<M>>,
        /// Intermediate nodes pending descent, kept in offset order so the
        /// lowest-offset subtree is always descended first.
        node_queue: VecDeque<Pending<M>>,
        /// Byte offset -> count of undescended intermediates (queued or in
        /// flight) starting there. The lowest key is the boundary below which a
        /// leaf is safe to admit.
        pending_intermediate_offsets: BTreeMap<u64, usize>,
        /// Count of in-flight intermediate fetches (capped).
        intermediate_in_flight: usize,
        /// Count of in-flight leaf fetches (excludes intermediates).
        leaf_in_flight: usize,
        in_flight: FuturesUnordered<BoxResolvedFuture<M>>,
        buffered: BTreeMap<u64, Bytes>,
    }

    // Seed the queues with the subtrees overlapping `[range_start, span)`,
    // separating leaves from intermediates and recording each intermediate's
    // offset as undescended.
    let mut leaf_queue = VecDeque::new();
    let mut node_queue = VecDeque::new();
    let mut pending_intermediate_offsets: BTreeMap<u64, usize> = BTreeMap::new();
    let range_start_byte = chunk_range.start * crate::cast::u64_from_usize(BODY_SIZE);
    let range_end_byte = chunk_range.end * crate::cast::u64_from_usize(BODY_SIZE);
    for st in subtrees {
        if st.byte_offset >= range_end_byte || st.byte_offset + st.span <= range_start_byte {
            continue;
        }
        let pending = Pending {
            node: st,
            retries: DEFAULT_LEAF_RETRIES,
        };
        if is_leaf::<M, BODY_SIZE>(&pending.node) {
            insert_by_offset(&mut leaf_queue, pending);
        } else {
            *pending_intermediate_offsets
                .entry(pending.node.byte_offset)
                .or_insert(0) += 1;
            insert_by_offset(&mut node_queue, pending);
        }
    }

    let state = State::<G, M> {
        getter,
        chunk_range,
        range_start,
        width,
        window,
        next_emit_offset: range_start,
        leaf_queue,
        node_queue,
        pending_intermediate_offsets,
        intermediate_in_flight: 0,
        leaf_in_flight: 0,
        in_flight: FuturesUnordered::new(),
        buffered: BTreeMap::new(),
    };

    stream::unfold(state, move |mut state| async move {
        loop {
            // Emit the head leaf as soon as it is buffered, advancing the cursor
            // and sliding the window so the refill below admits newly-in-range
            // leaves on the next turn.
            let head_ready = state
                .buffered
                .first_key_value()
                .is_some_and(|(&k, _)| k == state.next_emit_offset);
            if head_ready {
                let (_, body) = state.buffered.pop_first().expect("head just observed");
                state.next_emit_offset += crate::cast::u64_from_usize(body.len());
                return Some((Ok(body), state));
            }

            // Refill the pool. An intermediate is descended up to the cap,
            // reserving a slot for a ready safe leaf at tiny widths; a leaf is
            // admitted only when it is below every undescended intermediate (so
            // no lower leaf can still appear) and a window slot is free. With no
            // safe leaf ready, intermediates drive the descent to expose the
            // head's region.
            loop {
                if state.in_flight.len() >= state.width {
                    break;
                }

                let min_pending_intermediate =
                    state.pending_intermediate_offsets.keys().next().copied();
                let leaf_admissible = state.leaf_queue.front().is_some_and(|p| {
                    min_pending_intermediate.is_none_or(|m| p.node.byte_offset < m)
                }) && state.leaf_in_flight + state.buffered.len()
                    < state.window;

                let can_admit_intermediate = state.intermediate_in_flight
                    < MAX_INTERMEDIATE_IN_FLIGHT
                    && !state.node_queue.is_empty();
                let admit_intermediate = can_admit_intermediate
                    && (!leaf_admissible || state.intermediate_in_flight + 1 < state.width);

                let pending = if admit_intermediate {
                    state.intermediate_in_flight += 1;
                    state.node_queue.pop_front().expect("node queue non-empty")
                } else if leaf_admissible {
                    state.leaf_in_flight += 1;
                    state.leaf_queue.pop_front().expect("front admissible")
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

            // In-flight leaf fetches plus buffered leaves never exceed `window`.
            let occupancy = state.leaf_in_flight + state.buffered.len();
            debug_assert!(
                occupancy <= state.window,
                "windowed walk exceeded the leaf-body bound"
            );
            #[cfg(test)]
            record_occupancy(occupancy);

            // Pool empty and nothing left to admit: the file is drained.
            let resolved = match state.in_flight.next().await {
                Some(r) => r,
                None => return None,
            };
            match resolved {
                Resolved::Leaf(leaf_start, body) => {
                    state.leaf_in_flight -= 1;
                    // Clip the first partial leaf at the read position; offsets
                    // stay absolute, so a boundary leaf buffers only its in-range
                    // tail keyed at the read position.
                    let leaf_end = leaf_start + crate::cast::u64_from_usize(body.len());
                    if leaf_end <= state.range_start {
                        continue;
                    }
                    // clip_lo < body.len() (leaf_end > range_start above).
                    let clip_lo =
                        crate::cast::usize_from_u64(state.range_start.saturating_sub(leaf_start));
                    let offset = leaf_start.max(state.range_start);
                    state.buffered.insert(offset, body.slice(clip_lo..));
                }
                Resolved::Children(parent_offset, children) => {
                    state.intermediate_in_flight -= 1;
                    // Retire the descended parent from the undescended set.
                    if let Some(count) = state.pending_intermediate_offsets.get_mut(&parent_offset)
                    {
                        *count -= 1;
                        if *count == 0 {
                            state.pending_intermediate_offsets.remove(&parent_offset);
                        }
                    }
                    for child in children {
                        let pending = Pending {
                            node: child,
                            retries: DEFAULT_LEAF_RETRIES,
                        };
                        if is_leaf::<M, BODY_SIZE>(&pending.node) {
                            insert_by_offset(&mut state.leaf_queue, pending);
                        } else {
                            *state
                                .pending_intermediate_offsets
                                .entry(pending.node.byte_offset)
                                .or_insert(0) += 1;
                            insert_by_offset(&mut state.node_queue, pending);
                        }
                    }
                }
                Resolved::Retry(pending) => {
                    // Re-enqueue in offset order so the gate still sees the lowest
                    // leaf at the front and the slot it held is freed for refill.
                    state.leaf_in_flight -= 1;
                    insert_by_offset(&mut state.leaf_queue, pending);
                }
                Resolved::Failed(e) => return Some((Err(e), state)),
            }
        }
    })
}

/// Native `AsyncRead` + `AsyncSeek` shim, draining [`WindowedReader::stream`]
/// into a residual buffer.
#[cfg(feature = "tokio")]
pub struct WindowedJoinerReader<G, M: JoinMode, const BODY_SIZE: usize = DEFAULT_BODY_SIZE>
where
    G: TrustedStore<AnyChunkSet<BODY_SIZE>>,
{
    reader: WindowedReader<G, M, BODY_SIZE>,
    residual: Bytes,
    #[allow(clippy::type_complexity)]
    stream: Option<Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>>,
}

#[cfg(feature = "tokio")]
impl<G, M, const BODY_SIZE: usize> std::fmt::Debug for WindowedJoinerReader<G, M, BODY_SIZE>
where
    G: TrustedStore<AnyChunkSet<BODY_SIZE>>,
    M: JoinMode,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WindowedJoinerReader")
            .field("reader", &self.reader)
            .field("residual_len", &self.residual.len())
            .field("has_stream", &self.stream.is_some())
            .finish()
    }
}

#[cfg(feature = "tokio")]
impl<G, M, const BODY_SIZE: usize> WindowedReader<G, M, BODY_SIZE>
where
    G: TrustedStore<AnyChunkSet<BODY_SIZE>> + 'static,
    M: JoinMode + Send + Sync + 'static,
{
    /// Wrap as a tokio `AsyncRead` + `AsyncSeek` reader. Native-only.
    pub fn into_reader(self) -> WindowedJoinerReader<G, M, BODY_SIZE> {
        WindowedJoinerReader {
            reader: self,
            residual: Bytes::new(),
            stream: None,
        }
    }
}

#[cfg(feature = "tokio")]
impl<G: TrustedStore<AnyChunkSet<BODY_SIZE>>, M: JoinMode, const BODY_SIZE: usize> Unpin
    for WindowedJoinerReader<G, M, BODY_SIZE>
{
}

#[cfg(feature = "tokio")]
impl<G, M, const BODY_SIZE: usize> tokio::io::AsyncRead for WindowedJoinerReader<G, M, BODY_SIZE>
where
    G: TrustedStore<AnyChunkSet<BODY_SIZE>> + 'static,
    M: JoinMode + Send + Sync + 'static,
{
    #[allow(
        clippy::arithmetic_side_effects,
        clippy::indexing_slicing,
        clippy::expect_used
    )] // to_copy = min(len, remaining) bounds both slices; position advances by leaf lengths bounded by the file span; the expect follows the is_none() branch that just set the stream
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use bytes::Buf;
        use std::task::Poll;

        let this = self.get_mut();

        if !this.residual.is_empty() {
            let to_copy = this.residual.len().min(buf.remaining());
            buf.put_slice(&this.residual[..to_copy]);
            this.residual.advance(to_copy);
            return Poll::Ready(Ok(()));
        }

        if this.stream.is_none() {
            // Rebuild an owned `'static` stream from the retained joiner parts,
            // so the boxed stream does not borrow `this`. Cloning the parts is the
            // same cost as `stream()`; no frontier re-expansion.
            let r = &this.reader;
            this.stream = Some(Box::pin(windowed_walk::<G, M, BODY_SIZE>(
                Arc::clone(&r.getter),
                r.subtrees.clone(),
                r.tree,
                r.span,
                r.concurrency,
                r.position,
                r.window,
            )));
        }

        let stream = this.stream.as_mut().expect("stream just set");
        match stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(body))) => {
                let to_copy = body.len().min(buf.remaining());
                buf.put_slice(&body[..to_copy]);
                this.reader.position += crate::cast::u64_from_usize(body.len());
                if to_copy < body.len() {
                    this.residual = body.slice(to_copy..);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Err(std::io::Error::other(e))),
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(feature = "tokio")]
impl<G, M, const BODY_SIZE: usize> tokio::io::AsyncSeek for WindowedJoinerReader<G, M, BODY_SIZE>
where
    G: TrustedStore<AnyChunkSet<BODY_SIZE>> + 'static,
    M: JoinMode + Send + Sync + 'static,
{
    fn start_seek(self: Pin<&mut Self>, pos: SeekFrom) -> std::io::Result<()> {
        let this = self.get_mut();
        this.stream = None;
        this.residual = Bytes::new();
        this.reader.seek(pos)?;
        Ok(())
    }

    fn poll_complete(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        std::task::Poll::Ready(Ok(self.get_mut().reader.position))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bmt::DEFAULT_BODY_SIZE;
    use crate::chunk::{Chunk, StandardChunkSet, Verified};
    use crate::file::Joiner;
    use crate::file::split;
    use crate::store::ChunkGet;
    use nectar_testing::run;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn split_and_store(data: &[u8]) -> (ChunkAddress, HashMap<ChunkAddress, Chunk>) {
        let (root, store) = split::<DEFAULT_BODY_SIZE>(data).unwrap();
        (root, store.into_chunks())
    }

    async fn drain(
        reader: &mut WindowedReader<HashMap<ChunkAddress, Chunk>, super::super::mode::PlainMode>,
    ) -> Vec<u8> {
        let stream = reader.stream();
        futures::pin_mut!(stream);
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.extend_from_slice(&item.unwrap());
        }
        out
    }

    #[test]
    fn stream_from_zero_equals_whole_file() {
        run(async {
            let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 5 + 321)
                .map(|i| (i % 256) as u8)
                .collect();
            let (root, store) = split_and_store(&data);
            let joiner = Joiner::new(store, root).await.unwrap();
            let mut reader = joiner.into_windowed_reader(16);
            let got = drain(&mut reader).await;
            assert_eq!(got, data);
        })
    }

    #[test]
    fn seek_then_stream_yields_suffix() {
        run(async {
            let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 5 + 321)
                .map(|i| (i % 256) as u8)
                .collect();
            let bs = DEFAULT_BODY_SIZE as u64;
            let cases = [bs + 10, bs, bs * 3, bs / 2, data.len() as u64 - 1];
            let (root, store) = split_and_store(&data);
            for k in cases {
                let joiner = Joiner::new(store.clone(), root).await.unwrap();
                let mut reader = joiner.into_windowed_reader(16);
                reader.seek(SeekFrom::Start(k)).unwrap();
                let got = drain(&mut reader).await;
                assert_eq!(got, &data[k as usize..], "suffix from {k}");
            }
        })
    }

    #[test]
    fn backward_seek_then_read_is_correct() {
        run(async {
            let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 4 + 17)
                .map(|i| (i % 256) as u8)
                .collect();
            let bs = DEFAULT_BODY_SIZE as u64;
            let (root, store) = split_and_store(&data);
            let joiner = Joiner::new(store, root).await.unwrap();
            let mut reader = joiner.into_windowed_reader(16);
            reader.seek(SeekFrom::Start(bs * 3)).unwrap();
            let _ = drain(&mut reader).await;
            reader.seek(SeekFrom::Start(bs)).unwrap();
            let got = drain(&mut reader).await;
            assert_eq!(got, &data[bs as usize..]);
        })
    }

    #[test]
    fn width_one_correct() {
        run(async {
            let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 5 + 7)
                .map(|i| (i % 256) as u8)
                .collect();
            let (root, store) = split_and_store(&data);
            let joiner = Joiner::new(store, root).await.unwrap().with_concurrency(1);
            let mut reader = joiner.into_windowed_reader(1);
            let got = drain(&mut reader).await;
            assert_eq!(got, data);
        })
    }

    /// A self-waking yield: returns `Pending` once and immediately schedules a
    /// re-poll, so it makes progress under `futures::executor::block_on`.
    async fn yield_now() {
        struct YieldNow(bool);
        impl std::future::Future for YieldNow {
            type Output = ();
            fn poll(
                mut self: Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<()> {
                if self.0 {
                    std::task::Poll::Ready(())
                } else {
                    self.0 = true;
                    cx.waker().wake_by_ref();
                    std::task::Poll::Pending
                }
            }
        }
        YieldNow(false).await
    }

    /// A getter that records peak concurrent fetches so a test can assert the
    /// reorder buffer never admits more than `window` leaves at once.
    #[derive(Clone)]
    struct BoundProbe {
        chunks: Arc<HashMap<ChunkAddress, Chunk>>,
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
    }

    impl ChunkGet<StandardChunkSet> for BoundProbe {
        type Trust = Verified;
        type Error = crate::store::ChunkStoreError;

        async fn get(&self, address: &ChunkAddress) -> std::result::Result<Chunk, Self::Error> {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            // Hold the fetch open across several self-waking yields so
            // concurrently admitted leaves overlap here before any resolves.
            for _ in 0..4 {
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
    fn reorder_buffer_never_exceeds_window() {
        run(async {
            // Many leaves, small window: leaf fetches overlap, so without the bound
            // the in-flight peak would climb to the leaf count. The window caps it.
            let leaves = 40usize;
            let window = 6usize;
            let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * leaves)
                .map(|i| (i % 256) as u8)
                .collect();
            let (root, store) = split_and_store(&data);

            let probe = BoundProbe {
                chunks: Arc::new(store),
                in_flight: Arc::new(AtomicUsize::new(0)),
                max_in_flight: Arc::new(AtomicUsize::new(0)),
            };
            let max_seen = Arc::clone(&probe.max_in_flight);

            let joiner = Joiner::new(probe, root)
                .await
                .unwrap()
                .with_concurrency(leaves);
            let mut reader = joiner.into_windowed_reader(window);
            let stream = reader.stream();
            futures::pin_mut!(stream);
            let mut total = 0usize;
            while let Some(item) = stream.next().await {
                total += item.unwrap().len();
            }
            assert_eq!(total, data.len());

            // The reorder buffer admits at most `window` leaves, so concurrent leaf
            // fetches (plus the intermediate node fetches that seed children) never
            // climb far above the window. Allow a small slack for in-flight
            // intermediates, but assert it is nowhere near the leaf count.
            let peak = max_seen.load(Ordering::SeqCst);
            assert!(
                peak <= window + 2,
                "in-flight peak {peak} exceeds window {window} (+slack)"
            );
        })
    }

    /// A getter that delays the leaf at `slow_offset` until `gate` other leaves
    /// have resolved. This is the adversarial resolve order the original reorder
    /// buffer mishandled: with the head leaf landing last, the old buffer kept
    /// fetching and buffering later leaves, so resident leaf bodies (in-flight
    /// plus buffered) climbed to roughly twice the window. The sliding window
    /// refuses to fetch past a free slot, holding the total at the window.
    #[derive(Clone)]
    struct HeadSlow {
        chunks: Arc<HashMap<ChunkAddress, Chunk>>,
        /// File offset of the head leaf to delay.
        slow_offset: u64,
        /// Number of leaves released so far (used to gate the slow leaf).
        released: Arc<AtomicUsize>,
        /// How many other leaves must release before the slow leaf may.
        gate: usize,
        /// Address-to-offset map so the getter can recognise the head leaf.
        leaf_offsets: Arc<HashMap<ChunkAddress, u64>>,
    }

    impl ChunkGet<StandardChunkSet> for HeadSlow {
        type Trust = Verified;
        type Error = crate::store::ChunkStoreError;

        async fn get(&self, address: &ChunkAddress) -> std::result::Result<Chunk, Self::Error> {
            let is_slow = self.leaf_offsets.get(address) == Some(&self.slow_offset);
            if is_slow {
                // Park until the gate of other leaves has released, then a few
                // more yields so a buffer would overflow if it were unbounded.
                while self.released.load(Ordering::SeqCst) < self.gate {
                    yield_now().await;
                }
                for _ in 0..4 {
                    yield_now().await;
                }
            } else {
                for _ in 0..4 {
                    yield_now().await;
                }
                if self.leaf_offsets.contains_key(address) {
                    self.released.fetch_add(1, Ordering::SeqCst);
                }
            }

            self.chunks
                .get(address)
                .cloned()
                .ok_or_else(|| crate::store::ChunkStoreError::not_found(address))
        }
    }

    /// Distinct byte per file position, so every leaf body (and thus its
    /// content address) is unique. A `(i % 256)` fill would make all full leaves
    /// identical, collapsing the address-to-offset map and defeating the
    /// per-leaf delay below.
    fn unique_byte(i: u64) -> u8 {
        (i.wrapping_mul(2_654_435_761) >> 11) as u8
    }

    /// Map each leaf chunk address to its file offset. Leaf bodies are
    /// content-addressed, so rebuilding each leaf from `unique_byte` reproduces
    /// the stored address.
    fn leaf_offsets_of(total: u64) -> HashMap<ChunkAddress, u64> {
        use crate::chunk::ChunkOps;
        let mut map = HashMap::new();
        let leaves = total.div_ceil(DEFAULT_BODY_SIZE as u64);
        for i in 0..leaves {
            let off = i * DEFAULT_BODY_SIZE as u64;
            let end = (off + DEFAULT_BODY_SIZE as u64).min(total);
            let body: Vec<u8> = (off..end).map(unique_byte).collect();
            let chunk = crate::chunk::ContentChunk::<DEFAULT_BODY_SIZE>::new(body).unwrap();
            map.insert(*chunk.address(), off);
        }
        map
    }

    #[test]
    fn head_slowest_in_order_and_bounded() {
        run(async {
            // The head leaf at `position` resolves last among the leaves in its
            // window. Under the old buffer this leaks: with the head missing, the
            // walk kept `window` leaf fetches in flight while also buffering the
            // resolved stragglers, so resident leaf bodies reached roughly twice the
            // window. The sliding window keeps in-flight plus buffered at `window` and
            // delivery in order. A naive run at `gate = window` instead would deadlock
            // the correct walk (the head can never release), so the gate is the most a
            // non-deadlocking adversary can demand, and the old buffer still overruns
            // it.
            let leaves = 40usize;
            let window = 6usize;
            let total = (DEFAULT_BODY_SIZE * leaves) as u64;
            let data: Vec<u8> = (0..total).map(unique_byte).collect();
            let (root, store) = split_and_store(&data);

            PEAK_LEAF_OCCUPANCY.with(|p| p.set(0));
            let leaf_offsets = leaf_offsets_of(total);
            let getter = HeadSlow {
                chunks: Arc::new(store),
                slow_offset: 0,
                released: Arc::new(AtomicUsize::new(0)),
                gate: window - 1,
                leaf_offsets: Arc::new(leaf_offsets),
            };

            let joiner = Joiner::new(getter, root)
                .await
                .unwrap()
                .with_concurrency(leaves);
            let mut reader = joiner.into_windowed_reader(window);
            let stream = reader.stream();
            futures::pin_mut!(stream);
            let mut got = Vec::new();
            while let Some(item) = stream.next().await {
                got.extend_from_slice(&item.unwrap());
            }
            assert_eq!(got, data, "head-slowest still yields whole file in order");

            let peak = PEAK_LEAF_OCCUPANCY.with(std::cell::Cell::get);
            assert!(
                peak <= window,
                "leaf-body occupancy peak {peak} exceeds window {window}"
            );
        })
    }

    // --- Front-load / deep-frontier guards (small body size = deep tree) ---

    /// Branching for a 256-byte body is `256 / REF_SIZE` = 8, so a few hundred
    /// leaves build a wide intermediate frontier with little data.
    const TINY_BODY: usize = 256;

    /// Sealed chunk currency at the tiny body size.
    type TinyChunk = Chunk<Verified, AnyChunkSet<TINY_BODY>>;

    fn tiny_leaf_addresses(data: &[u8]) -> HashMap<ChunkAddress, ()> {
        use crate::chunk::ChunkOps;
        let mut set = HashMap::new();
        for block in data.chunks(TINY_BODY) {
            let chunk = crate::chunk::ContentChunk::<TINY_BODY>::new(block.to_vec()).unwrap();
            set.insert(*chunk.address(), ());
        }
        set
    }

    fn tiny_deep_data(leaves: usize) -> Vec<u8> {
        (0..TINY_BODY * leaves).map(|i| (i % 251) as u8).collect()
    }

    /// Records the leaf/intermediate kind of every fetch in start order and the
    /// peak concurrent intermediate fetches.
    #[derive(Clone)]
    struct TinyOrderProbe {
        chunks: Arc<HashMap<ChunkAddress, TinyChunk>>,
        leaves: Arc<HashMap<ChunkAddress, ()>>,
        kinds: Arc<std::sync::Mutex<Vec<bool>>>,
        intermediate_in_flight: Arc<AtomicUsize>,
        peak_intermediate: Arc<AtomicUsize>,
    }

    impl TinyOrderProbe {
        fn new(store: HashMap<ChunkAddress, TinyChunk>, data: &[u8]) -> Self {
            Self {
                chunks: Arc::new(store),
                leaves: Arc::new(tiny_leaf_addresses(data)),
                kinds: Arc::new(std::sync::Mutex::new(Vec::new())),
                intermediate_in_flight: Arc::new(AtomicUsize::new(0)),
                peak_intermediate: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn reset(&self) {
            self.kinds.lock().unwrap().clear();
            self.peak_intermediate.store(0, Ordering::SeqCst);
        }

        fn intermediates_before_first_leaf(&self) -> usize {
            self.kinds
                .lock()
                .unwrap()
                .iter()
                .take_while(|is_leaf| !**is_leaf)
                .count()
        }

        fn intermediate_fetches(&self) -> usize {
            self.kinds.lock().unwrap().iter().filter(|l| !**l).count()
        }
    }

    impl ChunkGet<AnyChunkSet<TINY_BODY>> for TinyOrderProbe {
        type Trust = Verified;
        type Error = crate::store::ChunkStoreError;

        async fn get(&self, address: &ChunkAddress) -> std::result::Result<TinyChunk, Self::Error> {
            let is_leaf = self.leaves.contains_key(address);
            self.kinds.lock().unwrap().push(is_leaf);
            if !is_leaf {
                let now = self.intermediate_in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak_intermediate.fetch_max(now, Ordering::SeqCst);
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

    /// The in-order regression guard: on a wide frontier the first data leaf is
    /// fetched after only a short descent, not after the whole frontier drains.
    #[test]
    fn windowed_first_leaf_before_frontier() {
        run(async {
            let data = tiny_deep_data(900);
            let (root, store) = split::<TINY_BODY>(&data).unwrap();
            let probe = TinyOrderProbe::new(store.into_chunks(), &data);

            let joiner = Joiner::<_, TINY_BODY>::new(probe.clone(), root)
                .await
                .unwrap();
            // Measure only the streaming walk, not the upfront expansion.
            probe.reset();
            let mut reader = joiner.into_windowed_reader(16);
            let stream = reader.stream();
            futures::pin_mut!(stream);
            let mut got = Vec::new();
            while let Some(item) = stream.next().await {
                got.extend_from_slice(&item.unwrap());
            }
            assert_eq!(got, data, "deep-tree in-order reassembly is byte-exact");

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
            let peak = probe.peak_intermediate.load(Ordering::SeqCst);
            assert!(
                peak <= MAX_INTERMEDIATE_IN_FLIGHT,
                "intermediate in-flight peak {peak} exceeds cap {MAX_INTERMEDIATE_IN_FLIGHT}"
            );
        })
    }

    /// A multi-level frontier (intermediates whose children are themselves
    /// intermediates) must still stream in order under a tight window without
    /// deadlocking: the undescended-offset gate keeps buffered leaves a
    /// contiguous emittable prefix, so the head is never starved.
    #[test]
    fn windowed_deep_frontier_in_order_and_bounded() {
        run(async {
            let leaves = 2000usize;
            let window = 5usize;
            let data = tiny_deep_data(leaves);
            let (root, store) = split::<TINY_BODY>(&data).unwrap();
            let probe = TinyOrderProbe::new(store.into_chunks(), &data);

            PEAK_LEAF_OCCUPANCY.with(|p| p.set(0));
            let joiner = Joiner::<_, TINY_BODY>::new(probe.clone(), root)
                .await
                .unwrap()
                .with_concurrency(leaves);
            probe.reset();
            let mut reader = joiner.into_windowed_reader(window);
            let stream = reader.stream();
            futures::pin_mut!(stream);
            let mut got = Vec::new();
            while let Some(item) = stream.next().await {
                got.extend_from_slice(&item.unwrap());
            }
            assert_eq!(got, data, "deep frontier still yields whole file in order");

            let occupancy = PEAK_LEAF_OCCUPANCY.with(std::cell::Cell::get);
            assert!(
                occupancy <= window,
                "leaf-body occupancy peak {occupancy} exceeds window {window}"
            );
            let peak = probe.peak_intermediate.load(Ordering::SeqCst);
            assert!(
                peak <= MAX_INTERMEDIATE_IN_FLIGHT,
                "intermediate in-flight peak {peak} exceeds cap {MAX_INTERMEDIATE_IN_FLIGHT}"
            );
        })
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn head_slowest_encrypted_in_order_and_bounded() {
        run(async {
            // Encrypted leaf bodies are shorter than BODY_SIZE, so the slide must
            // advance by body.len(), not the stride. Encrypted leaf addresses hash
            // the ciphertext and are not reconstructible from plaintext here, so the
            // head cannot be singled out; a uniform delay across all fetches still
            // overlaps concurrently admitted leaves, and the occupancy hook proves a
            // correct walk holds at most `window` leaf bodies on short bodies.
            use crate::file::EncryptedJoiner;
            use crate::file::split_encrypted;

            let leaves = 30usize;
            let window = 5usize;
            let total = (DEFAULT_BODY_SIZE * leaves) as u64;
            let data: Vec<u8> = (0..total).map(unique_byte).collect();
            let (root_ref, store) = split_encrypted::<DEFAULT_BODY_SIZE>(&data).unwrap();
            let store = store.into_chunks();

            PEAK_LEAF_OCCUPANCY.with(|p| p.set(0));
            let getter = UniformSlow {
                chunks: Arc::new(store),
            };

            let joiner = EncryptedJoiner::new(getter, root_ref)
                .await
                .unwrap()
                .with_concurrency(leaves);
            let mut reader = joiner.into_windowed_reader(window);
            let stream = reader.stream();
            futures::pin_mut!(stream);
            let mut out = Vec::new();
            while let Some(item) = stream.next().await {
                out.extend_from_slice(&item.unwrap());
            }
            assert_eq!(out, data, "encrypted short-body slide stays in order");

            let peak = PEAK_LEAF_OCCUPANCY.with(std::cell::Cell::get);
            assert!(
                peak <= window,
                "encrypted leaf-body occupancy peak {peak} exceeds window {window}"
            );
        })
    }

    /// A getter that holds every fetch open across several self-waking yields so
    /// concurrently admitted leaves overlap. Used where addresses cannot be
    /// mapped to offsets (encrypted leaves), so the head cannot be singled out;
    /// the occupancy hook still proves the window bound.
    #[cfg(feature = "encryption")]
    #[derive(Clone)]
    struct UniformSlow {
        chunks: Arc<HashMap<ChunkAddress, Chunk>>,
    }

    #[cfg(feature = "encryption")]
    impl ChunkGet<StandardChunkSet> for UniformSlow {
        type Trust = Verified;
        type Error = crate::store::ChunkStoreError;

        async fn get(&self, address: &ChunkAddress) -> std::result::Result<Chunk, Self::Error> {
            for _ in 0..4 {
                yield_now().await;
            }
            self.chunks
                .get(address)
                .cloned()
                .ok_or_else(|| crate::store::ChunkStoreError::not_found(address))
        }
    }

    #[cfg(feature = "encryption")]
    mod encrypted {
        use super::*;
        use crate::file::EncryptedJoiner;
        use crate::file::split_encrypted;

        async fn drain_enc(
            reader: &mut WindowedReader<
                HashMap<ChunkAddress, Chunk>,
                crate::file::mode::EncryptedMode,
            >,
        ) -> Vec<u8> {
            let stream = reader.stream();
            futures::pin_mut!(stream);
            let mut out = Vec::new();
            while let Some(item) = stream.next().await {
                out.extend_from_slice(&item.unwrap());
            }
            out
        }

        #[test]
        fn encrypted_stream_and_seek() {
            run(async {
                let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 5 + 321)
                    .map(|i| (i % 256) as u8)
                    .collect();
                let bs = DEFAULT_BODY_SIZE as u64;
                let (root_ref, store) = split_encrypted::<DEFAULT_BODY_SIZE>(&data).unwrap();
                let store = store.into_chunks();
                // whole file
                let joiner = EncryptedJoiner::new(store.clone(), root_ref.clone())
                    .await
                    .unwrap();
                let mut reader = joiner.into_windowed_reader(16);
                assert_eq!(drain_enc(&mut reader).await, data);

                // mid-leaf and leaf-aligned suffixes
                for k in [bs + 10, bs, bs * 3] {
                    let joiner = EncryptedJoiner::new(store.clone(), root_ref.clone())
                        .await
                        .unwrap();
                    let mut reader = joiner.into_windowed_reader(16);
                    reader.seek(SeekFrom::Start(k)).unwrap();
                    assert_eq!(drain_enc(&mut reader).await, &data[k as usize..]);
                }
            })
        }
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn reader_seek_and_read() {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3 + 50)
            .map(|i| (i % 256) as u8)
            .collect();
        let (root, store) = split_and_store(&data);
        let joiner = Joiner::new(store, root).await.unwrap();
        let mut reader = joiner.into_windowed_reader(16).into_reader();

        let mut all = Vec::new();
        reader.read_to_end(&mut all).await.unwrap();
        assert_eq!(all, data);

        reader
            .seek(SeekFrom::Start(DEFAULT_BODY_SIZE as u64))
            .await
            .unwrap();
        let mut tail = Vec::new();
        reader.read_to_end(&mut tail).await.unwrap();
        assert_eq!(tail, &data[DEFAULT_BODY_SIZE..]);
    }
}

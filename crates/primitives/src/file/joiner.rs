//! Async joiner with BFS expansion and concurrent chunk fetching.

use std::io::SeekFrom;
use std::marker::PhantomData;
use std::sync::Arc;

/// Default number of concurrent chunk fetches for async operations.
const DEFAULT_ASYNC_CONCURRENCY: usize = 8;

/// Default number of times a failed leaf fetch is re-enqueued before the
/// chunk-granular offset stream surfaces the error.
const DEFAULT_LEAF_RETRIES: u32 = 4;

#[cfg(feature = "tokio")]
use bytes::Buf;
use bytes::Bytes;
use futures::stream::{self, FuturesUnordered, Stream, StreamExt};

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::ChunkAddress;

use super::error::{FileError, Result};
use super::frontier::{
    SubtreeNode, expand_frontier_async, overlapping_children, read_subtree_bodies_async,
};
use super::mode::{JoinMode, PlainMode};
use super::tree::{ChunkRange, TreeParams};
use crate::store::{ChunkGet, MaybeSend};

#[cfg(feature = "encryption")]
use super::mode::EncryptedMode;

/// Generic async joiner parameterized by chunk mode.
pub struct GenericJoiner<G, M: JoinMode, const BODY_SIZE: usize = DEFAULT_BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE>,
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
    G: ChunkGet<BODY_SIZE>,
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
async fn collect_subtree_bodies_async<G, M, const BODY_SIZE: usize>(
    getter: &Arc<G>,
    subtrees: Vec<SubtreeNode<M>>,
    chunk_range: ChunkRange,
    concurrency: usize,
) -> Result<Vec<Bytes>>
where
    G: ChunkGet<BODY_SIZE>,
    M: JoinMode + MaybeSend + Sync,
{
    let bodies: Vec<Bytes> = stream::iter(subtrees)
        .map(|st| {
            let getter = Arc::clone(getter);
            async move {
                read_subtree_bodies_async::<G, M, BODY_SIZE>(&*getter, &st, &chunk_range).await
            }
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
    G: ChunkGet<BODY_SIZE>,
    M: JoinMode + MaybeSend + Sync,
{
    /// Create an async joiner from a root reference.
    pub async fn new(getter: G, input: M::RootRef) -> Result<Self> {
        const { super::constants::assert_valid_body_size::<BODY_SIZE>() };

        let (root, span, context) =
            super::mode::joiner_init_async::<M, G, BODY_SIZE>(&getter, input).await?;
        let tree = TreeParams::<BODY_SIZE>::new(span);

        let target = DEFAULT_ASYNC_CONCURRENCY * 2;
        let full_range = tree.chunks_for_range(0, span);
        let subtrees = expand_frontier_async::<G, M, BODY_SIZE>(
            &getter,
            &root,
            &context,
            span,
            &full_range,
            target,
        )
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
        self.read_range(0, self.span as usize).await
    }

    /// Shared read-range implementation used by both `read_range` and `poll_read`.
    #[allow(
        clippy::too_many_arguments,
        reason = "internal helper threading already-decomposed reader state from two call sites"
    )]
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
                let chunk = chunk.into_content().ok_or(FileError::InvalidChunkType {
                    type_name: "non-content",
                })?;
                let body = M::decode_body::<BODY_SIZE>(chunk, context, span)?;
                let start = offset as usize;
                let end = start + actual_len;
                return Ok(body[start..end].to_vec());
            }
            ReadRangeCheck::MultiChunk { offset, actual_len } => (offset, actual_len),
        };

        let chunk_range = tree.chunks_for_range(offset, actual_len as u64);
        let range_start_byte = chunk_range.start * BODY_SIZE as u64;
        let range_end_byte = chunk_range.end * BODY_SIZE as u64;

        let relevant: Vec<_> = subtrees
            .iter()
            .filter(|st| {
                st.byte_offset < range_end_byte && st.byte_offset + st.span > range_start_byte
            })
            .cloned()
            .collect();

        let bodies = collect_subtree_bodies_async::<G, M, BODY_SIZE>(
            getter,
            relevant,
            chunk_range,
            concurrency,
        )
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
                match read_subtree_bodies_async::<G, M, BODY_SIZE>(&*getter, &st, &chunk_range)
                    .await
                {
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
                        read_subtree_bodies_async::<G, M, BODY_SIZE>(&*getter, &st, &chunk_range)
                            .await?;
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
                    let offset = state.base + state.leaf_index as u64 * BODY_SIZE as u64;
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
            G: ChunkGet<BS>,
            M: JoinMode + MaybeSend + Sync,
        {
            let node = &pending.node;
            let body = match super::mode::read_chunk_body_async::<M, G, BS>(
                getter,
                &node.addr,
                &node.context,
                node.span,
            )
            .await
            {
                Ok(body) => body,
                Err(e) => {
                    if node.span <= BS as u64 && pending.retries > 0 {
                        return Resolved::Retry(Pending {
                            node: pending.node,
                            retries: pending.retries - 1,
                        });
                    }
                    return Resolved::Failed(e);
                }
            };

            if node.span <= BS as u64 {
                return Resolved::Leaf(node.byte_offset, body);
            }
            match overlapping_children::<M, BS>(&body, node, chunk_range) {
                Ok(children) => Resolved::Children(children),
                Err(e) => Resolved::Failed(e),
            }
        }

        struct State<G, M: JoinMode, const BS: usize> {
            getter: Arc<G>,
            chunk_range: ChunkRange,
            width: usize,
            queue: std::collections::VecDeque<Pending<M>>,
            in_flight: FuturesUnordered<BoxResolvedFuture<M>>,
        }

        let mut queue = std::collections::VecDeque::new();
        for st in self.subtrees {
            queue.push_back(Pending {
                node: st,
                retries: DEFAULT_LEAF_RETRIES,
            });
        }

        let state = State::<G, M, BODY_SIZE> {
            getter,
            chunk_range,
            width,
            queue,
            in_flight: FuturesUnordered::new(),
        };

        stream::unfold(state, move |mut state| async move {
            loop {
                // Refill the in-flight pool from the pending queue up to the
                // width, so at most `width` chunks are fetching at once.
                while state.in_flight.len() < state.width {
                    let Some(pending) = state.queue.pop_front() else {
                        break;
                    };
                    let getter = Arc::clone(&state.getter);
                    let range = state.chunk_range;
                    state.in_flight.push(Box::pin(async move {
                        fetch_one::<G, M, BODY_SIZE>(&*getter, &range, pending).await
                    }) as BoxResolvedFuture<M>);
                }

                // Nothing in flight and nothing queued: the tree is drained.
                let resolved = state.in_flight.next().await?;
                match resolved {
                    Resolved::Leaf(offset, body) => {
                        return Some((Ok((offset, body)), state));
                    }
                    Resolved::Children(children) => {
                        for child in children {
                            state.queue.push_back(Pending {
                                node: child,
                                retries: DEFAULT_LEAF_RETRIES,
                            });
                        }
                    }
                    Resolved::Retry(pending) => {
                        // Re-enqueue at the back so the worker pulls fresh work
                        // first; the failed chunk retries without holding a slot.
                        state.queue.push_back(pending);
                    }
                    Resolved::Failed(e) => return Some((Err(e), state)),
                }
            }
        })
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

/// Wrapper providing `tokio::io::AsyncRead` over a [`GenericJoiner`].
///
/// Created via [`GenericJoiner::into_reader`].
#[cfg(feature = "tokio")]
pub struct JoinerReader<G, M: JoinMode, const BODY_SIZE: usize = DEFAULT_BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE>,
{
    joiner: GenericJoiner<G, M, BODY_SIZE>,
    buffer: Bytes,
    #[allow(clippy::type_complexity)]
    future: Option<std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>>> + Send>>>,
}

#[cfg(feature = "tokio")]
impl<G, M, const BODY_SIZE: usize> std::fmt::Debug for JoinerReader<G, M, BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE>,
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
impl<G: ChunkGet<BODY_SIZE>, M: JoinMode, const BODY_SIZE: usize> Unpin
    for JoinerReader<G, M, BODY_SIZE>
{
}

#[cfg(feature = "tokio")]
impl<G, M, const BODY_SIZE: usize> tokio::io::AsyncRead for JoinerReader<G, M, BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE> + 'static,
    M: JoinMode + Send + Sync + 'static,
{
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
            let remaining = (this.joiner.span - position) as usize;
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
                this.joiner.position += bytes.len() as u64;
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
    G: ChunkGet<BODY_SIZE> + 'static,
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

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use super::*;
    use crate::chunk::AnyChunk;
    use crate::file::sync_split;
    use std::collections::HashMap;

    fn split_and_store(data: &[u8]) -> (ChunkAddress, HashMap<ChunkAddress, AnyChunk>) {
        let (root, store) = sync_split::<DEFAULT_BODY_SIZE>(data).unwrap();
        (root, store.into_chunks())
    }

    // --- Generated shared tests (async variants) ---
    generate_plain_joiner_tests!(tokio::test, Joiner, [async], [await]);

    // --- Async-only tests: Stream, AsyncRead, AsyncSeek ---

    #[tokio::test]
    async fn test_async_joiner_stream() {
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

    #[tokio::test]
    async fn test_offset_stream_small() {
        assert_offset_stream_matches(b"hello world").await;
    }

    #[tokio::test]
    async fn test_offset_stream_exact_chunk() {
        assert_offset_stream_matches(&vec![0xAB; DEFAULT_BODY_SIZE]).await;
    }

    #[tokio::test]
    async fn test_offset_stream_multi_chunk() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3 + 123)
            .map(|i| (i % 256) as u8)
            .collect();
        assert_offset_stream_matches(&data).await;
    }

    #[tokio::test]
    async fn test_offset_stream_129_chunks() {
        let refs_per_chunk = DEFAULT_BODY_SIZE / super::super::constants::REF_SIZE;
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * (refs_per_chunk + 1))
            .map(|i| (i % 256) as u8)
            .collect();
        assert_offset_stream_matches(&data).await;
    }

    #[tokio::test]
    async fn test_offset_stream_concurrency_one() {
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

    #[tokio::test]
    async fn test_offset_stream_chunked_small() {
        assert_offset_stream_chunked_matches(b"hello world").await;
    }

    #[tokio::test]
    async fn test_offset_stream_chunked_exact_chunk() {
        assert_offset_stream_chunked_matches(&vec![0xAB; DEFAULT_BODY_SIZE]).await;
    }

    #[tokio::test]
    async fn test_offset_stream_chunked_multi_chunk() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3 + 123)
            .map(|i| (i % 256) as u8)
            .collect();
        assert_offset_stream_chunked_matches(&data).await;
    }

    #[tokio::test]
    async fn test_offset_stream_chunked_three_level_tree() {
        // 129 leaves needs a three-level tree, exercising intermediate-node
        // re-queueing in the chunk-granular walk.
        let refs_per_chunk = DEFAULT_BODY_SIZE / super::super::constants::REF_SIZE;
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * (refs_per_chunk + 1))
            .map(|i| (i % 256) as u8)
            .collect();
        assert_offset_stream_chunked_matches(&data).await;
    }

    #[tokio::test]
    async fn test_offset_stream_chunked_concurrency_one() {
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
    }

    /// A getter that holds each fetch open across several executor yields so the
    /// test can observe how many fetches the consumer admits at once, proving the
    /// chunk-granular stream reaches per-chunk (not per-subtree) concurrency.
    #[derive(Clone)]
    struct ConcurrencyProbe {
        chunks: Arc<HashMap<ChunkAddress, AnyChunk>>,
        in_flight: Arc<std::sync::atomic::AtomicUsize>,
        max_in_flight: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl crate::store::ChunkGet<DEFAULT_BODY_SIZE> for ConcurrencyProbe {
        type Error = crate::store::ChunkStoreError;

        async fn get(&self, address: &ChunkAddress) -> std::result::Result<AnyChunk, Self::Error> {
            use std::sync::atomic::Ordering;
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            // Yield several times so concurrently-admitted fetches overlap here
            // before any resolves; the peak counter then reflects pool width.
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            self.chunks
                .get(address)
                .cloned()
                .ok_or_else(|| crate::store::ChunkStoreError::not_found(address))
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_offset_stream_chunked_per_chunk_concurrency() {
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
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_async_reader_small() {
        use tokio::io::AsyncReadExt;

        let data = b"hello world";
        let (root, store) = split_and_store(data);

        let joiner = Joiner::new(store, root).await.unwrap();
        let mut reader = joiner.into_reader();
        let mut result = Vec::new();
        reader.read_to_end(&mut result).await.unwrap();
        assert_eq!(result, data);
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_async_reader_multi_chunk() {
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

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_async_reader_seek() {
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

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_async_reader_seek_back_and_forth() {
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

    #[cfg(feature = "encryption")]
    mod encrypted {
        use super::*;
        use crate::file::sync_split_encrypted;

        fn encrypted_split_and_store(
            data: &[u8],
        ) -> (
            crate::chunk::encryption::EncryptedChunkRef,
            HashMap<ChunkAddress, AnyChunk>,
        ) {
            let (root_ref, store) = sync_split_encrypted::<DEFAULT_BODY_SIZE>(data).unwrap();
            (root_ref, store.into_chunks())
        }

        // --- Generated shared tests (async variants) ---
        generate_encrypted_joiner_tests!(tokio::test, EncryptedJoiner, [async], [await]);

        // --- Async-only tests: Stream ---

        #[tokio::test]
        async fn test_encrypted_async_joiner_stream() {
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
        }
    }
}

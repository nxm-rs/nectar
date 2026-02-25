//! Shared BFS frontier expansion for sync and async joiners.

use bytes::Bytes;

use crate::chunk::ChunkAddress;

use super::error::Result;
use super::mode::JoinMode;
use super::tree::ChunkRange;
use crate::store::ChunkGet;

/// A subtree root in the BFS frontier.
pub(crate) struct SubtreeNode<M: JoinMode> {
    pub(crate) addr: ChunkAddress,
    pub(crate) context: M::JoinerContext,
    pub(crate) span: u64,
    /// Byte offset in the file where this subtree's data starts.
    pub(crate) byte_offset: u64,
}

impl<M: JoinMode> Clone for SubtreeNode<M> {
    fn clone(&self) -> Self {
        Self {
            addr: self.addr,
            context: self.context.clone(),
            span: self.span,
            byte_offset: self.byte_offset,
        }
    }
}

/// Parse children of an intermediate node that overlap a byte range.
#[inline]
pub(crate) fn overlapping_children<M, const BS: usize>(
    body: &[u8],
    parent: &SubtreeNode<M>,
    chunk_range: &ChunkRange,
) -> Result<Vec<SubtreeNode<M>>>
where
    M: JoinMode,
{
    let subspan = M::subspan_size::<BS>(parent.span);
    let num_children = body.len() / M::REF_SIZE;
    let range_start = chunk_range.start * BS as u64;
    let range_end = chunk_range.end * BS as u64;

    let mut children = Vec::with_capacity(num_children);
    for i in 0..num_children {
        let byte_offset = parent.byte_offset + i as u64 * subspan;
        let span = M::child_span::<BS>(parent.span, subspan, i);

        if byte_offset >= range_end || byte_offset + span <= range_start {
            continue;
        }

        let ref_start = i * M::REF_SIZE;
        let (addr, context) = M::parse_child_ref(body, ref_start)?;
        children.push(SubtreeNode { addr, context, span, byte_offset });
    }

    Ok(children)
}

/// Create the initial frontier seed.
fn frontier_seed<M: JoinMode>(
    root: &ChunkAddress,
    context: &M::JoinerContext,
    span: u64,
) -> SubtreeNode<M> {
    SubtreeNode {
        addr: *root,
        context: context.clone(),
        span,
        byte_offset: 0,
    }
}

/// BFS state machine for frontier expansion.
///
/// Encapsulates the iterative BFS loop, leaving only the read strategy
/// to the caller (sync or async).
struct BfsExpander<M: JoinMode, const BS: usize> {
    frontier: Vec<SubtreeNode<M>>,
    next: Vec<SubtreeNode<M>>,
    ideal_span: u64,
    chunk_range: ChunkRange,
}

impl<M: JoinMode, const BS: usize> BfsExpander<M, BS> {
    /// Returns `None` if `span` fits in a single chunk (no expansion possible).
    fn new(
        root: &ChunkAddress,
        context: &M::JoinerContext,
        span: u64,
        chunk_range: &ChunkRange,
        target_subtrees: usize,
    ) -> Option<Self> {
        if span <= BS as u64 {
            return None;
        }
        Some(Self {
            frontier: vec![frontier_seed::<M>(root, context, span)],
            next: Vec::new(),
            ideal_span: span / target_subtrees as u64,
            chunk_range: *chunk_range,
        })
    }

    /// Indices of frontier nodes whose span exceeds the ideal threshold.
    fn nodes_to_expand(&self) -> Vec<usize> {
        self.frontier
            .iter()
            .enumerate()
            .filter(|(_, n)| n.span > self.ideal_span && n.span > BS as u64)
            .map(|(i, _)| i)
            .collect()
    }

    /// Apply pre-read bodies for expanded nodes, produce next frontier layer.
    /// Returns `false` if nothing was expanded (converged).
    fn advance(&mut self, expand_indices: &[usize], bodies: &[Bytes]) -> Result<bool> {
        if expand_indices.is_empty() {
            return Ok(false);
        }
        self.next.clear();
        let mut body_idx = 0;
        for (i, node) in self.frontier.iter().enumerate() {
            if body_idx < expand_indices.len() && expand_indices[body_idx] == i {
                self.next.extend(
                    overlapping_children::<M, BS>(&bodies[body_idx], node, &self.chunk_range)?,
                );
                body_idx += 1;
            } else {
                self.next.push(node.clone());
            }
        }
        std::mem::swap(&mut self.frontier, &mut self.next);
        Ok(true)
    }

    fn into_frontier(self) -> Vec<SubtreeNode<M>> {
        self.frontier
    }
}

// Sync/async dispatch macros — the single source of truth for the BFS and DFS
// algorithms. Only the chunk-read call differs between sync and async.

/// Dispatch a chunk body read. Sync calls `read_chunk_body`, async calls
/// `read_chunk_body_async` with `.await`.
macro_rules! read_body {
    (sync, $getter:expr, $addr:expr, $ctx:expr, $span:expr) => {
        super::mode::read_chunk_body::<M, G, BS>($getter, $addr, $ctx, $span)
    };
    (async_mode, $getter:expr, $addr:expr, $ctx:expr, $span:expr) => {
        super::mode::read_chunk_body_async::<M, G, BS>($getter, $addr, $ctx, $span).await
    };
}

/// Shared BFS expansion loop body.
macro_rules! expand_frontier_body {
    ($mode:ident, $getter:expr, $root:expr, $context:expr, $span:expr,
     $chunk_range:expr, $target_subtrees:expr) => {{
        if $chunk_range.is_empty() {
            return Ok(Vec::new());
        }
        let Some(mut bfs) =
            BfsExpander::<M, BS>::new($root, $context, $span, $chunk_range, $target_subtrees)
        else {
            return Ok(vec![frontier_seed::<M>($root, $context, $span)]);
        };
        loop {
            let indices = bfs.nodes_to_expand();
            let mut bodies = Vec::with_capacity(indices.len());
            for &i in &indices {
                let n = &bfs.frontier[i];
                bodies.push(read_body!($mode, $getter, &n.addr, &n.context, n.span)?);
            }
            if !bfs.advance(&indices, &bodies)? {
                break;
            }
        }
        Ok(bfs.into_frontier())
    }};
}

/// Iterative DFS subtree body collection (async only — sync uses recursive descent).
#[cfg(feature = "async")]
macro_rules! read_subtree_bodies_body {
    ($mode:ident, $getter:expr, $node:expr, $chunk_range:expr) => {{
        let mut out = Vec::new();
        let mut stack = vec![$node.clone()];
        while let Some(current) = stack.pop() {
            let body =
                read_body!($mode, $getter, &current.addr, &current.context, current.span)?;
            dispatch_body::<M, BS>(body, &current, $chunk_range, &mut out, &mut stack)?;
        }
        Ok(out)
    }};
}

// Sync implementations

/// BFS expansion with span-threshold balancing for parallel work distribution.
///
/// Iteratively expands intermediate nodes whose span exceeds an ideal threshold,
/// producing a frontier of roughly equal-sized subtrees suitable for parallel
/// processing. Only children overlapping `chunk_range` are retained.
pub(crate) fn expand_frontier<G, M, const BS: usize>(
    getter: &G,
    root: &ChunkAddress,
    context: &M::JoinerContext,
    span: u64,
    chunk_range: &ChunkRange,
    target_subtrees: usize,
) -> Result<Vec<SubtreeNode<M>>>
where
    G: ChunkGet<BS>,
    M: JoinMode,
{
    expand_frontier_body!(sync, getter, root, context, span, chunk_range, target_subtrees)
}

/// Recursive descent within a subtree, collecting leaf bodies into `out`.
pub(crate) fn read_subtree_bodies<G, M, const BS: usize>(
    getter: &G,
    node: &SubtreeNode<M>,
    chunk_range: &ChunkRange,
    out: &mut Vec<Bytes>,
) -> Result<()>
where
    G: ChunkGet<BS>,
    M: JoinMode,
{
    let body = super::mode::read_chunk_body::<M, G, BS>(getter, &node.addr, &node.context, node.span)?;

    if node.span <= BS as u64 {
        out.push(body);
        return Ok(());
    }

    for child in overlapping_children::<M, BS>(&body, node, chunk_range)? {
        read_subtree_bodies::<G, M, BS>(getter, &child, chunk_range, out)?;
    }

    Ok(())
}

// Async implementations

/// Async BFS expansion with concurrent chunk fetching per level.
///
/// See [`expand_frontier`] for the algorithm. The async variant fetches all
/// expandable nodes within a BFS level concurrently using buffered streams.
#[cfg(feature = "async")]
pub(crate) async fn expand_frontier_async<G, M, const BS: usize>(
    getter: &G,
    root: &ChunkAddress,
    context: &M::JoinerContext,
    span: u64,
    chunk_range: &ChunkRange,
    target_subtrees: usize,
) -> Result<Vec<SubtreeNode<M>>>
where
    G: crate::store::AsyncChunkGet<BS>,
    M: JoinMode + Send + Sync,
{
    use futures::stream::{self, StreamExt};

    if chunk_range.is_empty() {
        return Ok(Vec::new());
    }
    let Some(mut bfs) =
        BfsExpander::<M, BS>::new(root, context, span, chunk_range, target_subtrees)
    else {
        return Ok(vec![frontier_seed::<M>(root, context, span)]);
    };
    loop {
        let indices = bfs.nodes_to_expand();
        if indices.is_empty() {
            break;
        }

        // Fetch all expandable nodes concurrently.
        let futs: Vec<_> = indices
            .iter()
            .map(|&i| {
                let n = &bfs.frontier[i];
                super::mode::read_chunk_body_async::<M, G, BS>(getter, &n.addr, &n.context, n.span)
            })
            .collect();

        let bodies: Vec<Bytes> = stream::iter(futs)
            .buffered(indices.len())
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        if !bfs.advance(&indices, &bodies)? {
            break;
        }
    }
    Ok(bfs.into_frontier())
}

/// Async iterative DFS descent within a subtree, collecting leaf bodies.
#[cfg(feature = "async")]
pub(crate) async fn read_subtree_bodies_async<G, M, const BS: usize>(
    getter: &G,
    node: &SubtreeNode<M>,
    chunk_range: &ChunkRange,
) -> Result<Vec<Bytes>>
where
    G: crate::store::AsyncChunkGet<BS>,
    M: JoinMode + Send + Sync,
{
    read_subtree_bodies_body!(async_mode, getter, node, chunk_range)
}

// Shared helpers

/// Whether a subtree node is a leaf (its body is file data, not references).
#[cfg(feature = "async")]
#[inline]
fn is_leaf<const BS: usize>(span: u64) -> bool {
    span <= BS as u64
}

/// Dispatch a decoded body: leaf → push to output, intermediate → push children to stack.
#[cfg(feature = "async")]
fn dispatch_body<M: JoinMode, const BS: usize>(
    body: Bytes,
    node: &SubtreeNode<M>,
    chunk_range: &ChunkRange,
    out: &mut Vec<Bytes>,
    stack: &mut Vec<SubtreeNode<M>>,
) -> Result<()> {
    if is_leaf::<BS>(node.span) {
        out.push(body);
    } else {
        let children = overlapping_children::<M, BS>(&body, node, chunk_range)?;
        // Reverse so left-most child is popped first (preserves left-to-right order).
        stack.extend(children.into_iter().rev());
    }
    Ok(())
}


//! BFS frontier expansion for the async joiner.

use bytes::Bytes;

use crate::chunk::{ChunkAddress, ChunkRef};
use crate::wire::Cursor;

use super::error::{FileError, Result};
use super::mode::JoinMode;
use super::tree::ChunkRange;
use crate::store::MaybeSend;

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
#[allow(clippy::arithmetic_side_effects)]
// REF_SIZE is a nonzero constant; child byte offsets and chunk-range bounds stay within the u64 file span for any tree the splitters can produce
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
    // An intermediate body packs whole references back to back. A trailing run
    // shorter than one reference (body.len() % REF_SIZE bytes) is not a child:
    // the splitter never emits one, so it is tolerated and left unread.
    let num_children = body.len() / M::REF_SIZE;
    let range_start = chunk_range.start * crate::cast::u64_from_usize(BS);
    let range_end = chunk_range.end * crate::cast::u64_from_usize(BS);

    let mut cursor = Cursor::new(body);
    let mut children = Vec::with_capacity(num_children);
    for i in 0..num_children {
        // Each turn consumes exactly one reference: the address, then this
        // mode's trailing context. The Cursor is the only fallible read and
        // cannot underrun while i < num_children.
        let addr_bytes = cursor
            .take::<[u8; ChunkRef::SIZE]>()
            .map_err(|_| FileError::InvalidReference { level: 0 })?;
        let context = M::context_from_wire(&mut cursor)?;

        let byte_offset = parent.byte_offset + crate::cast::u64_from_usize(i) * subspan;
        let span = M::child_span::<BS>(parent.span, subspan, i);

        if byte_offset >= range_end || byte_offset + span <= range_start {
            continue;
        }

        children.push(SubtreeNode {
            addr: ChunkAddress::from(addr_bytes),
            context,
            span,
            byte_offset,
        });
    }

    Ok(children)
}

/// Create the initial frontier seed (the whole-file root subtree).
pub(crate) fn frontier_seed<M: JoinMode>(
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
    #[allow(clippy::arithmetic_side_effects)] // both callers pass a nonzero target_subtrees (DEFAULT_ASYNC_CONCURRENCY * 2)
    fn new(
        root: &ChunkAddress,
        context: &M::JoinerContext,
        span: u64,
        chunk_range: &ChunkRange,
        target_subtrees: usize,
    ) -> Option<Self> {
        if span <= crate::cast::u64_from_usize(BS) {
            return None;
        }
        Some(Self {
            frontier: vec![frontier_seed::<M>(root, context, span)],
            next: Vec::new(),
            ideal_span: span / crate::cast::u64_from_usize(target_subtrees),
            chunk_range: *chunk_range,
        })
    }

    /// Indices of frontier nodes whose span exceeds the ideal threshold.
    fn nodes_to_expand(&self) -> Vec<usize> {
        self.frontier
            .iter()
            .enumerate()
            .filter(|(_, n)| n.span > self.ideal_span && n.span > crate::cast::u64_from_usize(BS))
            .map(|(i, _)| i)
            .collect()
    }

    /// Apply pre-read bodies for expanded nodes, produce next frontier layer.
    /// Returns `false` if nothing was expanded (converged).
    #[allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)] // body_idx < expand_indices.len() is checked before both indexings, and the caller supplies exactly one body per expand index
    fn advance(&mut self, expand_indices: &[usize], bodies: &[Bytes]) -> Result<bool> {
        if expand_indices.is_empty() {
            return Ok(false);
        }
        self.next.clear();
        let mut body_idx = 0;
        for (i, node) in self.frontier.iter().enumerate() {
            if body_idx < expand_indices.len() && expand_indices[body_idx] == i {
                self.next.extend(overlapping_children::<M, BS>(
                    &bodies[body_idx],
                    node,
                    &self.chunk_range,
                )?);
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

/// BFS expansion with span-threshold balancing for concurrent work distribution.
///
/// Iteratively expands intermediate nodes whose span exceeds an ideal threshold,
/// producing a frontier of roughly equal-sized subtrees. All expandable nodes
/// within a BFS level are fetched concurrently. Only children overlapping
/// `chunk_range` are retained.
pub(crate) async fn expand_frontier<G, M, const BS: usize>(
    getter: &G,
    root: &ChunkAddress,
    context: &M::JoinerContext,
    span: u64,
    chunk_range: &ChunkRange,
    target_subtrees: usize,
) -> Result<Vec<SubtreeNode<M>>>
where
    G: crate::store::TrustedStore<crate::chunk::AnyChunkSet<BS>>,
    M: JoinMode + MaybeSend + Sync,
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
                #[allow(clippy::indexing_slicing)]
                // indices come from enumerating this same frontier
                let n = &bfs.frontier[i];
                super::mode::read_chunk_body::<M, G, BS>(getter, &n.addr, &n.context, n.span)
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
pub(crate) async fn read_subtree_bodies<G, M, const BS: usize>(
    getter: &G,
    node: &SubtreeNode<M>,
    chunk_range: &ChunkRange,
) -> Result<Vec<Bytes>>
where
    G: crate::store::TrustedStore<crate::chunk::AnyChunkSet<BS>>,
    M: JoinMode + MaybeSend + Sync,
{
    let mut out = Vec::new();
    let mut stack = vec![node.clone()];
    while let Some(current) = stack.pop() {
        let body = super::mode::read_chunk_body::<M, G, BS>(
            getter,
            &current.addr,
            &current.context,
            current.span,
        )
        .await?;
        if current.span <= crate::cast::u64_from_usize(BS) {
            out.push(body);
        } else {
            let children = overlapping_children::<M, BS>(&body, &current, chunk_range)?;
            stack.extend(children.into_iter().rev());
        }
    }
    Ok(out)
}

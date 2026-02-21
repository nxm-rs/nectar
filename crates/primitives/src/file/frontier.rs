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
    if chunk_range.is_empty() {
        return Ok(Vec::new());
    }

    if span <= BS as u64 {
        return Ok(vec![SubtreeNode {
            addr: *root,
            context: context.clone(),
            span,
            byte_offset: 0,
        }]);
    }

    let ideal_span = span / target_subtrees as u64;

    let mut frontier = vec![SubtreeNode {
        addr: *root,
        context: context.clone(),
        span,
        byte_offset: 0,
    }];
    let mut next = Vec::new();

    loop {
        next.clear();
        let mut any_expanded = false;

        for node in &frontier {
            if node.span > ideal_span && node.span > BS as u64 {
                let body =
                    M::read_chunk_body::<BS, G>(getter, &node.addr, &node.context, node.span)?;
                next.extend(overlapping_children::<M, BS>(&body, node, chunk_range)?);
                any_expanded = true;
            } else {
                next.push(node.clone());
            }
        }

        if !any_expanded {
            break;
        }
        std::mem::swap(&mut frontier, &mut next);
    }

    Ok(frontier)
}

/// Sequential recursive descent within a subtree, collecting leaf bodies.
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
    let body = M::read_chunk_body::<BS, G>(getter, &node.addr, &node.context, node.span)?;

    if node.span <= BS as u64 {
        out.push(body);
        return Ok(());
    }

    for child in overlapping_children::<M, BS>(&body, node, chunk_range)? {
        read_subtree_bodies::<G, M, BS>(getter, &child, chunk_range, out)?;
    }

    Ok(())
}

// Async variants
#[cfg(feature = "async")]
mod async_ops {
    use super::*;
    use crate::chunk::ContentChunk;
    use crate::store::AsyncChunkGet;
    use super::super::error::FileError;

    use futures::future::BoxFuture;

    /// Async BFS expansion — structurally identical to sync but uses async getter.
    pub(crate) async fn expand_frontier_async<G, M, const BS: usize>(
        getter: &G,
        root: &ChunkAddress,
        context: &M::JoinerContext,
        span: u64,
        chunk_range: &ChunkRange,
        target_subtrees: usize,
    ) -> Result<Vec<SubtreeNode<M>>>
    where
        G: AsyncChunkGet<BS>,
        M: JoinMode + Send + Sync,
    {
        if chunk_range.is_empty() {
            return Ok(Vec::new());
        }

        if span <= BS as u64 {
            return Ok(vec![SubtreeNode {
                addr: *root,
                context: context.clone(),
                span,
                byte_offset: 0,
            }]);
        }

        let ideal_span = span / target_subtrees as u64;

        let mut frontier = vec![SubtreeNode {
            addr: *root,
            context: context.clone(),
            span,
            byte_offset: 0,
        }];
        let mut next = Vec::new();

        loop {
            next.clear();
            let mut any_expanded = false;

            for node in &frontier {
                if node.span > ideal_span && node.span > BS as u64 {
                    let chunk: ContentChunk<BS> =
                        getter.get(&node.addr).await.map_err(FileError::getter)?;
                    let body = M::decode_body::<BS>(chunk, &node.context, node.span)?;
                    next.extend(overlapping_children::<M, BS>(&body, node, chunk_range)?);
                    any_expanded = true;
                } else {
                    next.push(node.clone());
                }
            }

            if !any_expanded {
                break;
            }
            std::mem::swap(&mut frontier, &mut next);
        }

        Ok(frontier)
    }

    /// Async recursive descent within a subtree, collecting leaf bodies.
    pub(crate) fn read_subtree_bodies_async<'a, G, M, const BS: usize>(
        getter: &'a G,
        node: &'a SubtreeNode<M>,
        chunk_range: &'a ChunkRange,
    ) -> BoxFuture<'a, Result<Vec<Bytes>>>
    where
        G: AsyncChunkGet<BS>,
        M: JoinMode + Send + Sync,
    {
        Box::pin(async move {
            let chunk: ContentChunk<BS> =
                getter.get(&node.addr).await.map_err(FileError::getter)?;
            let body = M::decode_body::<BS>(chunk, &node.context, node.span)?;

            if node.span <= BS as u64 {
                return Ok(vec![body]);
            }

            let children = overlapping_children::<M, BS>(&body, node, chunk_range)?;
            let mut out = Vec::new();
            for child in &children {
                let child_bodies =
                    read_subtree_bodies_async::<G, M, BS>(getter, child, chunk_range).await?;
                out.extend(child_bodies);
            }

            Ok(out)
        })
    }
}

#[cfg(feature = "async")]
pub(crate) use async_ops::{expand_frontier_async, read_subtree_bodies_async};

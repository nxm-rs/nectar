//! Parallel joiner with BFS fan-out and streaming read.
//!
//! Uses a pre-expanded subtree frontier to avoid redundant intermediate node
//! decryption. Subtrees are processed in parallel via rayon, and an `impl Read`
//! provides bounded-memory streaming.

use std::marker::PhantomData;

use bytes::Bytes;
use rayon::prelude::*;

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::ChunkAddress;

use super::error::Result;
use super::mode::{JoinMode, PlainMode};
use super::tree::{ChunkRange, TreeParams};
use crate::store::ChunkGet;

#[cfg(feature = "encryption")]
use super::mode::EncryptedMode;

/// A subtree root in the BFS frontier.
struct SubtreeNode<M: JoinMode> {
    addr: ChunkAddress,
    context: M::JoinerContext,
    span: u64,
    /// Byte offset in the file where this subtree's data starts.
    byte_offset: u64,
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
fn overlapping_children<M, const BS: usize>(
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

    let mut children = Vec::new();
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
fn expand_frontier<G, M, const BS: usize>(
    getter: &G,
    root: &ChunkAddress,
    context: &M::JoinerContext,
    span: u64,
    chunk_range: &ChunkRange,
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

    let target = rayon::current_num_threads().max(1) * 2;
    let ideal_span = span / target as u64;

    let mut frontier = vec![SubtreeNode {
        addr: *root,
        context: context.clone(),
        span,
        byte_offset: 0,
    }];

    loop {
        let mut next = Vec::new();
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
        frontier = next;
    }

    Ok(frontier)
}

/// Sequential recursive descent within a subtree, collecting leaf bodies.
fn read_subtree_bodies<G, M, const BS: usize>(
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

/// Generic parallel joiner parameterized by chunk mode.
///
/// Uses BFS fan-out to pre-expand intermediate nodes into a frontier of
/// roughly equal-sized subtrees. Implements `std::io::Read` for bounded-memory
/// streaming via batched subtree processing.
pub struct GenericParallelJoiner<G, M: JoinMode, const BODY_SIZE: usize = DEFAULT_BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE> + Clone + Send + Sync,
{
    getter: G,
    root: ChunkAddress,
    context: M::JoinerContext,
    span: u64,
    tree: TreeParams<BODY_SIZE>,

    /// Pre-expanded frontier for streaming (computed at construction).
    subtrees: Vec<SubtreeNode<M>>,

    /// Streaming state for Read impl.
    read_pos: u64,
    buffer: Vec<u8>,
    buffer_pos: usize,
    subtree_idx: usize,

    _mode: PhantomData<M>,
}

/// Plain (unencrypted) parallel joiner.
pub type ParallelJoiner<G, const BODY_SIZE: usize = DEFAULT_BODY_SIZE> =
    GenericParallelJoiner<G, PlainMode, BODY_SIZE>;

/// Encrypted parallel joiner.
#[cfg(feature = "encryption")]
pub type EncryptedParallelJoiner<G, const BODY_SIZE: usize = DEFAULT_BODY_SIZE> =
    GenericParallelJoiner<G, EncryptedMode, BODY_SIZE>;

impl<G, M, const BODY_SIZE: usize> std::fmt::Debug for GenericParallelJoiner<G, M, BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE> + Clone + Send + Sync,
    M: JoinMode,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericParallelJoiner")
            .field("root", &self.root)
            .field("span", &self.span)
            .finish_non_exhaustive()
    }
}

impl<G, M, const BODY_SIZE: usize> GenericParallelJoiner<G, M, BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE> + Clone + Send + Sync,
    M: JoinMode + Send + Sync,
{
    /// Create a parallel joiner from a root reference.
    pub fn new(getter: G, input: M::RootRef) -> Result<Self> {
        let (root, span, context) = M::joiner_init::<BODY_SIZE, G>(&getter, input)?;
        let tree = TreeParams::<BODY_SIZE>::new(span);

        let full_range = tree.chunks_for_range(0, span);
        let subtrees = expand_frontier::<G, M, BODY_SIZE>(
            &getter, &root, &context, span, &full_range,
        )?;

        Ok(Self {
            getter,
            root,
            context,
            span,
            tree,
            subtrees,
            read_pos: 0,
            buffer: Vec::new(),
            buffer_pos: 0,
            subtree_idx: 0,
            _mode: PhantomData,
        })
    }

    /// Total file size.
    pub fn size(&self) -> u64 {
        self.span
    }

    /// Root address.
    pub fn root(&self) -> &ChunkAddress {
        &self.root
    }

    /// Read a range of bytes, fetching required chunks in parallel.
    pub fn read_range(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        if offset >= self.span {
            return Ok(Vec::new());
        }

        let actual_len = len.min((self.span - offset) as usize);
        if actual_len == 0 {
            return Ok(Vec::new());
        }

        if self.span <= BODY_SIZE as u64 {
            return self.read_single_chunk(offset, actual_len);
        }

        let chunk_range = self.tree.chunks_for_range(offset, actual_len as u64);

        // Compute range-specific frontier (temporary, not stored)
        let subtrees = expand_frontier::<G, M, BODY_SIZE>(
            &self.getter, &self.root, &self.context, self.span, &chunk_range,
        )?;

        // Parallel subtree processing
        let bodies: Vec<Bytes> = subtrees
            .par_iter()
            .map(|st| {
                let mut bodies = Vec::new();
                read_subtree_bodies::<G, M, BODY_SIZE>(
                    &self.getter, st, &chunk_range, &mut bodies,
                )?;
                Ok(bodies)
            })
            .collect::<Result<Vec<Vec<Bytes>>>>()?
            .into_iter()
            .flatten()
            .collect();

        Ok(super::tree::assemble_range(
            &self.tree,
            offset,
            actual_len,
            &chunk_range,
            &bodies,
        ))
    }

    /// Read entire file into memory.
    pub fn read_all(&self) -> Result<Vec<u8>> {
        self.read_range(0, self.span as usize)
    }

    fn read_single_chunk(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let body =
            M::read_chunk_body::<BODY_SIZE, G>(&self.getter, &self.root, &self.context, self.span)?;
        let start = offset as usize;
        let end = start + len;
        Ok(body[start..end].to_vec())
    }

    /// Process the next batch of subtrees into the internal buffer.
    fn fill_buffer(&mut self) -> Result<()> {
        let batch_size = rayon::current_num_threads().max(1);
        let end_idx = (self.subtree_idx + batch_size).min(self.subtrees.len());

        let all_bodies = {
            let getter = &self.getter;
            let batch = &self.subtrees[self.subtree_idx..end_idx];
            if batch.is_empty() {
                return Ok(());
            }

            let batch_start_byte = batch[0].byte_offset;
            let last = &batch[batch.len() - 1];
            let batch_end_byte = last.byte_offset + last.span;
            let chunk_range = ChunkRange {
                start: batch_start_byte / BODY_SIZE as u64,
                end: batch_end_byte.div_ceil(BODY_SIZE as u64),
            };

            batch
                .par_iter()
                .map(|st| {
                    let mut bodies = Vec::new();
                    read_subtree_bodies::<G, M, BODY_SIZE>(
                        getter, st, &chunk_range, &mut bodies,
                    )?;
                    Ok(bodies)
                })
                .collect::<Result<Vec<Vec<Bytes>>>>()?
        };

        self.buffer.clear();
        for bodies in all_bodies {
            for body in bodies {
                self.buffer.extend_from_slice(&body);
            }
        }
        self.buffer_pos = 0;
        self.subtree_idx = end_idx;

        Ok(())
    }

    /// Copy bytes from the internal buffer to the caller's buffer.
    fn drain_buffer(&mut self, buf: &mut [u8]) -> usize {
        let available = self.buffer.len() - self.buffer_pos;
        let to_copy = buf.len().min(available);
        buf[..to_copy].copy_from_slice(&self.buffer[self.buffer_pos..self.buffer_pos + to_copy]);
        self.buffer_pos += to_copy;
        self.read_pos += to_copy as u64;
        to_copy
    }
}

impl<G, M, const BODY_SIZE: usize> std::io::Read for GenericParallelJoiner<G, M, BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE> + Clone + Send + Sync,
    M: JoinMode + Send + Sync,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() || self.read_pos >= self.span {
            return Ok(0);
        }

        if self.buffer_pos < self.buffer.len() {
            return Ok(self.drain_buffer(buf));
        }

        if self.subtree_idx >= self.subtrees.len() {
            return Ok(0);
        }

        self.fill_buffer().map_err(std::io::Error::other)?;

        if self.buffer.is_empty() {
            return Ok(0);
        }

        Ok(self.drain_buffer(buf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::{Chunk, ContentChunk};
    use crate::file::split;
    use std::collections::HashMap;

    fn split_and_store(data: &[u8]) -> (ChunkAddress, HashMap<ChunkAddress, ContentChunk>) {
        let (root, chunks) = split::<DEFAULT_BODY_SIZE>(data).unwrap();
        let store: HashMap<ChunkAddress, ContentChunk> =
            chunks.into_iter().map(|c| (*c.address(), c)).collect();
        (root, store)
    }

    #[test]
    fn test_parallel_joiner_small() {
        let data = b"hello world";
        let (root, sink) = split_and_store(data);

        let joiner = ParallelJoiner::new(sink, root).unwrap();
        let result = joiner.read_all().unwrap();

        assert_eq!(result, data);
    }

    #[test]
    fn test_parallel_joiner_range() {
        let data = b"hello world";
        let (root, sink) = split_and_store(data);

        let joiner = ParallelJoiner::new(sink, root).unwrap();
        let result = joiner.read_range(6, 5).unwrap();

        assert_eq!(result, b"world");
    }

    #[test]
    fn test_parallel_joiner_two_chunks() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE + 100).map(|i| (i % 256) as u8).collect();
        let (root, sink) = split_and_store(&data);

        let joiner = ParallelJoiner::new(sink, root).unwrap();
        let result = joiner.read_all().unwrap();

        assert_eq!(result, data);
    }

    #[test]
    fn test_parallel_joiner_range_spanning_chunks() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3).map(|i| (i % 256) as u8).collect();
        let (root, sink) = split_and_store(&data);

        let joiner = ParallelJoiner::new(sink, root).unwrap();

        // Read across chunk boundary
        let start = DEFAULT_BODY_SIZE - 50;
        let len = 100;
        let result = joiner.read_range(start as u64, len).unwrap();

        assert_eq!(result, &data[start..start + len]);
    }

    #[test]
    fn test_parallel_joiner_128_chunks() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 128).map(|i| (i % 256) as u8).collect();
        let (root, sink) = split_and_store(&data);

        let joiner = ParallelJoiner::new(sink, root).unwrap();
        let result = joiner.read_all().unwrap();

        assert_eq!(result, data);
    }

    #[test]
    fn test_parallel_joiner_129_chunks() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 129).map(|i| (i % 256) as u8).collect();
        let (root, sink) = split_and_store(&data);

        let joiner = ParallelJoiner::new(sink, root).unwrap();
        let result = joiner.read_all().unwrap();

        assert_eq!(result, data);
    }

    #[test]
    fn test_parallel_joiner_streaming() {
        use std::io::Read;
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3 + 500).map(|i| (i % 256) as u8).collect();
        let (root, sink) = split_and_store(&data);

        let mut joiner = ParallelJoiner::new(sink, root).unwrap();
        let mut result = vec![0u8; data.len()];
        joiner.read_exact(&mut result).unwrap();

        assert_eq!(result, data);
    }

    #[test]
    fn test_parallel_joiner_small_buffer_streaming() {
        use std::io::Read;
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 128).map(|i| (i % 256) as u8).collect();
        let (root, sink) = split_and_store(&data);

        let mut joiner = ParallelJoiner::new(sink, root).unwrap();
        let mut result = Vec::new();
        let mut buf = [0u8; 100];
        loop {
            let n = joiner.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            result.extend_from_slice(&buf[..n]);
        }

        assert_eq!(result, data);
    }

    #[cfg(feature = "encryption")]
    mod encrypted {
        use super::*;
        use crate::chunk::Chunk;
        use crate::file::split_encrypted;

        fn encrypted_split_and_store(
            data: &[u8],
        ) -> (
            crate::chunk::encryption::EncryptedChunkRef,
            HashMap<ChunkAddress, ContentChunk>,
        ) {
            let (root_ref, chunks) = split_encrypted::<DEFAULT_BODY_SIZE>(data).unwrap();
            let store: HashMap<ChunkAddress, ContentChunk> =
                chunks.into_iter().map(|c| (*c.address(), c)).collect();
            (root_ref, store)
        }

        #[test]
        fn test_encrypted_parallel_joiner_small() {
            let data = b"hello world";
            let (root_ref, store) = encrypted_split_and_store(data);

            let joiner = EncryptedParallelJoiner::new(store, root_ref).unwrap();
            let result = joiner.read_all().unwrap();

            assert_eq!(result, data);
        }

        #[test]
        fn test_encrypted_parallel_joiner_range() {
            let data = b"hello encrypted world";
            let (root_ref, store) = encrypted_split_and_store(data);

            let joiner = EncryptedParallelJoiner::new(store, root_ref).unwrap();
            let result = joiner.read_range(6, 9).unwrap();

            assert_eq!(result, b"encrypted");
        }

        #[test]
        fn test_encrypted_parallel_joiner_two_chunks() {
            let data: Vec<u8> =
                (0..DEFAULT_BODY_SIZE + 100).map(|i| (i % 256) as u8).collect();
            let (root_ref, store) = encrypted_split_and_store(&data);

            let joiner = EncryptedParallelJoiner::new(store, root_ref).unwrap();
            let result = joiner.read_all().unwrap();

            assert_eq!(result, data);
        }

        #[test]
        fn test_encrypted_parallel_joiner_128_chunks() {
            // 64 chunks fills one encrypted intermediate, 128 needs two intermediates + root
            let data: Vec<u8> =
                (0..DEFAULT_BODY_SIZE * 128).map(|i| (i % 256) as u8).collect();
            let (root_ref, store) = encrypted_split_and_store(&data);

            let joiner = EncryptedParallelJoiner::new(store, root_ref).unwrap();
            let result = joiner.read_all().unwrap();

            assert_eq!(result, data);
        }

        #[test]
        fn test_encrypted_parallel_joiner_streaming() {
            use std::io::Read;
            let data: Vec<u8> =
                (0..DEFAULT_BODY_SIZE * 65).map(|i| (i % 256) as u8).collect();
            let (root_ref, store) = encrypted_split_and_store(&data);

            let mut joiner = EncryptedParallelJoiner::new(store, root_ref).unwrap();
            let mut result = vec![0u8; data.len()];
            joiner.read_exact(&mut result).unwrap();

            assert_eq!(result, data);
        }

        #[test]
        fn test_encrypted_parallel_joiner_small_buffer_streaming() {
            use std::io::Read;
            let data: Vec<u8> =
                (0..DEFAULT_BODY_SIZE * 128).map(|i| (i % 256) as u8).collect();
            let (root_ref, store) = encrypted_split_and_store(&data);

            let mut joiner = EncryptedParallelJoiner::new(store, root_ref).unwrap();
            let mut result = Vec::new();
            let mut buf = [0u8; 100];
            loop {
                let n = joiner.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                result.extend_from_slice(&buf[..n]);
            }

            assert_eq!(result, data);
        }

        #[test]
        fn test_encrypted_parallel_joiner_range_from_middle() {
            let data: Vec<u8> =
                (0..DEFAULT_BODY_SIZE * 128).map(|i| (i % 256) as u8).collect();
            let (root_ref, store) = encrypted_split_and_store(&data);

            let joiner = EncryptedParallelJoiner::new(store, root_ref).unwrap();
            let start = DEFAULT_BODY_SIZE * 50;
            let len = DEFAULT_BODY_SIZE * 10;
            let result = joiner.read_range(start as u64, len).unwrap();

            assert_eq!(result, &data[start..start + len]);
        }

        #[test]
        fn test_encrypted_parallel_joiner_256_chunks() {
            // 256 encrypted chunks: 4 intermediates of 64 each, depth 3
            let data: Vec<u8> =
                (0..DEFAULT_BODY_SIZE * 256).map(|i| (i % 256) as u8).collect();
            let (root_ref, store) = encrypted_split_and_store(&data);

            let joiner = EncryptedParallelJoiner::new(store, root_ref).unwrap();
            let result = joiner.read_all().unwrap();

            assert_eq!(result, data);
        }

        #[test]
        fn test_encrypted_parallel_joiner_65_chunks() {
            // 65 encrypted chunks: 64+1 split, tests imbalanced tree handling
            let data: Vec<u8> =
                (0..DEFAULT_BODY_SIZE * 65).map(|i| (i % 256) as u8).collect();
            let (root_ref, store) = encrypted_split_and_store(&data);

            let joiner = EncryptedParallelJoiner::new(store, root_ref).unwrap();
            let result = joiner.read_all().unwrap();

            assert_eq!(result, data);
        }
    }
}

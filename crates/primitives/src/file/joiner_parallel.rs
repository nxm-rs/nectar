//! Parallel joiner for batch chunk fetching.

use std::marker::PhantomData;

use bytes::Bytes;
use rayon::prelude::*;

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::ChunkAddress;

use super::error::{FileError, Result};
use super::mode::{JoinMode, PlainMode};
use super::tree::TreeParams;
use crate::store::ChunkGet;

#[cfg(feature = "encryption")]
use super::mode::EncryptedMode;

/// Generic parallel joiner parameterized by chunk mode.
///
/// More efficient than sequential joiner for random access patterns
/// or when chunk retrieval has high latency.
pub struct GenericParallelJoiner<G, M: JoinMode, const BODY_SIZE: usize = DEFAULT_BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE> + Clone + Send + Sync,
{
    getter: G,
    root: ChunkAddress,
    context: M::JoinerContext,
    span: u64,
    tree: TreeParams<BODY_SIZE>,
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

        Ok(Self {
            getter,
            root,
            context,
            span,
            tree,
            _mode: PhantomData,
        })
    }

    /// Total file size.
    pub const fn size(&self) -> u64 {
        self.span
    }

    /// Root address.
    pub const fn root(&self) -> &ChunkAddress {
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

        // For small files, just use simple fetch
        if self.span <= BODY_SIZE as u64 {
            return self.read_single_chunk(offset, actual_len);
        }

        // Calculate required data chunks
        let chunk_range = self.tree.chunks_for_range(offset, actual_len as u64);

        // Traverse tree to find data chunk addresses and their contexts
        let data_refs = self.collect_data_chunk_refs(&chunk_range)?;

        // Compute spans for each data chunk
        let data_spans: Vec<u64> = chunk_range
            .iter()
            .map(|idx| {
                let (s, e) = self.tree.chunk_range(idx);
                e - s
            })
            .collect();

        // Fetch and decode all data chunks in parallel
        let bodies: Vec<Bytes> = data_refs
            .par_iter()
            .zip(data_spans.par_iter())
            .map(|((addr, ctx), span)| {
                M::read_chunk_body::<BODY_SIZE, G>(&self.getter, addr, ctx, *span)
            })
            .collect::<Result<Vec<_>>>()?;

        // Assemble result
        let mut result = vec![0u8; actual_len];
        let mut result_offset = 0;

        for (i, chunk_idx) in chunk_range.iter().enumerate() {
            let (chunk_start, chunk_end) = self.tree.chunk_range(chunk_idx);
            let chunk_data_len = (chunk_end - chunk_start) as usize;

            let read_start = if chunk_start < offset {
                (offset - chunk_start) as usize
            } else {
                0
            };

            let read_end = if chunk_end > offset + actual_len as u64 {
                chunk_data_len - ((chunk_end - offset - actual_len as u64) as usize)
            } else {
                chunk_data_len
            };

            let bytes_to_copy = read_end - read_start;
            let body = &bodies[i];

            result[result_offset..result_offset + bytes_to_copy]
                .copy_from_slice(&body[read_start..read_end]);
            result_offset += bytes_to_copy;
        }

        Ok(result)
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

    fn collect_data_chunk_refs(
        &self,
        chunk_range: &super::tree::ChunkRange,
    ) -> Result<Vec<(ChunkAddress, M::JoinerContext)>> {
        let mut refs = Vec::with_capacity(chunk_range.len() as usize);

        for chunk_idx in chunk_range.iter() {
            let r = self.find_data_chunk_ref(chunk_idx)?;
            refs.push(r);
        }

        Ok(refs)
    }

    fn find_data_chunk_ref(
        &self,
        data_chunk_idx: u64,
    ) -> Result<(ChunkAddress, M::JoinerContext)> {
        let offset = data_chunk_idx * BODY_SIZE as u64;
        self.traverse_to_data_chunk(&self.root, &self.context, self.span, offset)
    }

    fn traverse_to_data_chunk(
        &self,
        addr: &ChunkAddress,
        context: &M::JoinerContext,
        span: u64,
        offset: u64,
    ) -> Result<(ChunkAddress, M::JoinerContext)> {
        if span <= BODY_SIZE as u64 {
            return Ok((*addr, context.clone()));
        }

        let body = M::read_chunk_body::<BODY_SIZE, G>(&self.getter, addr, context, span)?;

        let subspan = M::subspan_size::<BODY_SIZE>(span);
        let child_index = (offset / subspan) as usize;
        let child_offset = offset % subspan;

        let ref_start = child_index * M::REF_SIZE;
        let ref_end = ref_start + M::REF_SIZE;

        if ref_end > body.len() {
            return Err(FileError::InvalidReference { level: 0 });
        }

        let (child_addr, child_context) = M::parse_child_ref(&body, ref_start)?;

        let refs_per_chunk = M::refs_per_chunk(BODY_SIZE);
        let child_span = if child_index == refs_per_chunk - 1 {
            let preceding = child_index as u64 * subspan;
            span.saturating_sub(preceding)
        } else {
            subspan.min(span - child_index as u64 * subspan)
        };

        self.traverse_to_data_chunk(&child_addr, &child_context, child_span, child_offset)
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
    }
}

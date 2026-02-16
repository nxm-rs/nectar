//! Parallel joiner for batch chunk fetching.

use std::collections::HashMap;

use rayon::prelude::*;

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::{BmtChunk, Chunk, ChunkAddress, ContentChunk};

use super::constants::REF_SIZE;
use super::subspan_size;
use super::error::{FileError, Result};
use super::traits::ChunkGet;
use super::tree::TreeParams;

/// Parallel joiner that batch-fetches required chunks.
///
/// More efficient than sequential joiner for random access patterns
/// or when chunk retrieval has high latency.
pub struct ParallelJoiner<G, const BODY_SIZE: usize = DEFAULT_BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE> + Clone + Send + Sync,
{
    getter: G,
    root: ChunkAddress,
    span: u64,
    tree: TreeParams<BODY_SIZE>,
}

impl<G, const BODY_SIZE: usize> std::fmt::Debug for ParallelJoiner<G, BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE> + Clone + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParallelJoiner")
            .field("root", &self.root)
            .field("span", &self.span)
            .finish_non_exhaustive()
    }
}

impl<G, const BODY_SIZE: usize> ParallelJoiner<G, BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE> + Clone + Send + Sync,
{
    /// Create a parallel joiner from a root address.
    pub fn new(getter: G, root: ChunkAddress) -> Result<Self> {
        let root_chunk = getter.get(&root).map_err(FileError::getter)?;
        let span = root_chunk.span();
        let tree = TreeParams::<BODY_SIZE>::new(span);

        Ok(Self {
            getter,
            root,
            span,
            tree,
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

        // Collect all addresses we need (traverse tree to find data chunk addresses)
        let data_addrs = self.collect_data_chunk_addrs(&chunk_range)?;

        // Fetch all data chunks in parallel
        let chunks: HashMap<ChunkAddress, ContentChunk<BODY_SIZE>> = data_addrs
            .par_iter()
            .map(|addr| {
                let chunk = self.getter.get(addr).map_err(FileError::getter)?;
                Ok((*addr, chunk))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        // Assemble result
        let mut result = vec![0u8; actual_len];
        let mut result_offset = 0;

        for chunk_idx in chunk_range.iter() {
            let (chunk_start, chunk_end) = self.tree.chunk_range(chunk_idx);
            let chunk_data_len = (chunk_end - chunk_start) as usize;

            // Find where this chunk's data goes in the result
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

            // Get the chunk data
            let addr = data_addrs[(chunk_idx - chunk_range.start) as usize];
            let chunk = chunks.get(&addr).ok_or(FileError::ChunkNotFound(addr))?;
            let data = chunk.data();

            result[result_offset..result_offset + bytes_to_copy]
                .copy_from_slice(&data[read_start..read_end]);
            result_offset += bytes_to_copy;
        }

        Ok(result)
    }

    /// Read entire file into memory.
    pub fn read_all(&self) -> Result<Vec<u8>> {
        self.read_range(0, self.span as usize)
    }

    fn read_single_chunk(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let chunk = self.getter.get(&self.root).map_err(FileError::getter)?;
        let data = chunk.data();
        let start = offset as usize;
        let end = start + len;
        Ok(data[start..end].to_vec())
    }

    fn collect_data_chunk_addrs(
        &self,
        chunk_range: &super::tree::ChunkRange,
    ) -> Result<Vec<ChunkAddress>> {
        // We need to traverse from root to find the data chunk addresses
        let mut addrs = Vec::with_capacity(chunk_range.len() as usize);

        for chunk_idx in chunk_range.iter() {
            let addr = self.find_data_chunk_addr(chunk_idx)?;
            addrs.push(addr);
        }

        Ok(addrs)
    }

    fn find_data_chunk_addr(&self, data_chunk_idx: u64) -> Result<ChunkAddress> {
        let offset = data_chunk_idx * BODY_SIZE as u64;
        self.traverse_to_data_chunk(&self.root, self.span, offset)
    }

    fn traverse_to_data_chunk(
        &self,
        addr: &ChunkAddress,
        span: u64,
        offset: u64,
    ) -> Result<ChunkAddress> {
        // If this is a data chunk, return its address
        if span <= BODY_SIZE as u64 {
            return Ok(*addr);
        }

        // Otherwise it's an intermediate chunk, find the right child
        let chunk = self.getter.get(addr).map_err(FileError::getter)?;
        let chunk_data = chunk.data();

        let subspan = subspan_size::<BODY_SIZE>(span);
        let child_index = (offset / subspan) as usize;
        let child_offset = offset % subspan;

        let ref_start = child_index * REF_SIZE;
        let ref_end = ref_start + REF_SIZE;

        if ref_end > chunk_data.len() {
            return Err(FileError::InvalidReference { level: 0 });
        }

        let child_addr_bytes: [u8; 32] = chunk_data[ref_start..ref_end]
            .try_into()
            .map_err(|_| FileError::InvalidReference { level: 0 })?;
        let child_addr = ChunkAddress::from(child_addr_bytes);

        // Calculate child span
        let refs_per_chunk = BODY_SIZE / REF_SIZE;
        let child_span = if child_index == refs_per_chunk - 1 {
            let preceding = child_index as u64 * subspan;
            span.saturating_sub(preceding)
        } else {
            subspan.min(span - child_index as u64 * subspan)
        };

        self.traverse_to_data_chunk(&child_addr, child_span, child_offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::split;

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
}

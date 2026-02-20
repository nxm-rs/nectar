//! Parallel file splitter using random-access data sources.

use std::sync::Mutex;

use bytes::Bytes;
use rayon::prelude::*;

use crate::bmt::{DEFAULT_BODY_SIZE, SPAN_SIZE};
use crate::chunk::{Chunk, ChunkAddress, ContentChunk};

use super::constants::{REFS_PER_CHUNK, SPANS};
use super::error::{FileError, Result};
use super::read_at::ReadAt;
use super::tree::TreeParams;
use crate::store::ChunkPut;

/// Parallel file splitter using random-access data sources.
///
/// Splits files by reading chunks at known offsets in parallel,
/// then building intermediate levels.
pub struct ParallelSplitter<S, const BODY_SIZE: usize = DEFAULT_BODY_SIZE>
where
    S: ChunkPut<BODY_SIZE> + Send,
{
    sink: Mutex<S>,
}

impl<S, const BODY_SIZE: usize> std::fmt::Debug for ParallelSplitter<S, BODY_SIZE>
where
    S: ChunkPut<BODY_SIZE> + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParallelSplitter").finish_non_exhaustive()
    }
}

impl<S, const BODY_SIZE: usize> ParallelSplitter<S, BODY_SIZE>
where
    S: ChunkPut<BODY_SIZE> + Send,
{
    /// Create a parallel splitter with the given chunk sink.
    pub const fn new(sink: S) -> Self {
        Self {
            sink: Mutex::new(sink),
        }
    }

    /// Split data from a random-access source.
    pub fn split<R: ReadAt + Sync>(&self, source: &R) -> Result<ChunkAddress> {
        let size = source.len();
        let tree = TreeParams::<BODY_SIZE>::new(size);

        if size == 0 {
            return self.handle_empty();
        }

        // Level 0: Create data chunks in parallel
        let level0_addrs = self.create_data_chunks(source, &tree)?;

        // Build intermediate levels
        self.build_intermediate_levels(level0_addrs, size)
    }

    /// Consume the splitter and return the sink.
    pub fn into_sink(self) -> S {
        self.sink.into_inner().unwrap()
    }

    fn handle_empty(&self) -> Result<ChunkAddress> {
        let chunk = ContentChunk::<BODY_SIZE>::new(Bytes::new()).map_err(|e| match e {
            crate::error::PrimitivesError::Chunk(c) => FileError::Chunk(c),
            other => FileError::Sink(Box::new(other)),
        })?;
        let address = *chunk.address();
        self.put_chunk(chunk)?;
        Ok(address)
    }

    fn create_data_chunks<R: ReadAt + Sync>(
        &self,
        source: &R,
        tree: &TreeParams<BODY_SIZE>,
    ) -> Result<Vec<ChunkAddress>> {
        let data_chunks = tree.data_chunks();
        let size = tree.size();

        // Parallel chunk creation
        let results: Vec<Result<ChunkAddress>> = (0..data_chunks)
            .into_par_iter()
            .map(|i| {
                let offset = i * BODY_SIZE as u64;
                let chunk_size = ((size - offset) as usize).min(BODY_SIZE);

                // Read chunk data
                let mut buf = vec![0u8; chunk_size];
                source
                    .read_at(offset, &mut buf)
                    .map_err(|e| FileError::Sink(Box::new(e)))?;

                // Calculate span for this chunk
                let span = if i + 1 == data_chunks {
                    size - offset // Last chunk
                } else {
                    BODY_SIZE as u64
                };

                // Create chunk with span header
                let mut chunk_bytes = Vec::with_capacity(SPAN_SIZE + chunk_size);
                chunk_bytes.extend_from_slice(&span.to_le_bytes());
                chunk_bytes.extend_from_slice(&buf);

                let chunk = ContentChunk::<BODY_SIZE>::try_from(Bytes::from(chunk_bytes))
                    .map_err(|e| match e {
                        crate::error::PrimitivesError::Chunk(c) => FileError::Chunk(c),
                        other => FileError::Sink(Box::new(other)),
                    })?;

                let address = *chunk.address();
                self.put_chunk(chunk)?;
                Ok(address)
            })
            .collect();

        // Collect results, propagating any errors
        results.into_iter().collect()
    }

    fn build_intermediate_levels(
        &self,
        mut addrs: Vec<ChunkAddress>,
        total_size: u64,
    ) -> Result<ChunkAddress> {
        let mut level = 1;

        while addrs.len() > 1 {
            addrs = self.build_level(&addrs, level, total_size)?;
            level += 1;
        }

        Ok(addrs.into_iter().next().unwrap())
    }

    fn build_level(
        &self,
        addrs: &[ChunkAddress],
        level: usize,
        total_size: u64,
    ) -> Result<Vec<ChunkAddress>> {
        let chunks_at_level = addrs.len().div_ceil(REFS_PER_CHUNK);


        // Build intermediate chunks in parallel
        let results: Vec<Result<ChunkAddress>> = (0..chunks_at_level)
            .into_par_iter()
            .map(|i| {
                let start = i * REFS_PER_CHUNK;
                let end = (start + REFS_PER_CHUNK).min(addrs.len());
                let refs = &addrs[start..end];

                // Single reference: carry up without wrapping (dangling chunk optimization)
                if refs.len() == 1 {
                    return Ok(refs[0]);
                }

                // Calculate span for this intermediate chunk
                let span = self.calculate_intermediate_span(level, i, chunks_at_level, total_size);

                // Build chunk data from references
                let mut chunk_bytes = Vec::with_capacity(SPAN_SIZE + refs.len() * 32);
                chunk_bytes.extend_from_slice(&span.to_le_bytes());
                for addr in refs {
                    chunk_bytes.extend_from_slice(addr.as_ref());
                }

                let chunk = ContentChunk::<BODY_SIZE>::try_from(Bytes::from(chunk_bytes))
                    .map_err(|e| match e {
                        crate::error::PrimitivesError::Chunk(c) => FileError::Chunk(c),
                        other => FileError::Sink(Box::new(other)),
                    })?;

                let address = *chunk.address();
                self.put_chunk(chunk)?;
                Ok(address)
            })
            .collect();

        results.into_iter().collect()
    }

    fn calculate_intermediate_span(
        &self,
        level: usize,
        chunk_index: usize,
        chunks_at_level: usize,
        total_size: u64,
    ) -> u64 {
        let max_span = SPANS[level] * BODY_SIZE as u64;

        if chunk_index + 1 == chunks_at_level {
            // Last chunk at this level
            let preceding = chunk_index as u64 * max_span;
            total_size.saturating_sub(preceding)
        } else {
            max_span
        }
    }

    fn put_chunk(&self, chunk: ContentChunk<BODY_SIZE>) -> Result<()> {
        self.sink
            .lock()
            .unwrap()
            .put(chunk)
            .map_err(FileError::sink)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::{join, split};
    use crate::store::{MemorySink, VecSink};

    #[test]
    fn test_parallel_splitter_empty() {
        let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
        let splitter = ParallelSplitter::new(sink);

        let data: &[u8] = &[];
        let root = splitter.split(&data).unwrap();
        let sink = splitter.into_sink();

        assert_eq!(sink.len(), 1);
        assert!(!root.is_zero());

        // Compare with sequential
        let (seq_root, _) = split::<DEFAULT_BODY_SIZE>(&[]).unwrap();
        assert_eq!(root, seq_root);
    }

    #[test]
    fn test_parallel_splitter_small() {
        let data = b"hello world";
        let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
        let splitter = ParallelSplitter::new(sink);

        let root = splitter.split(&data.as_slice()).unwrap();
        let sink = splitter.into_sink();

        assert_eq!(sink.len(), 1);

        // Compare with sequential
        let (seq_root, _) = split::<DEFAULT_BODY_SIZE>(data).unwrap();
        assert_eq!(root, seq_root);
    }

    #[test]
    fn test_parallel_splitter_exact_chunk() {
        let data = vec![0xAB; DEFAULT_BODY_SIZE];
        let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
        let splitter = ParallelSplitter::new(sink);

        let root = splitter.split(&data.as_slice()).unwrap();

        let (seq_root, _) = split::<DEFAULT_BODY_SIZE>(&data).unwrap();
        assert_eq!(root, seq_root);
    }

    #[test]
    fn test_parallel_splitter_two_chunks() {
        let data = vec![0xCD; DEFAULT_BODY_SIZE + 1];
        let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
        let splitter = ParallelSplitter::new(sink);

        let root = splitter.split(&data.as_slice()).unwrap();
        let sink = splitter.into_sink();

        assert_eq!(sink.len(), 3); // 2 data + 1 intermediate

        let (seq_root, _) = split::<DEFAULT_BODY_SIZE>(&data).unwrap();
        assert_eq!(root, seq_root);
    }

    #[test]
    fn test_parallel_splitter_128_chunks() {
        let data = vec![0xEF; DEFAULT_BODY_SIZE * 128];
        let sink = MemorySink::<DEFAULT_BODY_SIZE>::new();
        let splitter = ParallelSplitter::new(sink);

        let root = splitter.split(&data.as_slice()).unwrap();
        let sink = splitter.into_sink();

        let (seq_root, _) = split::<DEFAULT_BODY_SIZE>(&data).unwrap();
        assert_eq!(root, seq_root);

        // Verify round-trip
        let recovered = join(&sink, root).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_parallel_splitter_129_chunks() {
        let data = vec![0x12; DEFAULT_BODY_SIZE * 129];
        let sink = MemorySink::<DEFAULT_BODY_SIZE>::new();
        let splitter = ParallelSplitter::new(sink);

        let root = splitter.split(&data.as_slice()).unwrap();
        let sink = splitter.into_sink();

        let (seq_root, _) = split::<DEFAULT_BODY_SIZE>(&data).unwrap();
        assert_eq!(root, seq_root);

        // Verify round-trip
        let recovered = join(&sink, root).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_parallel_splitter_varying_data() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 5 + 123)
            .map(|i| (i % 256) as u8)
            .collect();

        let sink = MemorySink::<DEFAULT_BODY_SIZE>::new();
        let splitter = ParallelSplitter::new(sink);

        let root = splitter.split(&data.as_slice()).unwrap();
        let sink = splitter.into_sink();

        let (seq_root, _) = split::<DEFAULT_BODY_SIZE>(&data).unwrap();
        assert_eq!(root, seq_root);

        let recovered = join(&sink, root).unwrap();
        assert_eq!(recovered, data);
    }
}

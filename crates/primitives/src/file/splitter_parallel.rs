//! Parallel file splitter using random-access data sources.

use std::marker::PhantomData;
use std::sync::Mutex;

use rayon::prelude::*;

use crate::bmt::{DEFAULT_BODY_SIZE, SPAN_SIZE};
use crate::chunk::ContentChunk;

use super::error::{FileError, Result};
use super::mode::{PlainMode, SplitMode};
use super::read_at::ReadAt;
use super::tree::TreeParams;
use crate::store::ChunkPut;

#[cfg(feature = "encryption")]
use super::mode::EncryptedMode;

/// Parallel file splitter using random-access data sources.
///
/// Splits files by reading chunks at known offsets in parallel,
/// then building intermediate levels.
pub struct GenericParallelSplitter<S, M: SplitMode, const BODY_SIZE: usize = DEFAULT_BODY_SIZE>
where
    S: ChunkPut<BODY_SIZE> + Send,
{
    sink: Mutex<S>,
    _mode: PhantomData<M>,
}

/// Plain (unencrypted) parallel splitter.
pub type ParallelSplitter<S, const BODY_SIZE: usize = DEFAULT_BODY_SIZE> =
    GenericParallelSplitter<S, PlainMode, BODY_SIZE>;

/// Encrypted parallel splitter.
#[cfg(feature = "encryption")]
pub type EncryptedParallelSplitter<S, const BODY_SIZE: usize = DEFAULT_BODY_SIZE> =
    GenericParallelSplitter<S, EncryptedMode, BODY_SIZE>;

impl<S, M, const BODY_SIZE: usize> std::fmt::Debug for GenericParallelSplitter<S, M, BODY_SIZE>
where
    S: ChunkPut<BODY_SIZE> + Send,
    M: SplitMode,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericParallelSplitter")
            .finish_non_exhaustive()
    }
}

impl<S, M, const BODY_SIZE: usize> GenericParallelSplitter<S, M, BODY_SIZE>
where
    S: ChunkPut<BODY_SIZE> + Send,
    M: SplitMode + Send + Sync,
{
    /// Create a parallel splitter with the given chunk sink.
    pub const fn new(sink: S) -> Self {
        Self {
            sink: Mutex::new(sink),
            _mode: PhantomData,
        }
    }

    /// Split data from a random-access source.
    pub fn split<R: ReadAt + Sync>(&self, source: &R) -> Result<M::RootRef> {
        let size = source.len();
        let tree = TreeParams::<BODY_SIZE>::new(size);

        if size == 0 {
            return self.handle_empty();
        }

        // Level 0: Create data chunks in parallel
        let level0_refs = self.create_data_chunks(source, &tree)?;

        // Build intermediate levels
        self.build_intermediate_levels(level0_refs, size)
    }

    /// Consume the splitter and return the sink.
    pub fn into_sink(self) -> S {
        self.sink.into_inner().unwrap()
    }

    fn handle_empty(&self) -> Result<M::RootRef> {
        let mut sink = self.sink.lock().unwrap();
        M::process_empty::<BODY_SIZE, S>(&mut *sink)
    }

    fn create_data_chunks<R: ReadAt + Sync>(
        &self,
        source: &R,
        tree: &TreeParams<BODY_SIZE>,
    ) -> Result<Vec<M::RefBytes>> {
        let data_chunks = tree.data_chunks();
        let size = tree.size();

        let results: Vec<Result<M::RefBytes>> = (0..data_chunks)
            .into_par_iter()
            .map(|i| {
                let offset = i * BODY_SIZE as u64;
                let chunk_size = ((size - offset) as usize).min(BODY_SIZE);

                let mut buf = vec![0u8; chunk_size];
                source
                    .read_at(offset, &mut buf)
                    .map_err(|e| FileError::Sink(Box::new(e)))?;

                let span = if i + 1 == data_chunks {
                    size - offset
                } else {
                    BODY_SIZE as u64
                };

                let mut chunk_bytes = Vec::with_capacity(SPAN_SIZE + chunk_size);
                chunk_bytes.extend_from_slice(&span.to_le_bytes());
                chunk_bytes.extend_from_slice(&buf);

                let (chunk, ref_bytes) = M::prepare_chunk::<BODY_SIZE>(&chunk_bytes)?;
                self.put_chunk(chunk)?;
                Ok(ref_bytes)
            })
            .collect();

        results.into_iter().collect()
    }

    fn build_intermediate_levels(
        &self,
        mut refs: Vec<M::RefBytes>,
        total_size: u64,
    ) -> Result<M::RootRef> {
        let mut level = 1;

        while refs.len() > 1 {
            refs = self.build_level(&refs, level, total_size)?;
            level += 1;
        }

        // Extract root reference from the single remaining ref
        Ok(M::extract_root(refs[0].as_ref()))
    }

    fn build_level(
        &self,
        refs: &[M::RefBytes],
        level: usize,
        total_size: u64,
    ) -> Result<Vec<M::RefBytes>> {
        let refs_per_chunk = M::refs_per_chunk(BODY_SIZE);
        let chunks_at_level = refs.len().div_ceil(refs_per_chunk);

        let results: Vec<Result<M::RefBytes>> = (0..chunks_at_level)
            .into_par_iter()
            .map(|i| {
                let start = i * refs_per_chunk;
                let end = (start + refs_per_chunk).min(refs.len());
                let child_refs = &refs[start..end];

                // Single reference: carry up without wrapping (dangling chunk optimization)
                if child_refs.len() == 1 {
                    return Ok(child_refs[0].clone());
                }

                let span =
                    self.calculate_intermediate_span(level, i, chunks_at_level, total_size);

                let mut chunk_bytes =
                    Vec::with_capacity(SPAN_SIZE + child_refs.len() * M::REF_SIZE);
                chunk_bytes.extend_from_slice(&span.to_le_bytes());
                for r in child_refs {
                    chunk_bytes.extend_from_slice(r.as_ref());
                }

                let (chunk, ref_bytes) = M::prepare_chunk::<BODY_SIZE>(&chunk_bytes)?;
                self.put_chunk(chunk)?;
                Ok(ref_bytes)
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
        let spans = M::spans();
        let max_span = spans[level] * BODY_SIZE as u64;

        if chunk_index + 1 == chunks_at_level {
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

    #[cfg(feature = "encryption")]
    mod encrypted {
        use super::*;
        use crate::file::{join_encrypted, split_encrypted, EncryptedParallelSplitter};
        use crate::store::MemorySink;

        #[test]
        fn test_encrypted_parallel_splitter_empty() {
            let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
            let splitter = EncryptedParallelSplitter::new(sink);

            let data: &[u8] = &[];
            let root_ref = splitter.split(&data).unwrap();
            let sink = splitter.into_sink();

            assert_eq!(sink.len(), 1);
            assert_eq!(root_ref.to_vec().len(), 64);
        }

        #[test]
        fn test_encrypted_parallel_splitter_small() {
            let data = b"hello world";
            let sink = MemorySink::<DEFAULT_BODY_SIZE>::new();
            let splitter = EncryptedParallelSplitter::new(sink);

            let root_ref = splitter.split(&data.as_slice()).unwrap();
            let sink = splitter.into_sink();

            assert_eq!(sink.len(), 1);

            let recovered = join_encrypted(&sink, root_ref).unwrap();
            assert_eq!(recovered, data);
        }

        #[test]
        fn test_encrypted_parallel_splitter_two_chunks() {
            let data = vec![0xCD; DEFAULT_BODY_SIZE + 1];
            let sink = MemorySink::<DEFAULT_BODY_SIZE>::new();
            let splitter = EncryptedParallelSplitter::new(sink);

            let root_ref = splitter.split(&data.as_slice()).unwrap();
            let sink = splitter.into_sink();

            assert_eq!(sink.len(), 3);

            let recovered = join_encrypted(&sink, root_ref).unwrap();
            assert_eq!(recovered, data);
        }

        #[test]
        fn test_encrypted_parallel_matches_sequential() {
            let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 5 + 123)
                .map(|i| (i % 256) as u8)
                .collect();

            // Parallel encrypted split
            let sink = MemorySink::<DEFAULT_BODY_SIZE>::new();
            let splitter = EncryptedParallelSplitter::new(sink);
            let par_ref = splitter.split(&data.as_slice()).unwrap();
            let par_sink = splitter.into_sink();

            // Sequential encrypted split
            let (seq_ref, seq_chunks) = split_encrypted::<DEFAULT_BODY_SIZE>(&data).unwrap();

            // Chunk counts must match
            assert_eq!(par_sink.len(), seq_chunks.len());

            // Both must round-trip correctly
            let par_recovered = join_encrypted(&par_sink, par_ref).unwrap();
            assert_eq!(par_recovered, data);

            use std::collections::HashMap;
            use crate::chunk::Chunk;
            let seq_store: HashMap<_, _> =
                seq_chunks.into_iter().map(|c| (*c.address(), c)).collect();
            let seq_recovered = join_encrypted(&seq_store, seq_ref).unwrap();
            assert_eq!(seq_recovered, data);
        }

        #[test]
        fn test_encrypted_parallel_nondeterministic() {
            let data = b"test determinism";
            let sink1 = VecSink::<DEFAULT_BODY_SIZE>::new();
            let splitter1 = EncryptedParallelSplitter::new(sink1);
            let ref1 = splitter1.split(&data.as_slice()).unwrap();

            let sink2 = VecSink::<DEFAULT_BODY_SIZE>::new();
            let splitter2 = EncryptedParallelSplitter::new(sink2);
            let ref2 = splitter2.split(&data.as_slice()).unwrap();

            // Different random keys each time
            assert_ne!(ref1.address, ref2.address);
        }
    }
}

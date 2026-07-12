//! Sync parallel file splitter using random-access data sources.

use std::marker::PhantomData;

use rayon::prelude::*;

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::AnyChunk;

use super::constants::{LEVEL_LIMIT, compute_spans_inline};
use super::error::{FileError, Result};
use super::mode::{PlainMode, SplitMode};
use super::read_at::ReadAt;
use super::tree::TreeParams;

#[cfg(feature = "encryption")]
use super::mode::EncryptedMode;

/// Parallel file splitter using random-access data sources.
///
/// Produces chunks by reading data at known offsets in parallel, then building
/// intermediate levels. The caller decides where produced chunks go.
pub struct GenericParallelSplitter<M: SplitMode, const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    _mode: PhantomData<M>,
}

/// Plain (unencrypted) parallel splitter.
pub type ParallelSplitter<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> =
    GenericParallelSplitter<PlainMode, BODY_SIZE>;

/// Encrypted parallel splitter.
#[cfg(feature = "encryption")]
pub type EncryptedParallelSplitter<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> =
    GenericParallelSplitter<EncryptedMode, BODY_SIZE>;

impl<M, const BODY_SIZE: usize> std::fmt::Debug for GenericParallelSplitter<M, BODY_SIZE>
where
    M: SplitMode,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericParallelSplitter")
            .finish_non_exhaustive()
    }
}

impl<M, const BODY_SIZE: usize> GenericParallelSplitter<M, BODY_SIZE>
where
    M: SplitMode + Send + Sync,
{
    /// Split `source`, invoking `sink` for every produced chunk.
    ///
    /// `sink` is called from rayon worker threads, so it must be `Fn + Sync`.
    /// Returns the root reference.
    #[allow(clippy::arithmetic_side_effects)] // M::REF_SIZE is the nonzero constant 32 or 64
    pub fn split_into<R, F>(source: &R, sink: F) -> Result<M::RootRef>
    where
        R: ReadAt + Sync,
        F: Fn(AnyChunk<BODY_SIZE>) + Sync,
    {
        const { super::constants::assert_valid_body_size::<BODY_SIZE>() };
        let size = source.len();
        let tree = TreeParams::<BODY_SIZE>::new(size);

        if size == 0 {
            let (chunk, root) = M::empty_chunk::<BODY_SIZE>()?;
            sink(chunk.into());
            return Ok(root);
        }

        let spans = compute_spans_inline(BODY_SIZE / M::REF_SIZE);

        // Level 0: produce data chunks in parallel.
        let level0_refs = Self::create_data_chunks(source, &tree, &sink)?;

        // Build intermediate levels.
        Self::build_intermediate_levels(level0_refs, size, &spans, &sink)
    }

    /// Split `source`, collecting every produced chunk into a `Vec`.
    ///
    /// Chunk order is irrelevant; callers key by address. Returns the root
    /// reference and the produced chunks.
    #[allow(clippy::unwrap_used)] // Mutex poisoning requires a sink panic, which itself already aborts the rayon join; both unwraps are on that same local mutex
    pub fn split_to_vec<R: ReadAt + Sync>(
        source: &R,
    ) -> Result<(M::RootRef, Vec<AnyChunk<BODY_SIZE>>)> {
        let chunks = std::sync::Mutex::new(Vec::new());
        let root = Self::split_into(source, |chunk| chunks.lock().unwrap().push(chunk))?;
        Ok((root, chunks.into_inner().unwrap()))
    }

    #[allow(clippy::arithmetic_side_effects)] // i < data_chunks = ceil(size / BODY_SIZE), so offset = i * BODY_SIZE < size and size - offset cannot underflow
    fn create_data_chunks<R, F>(
        source: &R,
        tree: &TreeParams<BODY_SIZE>,
        sink: &F,
    ) -> Result<Vec<M::RefBytes>>
    where
        R: ReadAt + Sync,
        F: Fn(AnyChunk<BODY_SIZE>) + Sync,
    {
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
                    .map_err(|e| FileError::Store(Box::new(e)))?;

                let span = if i + 1 == data_chunks {
                    size - offset
                } else {
                    BODY_SIZE as u64
                };

                let chunk_bytes = super::helpers::build_intermediate_payload(span, &buf);

                let (chunk, ref_bytes) = M::prepare_chunk::<BODY_SIZE>(chunk_bytes)?;
                sink(chunk.into());
                Ok(ref_bytes)
            })
            .collect();

        results.into_iter().collect()
    }

    #[allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)] // refs starts non-empty (size > 0 produces >= 1 data chunk) and each level keeps >= 1 ref, so refs[0] exists; level increments at most log_branches(refs) < LEVEL_LIMIT times
    fn build_intermediate_levels<F>(
        mut refs: Vec<M::RefBytes>,
        total_size: u64,
        spans: &[u64; LEVEL_LIMIT],
        sink: &F,
    ) -> Result<M::RootRef>
    where
        F: Fn(AnyChunk<BODY_SIZE>) + Sync,
    {
        let mut level = 1;

        while refs.len() > 1 {
            refs = Self::build_level(&refs, level, total_size, spans, sink)?;
            level += 1;
        }

        // Extract root reference from the single remaining ref.
        M::extract_root(refs[0].as_ref())
    }

    #[allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)] // refs_per_chunk >= 1; level < LEVEL_LIMIT = spans.len() for any in-memory ref count; start = i * refs_per_chunk < refs.len() and end is clamped to refs.len(), so the slice holds and child_refs is non-empty
    fn build_level<F>(
        refs: &[M::RefBytes],
        level: usize,
        total_size: u64,
        spans: &[u64; LEVEL_LIMIT],
        sink: &F,
    ) -> Result<Vec<M::RefBytes>>
    where
        F: Fn(AnyChunk<BODY_SIZE>) + Sync,
    {
        let refs_per_chunk = M::refs_per_chunk(BODY_SIZE);
        let chunks_at_level = refs.len().div_ceil(refs_per_chunk);
        let max_span = spans[level] * BODY_SIZE as u64;

        let results: Vec<Result<M::RefBytes>> = (0..chunks_at_level)
            .into_par_iter()
            .map(|i| {
                let start = i * refs_per_chunk;
                let end = (start + refs_per_chunk).min(refs.len());
                let child_refs = &refs[start..end];

                // Single reference: carry up without wrapping (dangling chunk optimization).
                if child_refs.len() == 1 {
                    return Ok(child_refs[0].clone());
                }

                let span = if i + 1 == chunks_at_level {
                    total_size.saturating_sub(i as u64 * max_span)
                } else {
                    max_span
                };

                let ref_data: Vec<u8> = child_refs
                    .iter()
                    .flat_map(|r| r.as_ref())
                    .copied()
                    .collect();
                let chunk_bytes = super::helpers::build_intermediate_payload(span, &ref_data);

                let (chunk, ref_bytes) = M::prepare_chunk::<BODY_SIZE>(chunk_bytes)?;
                sink(chunk.into());
                Ok(ref_bytes)
            })
            .collect();

        results.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::join;
    use crate::store::MemoryStore;

    fn split_and_store(
        data: &[u8],
    ) -> (crate::chunk::ChunkAddress, MemoryStore<DEFAULT_BODY_SIZE>) {
        let (root, chunks) = ParallelSplitter::<DEFAULT_BODY_SIZE>::split_to_vec(&data).unwrap();
        (root, MemoryStore::from_chunks(chunks))
    }

    generate_plain_splitter_tests!(split_and_store);

    #[test]
    fn test_parallel_splitter_varying_data() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 5 + 123)
            .map(|i| (i % 256) as u8)
            .collect();

        let (root, store) = split_and_store(&data);

        let (seq_root, _) = crate::file::split::<DEFAULT_BODY_SIZE>(&data).unwrap();
        assert_eq!(root, seq_root);

        let recovered = futures::executor::block_on(join(&store, root)).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_split_into_lock_free_sink() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let data = vec![0xAB; DEFAULT_BODY_SIZE + 1];
        let count = AtomicUsize::new(0);
        let root = ParallelSplitter::<DEFAULT_BODY_SIZE>::split_into(&data.as_slice(), |_chunk| {
            count.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap();
        assert!(!root.is_zero());
        assert_eq!(count.load(Ordering::Relaxed), 3);
    }

    #[cfg(feature = "encryption")]
    mod encrypted {
        use super::*;
        use crate::file::{EncryptedParallelSplitter, split_encrypted};
        use crate::store::MemoryStore;

        fn encrypted_split_and_store(
            data: &[u8],
        ) -> (
            crate::chunk::encryption::EncryptedChunkRef,
            MemoryStore<DEFAULT_BODY_SIZE>,
        ) {
            let (root_ref, chunks) =
                EncryptedParallelSplitter::<DEFAULT_BODY_SIZE>::split_to_vec(&data).unwrap();
            (root_ref, MemoryStore::from_chunks(chunks))
        }

        generate_encrypted_splitter_tests!(encrypted_split_and_store);

        #[test]
        fn test_encrypted_parallel_matches_sequential() {
            let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 5 + 123)
                .map(|i| (i % 256) as u8)
                .collect();

            let (par_ref, par_store) = encrypted_split_and_store(&data);
            let (seq_ref, seq_store) = split_encrypted::<DEFAULT_BODY_SIZE>(&data).unwrap();

            assert_eq!(par_store.len(), seq_store.len());

            let par_recovered = futures::executor::block_on(join(&par_store, par_ref)).unwrap();
            assert_eq!(par_recovered, data);

            let seq_recovered = futures::executor::block_on(join(&seq_store, seq_ref)).unwrap();
            assert_eq!(seq_recovered, data);
        }

        #[test]
        fn test_encrypted_parallel_nondeterministic() {
            let data = b"test determinism";
            let (ref1, _) = encrypted_split_and_store(data);
            let (ref2, _) = encrypted_split_and_store(data);

            // Different random keys each time.
            assert_ne!(ref1.address(), ref2.address());
        }
    }
}

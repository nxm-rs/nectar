//! File splitter for producing BMT chunks from data streams.

use std::fmt;
use std::io::{self, Write};
use std::marker::PhantomData;

use crate::bmt::{DEFAULT_BODY_SIZE, SPAN_SIZE};

use super::constants::{LEVEL_LIMIT, compute_spans_inline};
use super::error::{FileError, Result};
use super::mode::{PlainMode, SplitMode};
use crate::store::SyncChunkPut;

#[cfg(feature = "encryption")]
use super::mode::EncryptedMode;

/// Generic splitter parameterized by chunk mode.
///
/// Uses a multi-level buffer to build the chunk tree:
/// - Level 0: Raw file data (up to BODY_SIZE bytes per chunk)
/// - Level 1+: References (M::REF_SIZE bytes each per chunk)
///
/// Chunks are emitted to the store as buffers fill. Call `finish()` to
/// finalize the tree and get the root reference.
pub struct GenericSyncSplitter<S, M: SplitMode, const BODY_SIZE: usize = DEFAULT_BODY_SIZE>
where
    S: SyncChunkPut<BODY_SIZE>,
{
    store: S,
    span_length: u64,
    length: u64,
    sum_counts: [usize; LEVEL_LIMIT],
    cursors: [usize; LEVEL_LIMIT],
    buffer: Vec<u8>,
    spans: [u64; LEVEL_LIMIT],
    _mode: PhantomData<M>,
}

/// Plain (unencrypted) file splitter.
pub type SyncSplitter<S, const BODY_SIZE: usize = DEFAULT_BODY_SIZE> =
    GenericSyncSplitter<S, PlainMode, BODY_SIZE>;

/// Encrypted file splitter.
#[cfg(feature = "encryption")]
pub type EncryptedSyncSplitter<S, const BODY_SIZE: usize = DEFAULT_BODY_SIZE> =
    GenericSyncSplitter<S, EncryptedMode, BODY_SIZE>;

impl<S, M, const BODY_SIZE: usize> fmt::Debug for GenericSyncSplitter<S, M, BODY_SIZE>
where
    S: SyncChunkPut<BODY_SIZE>,
    M: SplitMode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GenericSyncSplitter")
            .field("span_length", &self.span_length)
            .field("length", &self.length)
            .field("sum_counts", &self.sum_counts)
            .field("cursors", &self.cursors)
            .finish_non_exhaustive()
    }
}

impl<S, M, const BODY_SIZE: usize> GenericSyncSplitter<S, M, BODY_SIZE>
where
    S: SyncChunkPut<BODY_SIZE>,
    M: SplitMode,
{
    /// Create a splitter for data of known size.
    pub fn new(store: S, span_length: u64) -> Self {
        const { super::constants::assert_valid_body_size::<BODY_SIZE>() };

        // Double LEVEL_LIMIT for ping-pong buffering: one level builds while the prior flushes.
        let buffer_size = (BODY_SIZE + SPAN_SIZE) * LEVEL_LIMIT * 2;

        Self {
            store,
            span_length,
            length: 0,
            sum_counts: [0; LEVEL_LIMIT],
            cursors: [0; LEVEL_LIMIT],
            buffer: vec![0u8; buffer_size],
            spans: compute_spans_inline(BODY_SIZE / M::REF_SIZE),
            _mode: PhantomData,
        }
    }

    /// Bytes written so far.
    pub const fn len(&self) -> u64 {
        self.length
    }

    /// Whether any data has been written.
    pub const fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Declared span length.
    pub const fn span_length(&self) -> u64 {
        self.span_length
    }

    fn write_to_level(&mut self, level: usize, data: &[u8]) -> Result<()> {
        let start = self.cursors[level];
        let end = start + data.len();

        self.buffer[start..end].copy_from_slice(data);
        self.cursors[level] = end;

        let level_start = self.cursors[level + 1];
        if self.cursors[level] - level_start == BODY_SIZE {
            let reference = self.sum_level(level)?;
            self.write_to_level(level + 1, reference.as_ref())?;
            self.cursors[level] = self.cursors[level + 1];
        }

        Ok(())
    }

    fn sum_level(&mut self, level: usize) -> Result<M::RefBytes> {
        self.sum_counts[level] += 1;

        let max_span = self.spans[level] * BODY_SIZE as u64;
        let span = (self.length - 1) % max_span + 1;

        let level_start = self.cursors[level + 1];
        let level_end = self.cursors[level];
        let chunk_data = &self.buffer[level_start..level_end];

        let chunk_bytes = super::helpers::build_intermediate_payload(span, chunk_data);

        M::process_chunk::<BODY_SIZE, S>(chunk_bytes, &self.store)
    }

    fn hash_unfinished(&mut self) -> Result<()> {
        if !self.length.is_multiple_of(BODY_SIZE as u64) {
            let reference = self.sum_level(0)?;
            let next_cursor = self.cursors[1];
            self.buffer[next_cursor..next_cursor + M::REF_SIZE]
                .copy_from_slice(reference.as_ref());
            self.cursors[1] += M::REF_SIZE;
            self.cursors[0] = self.cursors[1];
        }
        Ok(())
    }

    fn move_dangling_chunk(&mut self) -> Result<()> {
        let target_level = M::levels(self.length, BODY_SIZE);

        for i in 1..target_level {
            let level_start = self.cursors[i + 1];
            let level_end = self.cursors[i];

            if level_end == level_start {
                continue;
            }

            let refs_at_level = (level_end - level_start) / M::REF_SIZE;

            // Single reference: carry up without wrapping (dangling chunk optimization)
            if refs_at_level == 1 {
                self.cursors[i + 1] = level_end;
                self.cursors[i] = level_end;
                continue;
            }

            let reference = self.sum_level(i)?;
            let next_cursor = self.cursors[i + 1];
            self.buffer[next_cursor..next_cursor + M::REF_SIZE]
                .copy_from_slice(reference.as_ref());
            self.cursors[i + 1] += M::REF_SIZE;
            self.cursors[i] = self.cursors[i + 1];
        }

        Ok(())
    }

    /// Finalize and return the root reference and store.
    pub fn finish(mut self) -> Result<(M::RootRef, S)> {
        if self.length != self.span_length {
            return Err(FileError::SpanMismatch {
                expected: self.span_length,
                actual: self.length,
            });
        }

        if self.length == 0 {
            let root = M::process_empty::<BODY_SIZE, S>(&self.store)?;
            return Ok((root, self.store));
        }

        self.hash_unfinished()?;
        self.move_dangling_chunk()?;

        let root = M::extract_root(&self.buffer)?;
        Ok((root, self.store))
    }

    fn write_chunk(&mut self, data: &[u8]) -> Result<()> {
        debug_assert!(data.len() <= BODY_SIZE);

        self.length += data.len() as u64;
        if self.length > self.span_length {
            return Err(FileError::WritePastSpan {
                span: self.span_length,
                written: self.length,
            });
        }

        self.write_to_level(0, data)
    }
}

impl<S, M, const BODY_SIZE: usize> Write for GenericSyncSplitter<S, M, BODY_SIZE>
where
    S: SyncChunkPut<BODY_SIZE>,
    M: SplitMode,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let mut written = 0;
        while written < buf.len() {
            let remaining = buf.len() - written;
            let chunk_space = BODY_SIZE - (self.cursors[0] - self.cursors[1]);
            let to_write = remaining.min(chunk_space);

            if to_write == 0 {
                break;
            }

            let data = &buf[written..written + to_write];
            self.write_chunk(data).map_err(io::Error::other)?;
            written += to_write;
        }

        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;

    use super::super::constants::REF_SIZE;

    const REFS_PER_CHUNK: usize = DEFAULT_BODY_SIZE / REF_SIZE;

    #[test]
    fn test_splitter_empty() {
        let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
        let splitter = SyncSplitter::new(store, 0);

        let (root, store) = splitter.finish().unwrap();
        assert_eq!(store.len(), 1);
        assert!(!root.is_zero());
    }

    #[test]
    fn test_splitter_small() {
        let data = b"hello world";
        let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
        let mut splitter = SyncSplitter::new(store, data.len() as u64);

        splitter.write_all(data).unwrap();
        let (root, store) = splitter.finish().unwrap();

        assert_eq!(store.len(), 1);
        assert!(!root.is_zero());
    }

    #[test]
    fn test_splitter_exact_chunk() {
        let data = vec![0xAB; DEFAULT_BODY_SIZE];
        let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
        let mut splitter = SyncSplitter::new(store, data.len() as u64);

        splitter.write_all(&data).unwrap();
        let (root, store) = splitter.finish().unwrap();

        assert_eq!(store.len(), 1);
        assert!(!root.is_zero());
    }

    #[test]
    fn test_splitter_two_chunks() {
        let data = vec![0xCD; DEFAULT_BODY_SIZE + 1];
        let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
        let mut splitter = SyncSplitter::new(store, data.len() as u64);

        splitter.write_all(&data).unwrap();
        let (root, store) = splitter.finish().unwrap();

        assert_eq!(store.len(), 3);
        assert!(!root.is_zero());
    }

    #[test]
    fn test_splitter_128_chunks_exact() {
        let mut data = vec![0u8; DEFAULT_BODY_SIZE * REFS_PER_CHUNK];
        rand::RngCore::fill_bytes(&mut rand::rng(), &mut data);
        let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
        let mut splitter = SyncSplitter::new(store, data.len() as u64);

        splitter.write_all(&data).unwrap();
        let (root, store) = splitter.finish().unwrap();

        // 128 data chunks + 1 intermediate (which is the root) = 129
        assert_eq!(store.len(), REFS_PER_CHUNK + 1);
        assert!(!root.is_zero());
    }

    #[test]
    fn test_splitter_129_chunks() {
        let mut data = vec![0u8; DEFAULT_BODY_SIZE * (REFS_PER_CHUNK + 1)];
        rand::RngCore::fill_bytes(&mut rand::rng(), &mut data);
        let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
        let mut splitter = SyncSplitter::new(store, data.len() as u64);

        splitter.write_all(&data).unwrap();
        let (root, store) = splitter.finish().unwrap();

        assert_eq!(store.len(), REFS_PER_CHUNK + 1 + 2);
        assert!(!root.is_zero());
    }

    #[test]
    fn test_splitter_incremental_writes() {
        let mut data = vec![0u8; DEFAULT_BODY_SIZE * 2 + 100];
        rand::RngCore::fill_bytes(&mut rand::rng(), &mut data);
        let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
        let mut splitter = SyncSplitter::new(store, data.len() as u64);

        for chunk in data.chunks(100) {
            splitter.write_all(chunk).unwrap();
        }
        let (root, store) = splitter.finish().unwrap();

        assert_eq!(store.len(), 4);
        assert!(!root.is_zero());
    }

    #[test]
    fn test_splitter_deterministic() {
        let data = vec![0x56; DEFAULT_BODY_SIZE * 3];

        let (root1, _) = {
            let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
            let mut splitter = SyncSplitter::new(store, data.len() as u64);
            splitter.write_all(&data).unwrap();
            splitter.finish().unwrap()
        };

        let (root2, _) = {
            let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
            let mut splitter = SyncSplitter::new(store, data.len() as u64);
            splitter.write_all(&data).unwrap();
            splitter.finish().unwrap()
        };

        assert_eq!(root1, root2);
    }

    #[test]
    fn test_splitter_write_past_span() {
        let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
        let mut splitter = SyncSplitter::new(store, 10);

        let result = splitter.write_all(b"this is more than 10 bytes");
        assert!(result.is_err());
    }

    #[test]
    fn test_splitter_span_mismatch() {
        let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
        let mut splitter = SyncSplitter::new(store, 100);

        splitter.write_all(b"short").unwrap();
        let result = splitter.finish();

        assert!(matches!(result, Err(FileError::SpanMismatch { .. })));
    }

    #[test]
    fn test_splitter_256_chunks_matches_parallel() {
        use crate::file::{sync_join, SyncParallelSplitter};
        use crate::store::MemoryStore;

        // 256 data chunks - this is the edge case that was causing hash mismatches
        let data = vec![0xAB; DEFAULT_BODY_SIZE * 256];

        // Sequential
        let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
        let mut splitter = SyncSplitter::new(store, data.len() as u64);
        splitter.write_all(&data).unwrap();
        let (seq_root, seq_store) = splitter.finish().unwrap();

        // Parallel
        let par_store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
        let par_splitter = SyncParallelSplitter::new(par_store);
        let par_root = par_splitter.split(&data).unwrap();
        let par_store = par_splitter.into_store();

        // Root hashes must match
        assert_eq!(seq_root, par_root, "Root hash mismatch between sequential and parallel");

        // Chunk counts must match (256 data + 2 intermediate + 1 root = 259)
        assert_eq!(seq_store.len(), par_store.len(), "Chunk count mismatch");

        // Verify round-trip works
        let recovered = sync_join(&seq_store, seq_root).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_splitter_128_chunks_matches_parallel() {
        use crate::file::{sync_join, SyncParallelSplitter};
        use crate::store::MemoryStore;

        // Exactly 128 data chunks - another edge case
        let data = vec![0xCD; DEFAULT_BODY_SIZE * REFS_PER_CHUNK];

        // Sequential
        let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
        let mut splitter = SyncSplitter::new(store, data.len() as u64);
        splitter.write_all(&data).unwrap();
        let (seq_root, seq_store) = splitter.finish().unwrap();

        // Parallel
        let par_store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
        let par_splitter = SyncParallelSplitter::new(par_store);
        let par_root = par_splitter.split(&data).unwrap();
        let par_store = par_splitter.into_store();

        assert_eq!(seq_root, par_root, "Root hash mismatch for 128 chunks");
        assert_eq!(seq_store.len(), par_store.len(), "Chunk count mismatch for 128 chunks");

        let recovered = sync_join(&seq_store, seq_root).unwrap();
        assert_eq!(recovered, data);
    }

    // Encrypted splitter tests
    #[cfg(feature = "encryption")]
    mod encrypted {
        use super::*;
        use super::super::super::constants::ENCRYPTED_REF_SIZE;

        const ENC_REFS_PER_CHUNK: usize = DEFAULT_BODY_SIZE / ENCRYPTED_REF_SIZE;

        #[test]
        fn test_encrypted_splitter_empty() {
            let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
            let splitter = EncryptedSyncSplitter::new(store, 0);

            let (_root_ref, store) = splitter.finish().unwrap();

            assert_eq!(store.len(), 1);
        }

        #[test]
        fn test_encrypted_splitter_small() {
            let data = b"hello world";
            let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
            let mut splitter = EncryptedSyncSplitter::new(store, data.len() as u64);

            splitter.write_all(data).unwrap();
            let (root_ref, store) = splitter.finish().unwrap();

            assert_eq!(Vec::from(&root_ref).len(), 64);
            assert_eq!(store.len(), 1);
        }

        #[test]
        fn test_encrypted_splitter_exact_chunk() {
            let data = vec![0xAB; DEFAULT_BODY_SIZE];
            let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
            let mut splitter = EncryptedSyncSplitter::new(store, data.len() as u64);

            splitter.write_all(&data).unwrap();
            let (_root_ref, store) = splitter.finish().unwrap();

            assert_eq!(store.len(), 1);
        }

        #[test]
        fn test_encrypted_splitter_two_chunks() {
            let data = vec![0xCD; DEFAULT_BODY_SIZE + 1];
            let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
            let mut splitter = EncryptedSyncSplitter::new(store, data.len() as u64);

            splitter.write_all(&data).unwrap();
            let (_root_ref, store) = splitter.finish().unwrap();

            // 2 data chunks + 1 intermediate = 3
            assert_eq!(store.len(), 3);
        }

        #[test]
        fn test_encrypted_splitter_64_chunks() {
            // 64 data chunks fills one encrypted intermediate chunk exactly
            let data = vec![0xEF; DEFAULT_BODY_SIZE * ENC_REFS_PER_CHUNK];
            let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
            let mut splitter = EncryptedSyncSplitter::new(store, data.len() as u64);

            splitter.write_all(&data).unwrap();
            let (_root_ref, store) = splitter.finish().unwrap();

            // 64 data chunks + 1 intermediate = 65
            assert_eq!(store.len(), ENC_REFS_PER_CHUNK + 1);
        }

        #[test]
        fn test_encrypted_splitter_65_chunks() {
            // 65 data chunks overflows one encrypted intermediate -> level 2
            let data = vec![0x12; DEFAULT_BODY_SIZE * (ENC_REFS_PER_CHUNK + 1)];
            let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
            let mut splitter = EncryptedSyncSplitter::new(store, data.len() as u64);

            splitter.write_all(&data).unwrap();
            let (_root_ref, store) = splitter.finish().unwrap();

            assert_eq!(store.len(), ENC_REFS_PER_CHUNK + 1 + 2);
        }

        #[test]
        fn test_encrypted_splitter_write_past_span() {
            let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
            let mut splitter = EncryptedSyncSplitter::<_, DEFAULT_BODY_SIZE>::new(store, 10);

            let result = splitter.write_all(b"this is more than 10 bytes");
            assert!(result.is_err());
        }

        #[test]
        fn test_encrypted_splitter_span_mismatch() {
            let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
            let mut splitter = EncryptedSyncSplitter::<_, DEFAULT_BODY_SIZE>::new(store, 100);

            splitter.write_all(b"short").unwrap();
            let result = splitter.finish();

            assert!(matches!(result, Err(FileError::SpanMismatch { .. })));
        }

        #[test]
        fn test_encrypted_differs_from_plaintext() {
            let data = b"test data for encryption comparison";
            let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
            let mut splitter = SyncSplitter::new(store, data.len() as u64);
            splitter.write_all(data).unwrap();
            let (plain_root, _) = splitter.finish().unwrap();

            let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
            let mut enc_splitter = EncryptedSyncSplitter::new(store, data.len() as u64);
            enc_splitter.write_all(data).unwrap();
            let (enc_root, _) = enc_splitter.finish().unwrap();

            assert_ne!(enc_root.address(), &plain_root);
        }
    }
}

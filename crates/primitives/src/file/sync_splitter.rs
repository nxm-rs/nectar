//! File splitter for producing BMT chunks from data streams.
//!
//! Buffers data via `Write`, then delegates to `GenericSyncParallelSplitter`
//! on `finish()` for parallel chunk hashing.

use std::fmt;
use std::io::{self, Write};
use std::marker::PhantomData;

use crate::bmt::DEFAULT_BODY_SIZE;

use super::error::{FileError, Result};
use super::mode::{PlainMode, SplitMode};
use super::sync_splitter_parallel::GenericSyncParallelSplitter;
use crate::store::SyncChunkPut;

#[cfg(feature = "encryption")]
use super::mode::EncryptedMode;

/// Generic splitter parameterized by chunk mode.
///
/// Buffers data written via `Write` and delegates to `GenericSyncParallelSplitter`
/// on `finish()` for parallel chunk hashing.
pub struct GenericSyncSplitter<S, M: SplitMode, const BODY_SIZE: usize = DEFAULT_BODY_SIZE>
where
    S: SyncChunkPut<BODY_SIZE>,
{
    store: S,
    span_length: u64,
    buffer: Vec<u8>,
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
            .field("length", &self.buffer.len())
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

        Self {
            store,
            span_length,
            buffer: Vec::with_capacity(span_length.min(BODY_SIZE as u64 * 2) as usize),
            _mode: PhantomData,
        }
    }

    /// Bytes written so far.
    pub fn len(&self) -> u64 {
        self.buffer.len() as u64
    }

    /// Whether any data has been written.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Declared span length.
    pub const fn span_length(&self) -> u64 {
        self.span_length
    }
}

impl<S, M, const BODY_SIZE: usize> GenericSyncSplitter<S, M, BODY_SIZE>
where
    S: SyncChunkPut<BODY_SIZE> + Send + Sync,
    M: SplitMode + Send + Sync,
{
    /// Finalize and return the root reference and store.
    pub fn finish(self) -> Result<(M::RootRef, S)> {
        if self.buffer.len() as u64 != self.span_length {
            return Err(FileError::SpanMismatch {
                expected: self.span_length,
                actual: self.buffer.len() as u64,
            });
        }

        if self.buffer.is_empty() {
            let root = M::process_empty::<BODY_SIZE, S>(&self.store)?;
            return Ok((root, self.store));
        }

        let parallel = GenericSyncParallelSplitter::<S, M, BODY_SIZE>::new(self.store);
        let root = parallel.split(&self.buffer)?;
        let store = parallel.into_store();
        Ok((root, store))
    }
}

impl<S, M, const BODY_SIZE: usize> Write for GenericSyncSplitter<S, M, BODY_SIZE>
where
    S: SyncChunkPut<BODY_SIZE>,
    M: SplitMode,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let remaining = self.span_length.saturating_sub(self.buffer.len() as u64) as usize;
        let to_write = buf.len().min(remaining);
        if to_write == 0 && !buf.is_empty() {
            return Err(io::Error::other(
                FileError::WritePastSpan {
                    span: self.span_length,
                    written: self.span_length + 1,
                }
                .to_string(),
            ));
        }
        self.buffer.extend_from_slice(&buf[..to_write]);
        Ok(to_write)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;

    fn split_and_store(
        data: &[u8],
    ) -> (crate::chunk::ChunkAddress, MemoryStore<DEFAULT_BODY_SIZE>) {
        let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
        let mut splitter = SyncSplitter::new(store, data.len() as u64);
        splitter.write_all(data).unwrap();
        splitter.finish().unwrap()
    }

    generate_plain_splitter_tests!(split_and_store);

    #[test]
    fn test_splitter_incremental_writes() {
        let mut data = vec![0u8; DEFAULT_BODY_SIZE * 2 + 100];
        rand::RngExt::fill(&mut rand::rng(), &mut data);
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

    #[cfg(feature = "encryption")]
    mod encrypted {
        use super::*;

        fn encrypted_split_and_store(
            data: &[u8],
        ) -> (
            crate::chunk::encryption::EncryptedChunkRef,
            MemoryStore<DEFAULT_BODY_SIZE>,
        ) {
            let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
            let mut splitter = EncryptedSyncSplitter::new(store, data.len() as u64);
            splitter.write_all(data).unwrap();
            splitter.finish().unwrap()
        }

        generate_encrypted_splitter_tests!(encrypted_split_and_store);

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

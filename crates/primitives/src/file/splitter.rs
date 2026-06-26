//! File splitter for producing BMT chunks from data streams.
//!
//! Buffers data via `Write`, then delegates to `GenericParallelSplitter`
//! on `finish()` for parallel chunk hashing. Produces chunks; the caller
//! decides where they go.

use std::fmt;
use std::io::{self, Write};
use std::marker::PhantomData;

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::AnyChunk;

use super::error::{FileError, Result};
use super::mode::{PlainMode, SplitMode};
use super::splitter_parallel::GenericParallelSplitter;

#[cfg(feature = "encryption")]
use super::mode::EncryptedMode;

/// Generic splitter parameterized by chunk mode.
///
/// Buffers data written via `Write` and delegates to `GenericParallelSplitter`
/// on `finish()` for parallel chunk hashing.
pub struct GenericSplitter<M: SplitMode, const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    span_length: u64,
    buffer: Vec<u8>,
    _mode: PhantomData<M>,
}

/// Plain (unencrypted) file splitter.
pub type Splitter<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> =
    GenericSplitter<PlainMode, BODY_SIZE>;

/// Encrypted file splitter.
#[cfg(feature = "encryption")]
pub type EncryptedSplitter<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> =
    GenericSplitter<EncryptedMode, BODY_SIZE>;

impl<M, const BODY_SIZE: usize> fmt::Debug for GenericSplitter<M, BODY_SIZE>
where
    M: SplitMode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GenericSplitter")
            .field("span_length", &self.span_length)
            .field("length", &self.buffer.len())
            .finish_non_exhaustive()
    }
}

impl<M, const BODY_SIZE: usize> GenericSplitter<M, BODY_SIZE>
where
    M: SplitMode,
{
    /// Create a splitter for data of known size.
    pub fn new(span_length: u64) -> Self {
        const { super::constants::assert_valid_body_size::<BODY_SIZE>() };

        Self {
            span_length,
            buffer: Vec::with_capacity(span_length.min(BODY_SIZE as u64 * 2) as usize),
            _mode: PhantomData,
        }
    }

    /// Bytes written so far.
    pub const fn len(&self) -> u64 {
        self.buffer.len() as u64
    }

    /// Whether any data has been written.
    pub const fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Declared span length.
    pub const fn span_length(&self) -> u64 {
        self.span_length
    }
}

impl<M, const BODY_SIZE: usize> GenericSplitter<M, BODY_SIZE>
where
    M: SplitMode + Send + Sync,
{
    /// Finalize, returning the root reference and the produced chunks.
    pub fn finish(self) -> Result<(M::RootRef, Vec<AnyChunk<BODY_SIZE>>)> {
        if self.buffer.len() as u64 != self.span_length {
            return Err(FileError::SpanMismatch {
                expected: self.span_length,
                actual: self.buffer.len() as u64,
            });
        }

        if self.buffer.is_empty() {
            let (chunk, root) = M::empty_chunk::<BODY_SIZE>()?;
            return Ok((root, vec![chunk.into()]));
        }

        GenericParallelSplitter::<M, BODY_SIZE>::split_to_vec(&self.buffer)
    }
}

impl<M, const BODY_SIZE: usize> Write for GenericSplitter<M, BODY_SIZE>
where
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
        let mut splitter = Splitter::<DEFAULT_BODY_SIZE>::new(data.len() as u64);
        splitter.write_all(data).unwrap();
        let (root, chunks) = splitter.finish().unwrap();
        (root, MemoryStore::from_chunks(chunks))
    }

    generate_plain_splitter_tests!(split_and_store);

    #[test]
    fn test_splitter_incremental_writes() {
        let mut data = vec![0u8; DEFAULT_BODY_SIZE * 2 + 100];
        rand::RngExt::fill(&mut rand::rng(), &mut data);
        let mut splitter = Splitter::<DEFAULT_BODY_SIZE>::new(data.len() as u64);

        for chunk in data.chunks(100) {
            splitter.write_all(chunk).unwrap();
        }
        let (root, chunks) = splitter.finish().unwrap();
        let store = MemoryStore::from_chunks(chunks);

        assert_eq!(store.len(), 4);
        assert!(!root.is_zero());
    }

    #[test]
    fn test_splitter_deterministic() {
        let data = vec![0x56; DEFAULT_BODY_SIZE * 3];

        let root1 = {
            let mut splitter = Splitter::<DEFAULT_BODY_SIZE>::new(data.len() as u64);
            splitter.write_all(&data).unwrap();
            splitter.finish().unwrap().0
        };

        let root2 = {
            let mut splitter = Splitter::<DEFAULT_BODY_SIZE>::new(data.len() as u64);
            splitter.write_all(&data).unwrap();
            splitter.finish().unwrap().0
        };

        assert_eq!(root1, root2);
    }

    #[test]
    fn test_splitter_write_past_span() {
        let mut splitter = Splitter::<DEFAULT_BODY_SIZE>::new(10);

        let result = splitter.write_all(b"this is more than 10 bytes");
        assert!(result.is_err());
    }

    #[test]
    fn test_splitter_span_mismatch() {
        let mut splitter = Splitter::<DEFAULT_BODY_SIZE>::new(100);

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
            let mut splitter = EncryptedSplitter::<DEFAULT_BODY_SIZE>::new(data.len() as u64);
            splitter.write_all(data).unwrap();
            let (root_ref, chunks) = splitter.finish().unwrap();
            (root_ref, MemoryStore::from_chunks(chunks))
        }

        generate_encrypted_splitter_tests!(encrypted_split_and_store);

        #[test]
        fn test_encrypted_splitter_write_past_span() {
            let mut splitter = EncryptedSplitter::<DEFAULT_BODY_SIZE>::new(10);

            let result = splitter.write_all(b"this is more than 10 bytes");
            assert!(result.is_err());
        }

        #[test]
        fn test_encrypted_splitter_span_mismatch() {
            let mut splitter = EncryptedSplitter::<DEFAULT_BODY_SIZE>::new(100);

            splitter.write_all(b"short").unwrap();
            let result = splitter.finish();

            assert!(matches!(result, Err(FileError::SpanMismatch { .. })));
        }

        #[test]
        fn test_encrypted_differs_from_plaintext() {
            let data = b"test data for encryption comparison";
            let mut splitter = Splitter::<DEFAULT_BODY_SIZE>::new(data.len() as u64);
            splitter.write_all(data).unwrap();
            let (plain_root, _) = splitter.finish().unwrap();

            let mut enc_splitter = EncryptedSplitter::<DEFAULT_BODY_SIZE>::new(data.len() as u64);
            enc_splitter.write_all(data).unwrap();
            let (enc_root, _) = enc_splitter.finish().unwrap();

            assert_ne!(enc_root.address(), &plain_root);
        }
    }
}

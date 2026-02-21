//! Fluent builder API for split operations.

use std::io::Read;
use std::marker::PhantomData;

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::ChunkAddress;

use super::error::{FileError, Result};
use super::read_at::ReadAt;
use super::splitter::Splitter;
use super::splitter_parallel::ParallelSplitter;
use crate::store::ChunkPut;

#[cfg(feature = "encryption")]
use crate::chunk::encryption::EncryptedChunkRef;
#[cfg(feature = "encryption")]
use super::splitter::EncryptedSplitter;
#[cfg(feature = "encryption")]
use super::splitter_parallel::EncryptedParallelSplitter;

mod sealed {
    pub trait Sealed {}
}

/// Build mode for split operations (sealed — cannot be implemented externally).
pub trait BuildMode: sealed::Sealed {
    /// Root reference type returned by split operations.
    type RootRef: std::fmt::Debug + Send;

    #[doc(hidden)]
    fn __split_reader<S: ChunkPut<BS>, R: Read, const BS: usize>(
        sink: S,
        size: u64,
        reader: R,
    ) -> Result<(Self::RootRef, S)>;

    #[doc(hidden)]
    fn __split_parallel<S: ChunkPut<BS> + Send, R: ReadAt + Sync, const BS: usize>(
        sink: S,
        source: &R,
    ) -> Result<(Self::RootRef, S)>;
}

/// Plain (unencrypted) build mode.
#[derive(Debug, Clone, Copy)]
pub struct Plain;

impl sealed::Sealed for Plain {}

impl BuildMode for Plain {
    type RootRef = ChunkAddress;

    fn __split_reader<S: ChunkPut<BS>, R: Read, const BS: usize>(
        sink: S,
        size: u64,
        mut reader: R,
    ) -> Result<(ChunkAddress, S)> {
        let mut splitter = Splitter::<S, BS>::new(sink, size);
        std::io::copy(&mut reader, &mut splitter).map_err(|e| FileError::Sink(Box::new(e)))?;
        splitter.finish()
    }

    fn __split_parallel<S: ChunkPut<BS> + Send, R: ReadAt + Sync, const BS: usize>(
        sink: S,
        source: &R,
    ) -> Result<(ChunkAddress, S)> {
        let splitter = ParallelSplitter::<S, BS>::new(sink);
        let root = splitter.split(source)?;
        Ok((root, splitter.into_sink()))
    }
}

/// Encrypted build mode.
#[cfg(feature = "encryption")]
#[derive(Debug, Clone, Copy)]
pub struct Encrypted;

#[cfg(feature = "encryption")]
impl sealed::Sealed for Encrypted {}

#[cfg(feature = "encryption")]
impl BuildMode for Encrypted {
    type RootRef = EncryptedChunkRef;

    fn __split_reader<S: ChunkPut<BS>, R: Read, const BS: usize>(
        sink: S,
        size: u64,
        mut reader: R,
    ) -> Result<(EncryptedChunkRef, S)> {
        let mut splitter = EncryptedSplitter::<S, BS>::new(sink, size);
        std::io::copy(&mut reader, &mut splitter).map_err(|e| FileError::Sink(Box::new(e)))?;
        splitter.finish()
    }

    fn __split_parallel<S: ChunkPut<BS> + Send, R: ReadAt + Sync, const BS: usize>(
        sink: S,
        source: &R,
    ) -> Result<(EncryptedChunkRef, S)> {
        let splitter = EncryptedParallelSplitter::<S, BS>::new(sink);
        let root_ref = splitter.split(source)?;
        Ok((root_ref, splitter.into_sink()))
    }
}

/// Builder for configuring split operations.
#[derive(Debug)]
pub struct SplitBuilder<S, M: BuildMode = Plain, const BODY_SIZE: usize = DEFAULT_BODY_SIZE>
where
    S: ChunkPut<BODY_SIZE>,
{
    sink: S,
    size: Option<u64>,
    parallel: bool,
    _mode: PhantomData<M>,
}

/// Encrypted split builder (convenience alias).
#[cfg(feature = "encryption")]
pub type EncryptedSplitBuilder<S, const BODY_SIZE: usize = DEFAULT_BODY_SIZE> =
    SplitBuilder<S, Encrypted, BODY_SIZE>;

// Plain-mode constructor and encrypted() transition.
impl<S, const BODY_SIZE: usize> SplitBuilder<S, Plain, BODY_SIZE>
where
    S: ChunkPut<BODY_SIZE>,
{
    /// Create a new split builder with the given sink.
    pub fn new(sink: S) -> Self {
        Self {
            sink,
            size: None,
            parallel: false,
            _mode: PhantomData,
        }
    }

    /// Switch to encrypted mode.
    #[cfg(feature = "encryption")]
    pub fn encrypted(self) -> SplitBuilder<S, Encrypted, BODY_SIZE> {
        SplitBuilder {
            sink: self.sink,
            size: self.size,
            parallel: self.parallel,
            _mode: PhantomData,
        }
    }
}

// Common methods for any build mode.
impl<S, M, const BODY_SIZE: usize> SplitBuilder<S, M, BODY_SIZE>
where
    S: ChunkPut<BODY_SIZE>,
    M: BuildMode,
{
    /// Set the expected data size for validation.
    pub fn with_size(mut self, size: u64) -> Self {
        self.size = Some(size);
        self
    }

    /// Enable parallel splitting (requires `ReadAt` source).
    pub fn parallel(mut self) -> Self {
        self.parallel = true;
        self
    }

    /// Split from a reader with known size.
    pub fn split_reader<R: Read>(self, reader: R) -> Result<(M::RootRef, S)> {
        let size = self.size.expect("size must be set for reader-based split");
        M::__split_reader::<S, R, BODY_SIZE>(self.sink, size, reader)
    }
}

// Methods requiring S: Send (parallel fallback reads source into buffer).
impl<S, M, const BODY_SIZE: usize> SplitBuilder<S, M, BODY_SIZE>
where
    S: ChunkPut<BODY_SIZE> + Send,
    M: BuildMode,
{
    /// Split from a byte slice.
    pub fn split_bytes(self, data: &[u8]) -> Result<(M::RootRef, S)> {
        self.split(&data)
    }

    /// Split from a random-access source (uses parallel if enabled).
    pub fn split<R: ReadAt + Sync>(self, source: &R) -> Result<(M::RootRef, S)> {
        if self.parallel {
            M::__split_parallel::<S, R, BODY_SIZE>(self.sink, source)
        } else {
            let size = source.len();
            let mut buf = vec![0u8; size as usize];
            source
                .read_at(0, &mut buf)
                .map_err(|e| FileError::Sink(Box::new(e)))?;
            M::__split_reader::<S, &[u8], BODY_SIZE>(self.sink, size, buf.as_slice())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::join;
    use crate::store::{MemorySink, VecSink};

    #[test]
    fn test_builder_split_bytes() {
        let data = b"hello world";
        let sink = VecSink::<DEFAULT_BODY_SIZE>::new();

        let (root, sink) = SplitBuilder::new(sink).split_bytes(data).unwrap();

        assert_eq!(sink.len(), 1);
        assert!(!root.is_zero());
    }

    #[test]
    fn test_builder_split_bytes_parallel() {
        let data = vec![0xAB; DEFAULT_BODY_SIZE * 3];
        let sink = MemorySink::<DEFAULT_BODY_SIZE>::new();

        let (root, sink) = SplitBuilder::new(sink)
            .parallel()
            .split_bytes(&data)
            .unwrap();

        let recovered = join(&sink, root).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_builder_split_reader() {
        let data = b"test data from reader";
        let sink = VecSink::<DEFAULT_BODY_SIZE>::new();

        let (root, _) = SplitBuilder::new(sink)
            .with_size(data.len() as u64)
            .split_reader(data.as_slice())
            .unwrap();

        assert!(!root.is_zero());
    }

    #[test]
    fn test_builder_split() {
        let data = vec![0xCD; DEFAULT_BODY_SIZE + 100];
        let sink = MemorySink::<DEFAULT_BODY_SIZE>::new();

        let (root, sink) = SplitBuilder::new(sink)
            .split(&data.as_slice())
            .unwrap();

        let recovered = join(&sink, root).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_builder_split_parallel() {
        let data = vec![0xEF; DEFAULT_BODY_SIZE * 5];
        let sink = MemorySink::<DEFAULT_BODY_SIZE>::new();

        let (root, sink) = SplitBuilder::new(sink)
            .parallel()
            .split(&data.as_slice())
            .unwrap();

        let recovered = join(&sink, root).unwrap();
        assert_eq!(recovered, data);
    }

    #[cfg(feature = "encryption")]
    mod encrypted {
        use super::*;
        use crate::file::join_encrypted;

        #[test]
        fn test_builder_encrypted_split_bytes() {
            let data = b"hello encrypted world";
            let sink = VecSink::<DEFAULT_BODY_SIZE>::new();

            let (root_ref, sink) = SplitBuilder::new(sink)
                .encrypted()
                .split_bytes(data)
                .unwrap();

            assert_eq!(root_ref.to_vec().len(), 64);
            assert_eq!(sink.len(), 1);
        }

        #[test]
        fn test_builder_encrypted_split_parallel() {
            let data = vec![0xAB; DEFAULT_BODY_SIZE * 3];
            let sink = MemorySink::<DEFAULT_BODY_SIZE>::new();

            let (root_ref, sink) = SplitBuilder::new(sink)
                .parallel()
                .encrypted()
                .split_bytes(&data)
                .unwrap();

            let recovered = join_encrypted(&sink, root_ref).unwrap();
            assert_eq!(recovered, data);
        }

        #[test]
        fn test_builder_encrypted_split_reader() {
            let data = b"encrypted reader data";
            let sink = VecSink::<DEFAULT_BODY_SIZE>::new();

            let (root_ref, _) = SplitBuilder::new(sink)
                .with_size(data.len() as u64)
                .encrypted()
                .split_reader(data.as_slice())
                .unwrap();

            assert_eq!(root_ref.to_vec().len(), 64);
        }

        #[test]
        fn test_builder_encrypted_roundtrip() {
            let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 5 + 123)
                .map(|i| (i % 256) as u8)
                .collect();
            let sink = MemorySink::<DEFAULT_BODY_SIZE>::new();

            let (root_ref, sink) = SplitBuilder::new(sink)
                .encrypted()
                .split_bytes(&data)
                .unwrap();

            let recovered = join_encrypted(&sink, root_ref).unwrap();
            assert_eq!(recovered, data);
        }
    }
}

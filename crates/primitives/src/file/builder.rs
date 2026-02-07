//! Fluent builder API for split operations.

use std::io::Read;

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::ChunkAddress;

use super::error::Result;
use super::read_at::ReadAt;
use super::splitter::Splitter;
use super::splitter_parallel::ParallelSplitter;
use super::traits::ChunkPut;

/// Builder for configuring split operations.
#[derive(Debug)]
pub struct SplitBuilder<S, const BODY_SIZE: usize = DEFAULT_BODY_SIZE>
where
    S: ChunkPut<BODY_SIZE>,
{
    sink: S,
    size: Option<u64>,
    parallel: bool,
}

impl<S, const BODY_SIZE: usize> SplitBuilder<S, BODY_SIZE>
where
    S: ChunkPut<BODY_SIZE>,
{
    /// Create a new split builder with the given sink.
    pub fn new(sink: S) -> Self {
        Self {
            sink,
            size: None,
            parallel: false,
        }
    }

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
}

impl<S, const BODY_SIZE: usize> SplitBuilder<S, BODY_SIZE>
where
    S: ChunkPut<BODY_SIZE>,
{
    /// Split from a reader with known size.
    pub fn from_reader<R: Read>(self, reader: R) -> Result<(ChunkAddress, S)> {
        let size = self.size.expect("size must be set for reader-based split");
        self.from_reader_with_size(reader, size)
    }

    fn from_reader_with_size<R: Read>(self, mut reader: R, size: u64) -> Result<(ChunkAddress, S)> {
        let mut splitter = Splitter::new(self.sink, size);
        std::io::copy(&mut reader, &mut splitter)
            .map_err(|e| super::error::FileError::Sink(Box::new(e)))?;
        splitter.finish()
    }
}

impl<S, const BODY_SIZE: usize> SplitBuilder<S, BODY_SIZE>
where
    S: ChunkPut<BODY_SIZE> + Send,
{
    /// Split from a byte slice.
    pub fn from_bytes(self, data: &[u8]) -> Result<(ChunkAddress, S)> {
        self.from_source(&data)
    }

    /// Split from a random-access source (uses parallel if enabled).
    pub fn from_source<R: ReadAt + Sync>(self, source: &R) -> Result<(ChunkAddress, S)> {
        if self.parallel {
            let splitter = ParallelSplitter::new(self.sink);
            let root = splitter.split(source)?;
            Ok((root, splitter.into_sink()))
        } else {
            // Fall back to sequential
            let size = source.len();
            let mut buf = vec![0u8; size as usize];
            source
                .read_at(0, &mut buf)
                .map_err(|e| super::error::FileError::Sink(Box::new(e)))?;
            self.from_reader_with_size(buf.as_slice(), size)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::{join, MemorySink, VecSink};

    #[test]
    fn test_builder_from_bytes() {
        let data = b"hello world";
        let sink = VecSink::<DEFAULT_BODY_SIZE>::new();

        let (root, sink) = SplitBuilder::new(sink).from_bytes(data).unwrap();

        assert_eq!(sink.len(), 1);
        assert!(!root.is_zero());
    }

    #[test]
    fn test_builder_from_bytes_parallel() {
        let data = vec![0xAB; DEFAULT_BODY_SIZE * 3];
        let sink = MemorySink::<DEFAULT_BODY_SIZE>::new();

        let (root, sink) = SplitBuilder::new(sink)
            .parallel()
            .from_bytes(&data)
            .unwrap();

        let recovered = join(&sink, root).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_builder_from_reader() {
        let data = b"test data from reader";
        let sink = VecSink::<DEFAULT_BODY_SIZE>::new();

        let (root, _) = SplitBuilder::new(sink)
            .with_size(data.len() as u64)
            .from_reader(data.as_slice())
            .unwrap();

        assert!(!root.is_zero());
    }

    #[test]
    fn test_builder_from_source() {
        let data = vec![0xCD; DEFAULT_BODY_SIZE + 100];
        let sink = MemorySink::<DEFAULT_BODY_SIZE>::new();

        let (root, sink) = SplitBuilder::new(sink)
            .from_source(&data.as_slice())
            .unwrap();

        let recovered = join(&sink, root).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_builder_from_source_parallel() {
        let data = vec![0xEF; DEFAULT_BODY_SIZE * 5];
        let sink = MemorySink::<DEFAULT_BODY_SIZE>::new();

        let (root, sink) = SplitBuilder::new(sink)
            .parallel()
            .from_source(&data.as_slice())
            .unwrap();

        let recovered = join(&sink, root).unwrap();
        assert_eq!(recovered, data);
    }
}

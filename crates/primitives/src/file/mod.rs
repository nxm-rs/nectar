//! File splitting and joining for arbitrary-size data.
//!
//! This module provides streaming file operations using BMT chunks:
//! - [`Splitter`]: Splits data into chunks, producing intermediate chunks as needed
//! - [`Joiner`]: Reconstructs data from a root chunk address
//!
//! # Example
//!
//! ```
//! use nectar_primitives::file::{split, join, MemorySink};
//! use nectar_primitives::{Chunk, DEFAULT_BODY_SIZE};
//!
//! let data = b"Hello, Swarm!";
//! let (root, chunks) = split::<DEFAULT_BODY_SIZE>(data).unwrap();
//!
//! // Reconstruct from chunks
//! use std::collections::HashMap;
//! let store: HashMap<_, _> = chunks.iter().map(|c| (*c.address(), c.clone())).collect();
//! let recovered = join(&store, root).unwrap();
//! assert_eq!(recovered, data);
//! ```

mod builder;
mod constants;
pub mod error;
mod joiner;
#[cfg(feature = "async")]
mod joiner_async;
mod joiner_parallel;
mod read_at;
mod sink;
mod splitter;
mod splitter_parallel;
pub mod traits;
#[cfg(feature = "async")]
pub mod traits_async;
mod tree;

use std::collections::HashMap;
use std::io::{Read, Write};

use crate::chunk::{ChunkAddress, ContentChunk};

pub use builder::SplitBuilder;
pub use error::FileError;
pub use joiner::Joiner;
#[cfg(feature = "async")]
pub use joiner_async::AsyncJoiner;
pub use joiner_parallel::ParallelJoiner;
pub use read_at::ReadAt;
pub use sink::{MemorySink, VecSink};
pub use splitter::Splitter;
pub use splitter_parallel::ParallelSplitter;
pub use traits::{ChunkGet, ChunkHas, ChunkPut};
#[cfg(feature = "async")]
pub use traits_async::{AsyncChunkGet, AsyncChunkPut, AsyncReadAt};
pub use tree::{ChunkRange, TreeParams};
pub(crate) use tree::subspan_size;

// Extension traits are defined below, after all types are available

/// Split data into chunks, returning root address and chunk list.
pub fn split<const BODY_SIZE: usize>(
    data: &[u8],
) -> error::Result<(ChunkAddress, Vec<ContentChunk<BODY_SIZE>>)> {
    let sink = VecSink::<BODY_SIZE>::new();
    let mut splitter = Splitter::new(sink, data.len() as u64);
    splitter
        .write_all(data)
        .map_err(|e| FileError::Sink(Box::new(e)))?;
    let (root, sink) = splitter.finish()?;
    Ok((root, sink.into_chunks()))
}

/// Split data from a reader into chunks.
pub fn split_reader<R, S, const BODY_SIZE: usize>(
    mut reader: R,
    size: u64,
    sink: S,
) -> error::Result<(ChunkAddress, S)>
where
    R: Read,
    S: ChunkPut<BODY_SIZE>,
{
    let mut splitter = Splitter::new(sink, size);
    std::io::copy(&mut reader, &mut splitter).map_err(|e| FileError::Sink(Box::new(e)))?;
    splitter.finish()
}

/// Join chunks into a byte vector.
pub fn join<G, const BODY_SIZE: usize>(
    getter: G,
    root: ChunkAddress,
) -> error::Result<Vec<u8>>
where
    G: ChunkGet<BODY_SIZE>,
{
    let mut joiner = Joiner::new(getter, root)?;
    let mut data = vec![0u8; joiner.size() as usize];
    joiner
        .read_exact(&mut data)
        .map_err(|e| FileError::Getter(Box::new(e)))?;
    Ok(data)
}

impl<const BODY_SIZE: usize> ChunkGet<BODY_SIZE> for HashMap<ChunkAddress, ContentChunk<BODY_SIZE>> {
    type Error = FileError;

    fn get(&self, address: &ChunkAddress) -> Result<ContentChunk<BODY_SIZE>, Self::Error> {
        self.get(address)
            .cloned()
            .ok_or_else(|| FileError::ChunkNotFound(*address))
    }
}

impl<const BODY_SIZE: usize> ChunkGet<BODY_SIZE> for &HashMap<ChunkAddress, ContentChunk<BODY_SIZE>> {
    type Error = FileError;

    fn get(&self, address: &ChunkAddress) -> Result<ContentChunk<BODY_SIZE>, Self::Error> {
        HashMap::get(self, address)
            .cloned()
            .ok_or_else(|| FileError::ChunkNotFound(*address))
    }
}

impl<const BODY_SIZE: usize> ChunkHas<BODY_SIZE> for HashMap<ChunkAddress, ContentChunk<BODY_SIZE>> {
    fn has(&self, address: &ChunkAddress) -> bool {
        self.contains_key(address)
    }
}

impl<const BODY_SIZE: usize> ChunkHas<BODY_SIZE> for &HashMap<ChunkAddress, ContentChunk<BODY_SIZE>> {
    fn has(&self, address: &ChunkAddress) -> bool {
        self.contains_key(address)
    }
}

/// Calculate tree depth for a given file size.
pub(crate) fn levels(length: u64, chunk_size: usize) -> usize {
    use constants::REF_SIZE;

    if length == 0 {
        return 0;
    }

    let section_size = REF_SIZE as u64;
    let branches = (chunk_size / REF_SIZE) as u64;

    if length <= section_size * branches {
        return 1;
    }

    let chunks = (length - 1) / section_size;
    (chunks as f64).log(branches as f64) as usize + 1
}

/// Extension methods for chunk getters.
pub trait ChunkGetExt<const BODY_SIZE: usize>: ChunkGet<BODY_SIZE> {
    /// Create a joiner for reading file data.
    fn joiner(self, root: ChunkAddress) -> error::Result<Joiner<Self, BODY_SIZE>>
    where
        Self: Sized,
    {
        Joiner::new(self, root)
    }

    /// Read entire file into memory.
    fn read_file(self, root: ChunkAddress) -> error::Result<Vec<u8>>
    where
        Self: Sized,
    {
        join(self, root)
    }
}

impl<T, const BODY_SIZE: usize> ChunkGetExt<BODY_SIZE> for T where T: ChunkGet<BODY_SIZE> {}

/// Extension methods for chunk putters.
pub trait ChunkPutExt<const BODY_SIZE: usize>: ChunkPut<BODY_SIZE> + Sized {
    /// Create a splitter for writing file data.
    fn splitter(self, size: u64) -> Splitter<Self, BODY_SIZE> {
        Splitter::new(self, size)
    }
}

impl<T, const BODY_SIZE: usize> ChunkPutExt<BODY_SIZE> for T where T: ChunkPut<BODY_SIZE> {}

#[cfg(test)]
mod tests;

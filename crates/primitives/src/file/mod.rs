//! File splitting and joining for arbitrary-size data.
//!
//! This module provides streaming file operations using BMT chunks:
//! - [`Splitter`]: Splits data into chunks, producing intermediate chunks as needed
//! - [`Joiner`]: Reconstructs data from a root chunk address
//!
//! # Example
//!
//! ```
//! use nectar_primitives::file::{split, join};
//! use nectar_primitives::store::MemorySink;
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
pub(crate) mod mode;
mod read_at;
mod splitter;
mod splitter_parallel;
#[cfg(feature = "async")]
pub mod traits_async;
mod tree;

use std::collections::HashMap;
use std::io::{Read, Write};

use crate::chunk::{ChunkAddress, ContentChunk};
use crate::chunk::encryption::EncryptedChunkRef;
use crate::store::{ChunkGet, ChunkHas, ChunkPut};

pub use builder::SplitBuilder;
#[cfg(feature = "encryption")]
pub use builder::EncryptedSplitBuilder;
pub use error::FileError;
pub use joiner::{EncryptedJoiner, Joiner};
#[cfg(feature = "async")]
pub use joiner_async::AsyncJoiner;
#[cfg(all(feature = "async", feature = "encryption"))]
pub use joiner_async::EncryptedAsyncJoiner;
pub use joiner_parallel::ParallelJoiner;
#[cfg(feature = "encryption")]
pub use joiner_parallel::EncryptedParallelJoiner;
pub use read_at::ReadAt;
pub use splitter::Splitter;
#[cfg(feature = "encryption")]
pub use splitter::EncryptedSplitter;
pub use splitter_parallel::ParallelSplitter;
#[cfg(feature = "encryption")]
pub use splitter_parallel::EncryptedParallelSplitter;
#[cfg(feature = "async")]
pub use traits_async::AsyncReadAt;
pub use tree::{ChunkRange, TreeParams};

// Extension traits are defined below, after all types are available

/// Split data into chunks, returning root address and chunk list.
pub fn split<const BODY_SIZE: usize>(
    data: &[u8],
) -> error::Result<(ChunkAddress, Vec<ContentChunk<BODY_SIZE>>)> {
    let sink = crate::store::VecSink::<BODY_SIZE>::new();
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

/// Split data into encrypted chunks, returning root reference and chunk list.
#[cfg(feature = "encryption")]
pub fn split_encrypted<const BODY_SIZE: usize>(
    data: &[u8],
) -> error::Result<(EncryptedChunkRef, Vec<ContentChunk<BODY_SIZE>>)> {
    let sink = crate::store::VecSink::<BODY_SIZE>::new();
    let mut splitter = EncryptedSplitter::new(sink, data.len() as u64);
    splitter
        .write_all(data)
        .map_err(|e| FileError::Sink(Box::new(e)))?;
    let (root_ref, sink) = splitter.finish()?;
    Ok((root_ref, sink.into_chunks()))
}

/// Join encrypted chunks into a byte vector.
pub fn join_encrypted<G, const BODY_SIZE: usize>(
    getter: G,
    root_ref: EncryptedChunkRef,
) -> error::Result<Vec<u8>>
where
    G: ChunkGet<BODY_SIZE>,
{
    let mut joiner = EncryptedJoiner::new(getter, root_ref)?;
    let mut data = vec![0u8; joiner.size() as usize];
    joiner
        .read_exact(&mut data)
        .map_err(|e| FileError::Getter(Box::new(e)))?;
    Ok(data)
}

/// Split data into chunks using parallel processing.
pub fn split_parallel<const BODY_SIZE: usize>(
    data: &[u8],
) -> error::Result<(ChunkAddress, Vec<ContentChunk<BODY_SIZE>>)> {
    let sink = crate::store::VecSink::<BODY_SIZE>::new();
    let splitter = ParallelSplitter::new(sink);
    let root = splitter.split(&data)?;
    Ok((root, splitter.into_sink().into_chunks()))
}

/// Split data into encrypted chunks using parallel processing.
#[cfg(feature = "encryption")]
pub fn split_encrypted_parallel<const BODY_SIZE: usize>(
    data: &[u8],
) -> error::Result<(EncryptedChunkRef, Vec<ContentChunk<BODY_SIZE>>)> {
    let sink = crate::store::VecSink::<BODY_SIZE>::new();
    let splitter = EncryptedParallelSplitter::new(sink);
    let root_ref = splitter.split(&data)?;
    Ok((root_ref, splitter.into_sink().into_chunks()))
}

/// Join chunks into a byte vector using parallel chunk fetching.
pub fn join_parallel<G, const BODY_SIZE: usize>(
    getter: G,
    root: ChunkAddress,
) -> error::Result<Vec<u8>>
where
    G: ChunkGet<BODY_SIZE> + Clone + Send + Sync,
{
    let joiner = ParallelJoiner::new(getter, root)?;
    joiner.read_all()
}

/// Join encrypted chunks into a byte vector using parallel chunk fetching.
#[cfg(feature = "encryption")]
pub fn join_encrypted_parallel<G, const BODY_SIZE: usize>(
    getter: G,
    root_ref: EncryptedChunkRef,
) -> error::Result<Vec<u8>>
where
    G: ChunkGet<BODY_SIZE> + Clone + Send + Sync,
{
    let joiner = EncryptedParallelJoiner::new(getter, root_ref)?;
    joiner.read_all()
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

/// Calculate tree depth for a given file size (plain mode).
#[cfg(test)]
pub(crate) fn levels(length: u64, chunk_size: usize) -> usize {
    constants::tree_depth(length, chunk_size, constants::REF_SIZE)
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

    /// Create an encrypted joiner for reading encrypted file data.
    #[cfg(feature = "encryption")]
    fn encrypted_joiner(
        self,
        root_ref: EncryptedChunkRef,
    ) -> error::Result<EncryptedJoiner<Self, BODY_SIZE>>
    where
        Self: Sized,
    {
        EncryptedJoiner::new(self, root_ref)
    }

    /// Read entire encrypted file into memory.
    #[cfg(feature = "encryption")]
    fn read_encrypted_file(self, root_ref: EncryptedChunkRef) -> error::Result<Vec<u8>>
    where
        Self: Sized,
    {
        join_encrypted(self, root_ref)
    }
}

impl<T, const BODY_SIZE: usize> ChunkGetExt<BODY_SIZE> for T where T: ChunkGet<BODY_SIZE> {}

/// Extension methods for chunk putters.
pub trait ChunkPutExt<const BODY_SIZE: usize>: ChunkPut<BODY_SIZE> + Sized {
    /// Create a splitter for writing file data.
    fn splitter(self, size: u64) -> Splitter<Self, BODY_SIZE> {
        Splitter::new(self, size)
    }

    /// Create an encrypted splitter for writing encrypted file data.
    #[cfg(feature = "encryption")]
    fn encrypted_splitter(self, size: u64) -> EncryptedSplitter<Self, BODY_SIZE> {
        EncryptedSplitter::new(self, size)
    }
}

impl<T, const BODY_SIZE: usize> ChunkPutExt<BODY_SIZE> for T where T: ChunkPut<BODY_SIZE> {}

#[cfg(test)]
mod tests;

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

mod constants;
pub mod error;
mod joiner;
mod sink;
mod splitter;

use std::collections::HashMap;
use std::io::{Read, Write};

use crate::chunk::{ChunkAddress, ContentChunk};

pub use constants::{LEVEL_LIMIT, REFS_PER_CHUNK, REF_SIZE, SPANS, SPAN_SIZE, levels, span_for_level};
pub use error::FileError;
pub use joiner::{ChunkGetter, Joiner};
pub use sink::{ChunkSink, MemorySink, VecSink};
pub use splitter::Splitter;

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
    S: ChunkSink<BODY_SIZE>,
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
    G: ChunkGetter<BODY_SIZE>,
{
    let mut joiner = Joiner::new(getter, root)?;
    let mut data = vec![0u8; joiner.size() as usize];
    joiner
        .read_exact(&mut data)
        .map_err(|e| FileError::Getter(Box::new(e)))?;
    Ok(data)
}

impl<const BODY_SIZE: usize> ChunkGetter<BODY_SIZE> for HashMap<ChunkAddress, ContentChunk<BODY_SIZE>> {
    type Error = FileError;

    fn get(&self, address: &ChunkAddress) -> Result<ContentChunk<BODY_SIZE>, Self::Error> {
        self.get(address)
            .cloned()
            .ok_or_else(|| FileError::ChunkNotFound(*address))
    }
}

impl<const BODY_SIZE: usize> ChunkGetter<BODY_SIZE> for &HashMap<ChunkAddress, ContentChunk<BODY_SIZE>> {
    type Error = FileError;

    fn get(&self, address: &ChunkAddress) -> Result<ContentChunk<BODY_SIZE>, Self::Error> {
        HashMap::get(self, address)
            .cloned()
            .ok_or_else(|| FileError::ChunkNotFound(*address))
    }
}

#[cfg(test)]
mod tests;

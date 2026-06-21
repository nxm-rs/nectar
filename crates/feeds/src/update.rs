//! A single feed update.

use alloy_primitives::Address;
use bytes::Bytes;
use nectar_primitives::chunk::{Chunk, ChunkAddress, SingleOwnerChunk};

use crate::error::Result;
use crate::index::Index;

/// One published feed update: the index it occupies and the single-owner chunk
/// that carries it.
///
/// The payload is the SOC body data (the bytes the publisher signed over). The
/// address and owner are read straight off the chunk.
#[derive(Debug, Clone)]
pub struct FeedUpdate<I, const BS: usize> {
    index: I,
    soc: SingleOwnerChunk<BS>,
}

impl<I: Index, const BS: usize> FeedUpdate<I, BS> {
    /// Construct an update from its index and single-owner chunk.
    pub const fn new(index: I, soc: SingleOwnerChunk<BS>) -> Self {
        Self { index, soc }
    }

    /// The index this update occupies.
    pub const fn index(&self) -> &I {
        &self.index
    }

    /// The single-owner chunk carrying this update.
    pub const fn chunk(&self) -> &SingleOwnerChunk<BS> {
        &self.soc
    }

    /// The update payload (the SOC body data).
    pub fn payload(&self) -> &Bytes {
        self.soc.data()
    }

    /// The single-owner chunk address of this update.
    pub fn address(&self) -> &ChunkAddress {
        self.soc.address()
    }

    /// The owner recovered from the update signature.
    pub fn owner(&self) -> Result<Address> {
        self.soc
            .owner()
            .map_err(|e| crate::FeedError::Primitives(e.into()))
    }

    /// Consume the update, returning its index and single-owner chunk.
    pub fn into_parts(self) -> (I, SingleOwnerChunk<BS>) {
        (self.index, self.soc)
    }
}

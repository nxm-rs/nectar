//! Interpreter view of one published update.

use bytes::Bytes;
use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::{ChunkAddress, ChunkOps, ContentChunk, SingleOwnerChunk};

use crate::index::Index;

/// One published update: its index and the single-owner chunk carrying it.
///
/// Only [`Getter`](crate::Getter) and [`Updater`](crate::Updater) construct
/// one, so the chunk is always certified against the feed-derived address.
#[derive(Debug, Clone)]
pub struct FeedUpdate<I, const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    index: I,
    chunk: SingleOwnerChunk<BODY_SIZE>,
}

impl<I: Index, const BODY_SIZE: usize> FeedUpdate<I, BODY_SIZE> {
    pub(crate) const fn new(index: I, chunk: SingleOwnerChunk<BODY_SIZE>) -> Self {
        Self { index, chunk }
    }

    /// The index this update occupies.
    pub const fn index(&self) -> &I {
        &self.index
    }

    /// The single-owner chunk carrying this update.
    pub const fn chunk(&self) -> &SingleOwnerChunk<BODY_SIZE> {
        &self.chunk
    }

    /// The payload bytes the owner signed over.
    pub fn payload(&self) -> &Bytes {
        self.chunk.data()
    }

    /// The single-owner chunk address of this update.
    pub fn address(&self) -> &ChunkAddress {
        self.chunk.address()
    }

    /// The content-addressed chunk wrapped by this update's body.
    #[must_use]
    pub fn content(&self) -> ContentChunk<BODY_SIZE> {
        self.chunk.unwrap_cac()
    }

    /// Consume into the index and the single-owner chunk.
    pub fn into_parts(self) -> (I, SingleOwnerChunk<BODY_SIZE>) {
        (self.index, self.chunk)
    }
}

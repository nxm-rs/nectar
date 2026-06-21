//! Chunk storage traits and implementations.
//!
//! Async traits (`ChunkGet`, `ChunkPut`, `ChunkHas`) are the primary API.
//! Sync traits (`SyncChunkGet`, `SyncChunkPut`, `SyncChunkHas`) are helpers
//! for CPU-bound paths (splitter, mantaray). Blanket impls bridge sync → async
//! automatically for types that are `Send + Sync`.

mod maybe_send;
mod memory;
mod typed;

pub use maybe_send::{MaybeSend, MaybeSync};
pub use memory::MemoryStore;
pub use typed::{ChunkGet, ChunkHas, ChunkPut, SyncChunkGet, SyncChunkHas, SyncChunkPut};

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::{AnyChunk, ChunkAddress};

/// Errors from chunk storage operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChunkStoreError {
    /// Chunk not found at the given address.
    #[error("chunk not found: {address_hex}")]
    NotFound {
        /// Hex-encoded address of the missing chunk.
        address_hex: String,
    },
    /// Catch-all for backend-specific errors.
    #[error("{0}")]
    Other(String),
}

impl ChunkStoreError {
    /// Create a `NotFound` error for the given address.
    pub fn not_found(address: &ChunkAddress) -> Self {
        Self::NotFound {
            address_hex: format!("{address}"),
        }
    }

    /// Whether this is a genuine miss (`NotFound`) rather than a backend error.
    pub const fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound { .. })
    }
}

/// A no-op loader that always returns [`ChunkStoreError::NotFound`].
///
/// Used by `Node`'s public convenience methods to satisfy the generic
/// constraint without requiring callers to specify a store type.
#[derive(Debug)]
pub struct NullLoader<const BODY_SIZE: usize = DEFAULT_BODY_SIZE>;

impl<const BODY_SIZE: usize> SyncChunkGet<BODY_SIZE> for NullLoader<BODY_SIZE> {
    type Error = ChunkStoreError;

    fn get(&self, address: &ChunkAddress) -> Result<AnyChunk<BODY_SIZE>, Self::Error> {
        Err(ChunkStoreError::not_found(address))
    }

    fn is_not_found(&self, error: &Self::Error) -> bool {
        error.is_not_found()
    }
}

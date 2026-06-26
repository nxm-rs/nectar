//! Chunk storage traits and implementations.
//!
//! `ChunkGet`, `ChunkPut`, and `ChunkHas` are async and carry `MaybeSend`/
//! `MaybeSync` bounds so a store may be `!Send` on wasm.

mod maybe_send;
mod memory;
mod typed;

pub use maybe_send::{MaybeSend, MaybeSync};
pub use memory::MemoryStore;
pub use typed::{ChunkGet, ChunkHas, ChunkPut};

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
}

/// A no-op loader that always returns [`ChunkStoreError::NotFound`].
///
/// Used by `Node`'s public convenience methods to satisfy the generic
/// constraint without requiring callers to specify a store type.
#[derive(Debug)]
pub struct NullLoader<const BODY_SIZE: usize = DEFAULT_BODY_SIZE>;

impl<const BODY_SIZE: usize> ChunkGet<BODY_SIZE> for NullLoader<BODY_SIZE> {
    type Error = ChunkStoreError;

    async fn get(&self, address: &ChunkAddress) -> Result<AnyChunk<BODY_SIZE>, Self::Error> {
        Err(ChunkStoreError::not_found(address))
    }
}

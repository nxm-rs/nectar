//! Chunk storage traits and implementations.

mod sink;
mod typed;
#[cfg(feature = "async")]
mod typed_async;

pub use sink::{MemorySink, VecSink};
pub use typed::{ChunkGet, ChunkHas, ChunkPut};
#[cfg(feature = "async")]
pub use typed_async::{AsyncChunkGet, AsyncChunkPut};

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

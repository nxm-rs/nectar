//! Chunk storage traits and implementations.
//!
//! `ChunkGet`, `ChunkPut`, and `ChunkHas` are async and carry `MaybeSend`/
//! `MaybeSync` bounds so a store may be `!Send` on single-threaded targets
//! (wasm32, or any target under the `unsync` feature).

mod maybe_send;
mod memory;
mod retry;
mod typed;

pub use maybe_send::{MaybeSend, MaybeSync};
pub use memory::MemoryStore;
pub use retry::{RetryConfig, RetryingChunkGet, Sleeper};
pub use typed::{ChunkGet, ChunkHas, ChunkPut, TrustedGet};

use crate::chunk::{Chunk, ChunkAddress, ChunkRegistry, Verified};

/// Boxed store error: `Send + Sync` on multi-threaded targets, unbounded on
/// wasm32 and under the `unsync` feature where a backend error may hold
/// single-thread state (a JS handle).
#[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
pub type BoxedError = Box<dyn core::error::Error + Send + Sync>;
/// Boxed store error: `Send + Sync` on multi-threaded targets, unbounded on
/// wasm32 and under the `unsync` feature where a backend error may hold
/// single-thread state (a JS handle).
#[cfg(any(target_arch = "wasm32", feature = "unsync"))]
pub type BoxedError = Box<dyn core::error::Error>;

/// Shared store error: `Send + Sync` on multi-threaded targets, unbounded on
/// wasm32 and under the `unsync` feature where a backend error may hold
/// single-thread state (a JS handle).
#[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
pub type SharedError = std::sync::Arc<dyn core::error::Error + Send + Sync>;
/// Shared store error: `Send + Sync` on multi-threaded targets, unbounded on
/// wasm32 and under the `unsync` feature where a backend error may hold
/// single-thread state (a JS handle).
#[cfg(any(target_arch = "wasm32", feature = "unsync"))]
pub type SharedError = std::sync::Arc<dyn core::error::Error>;

/// Errors from chunk storage operations.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum ChunkStoreError {
    /// Chunk not found at the given address.
    #[error("chunk not found: {0}")]
    NotFound(ChunkAddress),
    /// Catch-all for backend-specific errors.
    #[error("{0}")]
    Other(#[source] BoxedError),
}

impl ChunkStoreError {
    /// Create a `NotFound` error for the given address.
    pub const fn not_found(address: &ChunkAddress) -> Self {
        Self::NotFound(*address)
    }
}

/// A no-op loader that always returns [`ChunkStoreError::NotFound`].
///
/// Used by `Node`'s public convenience methods to satisfy the generic
/// constraint without requiring callers to specify a store type. It yields
/// nothing, so its `Verified` trust declaration is vacuously true.
#[derive(Debug)]
pub struct NullLoader;

impl<R: ChunkRegistry> ChunkGet<R> for NullLoader {
    type Trust = Verified;
    type Error = ChunkStoreError;

    async fn get(&self, address: &ChunkAddress) -> Result<Chunk<Verified, R>, Self::Error> {
        Err(ChunkStoreError::not_found(address))
    }
}

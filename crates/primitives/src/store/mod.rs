//! Chunk storage traits and implementations.
//!
//! `ChunkGet` and `ChunkPut` are async and carry `MaybeSend`/`MaybeSync`
//! bounds so a store may be `!Send` on single-threaded targets (wasm32, bare
//! metal, or any target under the `unsync` feature). The synchronous seam
//! [`ChunkGetSync`] answers the same stampless address read without a
//! future. The seam error is classified through `StoreError`, so a definite
//! miss never reads as a failure and vice versa.

mod content;
mod memory;
#[cfg(feature = "std")]
mod retry;
mod tee;
mod typed;
mod verify;

pub use crate::marker::{MaybeSend, MaybeSync};
pub use content::{ContentGet, ContentGetError};
pub use memory::MemoryStore;
#[cfg(feature = "std")]
pub use nectar_tasks::Sleeper;
#[cfg(feature = "std")]
pub use retry::{RetryConfig, RetryingChunkGet};
pub use tee::{Tee, TeeError};
pub use typed::{ChunkGet, ChunkGetSync, ChunkPut, PutUnit, TrustedGet};
pub use verify::{VerifyError, VerifyingStore};

use crate::chunk::{Chunk, ChunkAddress, ChunkRegistry, Verified};

// The store error family and its classification are defined in the core
// crate because `PrimitivesError` wraps them; the stores themselves are
// here.
pub use nectar_primitives_core::error::{BoxedError, ChunkStoreError, SharedError, StoreError};

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

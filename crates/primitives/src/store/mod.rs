//! Chunk storage traits and implementations.
//!
//! `ChunkGet`, `ChunkPut`, and `ChunkHas` are async and carry `MaybeSend`/
//! `MaybeSync` bounds so a store may be `!Send` on single-threaded targets
//! (wasm32, bare metal, or any target under the `unsync` feature).

mod content;
mod memory;
#[cfg(feature = "std")]
mod retry;
mod single_owner;
mod tee;
mod typed;
mod verify;

pub use crate::marker::{MaybeSend, MaybeSync};
pub use content::{ContentGet, ContentGetError};
pub use memory::MemoryStore;
#[cfg(feature = "std")]
pub use retry::{RetryConfig, RetryingChunkGet, Sleeper};
pub use single_owner::{SingleOwnerGet, SingleOwnerGetError};
pub use tee::{Tee, TeeError};
pub use typed::{ChunkGet, ChunkHas, ChunkPut, PutUnit, TrustedGet};
pub use verify::{VerifyError, VerifyingStore};

use crate::chunk::{Chunk, ChunkAddress, ChunkRegistry, Verified};

// The store error and its boxed aliases are defined in the core crate because
// `PrimitivesError` wraps them; the stores themselves are here.
pub use nectar_primitives_core::error::{BoxedError, ChunkStoreError, SharedError};

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

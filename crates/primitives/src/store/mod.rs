//! Chunk storage traits and implementations.

mod mock;
mod raw;
#[cfg(feature = "async")]
mod raw_async;
mod sink;
mod typed;
#[cfg(feature = "async")]
mod typed_async;

pub use mock::MockChunkStore;
pub use raw::{ChunkGetter, ChunkPutter, ChunkStore, ChunkStoreError};
#[cfg(feature = "async")]
pub use raw_async::{AsyncChunkGetter, AsyncChunkPutter, AsyncChunkStore};
pub use sink::{MemorySink, VecSink};
pub use typed::{ChunkGet, ChunkHas, ChunkPut};
#[cfg(feature = "async")]
pub use typed_async::{AsyncChunkGet, AsyncChunkPut};

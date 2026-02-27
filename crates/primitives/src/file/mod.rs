//! File splitting and joining for arbitrary-size data.
//!
//! Standard traits: `Joiner` implements `Read + Seek`, `Splitter` implements `Write`.
//!
//! # Store-centric API (extension traits)
//!
//! ```
//! use nectar_primitives::file::{ChunkGetExt, ChunkPutExt};
//! use nectar_primitives::store::MemoryStore;
//! use nectar_primitives::DEFAULT_BODY_SIZE;
//!
//! let mut store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
//! let addr = store.write_file(b"hello swarm").unwrap();
//! let data = store.read_file(addr).unwrap();
//! assert_eq!(data, b"hello swarm");
//! ```
//!
//! # Data-centric API (free functions)
//!
//! ```
//! use nectar_primitives::file::{split_source, ChunkGetExt};
//! use nectar_primitives::DEFAULT_BODY_SIZE;
//!
//! let data = b"Hello, Swarm!";
//! let (root, store) = split_source::<_, DEFAULT_BODY_SIZE>(data.as_slice()).unwrap();
//! let recovered = store.read_file(root).unwrap();
//! assert_eq!(recovered, data);
//! ```
//!
//! # Free function API (custom `BODY_SIZE`)
//!
//! ```
//! use nectar_primitives::file::{split, join};
//! use nectar_primitives::DEFAULT_BODY_SIZE;
//!
//! let data = b"Hello, Swarm!";
//! let (root, store) = split::<DEFAULT_BODY_SIZE>(data).unwrap();
//! let recovered = join(&store, root).unwrap();
//! assert_eq!(recovered, data);
//! ```
//!
//! # Encrypted split and join
//!
//! ```
//! # #[cfg(feature = "encryption")] {
//! use nectar_primitives::file::{split_encrypted, join};
//! use nectar_primitives::DEFAULT_BODY_SIZE;
//!
//! let data = b"secret data";
//! let (root_ref, store) = split_encrypted::<DEFAULT_BODY_SIZE>(data).unwrap();
//! let recovered = join(&store, root_ref).unwrap();
//! assert_eq!(recovered, data);
//! # }
//! ```

mod constants;
pub mod entry_ref;
pub mod error;
mod frontier;
mod helpers;
#[cfg(test)]
#[macro_use]
mod joiner_tests;
mod joiner;
#[cfg(feature = "async")]
mod joiner_async;
pub mod mode;
mod read_at;
mod splitter;
mod splitter_parallel;
#[cfg(feature = "async")]
pub mod traits_async;
mod tree;

use std::io::Write;

use crate::chunk::ChunkAddress;
#[cfg(feature = "encryption")]
use crate::chunk::encryption::EncryptedChunkRef;
use crate::store::{ChunkGet, ChunkPut};

pub use entry_ref::EntryRef;
pub use error::FileError;
pub use joiner::{GenericJoiner, Joiner};
#[cfg(feature = "encryption")]
pub use joiner::EncryptedJoiner;
#[cfg(feature = "async")]
pub use joiner_async::{AsyncJoiner, AsyncJoinerReader, GenericAsyncJoiner};
#[cfg(all(feature = "async", feature = "encryption"))]
pub use joiner_async::EncryptedAsyncJoiner;
pub use read_at::ReadAt;
pub use splitter::Splitter;
#[cfg(feature = "encryption")]
pub use splitter::EncryptedSplitter;
pub use splitter_parallel::ParallelSplitter;
#[cfg(feature = "encryption")]
pub use splitter_parallel::EncryptedParallelSplitter;
#[cfg(feature = "async")]
pub use traits_async::AsyncReadAt;
pub use tree::{ChunkRange, TreeParams};

// --- JoinRef: sealed trait mapping reference types to join modes ---

mod join_ref_sealed {
    pub trait Sealed {}
    impl Sealed for crate::ChunkAddress {}
    #[cfg(feature = "encryption")]
    impl Sealed for crate::EncryptedChunkRef {}
}

/// Maps a reference type to its join mode.
/// Sealed — implemented for `ChunkAddress` and `EncryptedChunkRef`.
pub trait JoinRef: join_ref_sealed::Sealed + Clone + Send + Sync + 'static {
    /// The join mode associated with this reference type.
    type Mode: mode::JoinMode + Send + Sync;

    /// Convert into the root reference expected by the joiner.
    fn into_root_ref(self) -> <Self::Mode as mode::JoinMode>::RootRef;
}

impl JoinRef for ChunkAddress {
    type Mode = mode::PlainMode;

    fn into_root_ref(self) -> Self {
        self
    }
}

#[cfg(feature = "encryption")]
impl JoinRef for EncryptedChunkRef {
    type Mode = mode::EncryptedMode;

    fn into_root_ref(self) -> Self {
        self
    }
}

// --- Seek helper ---

/// Resolve a `SeekFrom` position to an absolute byte offset.
pub(crate) fn resolve_seek_position(
    pos: std::io::SeekFrom,
    current: u64,
    span: u64,
) -> std::io::Result<u64> {
    let new_pos = match pos {
        std::io::SeekFrom::Start(offset) => i64::try_from(offset).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "seek offset exceeds i64::MAX",
            )
        })?,
        std::io::SeekFrom::End(offset) => {
            let span_i64 = i64::try_from(span).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "file span exceeds i64::MAX",
                )
            })?;
            span_i64 + offset
        }
        std::io::SeekFrom::Current(offset) => {
            let current_i64 = i64::try_from(current).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "current position exceeds i64::MAX",
                )
            })?;
            current_i64 + offset
        }
    };

    if new_pos < 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "seek to negative position",
        ));
    }

    Ok(new_pos as u64)
}

// --- Free functions ---

/// Split data into chunks, returning root address and chunk store.
pub fn split<const BODY_SIZE: usize>(
    data: &[u8],
) -> error::Result<(ChunkAddress, crate::store::MemoryStore<BODY_SIZE>)> {
    let store = crate::store::MemoryStore::<BODY_SIZE>::new();
    let mut splitter = Splitter::new(store, data.len() as u64);
    splitter
        .write_all(data)
        .map_err(|e| FileError::Store(Box::new(e)))?;
    let (root, store) = splitter.finish()?;
    Ok((root, store))
}

/// Split data from a reader into chunks.
pub fn split_reader<R, S, const BODY_SIZE: usize>(
    mut reader: R,
    size: u64,
    store: S,
) -> error::Result<(ChunkAddress, S)>
where
    R: std::io::Read,
    S: ChunkPut<BODY_SIZE>,
{
    let mut splitter = Splitter::new(store, size);
    std::io::copy(&mut reader, &mut splitter).map_err(|e| FileError::Store(Box::new(e)))?;
    splitter.finish()
}

/// Split data into encrypted chunks, returning root reference and chunk store.
#[cfg(feature = "encryption")]
pub fn split_encrypted<const BODY_SIZE: usize>(
    data: &[u8],
) -> error::Result<(EncryptedChunkRef, crate::store::MemoryStore<BODY_SIZE>)> {
    let store = crate::store::MemoryStore::<BODY_SIZE>::new();
    let mut splitter = EncryptedSplitter::new(store, data.len() as u64);
    splitter
        .write_all(data)
        .map_err(|e| FileError::Store(Box::new(e)))?;
    let (root_ref, store) = splitter.finish()?;
    Ok((root_ref, store))
}

/// Join chunks into a byte vector. Dispatches plain/encrypted via [`JoinRef`].
pub fn join<R, G, const BODY_SIZE: usize>(getter: G, root: R) -> error::Result<Vec<u8>>
where
    R: JoinRef,
    G: ChunkGet<BODY_SIZE> + Clone + Send + Sync,
{
    GenericJoiner::<G, R::Mode, BODY_SIZE>::new(getter, root.into_root_ref())?.read_all()
}

/// Join chunks asynchronously. Dispatches plain/encrypted via [`JoinRef`].
#[cfg(feature = "async")]
pub async fn join_async<R, G, const BODY_SIZE: usize>(
    getter: G,
    root: R,
) -> error::Result<Vec<u8>>
where
    R: JoinRef,
    G: crate::store::AsyncChunkGet<BODY_SIZE>,
{
    GenericAsyncJoiner::<G, R::Mode, BODY_SIZE>::new(getter, root.into_root_ref())
        .await?
        .read_all()
        .await
}

/// Calculate tree depth for a given file size (plain mode).
#[cfg(test)]
pub(crate) fn levels(length: u64, chunk_size: usize) -> usize {
    constants::tree_depth(length, chunk_size, constants::REF_SIZE)
}

// --- Extension traits ---

/// Extension methods for chunk getters.
///
/// Automatically implemented for all types that implement [`ChunkGet`].
/// Uses [`JoinRef`] for unified plain/encrypted dispatch.
///
/// ```
/// use nectar_primitives::file::{ChunkPutExt, ChunkGetExt};
/// use nectar_primitives::store::MemoryStore;
/// use nectar_primitives::DEFAULT_BODY_SIZE;
///
/// let mut store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
/// let addr = store.write_file(b"hello swarm").unwrap();
/// let recovered = store.read_file(addr).unwrap();
/// assert_eq!(recovered, b"hello swarm");
/// ```
pub trait ChunkGetExt<const BODY_SIZE: usize>: ChunkGet<BODY_SIZE> {
    /// Open a file for reading. Returns a joiner implementing `Read + Seek`.
    fn joiner<R: JoinRef>(
        self,
        root: R,
    ) -> error::Result<GenericJoiner<Self, R::Mode, BODY_SIZE>>
    where
        Self: Clone + Send + Sync + Sized,
    {
        GenericJoiner::new(self, root.into_root_ref())
    }

    /// Read entire file into memory (like `fs::read`).
    fn read_file<R: JoinRef>(self, root: R) -> error::Result<Vec<u8>>
    where
        Self: Clone + Send + Sync + Sized,
    {
        join(self, root)
    }
}

impl<T, const BODY_SIZE: usize> ChunkGetExt<BODY_SIZE> for T where T: ChunkGet<BODY_SIZE> {}

/// Extension methods for async chunk getters.
///
/// Uses [`JoinRef`] for unified plain/encrypted dispatch.
#[cfg(feature = "async")]
pub trait AsyncChunkGetExt<const BODY_SIZE: usize>: crate::store::AsyncChunkGet<BODY_SIZE> {
    /// Open a file for async reading.
    fn async_joiner<R: JoinRef>(
        self,
        root: R,
    ) -> impl std::future::Future<Output = error::Result<GenericAsyncJoiner<Self, R::Mode, BODY_SIZE>>> + Send
    where
        Self: Sized + Clone + Send + Sync + 'static,
    {
        GenericAsyncJoiner::new(self, root.into_root_ref())
    }

    /// Read entire file into memory asynchronously.
    fn read_file_async<R: JoinRef>(
        self,
        root: R,
    ) -> impl std::future::Future<Output = error::Result<Vec<u8>>> + Send
    where
        Self: Sized + Clone + Send + Sync + 'static,
    {
        join_async(self, root)
    }
}

#[cfg(feature = "async")]
impl<T, const BODY_SIZE: usize> AsyncChunkGetExt<BODY_SIZE> for T where
    T: crate::store::AsyncChunkGet<BODY_SIZE>
{
}

/// Extension methods for chunk putters.
///
/// Automatically implemented for all types that implement [`ChunkPut`].
///
/// ```
/// use nectar_primitives::file::{ChunkPutExt, ChunkGetExt};
/// use nectar_primitives::store::MemoryStore;
/// use nectar_primitives::DEFAULT_BODY_SIZE;
/// use std::io::Write;
///
/// // Filesystem-style write/read
/// let mut store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
/// let addr = store.write_file(b"hello").unwrap();
///
/// // Streaming write via std::io::Write
/// let mut writer = store.writer(6);
/// writer.write_all(b"world!").unwrap();
/// let (root, _) = writer.finish().unwrap();
/// ```
pub trait ChunkPutExt<const BODY_SIZE: usize>: ChunkPut<BODY_SIZE> {
    /// Create a writer for streaming data. Returns a `Splitter` implementing `Write`.
    /// Call `.finish()` on the returned writer to get the root address.
    fn writer(&mut self, size: u64) -> Splitter<&mut Self, BODY_SIZE>
    where
        Self: Sized,
    {
        Splitter::new(self, size)
    }

    /// Create an encrypted writer. Returns an `EncryptedSplitter` implementing `Write`.
    #[cfg(feature = "encryption")]
    fn encrypted_writer(&mut self, size: u64) -> EncryptedSplitter<&mut Self, BODY_SIZE>
    where
        Self: Sized,
    {
        EncryptedSplitter::new(self, size)
    }

    /// Write file data into the store (like `fs::write`).
    fn write_file(&mut self, data: &[u8]) -> error::Result<ChunkAddress>
    where
        Self: Send + Sized,
    {
        ParallelSplitter::<&mut Self, BODY_SIZE>::new(self).split(&data)
    }

    /// Write encrypted file data into the store.
    #[cfg(feature = "encryption")]
    fn write_encrypted_file(&mut self, data: &[u8]) -> error::Result<EncryptedChunkRef>
    where
        Self: Send + Sized,
    {
        EncryptedParallelSplitter::<&mut Self, BODY_SIZE>::new(self).split(&data)
    }
}

impl<T, const BODY_SIZE: usize> ChunkPutExt<BODY_SIZE> for T where T: ChunkPut<BODY_SIZE> {}

/// Split a `ReadAt` source into chunks stored in a [`MemoryStore`](crate::store::MemoryStore).
///
/// ```
/// use nectar_primitives::file::{split_source, ChunkGetExt};
/// use nectar_primitives::store::MemoryStore;
/// use nectar_primitives::DEFAULT_BODY_SIZE;
///
/// let data = b"Hello, Swarm!";
/// let (root, store) = split_source::<_, DEFAULT_BODY_SIZE>(data.as_slice()).unwrap();
/// let recovered = store.read_file(root).unwrap();
/// assert_eq!(recovered, data);
/// ```
pub fn split_source<R: ReadAt + Sync, const BODY_SIZE: usize>(
    source: R,
) -> error::Result<(ChunkAddress, crate::store::MemoryStore<BODY_SIZE>)> {
    let store = crate::store::MemoryStore::<BODY_SIZE>::new();
    let splitter = ParallelSplitter::new(store);
    let root = splitter.split(&source)?;
    Ok((root, splitter.into_store()))
}

/// Split a `ReadAt` source into chunks stored in the provided store.
pub fn split_source_into<R: ReadAt + Sync, S: ChunkPut<BODY_SIZE> + Send, const BODY_SIZE: usize>(
    source: R,
    store: S,
) -> error::Result<(ChunkAddress, S)> {
    let splitter = ParallelSplitter::new(store);
    let root = splitter.split(&source)?;
    Ok((root, splitter.into_store()))
}

/// Split a `ReadAt` source into encrypted chunks stored in a [`MemoryStore`](crate::store::MemoryStore).
#[cfg(feature = "encryption")]
pub fn split_source_encrypted<R: ReadAt + Sync, const BODY_SIZE: usize>(
    source: R,
) -> error::Result<(EncryptedChunkRef, crate::store::MemoryStore<BODY_SIZE>)> {
    let store = crate::store::MemoryStore::<BODY_SIZE>::new();
    let splitter = EncryptedParallelSplitter::new(store);
    let root_ref = splitter.split(&source)?;
    Ok((root_ref, splitter.into_store()))
}

/// Split a `ReadAt` source into encrypted chunks stored in the provided store.
#[cfg(feature = "encryption")]
pub fn split_source_encrypted_into<R: ReadAt + Sync, S: ChunkPut<BODY_SIZE> + Send, const BODY_SIZE: usize>(
    source: R,
    store: S,
) -> error::Result<(EncryptedChunkRef, S)> {
    let splitter = EncryptedParallelSplitter::new(store);
    let root_ref = splitter.split(&source)?;
    Ok((root_ref, splitter.into_store()))
}

#[cfg(test)]
mod tests;

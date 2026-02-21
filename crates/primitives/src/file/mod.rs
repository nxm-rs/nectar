//! File splitting and joining for arbitrary-size data.
//!
//! Standard traits: `Joiner` implements `Read + Seek`, `Splitter` implements `Write`.
//!
//! # Store-centric API (extension traits)
//!
//! ```
//! use nectar_primitives::file::{ChunkGetExt, ChunkPutExt};
//! use nectar_primitives::store::MemorySink;
//! use nectar_primitives::DEFAULT_BODY_SIZE;
//!
//! let mut store = MemorySink::<DEFAULT_BODY_SIZE>::new();
//! let addr = store.write_file(b"hello swarm").unwrap();
//! let data = store.read_file(addr).unwrap();
//! assert_eq!(data, b"hello swarm");
//! ```
//!
//! # Data-centric API (SplitExt)
//!
//! ```
//! use nectar_primitives::file::{SplitExt, ChunkGetExt};
//!
//! let data = b"Hello, Swarm!";
//! let (root, store) = data.as_slice().split_and_store().unwrap();
//! let recovered = store.read_file(root).unwrap();
//! assert_eq!(recovered, data);
//! ```
//!
//! # Free function API (custom `BODY_SIZE`)
//!
//! ```
//! use nectar_primitives::file::{split, join};
//! use nectar_primitives::{Chunk, DEFAULT_BODY_SIZE};
//!
//! let data = b"Hello, Swarm!";
//! let (root, chunks) = split::<DEFAULT_BODY_SIZE>(data).unwrap();
//!
//! use std::collections::HashMap;
//! let store: HashMap<_, _> = chunks.iter().map(|c| (*c.address(), c.clone())).collect();
//! let recovered = join(&store, root).unwrap();
//! assert_eq!(recovered, data);
//! ```
//!
//! # Encrypted split and join
//!
//! ```
//! # #[cfg(feature = "encryption")] {
//! use nectar_primitives::file::{split_encrypted, join};
//! use nectar_primitives::{Chunk, DEFAULT_BODY_SIZE};
//! use std::collections::HashMap;
//!
//! let data = b"secret data";
//! let (root_ref, chunks) = split_encrypted::<DEFAULT_BODY_SIZE>(data).unwrap();
//!
//! let store: HashMap<_, _> = chunks.iter().map(|c| (*c.address(), c.clone())).collect();
//! let recovered = join(&store, root_ref).unwrap();
//! assert_eq!(recovered, data);
//! # }
//! ```

mod constants;
pub mod error;
mod frontier;
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

use std::collections::HashMap;
use std::io::Write;

use crate::chunk::{ChunkAddress, ContentChunk};
#[cfg(feature = "encryption")]
use crate::chunk::encryption::EncryptedChunkRef;
use crate::store::{ChunkGet, ChunkHas, ChunkPut};

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

    fn into_root_ref(self) -> ChunkAddress {
        self
    }
}

#[cfg(feature = "encryption")]
impl JoinRef for EncryptedChunkRef {
    type Mode = mode::EncryptedMode;

    fn into_root_ref(self) -> EncryptedChunkRef {
        self
    }
}

// --- Free functions ---

/// Split data into chunks, returning root address and chunk list.
pub fn split<const BODY_SIZE: usize>(
    data: &[u8],
) -> error::Result<(ChunkAddress, Vec<ContentChunk<BODY_SIZE>>)> {
    let sink = crate::store::VecSink::<BODY_SIZE>::new();
    let mut splitter = Splitter::new(sink, data.len() as u64);
    splitter
        .write_all(data)
        .map_err(|e| FileError::Sink(Box::new(e)))?;
    let (root, sink) = splitter.finish()?;
    Ok((root, sink.into_chunks()))
}

/// Split data from a reader into chunks.
pub fn split_reader<R, S, const BODY_SIZE: usize>(
    mut reader: R,
    size: u64,
    sink: S,
) -> error::Result<(ChunkAddress, S)>
where
    R: std::io::Read,
    S: ChunkPut<BODY_SIZE>,
{
    let mut splitter = Splitter::new(sink, size);
    std::io::copy(&mut reader, &mut splitter).map_err(|e| FileError::Sink(Box::new(e)))?;
    splitter.finish()
}

/// Split data into encrypted chunks, returning root reference and chunk list.
#[cfg(feature = "encryption")]
pub fn split_encrypted<const BODY_SIZE: usize>(
    data: &[u8],
) -> error::Result<(EncryptedChunkRef, Vec<ContentChunk<BODY_SIZE>>)> {
    let sink = crate::store::VecSink::<BODY_SIZE>::new();
    let mut splitter = EncryptedSplitter::new(sink, data.len() as u64);
    splitter
        .write_all(data)
        .map_err(|e| FileError::Sink(Box::new(e)))?;
    let (root_ref, sink) = splitter.finish()?;
    Ok((root_ref, sink.into_chunks()))
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

// --- ChunkGet / ChunkHas impls for HashMap ---

impl<const BODY_SIZE: usize> ChunkGet<BODY_SIZE> for HashMap<ChunkAddress, ContentChunk<BODY_SIZE>> {
    type Error = FileError;

    fn get(&self, address: &ChunkAddress) -> Result<ContentChunk<BODY_SIZE>, Self::Error> {
        self.get(address)
            .cloned()
            .ok_or_else(|| FileError::ChunkNotFound(*address))
    }
}

impl<const BODY_SIZE: usize> ChunkGet<BODY_SIZE> for &HashMap<ChunkAddress, ContentChunk<BODY_SIZE>> {
    type Error = FileError;

    fn get(&self, address: &ChunkAddress) -> Result<ContentChunk<BODY_SIZE>, Self::Error> {
        HashMap::get(self, address)
            .cloned()
            .ok_or_else(|| FileError::ChunkNotFound(*address))
    }
}

impl<const BODY_SIZE: usize> ChunkHas<BODY_SIZE> for HashMap<ChunkAddress, ContentChunk<BODY_SIZE>> {
    fn has(&self, address: &ChunkAddress) -> bool {
        self.contains_key(address)
    }
}

impl<const BODY_SIZE: usize> ChunkHas<BODY_SIZE> for &HashMap<ChunkAddress, ContentChunk<BODY_SIZE>> {
    fn has(&self, address: &ChunkAddress) -> bool {
        self.contains_key(address)
    }
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
/// use nectar_primitives::store::MemorySink;
/// use nectar_primitives::DEFAULT_BODY_SIZE;
///
/// let mut store = MemorySink::<DEFAULT_BODY_SIZE>::new();
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
/// use nectar_primitives::store::MemorySink;
/// use nectar_primitives::DEFAULT_BODY_SIZE;
/// use std::io::Write;
///
/// // Filesystem-style write/read
/// let mut store = MemorySink::<DEFAULT_BODY_SIZE>::new();
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

/// Extension methods for splitting data sources into chunks.
///
/// Automatically implemented for all `ReadAt + Sync` types:
/// `&[u8]`, `Vec<u8>`, `Bytes`, `File`.
///
/// All methods use [`DEFAULT_BODY_SIZE`](crate::bmt::DEFAULT_BODY_SIZE) — no turbofish needed.
///
/// ```
/// use nectar_primitives::file::{SplitExt, ChunkGetExt};
///
/// let data = b"Hello, Swarm!";
/// let (root, store) = data.as_slice().split_and_store().unwrap();
/// let recovered = store.read_file(root).unwrap();
/// assert_eq!(recovered, data);
/// ```
pub trait SplitExt: ReadAt + Sync {
    /// Split into chunks stored in a [`MemorySink`](crate::store::MemorySink).
    fn split_and_store(
        &self,
    ) -> error::Result<(
        ChunkAddress,
        crate::store::MemorySink<{ crate::bmt::DEFAULT_BODY_SIZE }>,
    )>
    where
        Self: Sized,
    {
        let sink = crate::store::MemorySink::<{ crate::bmt::DEFAULT_BODY_SIZE }>::new();
        let splitter = ParallelSplitter::new(sink);
        let root = splitter.split(self)?;
        Ok((root, splitter.into_sink()))
    }

    /// Split into chunks stored in the provided sink.
    fn split_into<S: ChunkPut<{ crate::bmt::DEFAULT_BODY_SIZE }> + Send>(
        &self,
        sink: S,
    ) -> error::Result<(ChunkAddress, S)>
    where
        Self: Sized,
    {
        let splitter = ParallelSplitter::new(sink);
        let root = splitter.split(self)?;
        Ok((root, splitter.into_sink()))
    }

    /// Split into encrypted chunks stored in a [`MemorySink`](crate::store::MemorySink).
    #[cfg(feature = "encryption")]
    fn split_encrypted_and_store(
        &self,
    ) -> error::Result<(
        EncryptedChunkRef,
        crate::store::MemorySink<{ crate::bmt::DEFAULT_BODY_SIZE }>,
    )>
    where
        Self: Sized,
    {
        let sink = crate::store::MemorySink::<{ crate::bmt::DEFAULT_BODY_SIZE }>::new();
        let splitter = EncryptedParallelSplitter::new(sink);
        let root_ref = splitter.split(self)?;
        Ok((root_ref, splitter.into_sink()))
    }

    /// Split into encrypted chunks stored in the provided sink.
    #[cfg(feature = "encryption")]
    fn split_encrypted_into<S: ChunkPut<{ crate::bmt::DEFAULT_BODY_SIZE }> + Send>(
        &self,
        sink: S,
    ) -> error::Result<(EncryptedChunkRef, S)>
    where
        Self: Sized,
    {
        let splitter = EncryptedParallelSplitter::new(sink);
        let root_ref = splitter.split(self)?;
        Ok((root_ref, splitter.into_sink()))
    }
}

impl<T: ReadAt + Sync> SplitExt for T {}

#[cfg(test)]
mod tests;

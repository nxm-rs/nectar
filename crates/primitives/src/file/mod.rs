//! File splitting and joining for arbitrary-size data.
//!
//! Async is the primary API. `SyncJoiner` implements `Read + Seek`, `SyncSplitter`
//! implements `Write`.
//!
//! # Store-centric API (extension traits)
//!
//! ```
//! use nectar_primitives::file::{SyncChunkGetExt, SyncChunkPutExt};
//! use nectar_primitives::store::MemoryStore;
//! use nectar_primitives::DEFAULT_BODY_SIZE;
//!
//! let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
//! let addr = store.write_file(b"hello swarm").unwrap();
//! let data = store.read_file(addr).unwrap();
//! assert_eq!(data, b"hello swarm");
//! ```
//!
//! # Free function API
//!
//! ```
//! use nectar_primitives::file::{sync_split, sync_join};
//! use nectar_primitives::DEFAULT_BODY_SIZE;
//!
//! let data = b"Hello, Swarm!";
//! let (root, store) = sync_split::<DEFAULT_BODY_SIZE>(data).unwrap();
//! let recovered = sync_join(&store, root).unwrap();
//! assert_eq!(recovered, data);
//! ```
//!
//! # Encrypted split and join
//!
//! ```
//! # #[cfg(feature = "encryption")] {
//! use nectar_primitives::file::{sync_split_encrypted, sync_join};
//! use nectar_primitives::DEFAULT_BODY_SIZE;
//!
//! let data = b"secret data";
//! let (root_ref, store) = sync_split_encrypted::<DEFAULT_BODY_SIZE>(data).unwrap();
//! let recovered = sync_join(&store, root_ref).unwrap();
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
#[cfg(test)]
#[macro_use]
mod splitter_tests;
mod joiner;
pub mod mode;
mod sync_joiner;
mod sync_read_at;
mod sync_splitter;
mod sync_splitter_parallel;
mod tree;
mod write_at;

use crate::chunk::ChunkAddress;
#[cfg(feature = "encryption")]
use crate::chunk::encryption::EncryptedChunkRef;
use crate::store::{MaybeSend, MaybeSync, SyncChunkGet, SyncChunkPut};

// Async (primary) re-exports
#[cfg(feature = "encryption")]
pub use joiner::EncryptedJoiner;
#[cfg(feature = "tokio")]
pub use joiner::JoinerReader;
pub use joiner::{GenericJoiner, Joiner};
// Sync (secondary) re-exports
#[cfg(feature = "encryption")]
pub use sync_joiner::EncryptedSyncJoiner;
pub use sync_joiner::{GenericSyncJoiner, SyncJoiner};
pub use sync_read_at::SyncReadAt;
#[cfg(feature = "encryption")]
pub use sync_splitter::EncryptedSyncSplitter;
pub use sync_splitter::SyncSplitter;
#[cfg(feature = "encryption")]
pub use sync_splitter_parallel::EncryptedSyncParallelSplitter;
pub use sync_splitter_parallel::SyncParallelSplitter;
pub use write_at::WriteAt;

pub use entry_ref::EntryRef;
pub use error::FileError;
pub use tree::{ChunkRange, TreeParams};

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

/// Resolve a `SeekFrom` position to an absolute byte offset.
pub(crate) fn resolve_seek_position(
    pos: std::io::SeekFrom,
    current: u64,
    span: u64,
) -> std::io::Result<u64> {
    use std::io::{Error, ErrorKind::InvalidInput, SeekFrom};
    let to_i64 = |v: u64, msg: &str| i64::try_from(v).map_err(|_| Error::new(InvalidInput, msg));
    let new_pos = match pos {
        SeekFrom::Start(off) => to_i64(off, "seek offset exceeds i64::MAX")?,
        SeekFrom::End(off) => to_i64(span, "file span exceeds i64::MAX")? + off,
        SeekFrom::Current(off) => to_i64(current, "current position exceeds i64::MAX")? + off,
    };
    if new_pos < 0 {
        return Err(Error::new(InvalidInput, "seek to negative position"));
    }
    Ok(new_pos as u64)
}

// ---- Primary async API ----

/// Join chunks asynchronously. Dispatches plain/encrypted via [`JoinRef`].
pub async fn join<R, G, const BODY_SIZE: usize>(getter: G, root: R) -> error::Result<Vec<u8>>
where
    R: JoinRef,
    G: crate::store::ChunkGet<BODY_SIZE>,
{
    GenericJoiner::<G, R::Mode, BODY_SIZE>::new(getter, root.into_root_ref())
        .await?
        .read_all()
        .await
}

// ---- Secondary sync API ----

/// Split data into chunks synchronously, returning root address and chunk store.
///
/// Uses `SyncParallelSplitter` for best performance on in-memory data.
pub fn sync_split<const BODY_SIZE: usize>(
    data: &[u8],
) -> error::Result<(ChunkAddress, crate::store::MemoryStore<BODY_SIZE>)> {
    let store = crate::store::MemoryStore::<BODY_SIZE>::new();
    let splitter = SyncParallelSplitter::new(store);
    let root = splitter.split(&data)?;
    Ok((root, splitter.into_store()))
}

/// Split data into encrypted chunks synchronously.
#[cfg(feature = "encryption")]
pub fn sync_split_encrypted<const BODY_SIZE: usize>(
    data: &[u8],
) -> error::Result<(EncryptedChunkRef, crate::store::MemoryStore<BODY_SIZE>)> {
    let store = crate::store::MemoryStore::<BODY_SIZE>::new();
    let splitter = EncryptedSyncParallelSplitter::new(store);
    let root_ref = splitter.split(&data)?;
    Ok((root_ref, splitter.into_store()))
}

/// Join chunks synchronously. Dispatches plain/encrypted via [`JoinRef`].
pub fn sync_join<R, G, const BODY_SIZE: usize>(getter: G, root: R) -> error::Result<Vec<u8>>
where
    R: JoinRef,
    G: SyncChunkGet<BODY_SIZE> + Clone + Send + Sync,
{
    GenericSyncJoiner::<G, R::Mode, BODY_SIZE>::new(getter, root.into_root_ref())?.read_all()
}

/// Calculate tree depth for a given file size (plain mode).
#[cfg(test)]
pub(crate) const fn levels(length: u64, chunk_size: usize) -> usize {
    constants::tree_depth(length, chunk_size, constants::REF_SIZE)
}

// ---- Extension traits ----

/// Extension methods for async chunk getters.
///
/// Uses [`JoinRef`] for unified plain/encrypted dispatch.
pub trait ChunkGetExt<const BODY_SIZE: usize>: crate::store::ChunkGet<BODY_SIZE> {
    /// Open a file for async reading.
    fn joiner<R: JoinRef>(
        self,
        root: R,
    ) -> impl std::future::Future<Output = error::Result<GenericJoiner<Self, R::Mode, BODY_SIZE>>>
    + MaybeSend
    where
        Self: Sized + Clone + MaybeSend + MaybeSync + 'static,
    {
        GenericJoiner::new(self, root.into_root_ref())
    }

    /// Read entire file into memory asynchronously.
    fn read_file<R: JoinRef>(
        self,
        root: R,
    ) -> impl std::future::Future<Output = error::Result<Vec<u8>>> + MaybeSend
    where
        Self: Sized + Clone + MaybeSend + MaybeSync + 'static,
    {
        join(self, root)
    }
}

impl<T, const BODY_SIZE: usize> ChunkGetExt<BODY_SIZE> for T where
    T: crate::store::ChunkGet<BODY_SIZE>
{
}

/// Extension methods for sync chunk getters.
///
/// Automatically implemented for all types that implement [`SyncChunkGet`].
/// Uses [`JoinRef`] for unified plain/encrypted dispatch.
///
/// ```
/// use nectar_primitives::file::{SyncChunkPutExt, SyncChunkGetExt};
/// use nectar_primitives::store::MemoryStore;
/// use nectar_primitives::DEFAULT_BODY_SIZE;
///
/// let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
/// let addr = store.write_file(b"hello swarm").unwrap();
/// let recovered = store.read_file(addr).unwrap();
/// assert_eq!(recovered, b"hello swarm");
/// ```
pub trait SyncChunkGetExt<const BODY_SIZE: usize>: SyncChunkGet<BODY_SIZE> {
    /// Open a file for reading. Returns a joiner implementing `Read + Seek`.
    fn joiner<R: JoinRef>(
        self,
        root: R,
    ) -> error::Result<GenericSyncJoiner<Self, R::Mode, BODY_SIZE>>
    where
        Self: Clone + Send + Sync + Sized,
    {
        GenericSyncJoiner::new(self, root.into_root_ref())
    }

    /// Read entire file into memory (like `fs::read`).
    fn read_file<R: JoinRef>(self, root: R) -> error::Result<Vec<u8>>
    where
        Self: Clone + Send + Sync + Sized,
    {
        sync_join(self, root)
    }
}

impl<T, const BODY_SIZE: usize> SyncChunkGetExt<BODY_SIZE> for T where T: SyncChunkGet<BODY_SIZE> {}

/// Extension methods for sync chunk putters.
///
/// Automatically implemented for all types that implement [`SyncChunkPut`].
///
/// ```
/// use nectar_primitives::file::{SyncChunkPutExt, SyncChunkGetExt};
/// use nectar_primitives::store::MemoryStore;
/// use nectar_primitives::DEFAULT_BODY_SIZE;
/// use std::io::Write;
///
/// // Filesystem-style write/read
/// let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
/// let addr = store.write_file(b"hello").unwrap();
///
/// // Streaming write via std::io::Write
/// let mut writer = store.writer(6);
/// writer.write_all(b"world!").unwrap();
/// let (root, _) = writer.finish().unwrap();
/// ```
pub trait SyncChunkPutExt<const BODY_SIZE: usize>: SyncChunkPut<BODY_SIZE> {
    /// Create a writer for streaming data. Returns a `SyncSplitter` implementing `Write`.
    /// Call `.finish()` on the returned writer to get the root address.
    fn writer(&self, size: u64) -> SyncSplitter<&Self, BODY_SIZE>
    where
        Self: Sized,
    {
        SyncSplitter::new(self, size)
    }

    /// Create an encrypted writer. Returns an `EncryptedSyncSplitter` implementing `Write`.
    #[cfg(feature = "encryption")]
    fn encrypted_writer(&self, size: u64) -> EncryptedSyncSplitter<&Self, BODY_SIZE>
    where
        Self: Sized,
    {
        EncryptedSyncSplitter::new(self, size)
    }

    /// Write file data into the store (like `fs::write`).
    fn write_file(&self, data: &[u8]) -> error::Result<ChunkAddress>
    where
        Self: Send + Sync + Sized,
    {
        SyncParallelSplitter::<&Self, BODY_SIZE>::new(self).split(&data)
    }

    /// Write encrypted file data into the store.
    #[cfg(feature = "encryption")]
    fn write_encrypted_file(&self, data: &[u8]) -> error::Result<EncryptedChunkRef>
    where
        Self: Send + Sync + Sized,
    {
        EncryptedSyncParallelSplitter::<&Self, BODY_SIZE>::new(self).split(&data)
    }
}

impl<T, const BODY_SIZE: usize> SyncChunkPutExt<BODY_SIZE> for T where T: SyncChunkPut<BODY_SIZE> {}

#[cfg(test)]
mod tests;

//! File splitting and joining for arbitrary-size data.
//!
//! Joining is async; splitting is CPU-bound and runs on rayon. `Splitter`
//! implements `Write` for streaming producers.
//!
//! # Store-centric API (extension traits)
//!
//! ```
//! use futures::executor::block_on;
//! use nectar_primitives::file::{ChunkGetExt, ChunkPutExt};
//! use nectar_primitives::DefaultMemoryStore;
//!
//! let store = DefaultMemoryStore::new();
//! let addr = block_on(store.write_file(b"hello swarm".to_vec())).unwrap();
//! let data = block_on(store.read_file(addr)).unwrap();
//! assert_eq!(data, b"hello swarm");
//! ```
//!
//! # Free function API
//!
//! ```
//! use futures::executor::block_on;
//! use nectar_primitives::file::{split, join};
//! use nectar_primitives::DEFAULT_BODY_SIZE;
//!
//! let data = b"Hello, Swarm!";
//! let (root, store) = split::<DEFAULT_BODY_SIZE>(data).unwrap();
//! let recovered = block_on(join(&store, root)).unwrap();
//! assert_eq!(recovered, data);
//! ```
//!
//! # Encrypted split and join
//!
//! ```
//! # #[cfg(feature = "encryption")] {
//! use futures::executor::block_on;
//! use nectar_primitives::file::{split_encrypted, join};
//! use nectar_primitives::DEFAULT_BODY_SIZE;
//!
//! let data = b"secret data";
//! let (root_ref, store) = split_encrypted::<DEFAULT_BODY_SIZE>(data).unwrap();
//! let recovered = block_on(join(&store, root_ref)).unwrap();
//! assert_eq!(recovered, data);
//! # }
//! ```

mod constants;
pub mod entry_ref;
pub mod error;
mod fold;
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
mod read_at;
mod splitter;
mod splitter_parallel;
mod tree;
mod windowed;
mod write_at;

#[cfg(feature = "encryption")]
use crate::chunk::encryption::EncryptedChunkRef;
use crate::chunk::{AnyChunkSet, Chunk, ChunkAddress, ContentChunk, Verified};
use crate::store::{ChunkPut, MaybeSend, MaybeSync, TrustedGet};

// Async (primary) re-exports
#[cfg(feature = "encryption")]
pub use joiner::EncryptedJoiner;
#[cfg(feature = "tokio")]
pub use joiner::JoinerReader;
pub use joiner::{GenericJoiner, Joiner};
#[cfg(feature = "tokio")]
pub use windowed::WindowedJoinerReader;
pub use windowed::WindowedReader;
// Splitter re-exports
pub use read_at::ReadAt;
#[cfg(feature = "encryption")]
pub use splitter::EncryptedSplitter;
pub use splitter::Splitter;
#[cfg(feature = "encryption")]
pub use splitter_parallel::EncryptedParallelSplitter;
pub use splitter_parallel::ParallelSplitter;
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
/// Sealed; implemented for `ChunkAddress` and `EncryptedChunkRef`.
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
    // Match std's seek semantics: a relative offset that overflows i64 is an
    // invalid-input error, never a wrap or panic.
    let overflow = || Error::new(InvalidInput, "seek position overflows i64");
    let new_pos = match pos {
        SeekFrom::Start(off) => to_i64(off, "seek offset exceeds i64::MAX")?,
        SeekFrom::End(off) => to_i64(span, "file span exceeds i64::MAX")?
            .checked_add(off)
            .ok_or_else(overflow)?,
        SeekFrom::Current(off) => to_i64(current, "current position exceeds i64::MAX")?
            .checked_add(off)
            .ok_or_else(overflow)?,
    };
    if new_pos < 0 {
        return Err(Error::new(InvalidInput, "seek to negative position"));
    }
    // new_pos >= 0 was just checked, so the i64 -> u64 conversion is lossless.
    #[allow(clippy::as_conversions)]
    let new_pos = new_pos as u64;
    Ok(new_pos)
}

// ---- Primary async API ----

/// Join chunks asynchronously. Dispatches plain/encrypted via [`JoinRef`].
pub async fn join<R, G, const BODY_SIZE: usize>(getter: G, root: R) -> error::Result<Vec<u8>>
where
    R: JoinRef,
    G: TrustedGet<AnyChunkSet<BODY_SIZE>>,
{
    GenericJoiner::<G, R::Mode, BODY_SIZE>::new(getter, root.into_root_ref())
        .await?
        .read_all()
        .await
}

// ---- Splitting (CPU-bound, rayon) ----

/// Seal freshly split chunks for the store boundary: the sole upcast into
/// the store's envelope.
fn seal_chunks<const BODY_SIZE: usize>(
    chunks: Vec<ContentChunk<BODY_SIZE>>,
) -> error::Result<Vec<Chunk<Verified, AnyChunkSet<BODY_SIZE>>>> {
    chunks
        .into_iter()
        .map(|chunk| Chunk::from_envelope(chunk.into()).map_err(mode::chunk_creation_error))
        .collect()
}

/// Split data into chunks, returning root address and chunk store.
///
/// Uses `ParallelSplitter` for best performance on in-memory data.
pub fn split<const BODY_SIZE: usize>(
    data: &[u8],
) -> error::Result<(
    ChunkAddress,
    crate::store::MemoryStore<AnyChunkSet<BODY_SIZE>>,
)> {
    let (root, chunks) = ParallelSplitter::<BODY_SIZE>::split_to_vec(&data)?;
    Ok((
        root,
        crate::store::MemoryStore::from_chunks(seal_chunks(chunks)?),
    ))
}

/// Split data into encrypted chunks.
#[cfg(feature = "encryption")]
pub fn split_encrypted<const BODY_SIZE: usize>(
    data: &[u8],
) -> error::Result<(
    EncryptedChunkRef,
    crate::store::MemoryStore<AnyChunkSet<BODY_SIZE>>,
)> {
    let (root_ref, chunks) = EncryptedParallelSplitter::<BODY_SIZE>::split_to_vec(&data)?;
    Ok((
        root_ref,
        crate::store::MemoryStore::from_chunks(seal_chunks(chunks)?),
    ))
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
pub trait ChunkGetExt<const BODY_SIZE: usize>: TrustedGet<AnyChunkSet<BODY_SIZE>> {
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
    T: TrustedGet<AnyChunkSet<BODY_SIZE>>
{
}

/// Extension methods for chunk putters.
///
/// Splits data on rayon, then stores the chunks via the async [`ChunkPut`].
/// Uses [`JoinRef`] for unified plain/encrypted dispatch on read-back.
///
/// ```
/// use futures::executor::block_on;
/// use nectar_primitives::file::{ChunkGetExt, ChunkPutExt};
/// use nectar_primitives::DefaultMemoryStore;
///
/// let store = DefaultMemoryStore::new();
/// let addr = block_on(store.write_file(b"hello swarm".to_vec())).unwrap();
/// let recovered = block_on(store.read_file(addr)).unwrap();
/// assert_eq!(recovered, b"hello swarm");
/// ```
pub trait ChunkPutExt<const BODY_SIZE: usize>: ChunkPut<AnyChunkSet<BODY_SIZE>> {
    /// Split `data` and store every produced chunk, returning the root address.
    ///
    /// Splitting runs on rayon up front; `data` is dropped before the first
    /// store await, so the returned future never holds the source.
    fn write_file<D: ReadAt + Sync>(
        &self,
        data: D,
    ) -> impl std::future::Future<Output = error::Result<ChunkAddress>> + MaybeSend
    where
        Self: Sized + MaybeSync,
    {
        let split = ParallelSplitter::<BODY_SIZE>::split_to_vec(&data);
        async move {
            let (root, chunks) = split?;
            for chunk in seal_chunks(chunks)? {
                self.put(chunk).await.map_err(FileError::store)?;
            }
            Ok(root)
        }
    }

    /// Split `data` into encrypted chunks and store them, returning the root reference.
    #[cfg(feature = "encryption")]
    #[deprecated(
        note = "select encrypted mode through the reference type: an encrypted `ManifestBuilder::put_file`, or the `EncryptedParallelSplitter`/`split_encrypted` primitives directly"
    )]
    fn write_encrypted_file<D: ReadAt + Sync>(
        &self,
        data: D,
    ) -> impl std::future::Future<Output = error::Result<EncryptedChunkRef>> + MaybeSend
    where
        Self: Sized + MaybeSync,
    {
        let split = EncryptedParallelSplitter::<BODY_SIZE>::split_to_vec(&data);
        async move {
            let (root_ref, chunks) = split?;
            for chunk in seal_chunks(chunks)? {
                self.put(chunk).await.map_err(FileError::store)?;
            }
            Ok(root_ref)
        }
    }
}

impl<T, const BODY_SIZE: usize> ChunkPutExt<BODY_SIZE> for T where
    T: ChunkPut<AnyChunkSet<BODY_SIZE>>
{
}

#[cfg(test)]
mod tests;

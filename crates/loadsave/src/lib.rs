//! Manifest node persistence over the file pipeline.
//!
//! [`NodeLoadSaver`] adapts a chunk store to the mantaray persistence seam:
//! loads join a node's chunks through the file reader, saves split node
//! bytes through the file splitter. A node larger than one chunk spans
//! several and is addressed by its file root, matching the reference
//! client; a node of one chunk keeps the pre-seam content-chunk address, so
//! existing roots are unchanged.
//!
//! ```
//! use nectar_loadsave::NodeLoadSaver;
//! use nectar_mantaray::{ManifestEditor, Reader};
//! use nectar_primitives::StandardChunkSet;
//! use nectar_primitives::chunk::ChunkAddress;
//! use nectar_primitives::store::MemoryStore;
//!
//! # nectar_testing::run(async {
//! let loadsaver = NodeLoadSaver::new(MemoryStore::<StandardChunkSet>::new());
//! let mut editor = ManifestEditor::new(loadsaver);
//! editor.put("hello.txt", ChunkAddress::from([7u8; 32]));
//! let (root, loadsaver) = editor.commit().await.unwrap();
//! let entry = Reader::new(loadsaver).get(root, b"hello.txt").await.unwrap();
//! assert!(entry.is_some());
//! # });
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
// Test code may freely unwrap/index/panic; the runtime-safety restriction
// lints target production code paths.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::panic,
        clippy::as_conversions
    )
)]

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError};

use nectar_file::{CollectError, File, Policy, PutWindow, SaveError, SplitError};
use nectar_mantaray::persist::{MAX_NODE_BYTES, NodeLoader, NodeSaver};
use nectar_primitives::chunk::{ChunkAddress, ChunkRef, Verified};
use nectar_primitives::store::{ChunkGet, ChunkPut, ContentGet, ContentGetError, TrustedGet};
use nectar_primitives::{AnyChunkSet, DEFAULT_BODY_SIZE, EntryRef};


#[cfg(feature = "encryption")]
use nectar_primitives::EncryptedChunkRef;

/// Node loadsaver over a chunk store: the file joiner reads, the file
/// splitter writes.
///
/// Loads are capped at [`MAX_NODE_BYTES`], the largest image any valid node
/// can occupy. The put window bounds the splitter's in-flight puts.
#[derive(Debug, Clone)]
pub struct NodeLoadSaver<S, const B: usize = DEFAULT_BODY_SIZE> {
    store: S,
    window: PutWindow,
}

impl<S, const B: usize> NodeLoadSaver<S, B> {
    /// Adapter over `store` with the default put window.
    pub const fn new(store: S) -> Self {
        Self {
            store,
            window: PutWindow::DEFAULT,
        }
    }

    /// Replace the save-side put window.
    #[must_use]
    pub const fn with_put_window(mut self, window: PutWindow) -> Self {
        self.window = window;
        self
    }

    /// Borrow the backing store.
    pub const fn store(&self) -> &S {
        &self.store
    }

    /// Consume into the backing store.
    pub fn into_store(self) -> S {
        self.store
    }
}

impl<S, const B: usize> NodeLoadSaver<S, B> {
    /// One write handle over the borrowed store at the save-side window.
    const fn file(&self) -> File<&S, B> {
        File::new(&self.store, Policy::DEFAULT.with_put_window(self.window))
    }
}

/// Narrow a save failure to the split arm; an in-memory slice source never
/// fails, so the source arm is uninhabited.
fn unwrap_save<E>(error: SaveError<E, core::convert::Infallible>) -> SplitError<E> {
    match error {
        SaveError::Split(error) => error,
        SaveError::Source { source } => match source {},
    }
}

/// Failure loading one node through the file joiner: the open, the join, or
/// the [`MAX_NODE_BYTES`] bound.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct LoadError<E>(#[from] CollectError<ContentGetError<E>>);

impl<S, const B: usize> NodeLoader for NodeLoadSaver<S, B>
where
    S: TrustedGet<AnyChunkSet<B>> + Clone + 'static,
{
    type Error = LoadError<S::Error>;

    async fn load(&self, reference: &EntryRef) -> Result<Vec<u8>, Self::Error> {
        let file = File::<_, B>::new(ContentGet::new(self.store.clone()), Policy::DEFAULT);
        Ok(file.collect(reference.clone(), MAX_NODE_BYTES).await?)
    }

    async fn load_with_addresses(
        &self,
        reference: &EntryRef,
    ) -> Result<(Vec<u8>, Vec<ChunkAddress>), Self::Error> {
        let recorder = RecordingGet::new(self.store.clone());
        let file = File::<_, B>::new(ContentGet::new(recorder.clone()), Policy::DEFAULT);
        let bytes = file.collect(reference.clone(), MAX_NODE_BYTES).await?;
        Ok((bytes, recorder.addresses()))
    }
}

impl<S, const B: usize> NodeSaver<ChunkRef> for NodeLoadSaver<S, B>
where
    S: ChunkPut<AnyChunkSet<B>>,
{
    type Error = SplitError<S::Error>;

    async fn save(&self, data: Vec<u8>) -> Result<ChunkRef, Self::Error> {
        let root = self.file().save(data.as_slice()).await.map_err(unwrap_save)?;
        Ok(ChunkRef::new(root))
    }
}

/// Encrypted node persistence: each chunk seals under a fresh random key
/// and the returned reference carries the root's decryption key.
#[cfg(feature = "encryption")]
#[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
impl<S, const B: usize> NodeSaver<EncryptedChunkRef> for NodeLoadSaver<S, B>
where
    S: ChunkPut<AnyChunkSet<B>>,
{
    type Error = SplitError<S::Error>;

    async fn save(&self, data: Vec<u8>) -> Result<EncryptedChunkRef, Self::Error> {
        self.file()
            .save_encrypted(data.as_slice())
            .await
            .map_err(unwrap_save)
    }
}

/// Get decorator recording each distinct fetched address in first-fetch
/// order, so a node load reports every chunk its image occupies (root
/// first).
#[derive(Debug, Clone)]
struct RecordingGet<S> {
    inner: S,
    seen: Arc<Mutex<Seen>>,
}

#[derive(Debug, Default)]
struct Seen {
    order: Vec<ChunkAddress>,
    set: BTreeSet<ChunkAddress>,
}

impl<S> RecordingGet<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            seen: Arc::new(Mutex::new(Seen::default())),
        }
    }

    /// The distinct fetched addresses, in first-fetch order.
    fn addresses(&self) -> Vec<ChunkAddress> {
        self.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .order
            .clone()
    }
}

impl<S, const B: usize> ChunkGet<AnyChunkSet<B>> for RecordingGet<S>
where
    S: TrustedGet<AnyChunkSet<B>>,
{
    type Trust = Verified;
    type Error = S::Error;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<nectar_primitives::Chunk<Verified, AnyChunkSet<B>>, Self::Error> {
        let chunk = self.inner.get(address).await?;
        // A single push or a dropped lock cannot leave the pair torn.
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        if seen.set.insert(*address) {
            seen.order.push(*address);
        }
        drop(seen);
        Ok(chunk)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use nectar_primitives::StandardChunkSet;
    use nectar_primitives::chunk::{ChunkOps, ContentChunk};
    use nectar_primitives::store::MemoryStore;
    use nectar_testing::run;

    type Store = MemoryStore<StandardChunkSet>;
    type LoadSaver = NodeLoadSaver<Store>;

    fn save_plain(loadsaver: &LoadSaver, data: Vec<u8>) -> ChunkRef {
        run(NodeSaver::<ChunkRef>::save(loadsaver, data)).unwrap()
    }

    #[test]
    fn single_chunk_save_keeps_the_content_chunk_address() {
        let loadsaver = LoadSaver::new(Store::new());
        let data = vec![0x5au8; 1000];
        let want = *ContentChunk::<DEFAULT_BODY_SIZE>::new(Bytes::from(data.clone()))
            .unwrap()
            .address();
        let root = save_plain(&loadsaver, data.clone());
        assert_eq!(*root.address(), want);

        let reference = EntryRef::from(root);
        assert_eq!(run(loadsaver.load(&reference)).unwrap(), data);
        let (bytes, addresses) = run(loadsaver.load_with_addresses(&reference)).unwrap();
        assert_eq!(bytes, data);
        assert_eq!(addresses, vec![*reference.address()]);
    }

    #[test]
    fn multi_chunk_save_round_trips_and_reports_every_address() {
        let loadsaver = LoadSaver::new(Store::new());
        let data: Vec<u8> = (0..20_000u32).map(|i| (i % 251) as u8).collect();
        let root = save_plain(&loadsaver, data.clone());

        let reference = EntryRef::from(root);
        let (bytes, addresses) = run(loadsaver.load_with_addresses(&reference)).unwrap();
        assert_eq!(bytes, data);
        // 20000 bytes span five leaves plus the root: six stored chunks.
        let mut stored: Vec<ChunkAddress> = loadsaver
            .store()
            .clone()
            .into_chunks()
            .keys()
            .copied()
            .collect();
        stored.sort();
        let mut got = addresses.clone();
        got.sort();
        assert_eq!(got, stored);
        assert_eq!(addresses[0], *reference.address(), "root first");
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn encrypted_save_round_trips() {
        let loadsaver = LoadSaver::new(Store::new());
        let data: Vec<u8> = (0..9_000u32).map(|i| (i % 241) as u8).collect();
        let root = run(NodeSaver::<EncryptedChunkRef>::save(
            &loadsaver,
            data.clone(),
        ))
        .unwrap();
        let reference = EntryRef::from(root);
        assert_eq!(run(loadsaver.load(&reference)).unwrap(), data);
        let (bytes, addresses) = run(loadsaver.load_with_addresses(&reference)).unwrap();
        assert_eq!(bytes, data);
        assert!(addresses.len() > 1);
    }
}

//! Node persistence: the trie's instantiation of the shared node seam, at a
//! whole node image.
//!
//! The trie never touches chunk stores; the adapter decides the layout.
//! `NodeLoadSaver` (feature `manifest`) stores through the file pipeline, so a
//! node over one chunk spans several, matching the reference client.

use nectar_primitives::EncryptedChunkRef;

use crate::format::{FORK_INDEX_SIZE, ForkHeader, NodeHeader};

#[cfg(feature = "manifest")]
pub use pipeline::{NodeCollectError, NodeLoadSaver};

/// Widest fork record: type byte, length-prefixed 30-byte prefix, encrypted
/// reference, metadata length field, maximal metadata payload.
#[allow(clippy::as_conversions)] // u16::MAX -> usize widening; `usize::from` is not const-callable
const FORK_RECORD_MAX: usize = ForkHeader::PRE_REFERENCE_SIZE
    + EncryptedChunkRef::SIZE
    + ForkHeader::METADATA_LEN_SIZE
    + u16::MAX as usize;

/// Structural cap on one node's stored image: header, entry slot, fork
/// index, and a full 256-fork table at the widest record. Loaders reject
/// larger images, which no valid node can occupy.
#[allow(clippy::as_conversions)] // usize -> u64 widening; `u64::try_from` is not const-callable
pub const MAX_NODE_BYTES: u64 =
    (NodeHeader::SIZE + EncryptedChunkRef::SIZE + FORK_INDEX_SIZE + 256 * FORK_RECORD_MAX) as u64;

// Private: it scopes the `std` and `nectar-file` imports the production
// adapter needs behind one `manifest` gate. Its items surface through the
// re-export above, so the rendered docs live on the definitions.
#[cfg(feature = "manifest")]
mod pipeline {
    use alloc::vec::Vec;
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex, PoisonError};

    use nectar_file::{CollectError, File, Policy, PutWindow, SaveError, SplitError};
    use nectar_manifest::{NodeLoader, NodeSaver};
    use nectar_primitives::EntryRef;
    use nectar_primitives::chunk::{ChunkAddress, ChunkRef, Verified};
    use nectar_primitives::store::{ChunkGet, ChunkPut, ContentGet, ContentGetError, TrustedGet};
    use nectar_primitives::{AnyChunkSet, Chunk, DEFAULT_BODY_SIZE};

    use super::*;

    /// Node loadsaver over a chunk store: the file joiner reads, the file
    /// splitter writes.
    ///
    /// A node over one chunk is addressed by its file root; a single-chunk
    /// node keeps the content-chunk address, so existing roots are unchanged.
    ///
    /// ```
    /// use nectar_mantaray::{ManifestEditor, NodeLoadSaver, TrieLookup};
    /// use nectar_primitives::StandardChunkSet;
    /// use nectar_primitives::chunk::ChunkAddress;
    /// use nectar_primitives::store::MemoryStore;
    ///
    /// # nectar_testing::run(async {
    /// let loadsaver = NodeLoadSaver::new(MemoryStore::<StandardChunkSet>::new());
    /// let mut editor = ManifestEditor::new(loadsaver);
    /// editor.insert("hello.txt", ChunkAddress::from([7u8; 32]));
    /// let (root, loadsaver) = editor.commit().await.unwrap();
    /// let entry = TrieLookup::new(loadsaver).get(root, b"hello.txt").await.unwrap();
    /// assert!(entry.is_some());
    /// # });
    /// ```
    #[cfg_attr(docsrs, doc(cfg(feature = "manifest")))]
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

        /// One write handle over the borrowed store at the save-side window.
        const fn file(&self) -> File<&S, B> {
            File::new(&self.store, Policy::DEFAULT.with_put_window(self.window))
        }
    }

    /// Narrow a save failure to the split arm; an in-memory slice source
    /// never fails, so the source arm is uninhabited.
    fn unwrap_save<E>(error: SaveError<E, core::convert::Infallible>) -> SplitError<E> {
        match error {
            SaveError::Split(error) => error,
            SaveError::Source { source } => match source {},
        }
    }

    /// Failure collecting one node: the open, the join, or the size bound.
    #[cfg_attr(docsrs, doc(cfg(feature = "manifest")))]
    pub type NodeCollectError<E> = CollectError<ContentGetError<E>>;

    impl<S, const B: usize> NodeLoader<Vec<u8>> for NodeLoadSaver<S, B>
    where
        S: TrustedGet<AnyChunkSet<B>> + Clone + 'static,
    {
        type Error = NodeCollectError<S::Error>;

        async fn load(&self, reference: &EntryRef) -> Result<Vec<u8>, Self::Error> {
            let file = File::<_, B>::new(ContentGet::new(self.store.clone()), Policy::DEFAULT);
            file.collect(reference.clone(), MAX_NODE_BYTES).await
        }

        async fn load_traced(
            &self,
            reference: &EntryRef,
        ) -> Result<(Vec<u8>, Vec<ChunkAddress>), Self::Error> {
            let recorder = RecordingGet::new(self.store.clone());
            let file = File::<_, B>::new(ContentGet::new(recorder.clone()), Policy::DEFAULT);
            let bytes = file.collect(reference.clone(), MAX_NODE_BYTES).await?;
            Ok((bytes, recorder.addresses()))
        }
    }

    impl<S, const B: usize> NodeSaver<[u8], ChunkRef> for NodeLoadSaver<S, B>
    where
        S: ChunkPut<Chunk<Verified, AnyChunkSet<B>>>,
    {
        type Error = SplitError<S::Error>;

        async fn save(&self, data: &[u8]) -> Result<ChunkRef, Self::Error> {
            let root = self.file().save(data).await.map_err(unwrap_save)?;
            Ok(ChunkRef::new(root))
        }
    }

    /// Encrypted node persistence: each chunk seals under a fresh random key
    /// and the returned reference carries the root's decryption key.
    #[cfg(feature = "encryption")]
    #[cfg_attr(docsrs, doc(cfg(all(feature = "manifest", feature = "encryption"))))]
    impl<S, const B: usize> NodeSaver<[u8], EncryptedChunkRef> for NodeLoadSaver<S, B>
    where
        S: ChunkPut<Chunk<Verified, AnyChunkSet<B>>>,
    {
        type Error = SplitError<S::Error>;

        async fn save(&self, data: &[u8]) -> Result<EncryptedChunkRef, Self::Error> {
            self.file().save_encrypted(data).await.map_err(unwrap_save)
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
}

/// Single-chunk loadsaver for the in-crate tests and oracles: one node image
/// is one content chunk, the pre-seam layout, so every pinned root stays
/// byte-identical. The encrypted width carries an all-zero key. Exempt from
/// semver guarantees.
#[cfg(any(test, all(feature = "arbitrary", feature = "hazmat")))]
#[doc(hidden)]
pub mod single_chunk {
    use alloc::sync::Arc;
    use alloc::vec::Vec;

    use nectar_manifest::{NodeLoader, NodeSaver};
    use nectar_primitives::EntryRef;
    use nectar_primitives::chunk::{ChunkAddress, ChunkOps, ChunkRef, ContentChunk};
    use nectar_primitives::error::PrimitivesError;
    use nectar_primitives::store::{ChunkPut, SharedError, TrustedGet};
    use nectar_primitives::{AnyChunkSet, Chunk, DEFAULT_BODY_SIZE, EncryptionKey, Verified};

    use super::*;

    /// One store, both seams, single-chunk layout.
    #[derive(Debug, Clone, Default)]
    pub struct SingleChunkLoadSaver<S, const BS: usize = DEFAULT_BODY_SIZE>(S);

    impl<S, const BS: usize> SingleChunkLoadSaver<S, BS> {
        /// Wrap a store typed at [`AnyChunkSet`].
        pub const fn new(store: S) -> Self {
            Self(store)
        }

        /// Borrow the inner store.
        pub const fn store(&self) -> &S {
            &self.0
        }

        /// Consume into the inner store.
        pub fn into_store(self) -> S {
            self.0
        }
    }

    /// Single-chunk load or save failure.
    #[derive(Debug, thiserror::Error)]
    pub enum SingleChunkError {
        /// The store failed.
        #[error("store error: {0}")]
        Store(#[source] SharedError),
        /// The node bytes do not fit one content chunk.
        #[error(transparent)]
        Chunk(#[from] PrimitivesError),
    }

    impl<S, const BS: usize> NodeLoader<Vec<u8>> for SingleChunkLoadSaver<S, BS>
    where
        S: TrustedGet<AnyChunkSet<BS>>,
    {
        type Error = SingleChunkError;

        async fn load(&self, reference: &EntryRef) -> Result<Vec<u8>, Self::Error> {
            let chunk = self
                .0
                .get(reference.address())
                .await
                .map_err(|e| SingleChunkError::Store(Arc::new(e)))?;
            Ok(chunk.envelope().data().to_vec())
        }
    }

    impl<S, const BS: usize> SingleChunkLoadSaver<S, BS>
    where
        S: ChunkPut<Chunk<Verified, AnyChunkSet<BS>>>,
    {
        async fn put_node(&self, data: &[u8]) -> Result<ChunkAddress, SingleChunkError> {
            let chunk = ContentChunk::<BS>::new(data.to_vec())?;
            let address = *chunk.address();
            let sealed: Chunk<_, AnyChunkSet<BS>> = Chunk::from_envelope(chunk.into())?;
            self.0
                .put(sealed)
                .await
                .map_err(|e| SingleChunkError::Store(Arc::new(e)))?;
            Ok(address)
        }
    }

    impl<S, const BS: usize> NodeSaver<[u8], ChunkRef> for SingleChunkLoadSaver<S, BS>
    where
        S: ChunkPut<Chunk<Verified, AnyChunkSet<BS>>>,
    {
        type Error = SingleChunkError;

        async fn save(&self, data: &[u8]) -> Result<ChunkRef, Self::Error> {
            Ok(ChunkRef::new(self.put_node(data).await?))
        }
    }

    impl<S, const BS: usize> NodeSaver<[u8], EncryptedChunkRef> for SingleChunkLoadSaver<S, BS>
    where
        S: ChunkPut<Chunk<Verified, AnyChunkSet<BS>>>,
    {
        type Error = SingleChunkError;

        async fn save(&self, data: &[u8]) -> Result<EncryptedChunkRef, Self::Error> {
            Ok(EncryptedChunkRef::new(
                self.put_node(data).await?,
                EncryptionKey::from([0u8; EncryptionKey::SIZE]),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_node_bytes_matches_the_structural_layout() {
        // 64 header + 64 entry + 32 index + 256 * 65633 fork records.
        assert_eq!(MAX_NODE_BYTES, 16_802_208);
    }
}

#[cfg(all(test, feature = "manifest"))]
mod pipeline_tests {
    use bytes::Bytes;
    use nectar_manifest::{NodeLoader, NodeSaver};
    use nectar_primitives::EntryRef;
    use nectar_primitives::chunk::{ChunkAddress, ChunkOps, ChunkRef, ContentChunk};
    use nectar_primitives::store::MemoryStore;
    use nectar_primitives::{DEFAULT_BODY_SIZE, StandardChunkSet};
    use nectar_testing::run;

    use super::*;

    type Store = MemoryStore<StandardChunkSet>;
    type LoadSaver = NodeLoadSaver<Store>;

    fn save_plain(loadsaver: &LoadSaver, data: &[u8]) -> ChunkRef {
        run(NodeSaver::<[u8], ChunkRef>::save(loadsaver, data)).unwrap()
    }

    #[test]
    fn single_chunk_save_keeps_the_content_chunk_address() {
        let loadsaver = LoadSaver::new(Store::new());
        let data = vec![0x5au8; 1000];
        let want = *ContentChunk::<DEFAULT_BODY_SIZE>::new(Bytes::from(data.clone()))
            .unwrap()
            .address();
        let root = save_plain(&loadsaver, &data);
        assert_eq!(*root.address(), want);

        let reference = EntryRef::from(root);
        assert_eq!(run(loadsaver.load(&reference)).unwrap(), data);
        let (bytes, addresses) = run(loadsaver.load_traced(&reference)).unwrap();
        assert_eq!(bytes, data);
        assert_eq!(addresses, vec![*reference.address()]);
    }

    #[test]
    fn multi_chunk_save_round_trips_and_reports_every_address() {
        let loadsaver = LoadSaver::new(Store::new());
        let data: Vec<u8> = (0..20_000u32).map(|i| (i % 251) as u8).collect();
        let root = save_plain(&loadsaver, &data);

        let reference = EntryRef::from(root);
        let (bytes, addresses) = run(loadsaver.load_traced(&reference)).unwrap();
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
        let root = run(NodeSaver::<[u8], EncryptedChunkRef>::save(
            &loadsaver, &data,
        ))
        .unwrap();
        let reference = EntryRef::from(root);
        assert_eq!(run(loadsaver.load(&reference)).unwrap(), data);
        let (bytes, addresses) = run(loadsaver.load_traced(&reference)).unwrap();
        assert_eq!(bytes, data);
        assert!(addresses.len() > 1);
    }
}

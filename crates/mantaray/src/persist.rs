//! Node persistence seam: abstract loader and saver over full-width
//! references.
//!
//! The trie never touches chunk stores directly: a [`NodeLoader`] turns a
//! full-width reference into node bytes and a [`NodeSaver`] persists node
//! bytes under a new reference. Adapters decide the storage layout; the
//! workspace `nectar-loadsave` crate stores nodes through the file pipeline,
//! so a node larger than one chunk spans several, matching the reference
//! client.

use alloc::vec::Vec;
use core::future::Future;

use nectar_primitives::chunk::{ChunkAddress, Reference};
use nectar_primitives::store::{ChunkStoreError, MaybeSend, MaybeSync, NullLoader};
use nectar_primitives::{EncryptedChunkRef, EntryRef};

use crate::format::{FORK_INDEX_SIZE, ForkHeader, NodeHeader};

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

/// Read seam: node bytes behind a full-width reference.
pub trait NodeLoader: MaybeSend + MaybeSync {
    /// Loader failure, wrapped by the trie into its store errors.
    type Error: core::error::Error + MaybeSend + MaybeSync + 'static;

    /// The node bytes behind `reference`.
    fn load(
        &self,
        reference: &EntryRef,
    ) -> impl Future<Output = Result<Vec<u8>, Self::Error>> + MaybeSend;

    /// The node bytes plus every chunk address its stored image occupies,
    /// root first, for pinning and garbage collection.
    ///
    /// Default: the bytes and the root address, the single-chunk shape.
    fn load_with_addresses(
        &self,
        reference: &EntryRef,
    ) -> impl Future<Output = Result<(Vec<u8>, Vec<ChunkAddress>), Self::Error>> + MaybeSend {
        async move {
            let bytes = self.load(reference).await?;
            Ok((bytes, alloc::vec![*reference.address()]))
        }
    }
}

/// Write seam: persist node bytes under a new reference of width `R`.
pub trait NodeSaver<R: Reference>: MaybeSend + MaybeSync {
    /// Saver failure, wrapped by the trie into its store errors.
    type Error: core::error::Error + MaybeSend + MaybeSync + 'static;

    /// Persist one node image and return its full-width reference.
    fn save(&self, data: Vec<u8>) -> impl Future<Output = Result<R, Self::Error>> + MaybeSend;
}

/// The null loader satisfies the seam for purely in-memory tries: every load
/// is a typed not-found.
impl NodeLoader for NullLoader {
    type Error = ChunkStoreError;

    async fn load(&self, reference: &EntryRef) -> Result<Vec<u8>, Self::Error> {
        Err(ChunkStoreError::not_found(reference.address()))
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

    use nectar_primitives::chunk::{ChunkOps, ChunkRef, ContentChunk};
    use nectar_primitives::error::PrimitivesError;
    use nectar_primitives::store::{ChunkPut, SharedError, TrustedGet};
    use nectar_primitives::{AnyChunkSet, Chunk, DEFAULT_BODY_SIZE, EncryptionKey};

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

    impl<S, const BS: usize> NodeLoader for SingleChunkLoadSaver<S, BS>
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
        S: ChunkPut<AnyChunkSet<BS>>,
    {
        async fn put_node(&self, data: Vec<u8>) -> Result<ChunkAddress, SingleChunkError> {
            let chunk = ContentChunk::<BS>::new(data)?;
            let address = *chunk.address();
            let sealed: Chunk<_, AnyChunkSet<BS>> = Chunk::from_envelope(chunk.into())?;
            self.0
                .put(sealed)
                .await
                .map_err(|e| SingleChunkError::Store(Arc::new(e)))?;
            Ok(address)
        }
    }

    impl<S, const BS: usize> NodeSaver<ChunkRef> for SingleChunkLoadSaver<S, BS>
    where
        S: ChunkPut<AnyChunkSet<BS>>,
    {
        type Error = SingleChunkError;

        async fn save(&self, data: Vec<u8>) -> Result<ChunkRef, Self::Error> {
            Ok(ChunkRef::new(self.put_node(data).await?))
        }
    }

    impl<S, const BS: usize> NodeSaver<EncryptedChunkRef> for SingleChunkLoadSaver<S, BS>
    where
        S: ChunkPut<AnyChunkSet<BS>>,
    {
        type Error = SingleChunkError;

        async fn save(&self, data: Vec<u8>) -> Result<EncryptedChunkRef, Self::Error> {
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
    use nectar_testing::run;

    #[test]
    fn max_node_bytes_matches_the_structural_layout() {
        // 64 header + 64 entry + 32 index + 256 * 65633 fork records.
        assert_eq!(MAX_NODE_BYTES, 16_802_208);
    }

    #[test]
    fn null_loader_load_is_not_found() {
        let reference = EntryRef::from(ChunkAddress::from([7u8; 32]));
        let err = run(NullLoader.load(&reference)).unwrap_err();
        assert!(matches!(err, ChunkStoreError::NotFound(a) if a == *reference.address()));
    }

    #[test]
    fn default_load_with_addresses_is_bytes_plus_root() {
        struct Fixed;
        impl NodeLoader for Fixed {
            type Error = ChunkStoreError;
            async fn load(&self, _: &EntryRef) -> Result<Vec<u8>, Self::Error> {
                Ok(vec![1, 2, 3])
            }
        }
        let reference = EntryRef::from(ChunkAddress::from([9u8; 32]));
        let (bytes, addresses) = run(Fixed.load_with_addresses(&reference)).unwrap();
        assert_eq!(bytes, vec![1, 2, 3]);
        assert_eq!(addresses, vec![*reference.address()]);
    }
}

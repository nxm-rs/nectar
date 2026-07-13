//! Store seam: manifest nodes over the trusted chunk store.
//!
//! A node is a content-addressed chunk whose address is the BMT of its encoded
//! payload. The store is the trust boundary: a node read back from a
//! [`TrustedStore`] is decoded straight from the certified bytes, so the read
//! path never re-hashes. The write path seals a freshly built payload into a
//! [`Verified`] content chunk, deriving the address rather than trusting one.
//!
//! [`NodeGet`] and [`NodePut`] reuse the primitives store traits unchanged;
//! they are the seam the streaming builder and reader both sit on.

use core::future::Future;

use nectar_primitives::store::{ChunkPut, MaybeSend, MaybeSync, TrustedStore};
use nectar_primitives::{Chunk, ChunkAddress, ChunkOps, ContentChunk, Verified};

use crate::codec::{DecodeError, EncodeError};
use crate::format::Format;
use crate::node::Node;

/// A manifest node sealed as a verified content chunk over the standard
/// registry, whose content-chunk member carries the node payload.
pub type NodeChunk = Chunk<Verified>;

/// A node write or read failure across the store seam.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// Encoding the node payload failed.
    #[error(transparent)]
    Encode(#[from] EncodeError),
    /// Decoding a node from stored bytes failed.
    #[error(transparent)]
    Decode(#[from] DecodeError),
    /// Sealing the payload into a content chunk failed.
    #[error("seal node chunk")]
    Seal(#[source] nectar_primitives::PrimitivesError),
    /// The backing store failed.
    #[error("store")]
    Store(#[source] Box<dyn core::error::Error + Send + Sync>),
}

impl StoreError {
    /// Box a backend error behind the seam.
    fn store<E: core::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::Store(Box::new(err))
    }
}

impl<F: Format> Node<F> {
    /// Seal this node as its content chunk, deriving the content address by
    /// BMT over the encoded payload; the address is derived, never supplied.
    pub fn to_chunk(&self) -> Result<NodeChunk, StoreError> {
        let payload = self.encode()?;
        let content = ContentChunk::new(payload).map_err(StoreError::Seal)?;
        Chunk::from_envelope(content.into()).map_err(StoreError::Seal)
    }

    /// Decode a node from a chunk the store has already certified.
    ///
    /// The [`Verified`] type is the trust boundary: the payload is decoded
    /// from the certified bytes, never re-hashed.
    pub fn from_chunk(chunk: &NodeChunk) -> Result<Self, DecodeError> {
        Self::decode(chunk.envelope().data())
    }
}

/// Async node retrieval over a trusted store.
///
/// Blanket-implemented for every [`TrustedStore`]; the `Trust = Verified`
/// bound is what lets [`get_node`](Self::get_node) skip re-hashing.
pub trait NodeGet: TrustedStore {
    /// Load and decode the node at `address`.
    fn get_node<F: Format>(
        &self,
        address: &ChunkAddress,
    ) -> impl Future<Output = Result<Node<F>, StoreError>> + MaybeSend
    where
        Self: Sized + MaybeSync,
    {
        async move {
            let chunk = self.get(address).await.map_err(StoreError::store)?;
            Ok(Node::from_chunk(&chunk)?)
        }
    }
}

impl<T: TrustedStore> NodeGet for T {}

/// Async node storage over a chunk putter.
///
/// Blanket-implemented for every [`ChunkPut`]; sealing happens before the
/// first await, so the returned future never holds the source node.
pub trait NodePut: ChunkPut {
    /// Seal `node`, store its chunk, and return the derived address.
    fn put_node<F: Format>(
        &self,
        node: &Node<F>,
    ) -> impl Future<Output = Result<ChunkAddress, StoreError>> + MaybeSend
    where
        Self: Sized + MaybeSync,
    {
        let sealed = node.to_chunk();
        async move {
            let chunk = sealed?;
            let address = *chunk.address();
            self.put(chunk).await.map_err(StoreError::store)?;
            Ok(address)
        }
    }
}

impl<T: ChunkPut> NodePut for T {}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::executor::block_on;
    use nectar_primitives::store::MemoryStore;
    use nectar_primitives::{ChunkAddress, ChunkRef, DefaultContentChunk};

    use crate::bounded::Prefix;
    use crate::meta::{KeyId, Metadata};
    use crate::node::RootExtension;
    use crate::value::Entry;

    use super::*;

    fn sample() -> Node {
        let root = RootExtension::new(
            Some(Entry::from(ChunkRef::new(ChunkAddress::new([1; 32])))),
            Some(
                Metadata::new(
                    KeyId::WebsiteIndexDocument,
                    Bytes::from_static(b"index.html"),
                )
                .unwrap(),
            ),
        );
        let mut node = Node::new(root, Default::default());
        node.forks_mut()
            .insert(
                Prefix::try_from(&b"index.html"[..]).unwrap(),
                Entry::from(ChunkRef::new(ChunkAddress::new([7; 32]))).into(),
                None,
            )
            .unwrap();
        node
    }

    #[test]
    fn round_trips_through_a_memory_store() {
        let store = MemoryStore::default();
        let node = sample();

        let address = block_on(store.put_node(&node)).unwrap();
        let loaded: Node = block_on(store.get_node(&address)).unwrap();

        assert_eq!(loaded, node);
    }

    #[test]
    fn address_is_the_content_address_of_the_payload() {
        let node = sample();
        let chunk = node.to_chunk().unwrap();
        let expected = *DefaultContentChunk::new(node.encode().unwrap())
            .unwrap()
            .address();
        assert_eq!(chunk.address(), &expected);
    }

    #[test]
    fn from_chunk_decodes_without_a_store() {
        let node = sample();
        let chunk = node.to_chunk().unwrap();
        assert_eq!(Node::from_chunk(&chunk).unwrap(), node);
    }

    #[test]
    fn missing_address_is_a_store_error() {
        let store = MemoryStore::default();
        let err = block_on(store.get_node::<crate::V1>(&ChunkAddress::new([0; 32]))).unwrap_err();
        assert!(matches!(err, StoreError::Store(_)));
    }
}

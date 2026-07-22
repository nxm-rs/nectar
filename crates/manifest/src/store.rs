//! Store seam: manifest nodes over the trusted chunk store.
//!
//! A node is a content-addressed chunk whose address is the BMT of its encoded
//! payload. The store is the trust boundary: a node read back from a
//! [`TrustedGet`] is decoded straight from the certified bytes, so the read
//! path never re-hashes. The write path seals a freshly built payload into a
//! [`Verified`] content chunk, deriving the address rather than trusting one.
//!
//! [`NodeGet`] and [`NodePut`] reuse the primitives store traits unchanged;
//! they are the seam the streaming builder and reader both sit on.

use core::future::Future;

use nectar_primitives::store::{BoxedError, ChunkPut, MaybeSend, MaybeSync, TrustedGet};
use nectar_primitives::{Chunk, ChunkAddress, ChunkOps, ContentChunk, Verified};

use crate::codec::{DecodeError, DecodedChunk, EncodeError};
use crate::fork::ForkTable;
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
    Store(#[source] BoxedError),
}

impl StoreError {
    /// Box a backend error behind the seam.
    pub(crate) fn store<E: core::error::Error + MaybeSend + MaybeSync + 'static>(err: E) -> Self {
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
/// Blanket-implemented for every [`TrustedGet`]; the `Trust = Verified`
/// bound is what lets [`get_node`](Self::get_node) skip re-hashing.
pub trait NodeGet: TrustedGet {
    /// Load and decode the node at `address`, materializing a spilled node's
    /// forks from its segments so the caller always sees one logical node.
    ///
    /// Reassembling a segmented node fetches its segment chunks and holds only
    /// that one node's forks, bounded by the fork count, so peak retained state
    /// stays O(depth).
    fn get_node<F: Format>(
        &self,
        address: &ChunkAddress,
    ) -> impl Future<Output = Result<Node<F>, StoreError>> + MaybeSend
    where
        Self: Sized + MaybeSync,
    {
        materialize_node::<Self, F>(self, address)
    }
}

impl<T: TrustedGet> NodeGet for T {}

/// The greatest legal segment-directory depth (spec 5.4); a deeper nesting is a
/// malformed image, not a tree this format ever produces.
const MAX_DIR_DEPTH: usize = 2;

/// Load the node at `address`, reassembling a segmented node's forks in place.
async fn materialize_node<S, F>(store: &S, address: &ChunkAddress) -> Result<Node<F>, StoreError>
where
    S: TrustedGet + MaybeSync,
    F: Format,
{
    let chunk = store.get(address).await.map_err(StoreError::store)?;
    match Node::<F>::decode_chunk(chunk.envelope().data())? {
        DecodedChunk::Node(node) => Ok(node),
        DecodedChunk::Segmented(root, dir) => {
            let forks = Box::pin(collect_segment_forks::<S, F>(store, &dir, 0)).await?;
            Ok(Node::new(root, forks))
        }
        // A fork child reference names a node, never a bare segment.
        DecodedChunk::Leaf(_) | DecodedChunk::Directory(_) => {
            Err(StoreError::Decode(DecodeError::SegmentContext))
        }
    }
}

/// Gather every fork of a spilled node by fetching the segments its directory
/// routes to, descending one directory level at a time.
async fn collect_segment_forks<S, F>(
    store: &S,
    dir: &crate::codec::SegmentDir,
    depth: usize,
) -> Result<ForkTable<F>, StoreError>
where
    S: TrustedGet + MaybeSync,
    F: Format,
{
    if depth >= MAX_DIR_DEPTH {
        return Err(StoreError::Decode(DecodeError::SegmentContext));
    }
    let mut table = ForkTable::new();
    for descriptor in &dir.descriptors {
        // The plain read path cannot open an encrypted segment tree.
        if descriptor.key.is_some() {
            return Err(StoreError::Decode(DecodeError::SegmentContext));
        }
        let chunk = store
            .get(&descriptor.address)
            .await
            .map_err(StoreError::store)?;
        let sub = match Node::<F>::decode_chunk(chunk.envelope().data())? {
            DecodedChunk::Leaf(sub) => sub,
            DecodedChunk::Directory(inner) => {
                Box::pin(collect_segment_forks::<S, F>(
                    store,
                    &inner,
                    depth.saturating_add(1),
                ))
                .await?
            }
            DecodedChunk::Node(_) | DecodedChunk::Segmented(_, _) => {
                return Err(StoreError::Decode(DecodeError::SegmentContext));
            }
        };
        for (first, record) in sub.into_records() {
            if table.insert_record(first, record).is_some() {
                return Err(StoreError::Decode(DecodeError::SegmentContext));
            }
        }
    }
    Ok(table)
}

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
    use nectar_primitives::store::MemoryStore;
    use nectar_primitives::{ChunkAddress, ChunkRef, DefaultContentChunk};
    use nectar_testing::run;

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

        let address = run(store.put_node(&node)).unwrap();
        let loaded: Node = run(store.get_node(&address)).unwrap();

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
        let err = run(store.get_node::<crate::V1>(&ChunkAddress::new([0; 32]))).unwrap_err();
        assert!(matches!(err, StoreError::Store(_)));
    }
}

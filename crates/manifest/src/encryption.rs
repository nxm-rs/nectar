//! Per-reference encryption: deterministic, feature-gated crypto over nodes.
//!
//! A ref64 carries `address || key`: the parent record transports the child's
//! decryption key in band, with no side channel, so whoever reads a node can
//! open every child it references, recursively. The key is derived
//! deterministically as `keccak256(F::DERIVE_TAG || secret || plaintext)`, so
//! an identical subtree under the same secret yields the same key, the same
//! ciphertext and the same address: canonical bytes and cross-build dedup
//! survive encryption.
//!
//! Encryption is a stream cipher over the exact node payload, so ciphertext
//! length equals plaintext length and the sealed chunk stays within one chunk
//! body. The chunk body is opaque to a plain reader: its bytes are not a
//! manifest preamble, so a plain decode of an encrypted chunk fails loud.
//!
//! # Privacy
//!
//! A ref64 IS a read capability for the whole subtree beneath it. Writing a
//! ref64 into a PLAINTEXT parent therefore PUBLISHES that child's key to anyone
//! who can read the parent. Confidentiality rests entirely on the outermost
//! ref64 being distributed privately: the root reference of an encrypted tree
//! is the single secret. Never place an encrypted reference in a plaintext
//! manifest you intend to publish.
//!
//! Embedding never crosses the encryption boundary (see [`embed`]): an
//! encrypted subtree inlines only into an encrypted parent, so the boundary is
//! structural, never a runtime choice.
//!
//! [`embed`]: crate::embed

use core::future::Future;

use alloy_primitives::Keccak256;
use nectar_primitives::store::{ChunkPut, MaybeSend, MaybeSync, TrustedStore};
use nectar_primitives::{
    Chunk, ChunkOps, ContentChunk, EncryptedChunkRef, EncryptionKey, transcrypt_in_place,
};

use crate::codec::DecodeError;
use crate::format::Format;
use crate::node::Node;
use crate::store::{NodeChunk, StoreError};

/// Derive the deterministic reference key `keccak256(DERIVE_TAG || secret ||
/// plaintext)`.
///
/// Keyed on the child's own plaintext, so the same plaintext under the same
/// secret always derives the same key. The tag separates this derivation from
/// any other keccak use.
#[must_use]
pub fn derive_key<F: Format>(secret: &[u8], plaintext: &[u8]) -> EncryptionKey {
    let mut hasher = Keccak256::new();
    hasher.update(F::DERIVE_TAG);
    hasher.update(secret);
    hasher.update(plaintext);
    EncryptionKey::from(hasher.finalize())
}

/// A node sealed as an encrypted chunk together with the ref64 that opens it.
///
/// The chunk is the ciphertext under its own derived address; the reference is
/// `address || key`, the capability a parent record carries to reach and
/// decrypt this node.
#[derive(Debug)]
pub struct EncryptedNode {
    chunk: NodeChunk,
    reference: EncryptedChunkRef,
}

impl EncryptedNode {
    /// The ciphertext chunk, ready to store under its derived address.
    #[must_use]
    pub const fn chunk(&self) -> &NodeChunk {
        &self.chunk
    }

    /// The ref64 that reaches and decrypts this node.
    #[must_use]
    pub const fn reference(&self) -> &EncryptedChunkRef {
        &self.reference
    }

    /// Consume into the ciphertext chunk and its ref64.
    #[must_use]
    pub fn into_parts(self) -> (NodeChunk, EncryptedChunkRef) {
        (self.chunk, self.reference)
    }
}

impl<F: Format> Node<F> {
    /// Seal this node as an encrypted content chunk under a key derived from
    /// `secret` and the node's own plaintext.
    ///
    /// The address is the BMT of the ciphertext, so an identical node under the
    /// same secret always seals to the same chunk and ref64.
    pub fn to_encrypted_chunk(&self, secret: &[u8]) -> Result<EncryptedNode, StoreError> {
        let mut payload = self.encode()?;
        let key = derive_key::<F>(secret, &payload);
        transcrypt_in_place(&key, 0, &mut payload);
        let content = ContentChunk::new(payload).map_err(StoreError::Seal)?;
        let chunk = Chunk::from_envelope(content.into()).map_err(StoreError::Seal)?;
        let reference = EncryptedChunkRef::new(*chunk.address(), key);
        Ok(EncryptedNode { chunk, reference })
    }

    /// Decrypt and decode a node from a certified encrypted chunk and its key.
    ///
    /// A wrong key decrypts to bytes that are not a manifest and the decode
    /// fails loud rather than producing a spurious node.
    pub fn from_encrypted_chunk(
        chunk: &NodeChunk,
        key: &EncryptionKey,
    ) -> Result<Self, DecodeError> {
        let mut payload = chunk.envelope().data().to_vec();
        transcrypt_in_place(key, 0, &mut payload);
        Self::decode(&payload)
    }
}

/// Async encrypted-node storage over a chunk putter.
///
/// Blanket-implemented for every [`ChunkPut`]; sealing happens before the first
/// await, so the returned future never holds the source node.
pub trait EncryptedNodePut: ChunkPut {
    /// Seal `node` under `secret`, store its ciphertext chunk, and return the
    /// ref64 that reaches and decrypts it.
    fn put_node_encrypted<F: Format>(
        &self,
        node: &Node<F>,
        secret: &[u8],
    ) -> impl Future<Output = Result<EncryptedChunkRef, StoreError>> + MaybeSend
    where
        Self: Sized + MaybeSync,
    {
        let sealed = node.to_encrypted_chunk(secret);
        async move {
            let (chunk, reference) = sealed?.into_parts();
            self.put(chunk)
                .await
                .map_err(|err| StoreError::Store(Box::new(err)))?;
            Ok(reference)
        }
    }
}

impl<T: ChunkPut> EncryptedNodePut for T {}

/// Async encrypted-node retrieval over a trusted store.
///
/// Blanket-implemented for every [`TrustedStore`]; the ref64 carries both the
/// address to fetch and the key to decrypt with.
pub trait EncryptedNodeGet: TrustedStore {
    /// Load the chunk at `reference`'s address and decrypt it with its key.
    fn get_node_encrypted<F: Format>(
        &self,
        reference: &EncryptedChunkRef,
    ) -> impl Future<Output = Result<Node<F>, StoreError>> + MaybeSend
    where
        Self: Sized + MaybeSync,
    {
        async move {
            let chunk = self
                .get(reference.address())
                .await
                .map_err(|err| StoreError::Store(Box::new(err)))?;
            Ok(Node::from_encrypted_chunk(&chunk, reference.key())?)
        }
    }
}

impl<T: TrustedStore> EncryptedNodeGet for T {}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::executor::block_on;
    use nectar_primitives::store::MemoryStore;
    use nectar_primitives::{ChunkAddress, ChunkRef};

    use crate::bounded::Prefix;
    use crate::fork::Child;
    use crate::meta::{KeyId, Metadata};
    use crate::node::RootExtension;
    use crate::value::Entry;

    use super::*;

    const SECRET: &[u8] = b"correct horse battery staple";

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
    fn derivation_is_deterministic_and_secret_dependent() {
        let plaintext = b"payload bytes";
        let a = derive_key::<crate::V1>(SECRET, plaintext);
        let b = derive_key::<crate::V1>(SECRET, plaintext);
        assert_eq!(a, b);
        assert_ne!(a, derive_key::<crate::V1>(b"other secret", plaintext));
        assert_ne!(a, derive_key::<crate::V1>(SECRET, b"other payload"));
    }

    #[test]
    fn sealing_is_deterministic_and_dedups() {
        let node = sample();
        let first = node.to_encrypted_chunk(SECRET).unwrap();
        let second = node.to_encrypted_chunk(SECRET).unwrap();
        // Same plaintext, same secret: same key, ciphertext and address.
        assert_eq!(first.reference(), second.reference());
        assert_eq!(first.chunk().address(), second.chunk().address());
        // A different secret reseals to a different address and key.
        let other = node.to_encrypted_chunk(b"different").unwrap();
        assert_ne!(first.reference(), other.reference());
        assert_ne!(first.chunk().address(), other.chunk().address());
    }

    #[test]
    fn ciphertext_is_opaque_to_a_plain_reader() {
        let node = sample();
        let plaintext = node.encode().unwrap();
        let sealed = node.to_encrypted_chunk(SECRET).unwrap();
        // The stored body is neither the plaintext nor a decodable manifest.
        assert_ne!(
            sealed.chunk().envelope().data().as_ref(),
            plaintext.as_slice()
        );
        assert!(Node::<crate::V1>::from_chunk(sealed.chunk()).is_err());
    }

    #[test]
    fn round_trips_through_the_derived_key() {
        let node = sample();
        let sealed = node.to_encrypted_chunk(SECRET).unwrap();
        let (chunk, reference) = sealed.into_parts();
        let opened: Node = Node::from_encrypted_chunk(&chunk, reference.key()).unwrap();
        assert_eq!(opened, node);
    }

    #[test]
    fn a_wrong_key_fails_to_decode() {
        let node = sample();
        let sealed = node.to_encrypted_chunk(SECRET).unwrap();
        let wrong = derive_key::<crate::V1>(b"wrong", &node.encode().unwrap());
        assert!(Node::<crate::V1>::from_encrypted_chunk(sealed.chunk(), &wrong).is_err());
    }

    #[test]
    fn round_trips_through_a_memory_store() {
        let store = MemoryStore::default();
        let node = sample();
        let reference = block_on(store.put_node_encrypted(&node, SECRET)).unwrap();
        let opened: Node = block_on(store.get_node_encrypted(&reference)).unwrap();
        assert_eq!(opened, node);
    }

    #[test]
    fn a_ref64_transports_the_child_key_into_its_parent() {
        // The privacy rule made concrete: sealing a child yields a ref64 whose
        // key is exactly the child's derived key, so a parent that records the
        // ref64 carries that key in its own bytes.
        let store = MemoryStore::default();
        let child = sample();
        let reference = block_on(store.put_node_encrypted(&child, SECRET)).unwrap();
        assert_eq!(
            reference.key(),
            &derive_key::<crate::V1>(SECRET, &child.encode().unwrap())
        );

        let mut parent = Node::empty();
        parent
            .forks_mut()
            .insert(
                Prefix::try_from(&b"dir/"[..]).unwrap(),
                Child::Ref64(reference).into(),
                None,
            )
            .unwrap();
        // The child key round-trips through the parent's own wire bytes.
        let bytes = parent.encode().unwrap();
        let decoded: Node = Node::decode(&bytes).unwrap();
        assert_eq!(decoded, parent);
    }
}

//! Per-reference encryption: deterministic, feature-gated crypto over nodes.
//!
//! A database keyed by [`EncryptedChunkRef`] is structurally encrypted: every
//! node and segment chunk is ciphertext, and every structural reference carries
//! `address || key`. The parent record transports the child's decryption key in
//! band, with no side channel, so whoever reads a node can open every child it
//! references, recursively. The key is derived deterministically as
//! `keccak256(F::DERIVE_TAG || secret || plaintext)`, so an identical subtree
//! under the same secret yields the same key, the same ciphertext and the same
//! address: canonical bytes and cross-build dedup survive encryption.
//!
//! Encryption is a stream cipher over the exact node payload, so ciphertext
//! length equals plaintext length and the sealed chunk stays within one chunk
//! body. The chunk body is opaque to a plain reader: its bytes are not a
//! manifest preamble, so a plain decode of an encrypted chunk fails loud.
//!
//! Only the write side needs the secret. Reading takes the reference alone, so
//! [`KeyLookup`](crate::KeyLookup) and [`apply`](crate::apply) open an encrypted
//! database with no extra state.
//!
//! # Privacy
//!
//! An encrypted reference IS a read capability for the whole subtree beneath
//! it. Writing one into a PLAINTEXT parent therefore PUBLISHES that child's key
//! to anyone who can read the parent. Confidentiality rests entirely on the
//! outermost reference being distributed privately: the root reference of an
//! encrypted database is the single secret. Never place an encrypted reference
//! in a plaintext manifest you intend to publish.
//!
//! The structure itself cannot mix widths: a database is keyed by one reference
//! type throughout, so an encrypted subtree can never hang off a plaintext
//! node. Only the value layer stays width-free, where an
//! [`Entry`](crate::Entry) may name an encrypted value chunk from a plaintext
//! database.

use alloc::vec::Vec;
use core::marker::PhantomData;

use alloy_primitives::Keccak256;
use nectar_primitives::{
    Chunk, ContentChunk, EncryptedChunkRef, EncryptionKey, transcrypt_in_place,
};

use crate::format::{Format, V1};
use crate::store::{NodeChunk, Seal, StoreError};

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

/// Encrypted sealing: each payload is enciphered under a key derived from
/// `secret` and the payload itself, and the reference is `address || key`.
///
/// Deriving from the plaintext is what keeps an encrypted build canonical: the
/// same subtree under the same secret always seals to the same bytes, so dedup
/// and bit-exact rebuilds survive.
#[derive(Clone, Copy)]
pub struct Encrypted<'s, F: Format = V1> {
    secret: &'s [u8],
    _format: PhantomData<F>,
}

/// Redacted: the sealer holds the master secret of a whole database, so it
/// never prints it.
impl<F: Format> core::fmt::Debug for Encrypted<'_, F> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("Encrypted(..)")
    }
}

impl<'s, F: Format> Encrypted<'s, F> {
    /// A sealer deriving every reference key from `secret`.
    #[must_use]
    pub const fn new(secret: &'s [u8]) -> Self {
        Self {
            secret,
            _format: PhantomData,
        }
    }
}

impl<F: Format> Seal<EncryptedChunkRef> for Encrypted<'_, F> {
    fn seal(&self, mut payload: Vec<u8>) -> Result<(NodeChunk, EncryptedChunkRef), StoreError> {
        let key = derive_key::<F>(self.secret, &payload);
        transcrypt_in_place(&key, 0, &mut payload);
        let content = ContentChunk::new(payload).map_err(StoreError::Seal)?;
        let chunk = Chunk::from_envelope(content.into()).map_err(StoreError::Seal)?;
        let reference = EncryptedChunkRef::new(*chunk.address(), key);
        Ok((chunk, reference))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use nectar_primitives::ChunkOps;
    use nectar_primitives::store::{ContentGet, MemoryStore};
    use nectar_primitives::{ChunkAddress, ChunkRef};
    use nectar_testing::run;

    use crate::bounded::Prefix;
    use crate::fork::Child;
    use crate::meta::{KeyId, Metadata};
    use crate::node::{Node, RootExtension};
    use crate::store::{load_node, save_node};
    use crate::value::Entry;

    use super::*;

    const SECRET: &[u8] = b"correct horse battery staple";

    fn seal() -> Encrypted<'static, V1> {
        Encrypted::new(SECRET)
    }

    fn sample() -> Node<V1, EncryptedChunkRef> {
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
        let a = derive_key::<V1>(SECRET, plaintext);
        let b = derive_key::<V1>(SECRET, plaintext);
        assert_eq!(a, b);
        assert_ne!(a, derive_key::<V1>(b"other secret", plaintext));
        assert_ne!(a, derive_key::<V1>(SECRET, b"other payload"));
    }

    #[test]
    fn sealing_is_deterministic_and_dedups() {
        let node = sample();
        let (first_chunk, first) = node.to_sealed(&seal()).unwrap();
        let (second_chunk, second) = node.to_sealed(&seal()).unwrap();
        // Same plaintext, same secret: same key, ciphertext and address.
        assert_eq!(first, second);
        assert_eq!(first_chunk.address(), second_chunk.address());
        // A different secret reseals to a different address and key.
        let (other_chunk, other) = node.to_sealed(&Encrypted::<V1>::new(b"different")).unwrap();
        assert_ne!(first, other);
        assert_ne!(first_chunk.address(), other_chunk.address());
    }

    #[test]
    fn ciphertext_is_opaque_to_a_plain_reader() {
        let node = sample();
        let plaintext = node.encode().unwrap();
        let (chunk, _) = node.to_sealed(&seal()).unwrap();
        // The stored body is neither the plaintext nor a decodable manifest.
        assert_ne!(chunk.envelope().data().as_ref(), plaintext.as_slice());
        assert!(Node::<V1>::decode(chunk.envelope().data()).is_err());
    }

    #[test]
    fn round_trips_through_the_derived_key() {
        let node = sample();
        let (chunk, reference) = node.to_sealed(&seal()).unwrap();
        let opened = Node::<V1, EncryptedChunkRef>::from_chunk(&chunk, &reference).unwrap();
        assert_eq!(opened, node);
    }

    #[test]
    fn a_wrong_key_fails_to_decode() {
        let node = sample();
        let (chunk, reference) = node.to_sealed(&seal()).unwrap();
        let wrong = EncryptedChunkRef::new(
            *reference.address(),
            derive_key::<V1>(b"wrong", &node.encode().unwrap()),
        );
        assert!(Node::<V1, EncryptedChunkRef>::from_chunk(&chunk, &wrong).is_err());
    }

    #[test]
    fn round_trips_through_a_memory_store() {
        let store = ContentGet::new(MemoryStore::default());
        let node = sample();
        let reference = run(save_node(&store, &node, &seal())).unwrap();
        let opened: Node<V1, EncryptedChunkRef> = run(load_node(&store, &reference)).unwrap();
        assert_eq!(opened, node);
    }

    #[test]
    fn a_reference_transports_the_child_key_into_its_parent() {
        // The privacy rule made concrete: sealing a child yields a reference
        // whose key is exactly the child's derived key, so a parent that
        // records the reference carries that key in its own bytes.
        let store = ContentGet::new(MemoryStore::default());
        let child = sample();
        let reference = run(save_node(&store, &child, &seal())).unwrap();
        assert_eq!(
            reference.key(),
            &derive_key::<V1>(SECRET, &child.encode().unwrap())
        );

        let mut parent = Node::<V1, EncryptedChunkRef>::empty();
        parent
            .forks_mut()
            .insert(
                Prefix::try_from(&b"dir/"[..]).unwrap(),
                Child::Ref(reference).into(),
                None,
            )
            .unwrap();
        // The child key round-trips through the parent's own wire bytes.
        let bytes = parent.encode().unwrap();
        let decoded = Node::<V1, EncryptedChunkRef>::decode(&bytes).unwrap();
        assert_eq!(decoded, parent);
    }

    // The width witness in the flags byte is what makes a mis-typed read fail
    // loud instead of parsing 64-byte child references as 32-byte ones.
    #[test]
    fn the_width_witness_rejects_a_mis_typed_decode() {
        let encrypted = sample().encode().unwrap();
        assert!(Node::<V1>::decode(&encrypted).is_err());

        let plain = Node::<V1>::empty().encode().unwrap();
        assert!(Node::<V1, EncryptedChunkRef>::decode(&plain).is_err());
    }
}

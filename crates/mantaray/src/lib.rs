//! Mantaray manifest trie for Ethereum Swarm.
//!
//! Dedicated to the memory of ldeffenb, whose guidance on manifest generation
//! made this implementation possible.
//!
//! Mantaray is a trie-based manifest structure that maps human-readable paths
//! (e.g. `index.html`, `img/logo.png`) to content-addressed chunk references.
//! It supports XOR obfuscation, versioned binary serialization (v0.1 and v0.2),
//! and metadata per path.
//!
//! # Efficient Partial Updates
//!
//! The trie uses lazy loading and dirty-reference tracking so that updating a
//! single path in a million-entry manifest only re-serializes O(depth) nodes:
//!
//! 1. [`Manifest::add`] lazily loads only the affected path branch.
//! 2. Modified nodes have their reference cleared (dirty flag).
//! 3. [`Manifest::save`] skips nodes with non-empty references (unmodified).
//! 4. After save, child forks are dropped from memory.
//! 5. The next operation lazily reloads from the new state.
//!
//! # Unified Store
//!
//! Manifest operations use the async typed chunk store traits from
//! `nectar_primitives`: [`ChunkGet`](nectar_primitives::store::ChunkGet) for
//! loading and [`ChunkPut`](nectar_primitives::store::ChunkPut) for saving.
//! This means a single [`MemoryStore`] can hold both file chunks and manifest
//! trie nodes.
//!
//! ```no_run
//! # use nectar_mantaray::{PlainManifest, Entry, DefaultMemoryStore};
//! let store = DefaultMemoryStore::new();
//! let mut manifest: PlainManifest<_> = PlainManifest::new(store);
//! ```
//!
//! # Website Manifests
//!
//! Configure index and error documents for Swarm-hosted websites:
//!
//! ```no_run
//! # use nectar_mantaray::{PlainManifest, Entry, metadata, DefaultMemoryStore};
//! # let store = DefaultMemoryStore::new();
//! # let mut manifest = PlainManifest::new(store);
//! # futures::executor::block_on(async {
//! manifest.set_index_document("index.html").await.unwrap();
//! manifest.set_error_document("404.html").await.unwrap();
//! # });
//! ```
//!
//! # Metadata Constants
//!
//! Well-known metadata keys are available in the [`metadata`] module:
//!
//! ```
//! use nectar_mantaray::metadata;
//! assert_eq!(metadata::CONTENT_TYPE, "Content-Type");
//! ```
//!
//! # Raw encode containment
//!
//! Node bytes are produced only inside [`Manifest::save`] and consumed only
//! on load; a node handed out by [`Manifest::root`], [`Manifest::walk`], or
//! [`Manifest::into_parts`] carries no public encode:
//!
//! ```compile_fail
//! use nectar_mantaray::{DefaultMemoryStore, PlainManifest};
//!
//! let manifest: PlainManifest<_> = PlainManifest::new(DefaultMemoryStore::new());
//! let bytes: Vec<u8> = Vec::try_from(manifest.root()).unwrap();
//! ```
//!
//! The raw node internals exist only under the `hazmat` feature, for fuzz
//! harnesses and benches; without it the module does not resolve:
//!
#![cfg_attr(not(feature = "hazmat"), doc = "```compile_fail")]
#![cfg_attr(feature = "hazmat", doc = "```")]
//! use nectar_mantaray::hazmat::{self, Node};
//!
//! let node: Node = Node::new_unencrypted();
//! let bytes = hazmat::encode(&node).unwrap();
//! let decoded: Node = hazmat::decode(&bytes).unwrap();
//! assert!(decoded.entry().is_none());
//! ```
//!
//! # Upstream-bug workarounds
//!
//! Code that exists solely to tolerate a defect in an upstream reference
//! implementation is tagged with a grep-able `BEE-WORKAROUND(bee#NNNN)`
//! comment. When the upstream fix lands and downstream consumers have
//! upgraded past the buggy releases, every site tagged with that issue
//! number should be removed. Run `git grep -n BEE-WORKAROUND` to enumerate
//! them.

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::get_unwrap,
        clippy::indexing_slicing,
        clippy::string_slice,
        clippy::arithmetic_side_effects,
        clippy::panic,
        clippy::unreachable,
        clippy::panic_in_result_fn
    )
)]

// `alloc` backs the fork maps (`BTreeMap`) and shared error sources (`Arc`).
// `nectar-primitives`, a hard dependency, already requires an allocator.
extern crate alloc;

use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::ChunkRef;

pub mod builder;
pub mod codec;
mod constants;
pub mod entry;
pub mod error;
pub mod manifest;
pub mod manifest_ref;
mod node;
pub mod obfuscation;

// Re-export constants.
pub use constants::metadata;
pub(crate) use constants::*;

// Re-export public types.
pub use builder::ManifestBuilder;
pub use entry::Entry;
pub use error::{DecodeError, DecodeResult, MantarayError, Result};
pub use manifest::{Manifest, ManifestIter};
pub use manifest_ref::ManifestRef;
pub use node::NodeType;
pub use obfuscation::ObfuscationKey;

/// Raw node internals for fuzz harnesses and benches only.
///
/// Not part of the public API and exempt from semver guarantees. Compiled
/// only under the `hazmat` feature; normal builds carry no raw node types
/// and no raw encode or decode surface.
#[cfg(feature = "hazmat")]
#[doc(hidden)]
pub mod hazmat {
    use nectar_primitives::chunk::Reference;

    pub use crate::node::{Fork, Node};

    /// Encode a raw node into its wire image.
    pub fn encode<R: Reference>(node: &Node<R>) -> crate::Result<Vec<u8>> {
        node.encode()
    }

    /// Decode a wire image into a raw node.
    pub fn decode<R: Reference>(bytes: &[u8]) -> crate::DecodeResult<Node<R>> {
        Node::decode(bytes)
    }
}

// Re-export typed storage traits from primitives.
pub use nectar_primitives::DefaultMemoryStore;
pub use nectar_primitives::store::{ChunkGet, ChunkHas, ChunkPut, MemoryStore};

/// Default manifest type using [`DEFAULT_BODY_SIZE`] and plain mode.
pub type DefaultManifest<S> = PlainManifest<S, DEFAULT_BODY_SIZE>;

/// Plain manifest: 32-byte refs, no obfuscation.
pub type PlainManifest<S, const BS: usize = DEFAULT_BODY_SIZE> = Manifest<S, ChunkRef, BS>;

/// Encrypted manifest: 64-byte refs, random obfuscation key.
pub type EncryptedManifest<S, const BS: usize = DEFAULT_BODY_SIZE> =
    Manifest<S, nectar_primitives::EncryptedChunkRef, BS>;

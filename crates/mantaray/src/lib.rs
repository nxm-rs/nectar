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
//! # Streaming Surface
//!
//! Three complementary handles over a typed chunk store cover the manifest
//! lifecycle:
//!
//! - [`Reader`]: depth-guarded point lookups ([`Reader::get`],
//!   [`Reader::has_prefix`]) with `Ok(None)` on a miss.
//! - [`Cursor`] and [`AddressStream`]: ordered listing with bounded
//!   read-ahead.
//! - [`ManifestEditor`]: records puts and removes, then commits them in
//!   submission order with a bounded number of puts in flight.
//!
//! ```no_run
//! # use nectar_mantaray::{ManifestEditor, DefaultMemoryStore};
//! let mut editor: ManifestEditor<_> = ManifestEditor::new(DefaultMemoryStore::new());
//! ```
//!
//! The store traits come from `nectar_primitives` ([`ChunkGet`],
//! [`ChunkPut`]), so a single [`MemoryStore`] can hold both file chunks and
//! manifest trie nodes.
//!
//! The former lazy-trie surface lives in [`legacy`] with a migration table;
//! its removal is gated on the manifest 1.0 key-value store replacing it.
//!
//! # Website Manifests
//!
//! Configure index and error documents for Swarm-hosted websites:
//!
//! ```no_run
//! # use nectar_mantaray::{ManifestEditor, DefaultMemoryStore};
//! let mut editor: ManifestEditor<_> = ManifestEditor::new(DefaultMemoryStore::new());
//! editor.set_index_document("index.html");
//! editor.set_error_document("404.html");
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
//! Node bytes are produced only inside a save or commit and consumed only on
//! load; no public handle carries an encode:
//!
//! ```compile_fail
//! use nectar_mantaray::{DefaultMemoryStore, ManifestEditor};
//!
//! let editor: ManifestEditor<_> = ManifestEditor::new(DefaultMemoryStore::new());
//! let bytes: Vec<u8> = Vec::try_from(editor).unwrap();
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

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
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
// The in-crate tests drive the deprecated legacy surface as the differential
// oracle for the streaming modules.
#![cfg_attr(test, allow(deprecated))]

// `alloc` backs the fork maps (`BTreeMap`) and shared error sources (`Arc`).
// `nectar-primitives`, a hard dependency of the trie modules, already
// requires an allocator.
#[cfg(feature = "std")]
extern crate alloc;

#[cfg(feature = "std")]
use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
#[cfg(feature = "std")]
use nectar_primitives::chunk::ChunkRef;

#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub mod codec;
mod constants;
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub mod cursor;
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub mod editor;
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub mod entry;
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub mod error;
#[cfg(feature = "std")]
mod format;
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
#[deprecated(note = "superseded by Reader, Cursor, and ManifestEditor")]
pub mod legacy;
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub mod manifest_ref;
#[cfg(feature = "std")]
mod node;
pub mod obfuscation;
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub mod reader;
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub mod view;

// Re-export constants.
pub use constants::metadata;
#[cfg(feature = "std")]
pub(crate) use constants::*;

// Re-export public types.
#[cfg(feature = "std")]
pub use cursor::{AddressStream, Cursor, Window};
#[cfg(feature = "std")]
pub use editor::{DEFAULT_PUT_WIDTH, ManifestEditor, Op};
#[cfg(feature = "std")]
pub use entry::Entry;
#[cfg(feature = "std")]
pub use error::{
    CursorError, DecodeError, DecodeResult, EditorError, MantarayError, ReaderError, Result,
};
#[allow(deprecated)]
#[cfg(feature = "std")]
pub use legacy::{DEFAULT_LIST_CONCURRENCY, Manifest, ManifestBuilder, ManifestIter};
#[cfg(feature = "std")]
pub use manifest_ref::ManifestRef;
#[cfg(feature = "std")]
pub use node::NodeType;
pub use obfuscation::ObfuscationKey;
#[cfg(feature = "std")]
pub use reader::{DEFAULT_MAX_DEPTH, Reader};
#[cfg(feature = "std")]
pub use view::{ForkView, NodeView, RefWidth, Version};

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
#[cfg(feature = "std")]
pub use nectar_primitives::DefaultMemoryStore;
#[cfg(feature = "std")]
pub use nectar_primitives::store::{ChunkGet, ChunkHas, ChunkPut, MemoryStore, TrustedGet};

/// Default manifest type using [`DEFAULT_BODY_SIZE`] and plain mode.
#[allow(deprecated)]
#[deprecated(note = "superseded by ManifestEditor and Reader")]
#[cfg(feature = "std")]
pub type DefaultManifest<S> = PlainManifest<S, DEFAULT_BODY_SIZE>;

/// Plain manifest: 32-byte refs, no obfuscation.
#[allow(deprecated)]
#[deprecated(note = "superseded by ManifestEditor and Reader")]
#[cfg(feature = "std")]
pub type PlainManifest<S, const BS: usize = DEFAULT_BODY_SIZE> = Manifest<S, ChunkRef, BS>;

/// Encrypted manifest: 64-byte refs, random obfuscation key.
#[allow(deprecated)]
#[deprecated(note = "superseded by ManifestEditor and Reader")]
#[cfg(feature = "std")]
pub type EncryptedManifest<S, const BS: usize = DEFAULT_BODY_SIZE> =
    Manifest<S, nectar_primitives::EncryptedChunkRef, BS>;

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
//! Three complementary handles over the node persistence seam cover the
//! manifest lifecycle:
//!
//! - [`Reader`]: depth-guarded point lookups ([`Reader::get`],
//!   [`Reader::has_prefix`]) with `Ok(None)` on a miss.
//! - [`Cursor`] and [`AddressStream`]: ordered listing with bounded
//!   read-ahead.
//! - [`ManifestEditor`]: records puts and removes, then commits them in
//!   submission order.
//!
//! All four are generic over the [`persist`] seam ([`NodeLoader`],
//! [`NodeSaver`]): the trie never touches chunk stores directly, so the
//! storage layout is the adapter's. The production `NodeLoadSaver` adapter
//! (behind the `manifest` feature) adapts a chunk store through the file
//! pipeline, storing a node larger than one chunk across several, as the
//! reference client does.
//!
//! # Website Manifests
//!
//! Configure index and error documents for Swarm-hosted websites. They are
//! metadata on the [`metadata::ROOT_PATH`] node, where the reference client
//! writes them. Each is set through a merge, so the two stay independent:
//!
//! ```no_run
//! # use nectar_mantaray::{ManifestEditor, DefaultMemoryStore};
//! let mut editor: ManifestEditor<_> = ManifestEditor::new(DefaultMemoryStore::new());
//! editor.set_index_document("index.html");
//! editor.set_error_document("404.html");
//! ```
//!
//! [`ManifestEditor::clear_root_metadata`] takes one back out, and prunes the
//! node when it carries nothing else.
//!
//! Content paths are stored bare and verbatim, so `index.html` is the trie key
//! `index.html`. That is the reference client's v0.2 wire.
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
//! # Spec references
//!
//! Reference-client source citations for the wire format live in `SPEC.md`
//! at the crate root.
//!
//! # Legacy tolerances
//!
//! Code that exists solely to keep decoding historical images emitted by a
//! since-fixed defect in the reference client's writer is tagged with a
//! grep-able `LEGACY-TOLERANCE(name)` comment. Content-addressed data is
//! retrievable forever, so these decode-side tolerances are permanent; the
//! encoder never emits the shapes. Run `git grep -n LEGACY-TOLERANCE` to
//! enumerate them.

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
        clippy::panic_in_result_fn,
        clippy::as_conversions
    )
)]
// `alloc` backs the fork maps (`BTreeMap`) and shared error sources (`Arc`).
// `nectar-primitives`, a hard dependency of the trie modules, already
// requires an allocator.
#[cfg(feature = "std")]
extern crate alloc;

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
#[cfg(feature = "manifest")]
#[cfg_attr(docsrs, doc(cfg(feature = "manifest")))]
pub mod manifest;
#[cfg(feature = "std")]
mod node;
pub mod obfuscation;
/// Shared fuzz and test oracles over the raw node codec and the node view.
/// Compiled for in-crate tests and for fuzz builds (`hazmat` plus
/// `arbitrary`); exempt from semver guarantees.
#[cfg(any(test, all(feature = "arbitrary", feature = "hazmat")))]
#[doc(hidden)]
pub mod oracles;
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub mod persist;
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
pub use editor::{ManifestEditor, Op};
#[cfg(feature = "std")]
pub use entry::Entry;
#[cfg(feature = "std")]
pub use error::{
    CursorError, DecodeError, DecodeResult, EditorError, MantarayError, ReaderError, Result,
};
#[cfg(feature = "manifest")]
pub use manifest::{MantarayManifest, TrieCursor, TrieFormatError, TrieView};
#[cfg(feature = "std")]
pub use node::NodeType;
pub use obfuscation::ObfuscationKey;
#[cfg(feature = "std")]
pub use persist::{MAX_NODE_BYTES, NodeLoader, NodeSaver};
#[cfg(feature = "manifest")]
pub use persist::{NodeCollectError, NodeLoadSaver};
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

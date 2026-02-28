//! Mantaray manifest trie for Ethereum Swarm.
//!
//! Dedicated to the memory of ldeffenb, whose guidance on manifest generation
//! made this implementation possible.
//!
//! Mantaray is a trie-based manifest structure that maps human-readable paths
//! (e.g. `index.html`, `img/logo.png`) to content-addressed chunk references.
//! It supports XOR obfuscation, versioned binary serialisation (v0.1 and v0.2),
//! and metadata per path.
//!
//! # Efficient Partial Updates
//!
//! The trie uses lazy loading and dirty-reference tracking so that updating a
//! single path in a million-entry manifest only re-serialises O(depth) nodes:
//!
//! 1. [`Manifest::add`] lazily loads only the affected path branch.
//! 2. Modified nodes have their reference cleared (dirty flag).
//! 3. [`Manifest::save`] skips nodes with non-empty references (unmodified).
//! 4. After save, child forks are dropped from memory.
//! 5. The next operation lazily reloads from the new state.
//!
//! # Unified Store
//!
//! Manifest operations use the typed chunk store traits from `nectar_primitives`:
//! [`ChunkGet`] for loading and [`ChunkPut`] for saving. This means a single
//! [`MemoryStore`] can hold both file chunks and manifest trie nodes.
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
//! manifest.set_index_document("index.html").unwrap();
//! manifest.set_error_document("404.html").unwrap();
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

use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::ChunkAddress;

mod constants;
pub mod entry;
pub mod error;
pub mod manifest;
pub mod manifest_ref;
pub mod codec;
pub mod mode;
pub mod node;
pub mod obfuscation;

// Re-export constants.
pub use constants::metadata;
pub(crate) use constants::*;

// Re-export public types.
pub use entry::Entry;
pub use error::{MantarayError, Result};
pub use manifest::{Manifest, ManifestIter};
pub use manifest_ref::ManifestRef;
pub use mode::NodeEntry;
pub use node::{Fork, Node, NodeType, Prefix};
pub use obfuscation::ObfuscationKey;

// Re-export typed storage traits from primitives.
pub use nectar_primitives::store::{ChunkHas, MemoryStore};
pub use nectar_primitives::DefaultMemoryStore;

/// Default manifest type using [`DEFAULT_BODY_SIZE`] and plain mode.
pub type DefaultManifest<S> = PlainManifest<S, DEFAULT_BODY_SIZE>;

/// Plain manifest: 32-byte refs, no obfuscation.
pub type PlainManifest<S, const BS: usize = DEFAULT_BODY_SIZE> = Manifest<S, ChunkAddress, BS>;

/// Encrypted manifest: 64-byte refs, random obfuscation key.
#[cfg(feature = "encryption")]
pub type EncryptedManifest<S, const BS: usize = DEFAULT_BODY_SIZE> =
    Manifest<S, nectar_primitives::EncryptedChunkRef, BS>;
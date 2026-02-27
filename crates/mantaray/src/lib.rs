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

use std::collections::BTreeMap;

use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::ChunkAddress;
use nectar_primitives::file::EntryRef;
use nectar_primitives::store::{ChunkGet, ChunkPut};

pub mod error;
pub mod manifest_ref;
pub mod marshal;
pub mod mode;
pub mod node;
pub mod obfuscation;

pub use error::{MantarayError, Result};
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

/// Well-known metadata keys matching Go bee's `pkg/manifest/manifest.go`.
pub mod metadata {
    /// Root path for manifest-level metadata.
    pub const ROOT_PATH: &str = "/";

    /// Website index document suffix (e.g., "index.html").
    pub const WEBSITE_INDEX_DOCUMENT: &str = "website-index-document";

    /// Website error document path (e.g., "404.html").
    pub const WEBSITE_ERROR_DOCUMENT: &str = "website-error-document";

    /// Content type (MIME type) of an entry.
    pub const CONTENT_TYPE: &str = "Content-Type";

    /// Original filename of an entry.
    pub const FILENAME: &str = "Filename";
}

// Path separator used in Swarm manifests.
const PATH_SEPARATOR: &str = "/";

// Node header field sizes.
const NODE_OBFUSCATION_KEY_SIZE: usize = 32;
const VERSION_HASH_SIZE: usize = 31;
const NODE_REF_BYTES_SIZE: usize = 1;
const NODE_HEADER_SIZE: usize = NODE_OBFUSCATION_KEY_SIZE + VERSION_HASH_SIZE + NODE_REF_BYTES_SIZE;

// Fork layout constants.
const NODE_FORK_TYPE_BYTES_SIZE: usize = 1;
const NODE_FORK_PREFIX_BYTES_SIZE: usize = 1;
const NODE_FORK_HEADER_SIZE: usize = NODE_FORK_TYPE_BYTES_SIZE + NODE_FORK_PREFIX_BYTES_SIZE;
const NODE_FORK_PRE_REFERENCE_SIZE: usize = 32;
const NODE_PREFIX_MAX_SIZE: usize = NODE_FORK_PRE_REFERENCE_SIZE - NODE_FORK_HEADER_SIZE;
const NODE_FORK_METADATA_BYTES_SIZE: usize = 2;

/// A manifest entry: a path, typed reference, and optional metadata.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Entry {
    /// The path for this entry.
    path: Vec<u8>,
    /// The typed chunk reference (`None` for empty/non-value entries like root metadata).
    reference: Option<EntryRef>,
    /// Key-value metadata.
    metadata: BTreeMap<String, String>,
}

impl Entry {
    /// Create a new entry with the given chunk reference.
    pub fn new(reference: impl Into<EntryRef>) -> Self {
        Self {
            reference: Some(reference.into()),
            path: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }

    /// The path for this entry.
    pub fn path(&self) -> &[u8] {
        &self.path
    }

    /// The typed chunk reference.
    pub const fn reference(&self) -> Option<&EntryRef> {
        self.reference.as_ref()
    }

    /// Key-value metadata.
    pub const fn metadata(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }

    /// Set the content type (MIME type) metadata.
    pub fn with_content_type(mut self, ct: &str) -> Self {
        self.metadata
            .insert(metadata::CONTENT_TYPE.into(), ct.into());
        self
    }

    /// Set the filename metadata.
    pub fn with_filename(mut self, name: &str) -> Self {
        self.metadata
            .insert(metadata::FILENAME.into(), name.into());
        self
    }

    /// Set an arbitrary metadata key-value pair.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Get the content type metadata value, if present.
    pub fn content_type(&self) -> Option<&str> {
        self.metadata.get(metadata::CONTENT_TYPE).map(|s| s.as_str())
    }

    /// Get the filename metadata value, if present.
    pub fn filename(&self) -> Option<&str> {
        self.metadata.get(metadata::FILENAME).map(|s| s.as_str())
    }

    /// Path as a UTF-8 string. Returns `None` if the path is not valid UTF-8.
    pub fn path_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.path).ok()
    }

    /// Chunk address from the reference.
    pub fn address(&self) -> Option<&ChunkAddress> {
        self.reference.as_ref().map(|r| r.address())
    }

    /// Reconstruct an `Entry` from a trie node and its accumulated path.
    ///
    /// Converts the typed `Option<&E>` entry to an `Option<EntryRef>` via
    /// serialization, keeping `Entry` non-generic for the public API.
    pub(crate) fn from_node<E: NodeEntry>(path: &[u8], node: &Node<E>) -> Result<Self> {
        let reference = match node.entry() {
            Some(e) => {
                let bytes = e.to_bytes();
                Some(EntryRef::try_from_bytes(&bytes).map_err(|_| {
                    MantarayError::EntrySizeMismatch {
                        expected: E::SIZE,
                        actual: bytes.len(),
                    }
                })?)
            }
            None => None,
        };
        let metadata = if node.metadata().is_empty() {
            BTreeMap::new()
        } else {
            node.metadata().clone()
        };
        Ok(Self {
            path: path.to_vec(),
            reference,
            metadata,
        })
    }
}

impl From<ChunkAddress> for Entry {
    fn from(address: ChunkAddress) -> Self {
        Self::new(address)
    }
}

#[cfg(feature = "encryption")]
impl From<nectar_primitives::EncryptedChunkRef> for Entry {
    fn from(enc_ref: nectar_primitives::EncryptedChunkRef) -> Self {
        Self::new(enc_ref)
    }
}

/// High-level mantaray manifest backed by a typed chunk store.
///
/// The entry type parameter `E` determines:
/// - What reference types `add()` accepts (compile-time enforcement)
/// - The reference byte size via `E::SIZE`
/// - What `save()` returns (specialized per entry type)
#[derive(Debug)]
pub struct Manifest<S, E: NodeEntry = ChunkAddress, const BS: usize = DEFAULT_BODY_SIZE> {
    trie: Node<E>,
    store: S,
}

// --- Plain-mode constructors ---

impl<S, const BS: usize> Manifest<S, ChunkAddress, BS> {
    /// Create a new plain manifest (no obfuscation, 32-byte refs).
    pub fn new(store: S) -> Self {
        let trie = Node::new_unencrypted();
        Self { trie, store }
    }

    /// Load a plain manifest from storage by its root chunk address.
    pub fn open(root: ChunkAddress, store: S) -> Self {
        let trie = Node::from_reference(root);
        Self { trie, store }
    }
}

// --- Encrypted-mode constructors ---

#[cfg(feature = "encryption")]
impl<S, const BS: usize> Manifest<S, nectar_primitives::EncryptedChunkRef, BS> {
    /// Create a new encrypted manifest (random obfuscation key, 64-byte refs).
    pub fn new_encrypted(store: S) -> Self {
        let mut trie = Node::default();
        trie.obfuscation_key = ObfuscationKey::generate();
        Self { trie, store }
    }

    /// Load an encrypted manifest from storage by its manifest reference.
    pub fn open_encrypted(root: ManifestRef, store: S) -> Self {
        let (addr, key) = root.into_parts();
        let mut trie = Node::from_reference(addr);
        trie.obfuscation_key = key;
        Self { trie, store }
    }
}

// --- Entry-type-agnostic methods ---

impl<S, E: NodeEntry, const BS: usize> Manifest<S, E, BS> {
    /// Access the underlying chunk store.
    pub const fn store(&self) -> &S {
        &self.store
    }

    /// Access the root trie node.
    pub const fn root(&self) -> &Node<E> {
        &self.trie
    }

    /// Mutable access to the root trie node.
    pub const fn root_mut(&mut self) -> &mut Node<E> {
        &mut self.trie
    }

    /// Consume the manifest and return its parts.
    pub fn into_parts(self) -> (Node<E>, S) {
        (self.trie, self.store)
    }

    /// Get the root reference (`None` if not yet saved).
    pub const fn reference(&self) -> Option<&ChunkAddress> {
        self.trie.reference()
    }
}

impl<S: ChunkGet<BS>, E: NodeEntry, const BS: usize> Manifest<S, E, BS> {
    /// Add a path with a typed reference (compile-time enforced by entry type).
    pub fn add(&mut self, path: &str, reference: impl Into<E>) -> Result<()> {
        let entry = reference.into();
        self.trie.add_with_loader::<S, BS>(
            path.as_bytes(),
            Some(entry),
            BTreeMap::new(),
            &self.store,
        )
    }

    /// Add a path with a typed reference and metadata.
    pub fn add_with_metadata(
        &mut self,
        path: &str,
        reference: impl Into<E>,
        metadata: BTreeMap<String, String>,
    ) -> Result<()> {
        let entry = reference.into();
        self.trie.add_with_loader::<S, BS>(
            path.as_bytes(),
            Some(entry),
            metadata,
            &self.store,
        )
    }

    /// Add a path with a pre-built [`Entry`] (metadata + reference).
    pub fn add_entry(&mut self, path: &str, entry: Entry) -> Result<()> {
        let e = match entry.reference {
            Some(r) => {
                let bytes = Vec::from(&r);
                Some(E::try_from_bytes(&bytes)?)
            }
            None => None,
        };
        self.trie.add_with_loader::<S, BS>(
            path.as_bytes(),
            e,
            entry.metadata,
            &self.store,
        )
    }

    /// Remove a path from the manifest.
    pub fn remove(&mut self, path: &str) -> Result<()> {
        self.trie
            .remove_with_loader::<S, BS>(path.as_bytes(), &self.store)
    }

    /// Look up a path in the manifest.
    pub fn lookup(&mut self, path: &str) -> Result<Entry> {
        let node = self
            .trie
            .lookup_node_with_loader::<S, BS>(path.as_bytes(), &self.store)?;

        if !node.is_value() {
            return Err(MantarayError::NotValueType);
        }

        Entry::from_node(path.as_bytes(), node)
    }

    /// Test whether the manifest contains a prefix.
    pub fn has_prefix(&mut self, prefix: &str) -> Result<bool> {
        self.trie
            .has_prefix_with_loader::<S, BS>(prefix.as_bytes(), &self.store)
    }

    /// Walk all nodes depth-first, calling `f` for each node with its path.
    pub fn walk<F>(&mut self, f: &mut F) -> Result<()>
    where
        F: FnMut(&[u8], &Node<E>) -> Result<()>,
    {
        self.trie.walk_with_loader::<S, BS, _>(&self.store, f)
    }

    /// Collect all value entries from the manifest.
    ///
    /// Convenience wrapper around [`iter()`](Self::iter). Prefer `iter()` for
    /// lazy traversal.
    pub fn entries(&mut self) -> Result<Vec<Entry>> {
        self.iter().collect()
    }

    /// Set the website index document on the root path metadata.
    pub fn set_index_document(&mut self, filename: &str) -> Result<()> {
        self.set_root_metadata(metadata::WEBSITE_INDEX_DOCUMENT, filename)
    }

    /// Set the website error document on the root path metadata.
    pub fn set_error_document(&mut self, path: &str) -> Result<()> {
        self.set_root_metadata(metadata::WEBSITE_ERROR_DOCUMENT, path)
    }

    /// Get the website index document from root path metadata.
    pub fn index_document(&mut self) -> Result<Option<String>> {
        self.get_root_metadata(metadata::WEBSITE_INDEX_DOCUMENT)
    }

    /// Get the website error document from root path metadata.
    pub fn error_document(&mut self) -> Result<Option<String>> {
        self.get_root_metadata(metadata::WEBSITE_ERROR_DOCUMENT)
    }

    /// Walk all nodes, yielding both node references and entry references.
    ///
    /// This is useful for garbage collection and pinning — it enumerates every
    /// chunk address the manifest depends on. Matches Go bee's
    /// `IterateAddresses` in `pkg/manifest/mantaray.go`.
    pub fn iterate_addresses<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        self.walk(&mut |_path, node| {
            if let Some(addr) = node.reference() {
                f(addr.as_bytes())?;
            }

            if let Some(entry) = node.entry() {
                if node.is_value() {
                    let entry_bytes = entry.to_bytes();
                    f(&entry_bytes)?;
                }
            }

            Ok(())
        })
    }

    /// Create a lazy depth-first iterator over all entries in the manifest.
    ///
    /// Nodes are loaded from storage on demand, so the entire trie does not
    /// need to be in memory at once.
    pub const fn iter(&mut self) -> ManifestIter<'_, S, E, BS> {
        ManifestIter::new(&mut self.trie, &self.store)
    }

    fn set_root_metadata(&mut self, key: &str, value: &str) -> Result<()> {
        // Ensure the root path node exists.
        match self
            .trie
            .lookup_node_with_loader::<S, BS>(metadata::ROOT_PATH.as_bytes(), &self.store)
        {
            Ok(node) => {
                // Node exists — mutate metadata in place (no clone).
                node.metadata_mut().insert(key.into(), value.into());
                node.make_with_metadata();
                node.mark_dirty();
                Ok(())
            }
            Err(MantarayError::NoForkFound { .. }) => {
                // Root path doesn't exist yet — create it with the metadata.
                let mut meta = BTreeMap::new();
                meta.insert(key.into(), value.into());
                self.trie.add_with_loader::<S, BS>(
                    metadata::ROOT_PATH.as_bytes(),
                    None,
                    meta,
                    &self.store,
                )
            }
            Err(e) => Err(e),
        }
    }

    fn get_root_metadata(&mut self, key: &str) -> Result<Option<String>> {
        match self
            .trie
            .lookup_node_with_loader::<S, BS>(metadata::ROOT_PATH.as_bytes(), &self.store)
        {
            Ok(node) => Ok(node.metadata().get(key).cloned()),
            Err(MantarayError::NoForkFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

// --- Mode-specific save() ---

impl<S: ChunkGet<BS> + ChunkPut<BS>, const BS: usize> Manifest<S, ChunkAddress, BS> {
    /// Persist the plain manifest trie to storage, returning the root chunk address.
    pub fn save(&mut self) -> Result<ChunkAddress> {
        self.trie.save::<S, BS>(&mut self.store)?;
        Ok(*self.trie.reference().ok_or(MantarayError::MissingReference)?)
    }
}

#[cfg(feature = "encryption")]
impl<S: ChunkGet<BS> + ChunkPut<BS>, const BS: usize>
    Manifest<S, nectar_primitives::EncryptedChunkRef, BS>
{
    /// Persist the encrypted manifest trie, returning a [`ManifestRef`].
    pub fn save(&mut self) -> Result<ManifestRef> {
        self.trie.save::<S, BS>(&mut self.store)?;
        let addr = *self.trie.reference().ok_or(MantarayError::MissingReference)?;
        Ok(ManifestRef::new(addr, self.trie.obfuscation_key))
    }
}

/// Lazy depth-first iterator over manifest entries.
///
/// Loads nodes from storage on demand. Each call to `next()` may perform
/// storage reads as it traverses unloaded parts of the trie.
///
/// Uses raw node pointers for O(1) per-step traversal. This is sound because
/// the trie is exclusively borrowed (`&'a mut Node`) for the iterator's
/// lifetime, and `BTreeMap` values are stable (we never insert into or remove
/// from a parent's fork map during iteration).
pub struct ManifestIter<'a, S, E: NodeEntry = ChunkAddress, const BS: usize = DEFAULT_BODY_SIZE> {
    trie: &'a mut Node<E>,
    store: &'a S,
    stack: Vec<IterFrame<E>>,
    /// Running path buffer — extended when pushing frames, truncated when popping.
    path_buf: Vec<u8>,
    root_visited: bool,
}

impl<S, E: NodeEntry, const BS: usize> std::fmt::Debug for ManifestIter<'_, S, E, BS> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestIter")
            .field("stack_depth", &self.stack.len())
            .field("root_visited", &self.root_visited)
            .finish_non_exhaustive()
    }
}

struct IterFrame<E: NodeEntry> {
    /// Pointer to the node at this stack level.
    ///
    /// # Safety
    /// Valid for the iterator's `'a` lifetime. Points into the exclusively
    /// borrowed trie. Derived from `&mut Node` references obtained via
    /// `BTreeMap::get_mut`, whose values are stable across unrelated mutations.
    node: *mut Node<E>,
    /// Length of `path_buf` before this frame's prefix was appended.
    path_len_before: usize,
    /// This node's sorted fork keys.
    keys: Vec<u8>,
    /// Index into `keys` for the next fork to visit.
    key_idx: usize,
}

impl<'a, S: ChunkGet<BS>, E: NodeEntry, const BS: usize> ManifestIter<'a, S, E, BS> {
    const fn new(trie: &'a mut Node<E>, store: &'a S) -> Self {
        Self {
            trie,
            store,
            stack: Vec::new(),
            path_buf: Vec::new(),
            root_visited: false,
        }
    }
}

impl<S: ChunkGet<BS>, E: NodeEntry, const BS: usize> Iterator for ManifestIter<'_, S, E, BS> {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if !self.root_visited {
                self.root_visited = true;

                if !self.trie.loaded {
                    if let Err(e) = self.trie.load_from::<S, BS>(self.store) {
                        return Some(Err(e));
                    }
                }

                let keys: Vec<u8> = self.trie.forks.keys().copied().collect();
                let entry = if self.trie.is_value() {
                    match Entry::from_node(&[], self.trie) {
                        Ok(e) => Some(e),
                        Err(e) => return Some(Err(e)),
                    }
                } else {
                    None
                };

                self.stack.push(IterFrame {
                    node: self.trie as *mut Node<E>,
                    path_len_before: 0,
                    keys,
                    key_idx: 0,
                });

                if let Some(entry) = entry {
                    return Some(Ok(entry));
                }
                continue;
            }

            // Pop exhausted frames, truncating path_buf as we go.
            while self
                .stack
                .last()
                .is_some_and(|f| f.key_idx >= f.keys.len())
            {
                let frame = self.stack.pop().unwrap();
                self.path_buf.truncate(frame.path_len_before);
            }

            // Advance: get the next fork key and parent pointer from the top frame.
            let (key, parent_node) = {
                let frame = self.stack.last_mut()?;
                let key = frame.keys[frame.key_idx];
                frame.key_idx += 1;
                (key, frame.node)
            };

            // SAFETY: parent_node points into the exclusively borrowed trie.
            // No other mutable reference to this node exists — frames only hold
            // pointers to ancestors, which we do not dereference simultaneously.
            let parent = unsafe { &mut *parent_node };
            let fork = match parent.forks.get_mut(&key) {
                Some(f) => f,
                None => {
                    return Some(Err(MantarayError::NoForkFound {
                        reference: parent.reference,
                    }))
                }
            };

            let child = &mut fork.node as *mut Node<E>;

            // SAFETY: child is a descendant of the exclusively borrowed trie.
            let child_ref = unsafe { &mut *child };
            if !child_ref.loaded {
                if let Err(e) = child_ref.load_from::<S, BS>(self.store) {
                    return Some(Err(e));
                }
            }

            let child_keys: Vec<u8> = child_ref.forks.keys().copied().collect();
            let is_value = child_ref.is_value();

            // Extend path_buf with this fork's prefix, record restore point.
            let path_len_before = self.path_buf.len();
            self.path_buf.extend_from_slice(&fork.prefix);

            self.stack.push(IterFrame {
                node: child,
                path_len_before,
                keys: child_keys,
                key_idx: 0,
            });

            if is_value {
                match Entry::from_node(&self.path_buf, child_ref) {
                    Ok(e) => return Some(Ok(e)),
                    Err(e) => return Some(Err(e)),
                }
            }
        }
    }
}

impl<'a, S: ChunkGet<BS>, E: NodeEntry, const BS: usize> IntoIterator for &'a mut Manifest<S, E, BS> {
    type Item = Result<Entry>;
    type IntoIter = ManifestIter<'a, S, E, BS>;

    fn into_iter(self) -> Self::IntoIter {
        ManifestIter::new(&mut self.trie, &self.store)
    }
}

/// Compute keccak256 hash.
#[cfg(test)]
pub(crate) fn keccak256(data: &[u8]) -> [u8; 32] {
    use alloy_primitives::utils::keccak256 as alloy_keccak;
    *alloy_keccak(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
    use nectar_primitives::store::MemoryStore;

    type Store = MemoryStore<DEFAULT_BODY_SIZE>;

    /// Create a ChunkAddress from a string, right-padded with zeroes.
    fn make_addr(s: &str) -> ChunkAddress {
        let bytes = s.as_bytes();
        let mut buf = [0u8; 32];
        let len = bytes.len().min(32);
        buf[..len].copy_from_slice(&bytes[..len]);
        ChunkAddress::from(buf)
    }

    /// Create a ChunkAddress from a u32, left-padded with zeroes.
    fn make_addr_u32(i: u32) -> ChunkAddress {
        let mut buf = [0u8; 32];
        buf[28..].copy_from_slice(&i.to_be_bytes());
        ChunkAddress::from(buf)
    }

    #[test]
    fn persist_idempotence() {
        let store = Store::new();

        let mut m = PlainManifest::new(store);

        let paths = &[
            "aa", "b", "aaaaaa", "aaaaab", "abbbb", "abbba", "bbbbba", "bbbaaa", "bbbaab",
        ];

        for &path in paths {
            m.save().unwrap();
            let addr = make_addr(path);
            m.add(path, addr).unwrap();
        }

        m.save().unwrap();

        for &path in paths {
            let entry = m.lookup(path).unwrap();
            let expected = make_addr(path);
            assert_eq!(entry.address(), Some(&expected));
        }
    }

    #[test]
    fn manifest_entries() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);

        let paths = &["index.html", "img/1.png", "img/2.png", "robots.txt"];
        for &path in paths {
            let addr = make_addr(path);
            m.add(path, addr).unwrap();
        }

        let entries = m.entries().unwrap();
        assert_eq!(entries.len(), paths.len());

        for &path in paths {
            assert!(
                entries.iter().any(|e| e.path() == path.as_bytes()),
                "path {path} not found in entries"
            );
        }
    }

    #[test]
    fn entry_builder() {
        let addr = ChunkAddress::from([1u8; 32]);
        let entry = Entry::new(addr)
            .with_content_type("image/png")
            .with_filename("logo.png")
            .with_metadata("custom-key", "custom-value");

        assert_eq!(entry.address(), Some(&addr));
        assert!(entry.path().is_empty());
        assert_eq!(entry.content_type(), Some("image/png"));
        assert_eq!(entry.filename(), Some("logo.png"));
        assert_eq!(
            entry.metadata().get("custom-key").map(|s| s.as_str()),
            Some("custom-value")
        );
    }

    #[test]
    fn entry_builder_no_metadata() {
        let addr = ChunkAddress::from([2u8; 32]);
        let entry = Entry::new(addr);
        assert_eq!(entry.content_type(), None);
        assert_eq!(entry.filename(), None);
        assert!(entry.metadata().is_empty());
    }

    #[test]
    fn website_document_helpers() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);

        // Add a dummy entry so the root "/" path has an entry
        m.add("/", ChunkAddress::from([0u8; 32])).unwrap();

        m.set_index_document("index.html").unwrap();
        m.set_error_document("404.html").unwrap();

        assert_eq!(m.index_document().unwrap(), Some("index.html".to_string()));
        assert_eq!(m.error_document().unwrap(), Some("404.html".to_string()));
    }

    #[test]
    fn website_document_helpers_merge_metadata() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);

        m.set_index_document("index.html").unwrap();
        m.set_error_document("404.html").unwrap();

        assert_eq!(m.index_document().unwrap(), Some("index.html".to_string()));
        assert_eq!(m.error_document().unwrap(), Some("404.html".to_string()));
    }

    #[test]
    fn website_document_helpers_none_when_missing() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);

        assert_eq!(m.index_document().unwrap(), None);
        assert_eq!(m.error_document().unwrap(), None);
    }

    #[test]
    fn iterate_addresses_yields_all_refs() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);

        let paths = &["index.html", "img/1.png", "img/2.png", "robots.txt"];
        for &path in paths {
            let addr = make_addr(path);
            m.add(path, addr).unwrap();
        }

        let root_ref = m.save().unwrap();

        let (_, store) = m.into_parts();
        let mut m2 = PlainManifest::open(root_ref, store);
        let mut addresses = Vec::new();
        m2.iterate_addresses(|addr| {
            addresses.push(addr.to_vec());
            Ok(())
        })
        .unwrap();

        assert!(!addresses.is_empty());

        for &path in paths {
            let expected = make_addr(path);
            assert!(
                addresses.iter().any(|a| a == expected.as_bytes()),
                "entry ref for {path} not found in addresses"
            );
        }
    }

    #[test]
    fn partial_update_workflow() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);

        // Build a manifest with 100 entries
        for i in 0..100u32 {
            let path = format!("dir{}/file{}.txt", i / 10, i);
            let addr = make_addr_u32(i);
            m.add(&path, addr).unwrap();
        }
        let root_ref_1 = m.save().unwrap();

        // Update a single path
        let updated_addr = make_addr_u32(999);
        m.add("dir0/file0.txt", updated_addr).unwrap();
        let root_ref_2 = m.save().unwrap();

        assert_ne!(root_ref_1, root_ref_2);

        let entry = m.lookup("dir0/file0.txt").unwrap();
        assert_eq!(entry.address(), Some(&updated_addr));

        for i in 1..100u32 {
            let path = format!("dir{}/file{}.txt", i / 10, i);
            let entry = m.lookup(&path).unwrap();
            let expected = make_addr_u32(i);
            assert_eq!(entry.address(), Some(&expected), "entry at {path} was corrupted");
        }
    }

    #[test]
    fn into_iterator() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);

        let paths = &["index.html", "img/1.png", "img/2.png", "robots.txt"];
        for &path in paths {
            let addr = make_addr(path);
            m.add(path, addr).unwrap();
        }

        let mut all_entries = Vec::new();
        for result in &mut m {
            all_entries.push(result.unwrap());
        }

        assert_eq!(all_entries.len(), paths.len());
        for &path in paths {
            assert!(
                all_entries.iter().any(|e| e.path() == path.as_bytes()),
                "path {path} not found via IntoIterator"
            );
        }
    }

    #[test]
    fn manifest_iter_lazy() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);

        let paths = &["index.html", "img/1.png", "img/2.png", "robots.txt"];
        for &path in paths {
            let addr = make_addr(path);
            m.add(path, addr).unwrap();
        }

        // Save and reload to exercise lazy loading
        let root_ref = m.save().unwrap();

        let (_, store) = m.into_parts();
        let mut m2 = PlainManifest::open(root_ref, store);

        let mut visited = Vec::new();
        if let Some(result) = m2.iter().next() {
            let entry = result.unwrap();
            visited.push(entry.path);
        }
        assert_eq!(visited.len(), 1);

        // Full iteration
        let (_, store) = m2.into_parts();
        let mut m3 = PlainManifest::open(root_ref, store);
        let mut all_entries = Vec::new();
        for result in m3.iter() {
            all_entries.push(result.unwrap());
        }

        assert_eq!(all_entries.len(), paths.len());
        for &path in paths {
            assert!(
                all_entries.iter().any(|e| e.path() == path.as_bytes()),
                "path {path} not found via iterator"
            );
        }
    }

    #[test]
    fn iter_empty_manifest() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);
        let entries: Vec<_> = m.iter().collect();
        assert!(entries.is_empty(), "empty manifest should yield no entries");
    }

    #[test]
    fn iter_single_entry() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);
        let addr = make_addr("only");
        m.add("only.txt", addr).unwrap();

        let entries: Vec<_> = m.iter().map(|r| r.unwrap()).collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path(), b"only.txt");
        assert_eq!(entries[0].address(), Some(&addr));
    }

    #[test]
    fn iter_deep_trie_with_lazy_loading() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);

        // Build a deep trie: paths share long prefixes, forcing multiple
        // trie levels. After save+reload, iteration must lazily load each
        // intermediate node via raw-pointer traversal.
        let deep_paths: Vec<String> = (0..20)
            .map(|i| format!("a/b/c/d/e/f/g/h/file{i:02}.dat"))
            .collect();
        for path in &deep_paths {
            m.add(path, make_addr(path)).unwrap();
        }

        let root_ref = m.save().unwrap();
        let (_, store) = m.into_parts();
        let mut m2 = PlainManifest::open(root_ref, store);

        let entries: Vec<_> = m2.iter().map(|r| r.unwrap()).collect();
        assert_eq!(entries.len(), deep_paths.len());
        for path in &deep_paths {
            assert!(
                entries.iter().any(|e| e.path() == path.as_bytes()),
                "deep path {path} not found via iterator"
            );
        }
    }

    #[test]
    fn iter_partial_then_reiterate() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);

        let paths = &["a.txt", "b.txt", "c.txt", "d.txt", "e.txt"];
        for &path in paths {
            m.add(path, make_addr(path)).unwrap();
        }

        // Partial iteration: take only 2 entries, then drop iterator.
        {
            let mut iter = m.iter();
            let _first = iter.next().unwrap().unwrap();
            let _second = iter.next().unwrap().unwrap();
            // Iterator dropped here — must not corrupt trie state.
        }

        // Full re-iteration should still yield all entries.
        let all: Vec<_> = m.iter().map(|r| r.unwrap()).collect();
        assert_eq!(all.len(), paths.len());
        for &path in paths {
            assert!(
                all.iter().any(|e| e.path() == path.as_bytes()),
                "path {path} missing after partial iteration + re-iteration"
            );
        }
    }

    #[test]
    fn iter_partial_then_reiterate_lazy() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);

        let paths = &["x/1.txt", "x/2.txt", "y/1.txt", "y/2.txt", "z.txt"];
        for &path in paths {
            m.add(path, make_addr(path)).unwrap();
        }

        let root_ref = m.save().unwrap();
        let (_, store) = m.into_parts();
        let mut m2 = PlainManifest::open(root_ref, store);

        // Partial iteration on a lazy-loaded manifest.
        {
            let mut iter = m2.iter();
            let _first = iter.next().unwrap().unwrap();
        }

        // Re-iterate: previously loaded nodes stay loaded, the rest
        // are lazily fetched again through the raw-pointer path.
        let all: Vec<_> = m2.iter().map(|r| r.unwrap()).collect();
        assert_eq!(all.len(), paths.len());
        for &path in paths {
            assert!(
                all.iter().any(|e| e.path() == path.as_bytes()),
                "path {path} missing after partial lazy iteration"
            );
        }
    }
}

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
//! # Website Manifests
//!
//! Configure index and error documents for Swarm-hosted websites:
//!
//! ```no_run
//! # use nectar_mantaray::{Manifest, Entry, metadata, MockChunkStore};
//! # let store = MockChunkStore::new();
//! # let mut manifest = Manifest::new(&store, false);
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

pub mod error;
pub mod marshal;
pub mod node;
pub mod walker;

pub use error::{MantarayError, Result};
pub use node::{Fork, Node};

// Re-export storage traits from primitives.
pub use nectar_primitives::store::{
    ChunkGetter, ChunkPutter, ChunkStore, ChunkStoreError, MockChunkStore,
};

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

// Node type flags.
const NT_VALUE: u8 = 2;
const NT_EDGE: u8 = 4;
const NT_WITH_PATH_SEPARATOR: u8 = 8;
const NT_WITH_METADATA: u8 = 16;
const NT_MASK: u8 = 255;

/// A manifest entry: a path, reference, and optional metadata.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Entry {
    /// The path for this entry.
    pub path: Vec<u8>,
    /// The chunk reference.
    pub reference: Vec<u8>,
    /// Key-value metadata.
    pub metadata: BTreeMap<String, String>,
}

impl Entry {
    /// Create a new entry with the given chunk reference.
    pub const fn new(reference: Vec<u8>) -> Self {
        Self {
            reference,
            path: Vec::new(),
            metadata: BTreeMap::new(),
        }
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
}

/// High-level mantaray manifest backed by a storage backend.
#[derive(Debug)]
pub struct Manifest<S> {
    trie: Node,
    store: S,
}

impl<S> Manifest<S> {
    /// Create a new manifest. If `encrypted` is false, the obfuscation key is zeroed.
    pub fn new(store: S, encrypted: bool) -> Self {
        let trie = if encrypted {
            Node::default()
        } else {
            Node::new_unencrypted()
        };
        Self { trie, store }
    }

    /// Create a manifest from an existing root reference.
    pub fn new_manifest_reference(reference: &[u8], store: S) -> Self {
        Self {
            trie: Node::from_reference(reference),
            store,
        }
    }

    /// Access the root trie node.
    pub const fn root(&self) -> &Node {
        &self.trie
    }

    /// Mutable access to the root trie node.
    pub const fn root_mut(&mut self) -> &mut Node {
        &mut self.trie
    }

    /// Consume the manifest and return its parts.
    pub fn into_parts(self) -> (Node, S) {
        (self.trie, self.store)
    }

    /// Get the root reference (empty if not yet saved).
    pub fn reference(&self) -> &[u8] {
        self.trie.reference()
    }
}

impl<S: ChunkGetter> Manifest<S> {
    /// Add a path and entry to the manifest.
    pub fn add(&mut self, path: &str, entry: Entry) -> Result<()> {
        self.trie.add(
            path.as_bytes(),
            &entry.reference,
            entry.metadata,
            Some(&self.store as &dyn ChunkGetter),
        )
    }

    /// Remove a path from the manifest.
    pub fn remove(&mut self, path: &str) -> Result<()> {
        self.trie.remove(
            path.as_bytes(),
            Some(&self.store as &dyn ChunkGetter),
        )
    }

    /// Look up a path in the manifest.
    pub fn lookup(&mut self, path: &str) -> Result<Entry> {
        let node = self.trie.lookup_node(
            path.as_bytes(),
            Some(&self.store as &dyn ChunkGetter),
        )?;

        if !node.is_value() {
            return Err(MantarayError::NotValueType);
        }

        Ok(Entry {
            path: path.as_bytes().to_vec(),
            reference: node.entry().to_vec(),
            metadata: node.metadata().clone(),
        })
    }

    /// Test whether the manifest contains a prefix.
    pub fn has_prefix(&mut self, prefix: &str) -> Result<bool> {
        self.trie.has_prefix(
            prefix.as_bytes(),
            Some(&self.store as &dyn ChunkGetter),
        )
    }

    /// Walk all nodes depth-first, calling `f` for each node with its path.
    pub fn walk<F>(&mut self, f: &mut F) -> Result<()>
    where
        F: FnMut(&[u8], &Node) -> Result<()>,
    {
        self.trie.walk(Some(&self.store as &dyn ChunkGetter), f)
    }

    /// Collect all value entries from the manifest.
    pub fn entries(&mut self) -> Result<Vec<Entry>> {
        let mut result = Vec::new();
        self.trie.walk(Some(&self.store as &dyn ChunkGetter), &mut |path, node| {
            if node.is_value() {
                result.push(Entry {
                    path: path.to_vec(),
                    reference: node.entry().to_vec(),
                    metadata: node.metadata().clone(),
                });
            }
            Ok(())
        })?;
        Ok(result)
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
        self.trie
            .walk(Some(&self.store as &dyn ChunkGetter), &mut |_path, node| {
                let node_ref = node.reference();
                if !node_ref.is_empty() {
                    f(node_ref)?;
                }

                let entry_ref = node.entry();
                if node.is_value() && !entry_ref.is_empty() && entry_ref.iter().any(|&b| b != 0) {
                    f(entry_ref)?;
                }

                Ok(())
            })
    }

    /// Create a lazy depth-first iterator over all entries in the manifest.
    ///
    /// Nodes are loaded from storage on demand, so the entire trie does not
    /// need to be in memory at once.
    pub const fn iter(&mut self) -> ManifestIter<'_, S> {
        ManifestIter::new(&mut self.trie, &self.store)
    }

    fn set_root_metadata(&mut self, key: &str, value: &str) -> Result<()> {
        // Try to preserve existing root metadata
        let mut meta = self
            .trie
            .lookup_node(
                metadata::ROOT_PATH.as_bytes(),
                Some(&self.store as &dyn ChunkGetter),
            )
            .map_or_else(|_| BTreeMap::new(), |node| node.metadata().clone());
        meta.insert(key.into(), value.into());
        self.trie.add(
            metadata::ROOT_PATH.as_bytes(),
            &[],
            meta,
            Some(&self.store as &dyn ChunkGetter),
        )
    }

    fn get_root_metadata(&mut self, key: &str) -> Result<Option<String>> {
        match self.trie.lookup_node(
            metadata::ROOT_PATH.as_bytes(),
            Some(&self.store as &dyn ChunkGetter),
        ) {
            Ok(node) => Ok(node.metadata().get(key).cloned()),
            Err(MantarayError::NoForkFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl<S: ChunkStore> Manifest<S> {
    /// Persist the manifest trie to storage, returning the root reference.
    pub fn save(&mut self) -> Result<Vec<u8>> {
        self.trie.save(&self.store)?;
        Ok(self.trie.reference().to_vec())
    }
}

/// Lazy depth-first iterator over manifest entries.
///
/// Loads nodes from storage on demand. Each call to `next()` may perform
/// storage reads as it traverses unloaded parts of the trie.
///
/// Internally, the iterator stores a navigation path (sequence of fork keys)
/// for each stack frame and re-navigates from the root on each step. This
/// avoids holding multiple simultaneous mutable references into the trie,
/// keeping the implementation fully safe. The overhead is O(depth) per step,
/// which is negligible for typical manifest depths of 3–5.
#[derive(Debug)]
pub struct ManifestIter<'a, S> {
    trie: &'a mut Node,
    store: &'a S,
    stack: Vec<IterFrame>,
    root_visited: bool,
}

#[derive(Debug)]
struct IterFrame {
    /// Human-readable path to this node.
    path: Vec<u8>,
    /// Fork keys from root to reach this node (used to re-navigate).
    nav: Vec<u8>,
    /// This node's sorted fork keys.
    keys: Vec<u8>,
    /// Index into `keys` for the next fork to visit.
    key_idx: usize,
}

impl<'a, S: ChunkGetter> ManifestIter<'a, S> {
    const fn new(trie: &'a mut Node, store: &'a S) -> Self {
        Self {
            trie,
            store,
            stack: Vec::new(),
            root_visited: false,
        }
    }
}

impl<S: ChunkGetter> Iterator for ManifestIter<'_, S> {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if !self.root_visited {
                self.root_visited = true;

                if self.trie.forks.is_empty() {
                    if let Err(e) = self.trie.load(Some(self.store as &dyn ChunkGetter)) {
                        return Some(Err(e));
                    }
                }

                let keys: Vec<u8> = self.trie.forks.keys().copied().collect();
                let entry = if self.trie.is_value() {
                    Some(Entry {
                        path: Vec::new(),
                        reference: self.trie.entry().to_vec(),
                        metadata: self.trie.metadata().clone(),
                    })
                } else {
                    None
                };

                self.stack.push(IterFrame {
                    path: Vec::new(),
                    nav: Vec::new(),
                    keys,
                    key_idx: 0,
                });

                if let Some(entry) = entry {
                    return Some(Ok(entry));
                }
                continue;
            }

            // Pop exhausted frames
            while self
                .stack
                .last()
                .is_some_and(|f| f.key_idx >= f.keys.len())
            {
                self.stack.pop();
            }

            // Extract next key and navigation info from top frame
            let (key, child_nav, parent_path) = {
                let frame = self.stack.last_mut()?;
                let key = frame.keys[frame.key_idx];
                frame.key_idx += 1;
                let mut nav = frame.nav.clone();
                nav.push(key);
                (key, nav, frame.path.clone())
            };

            // Navigate from root to the child node, collect its data,
            // then release the trie borrow before pushing to the stack.
            let (child_path, child_keys, entry) = {
                let mut node: &mut Node = self.trie;
                for &k in &child_nav[..child_nav.len() - 1] {
                    node = &mut node.forks.get_mut(&k).expect("valid nav key").node;
                }
                let fork = node.forks.get_mut(&key).expect("valid nav key");

                let mut child_path = parent_path;
                child_path.extend_from_slice(&fork.prefix);

                let child = &mut fork.node;
                if child.forks.is_empty() {
                    if let Err(e) = child.load(Some(self.store as &dyn ChunkGetter)) {
                        return Some(Err(e));
                    }
                }

                let child_keys: Vec<u8> = child.forks.keys().copied().collect();
                let entry = if child.is_value() {
                    Some(Entry {
                        path: child_path.clone(),
                        reference: child.entry().to_vec(),
                        metadata: child.metadata().clone(),
                    })
                } else {
                    None
                };

                (child_path, child_keys, entry)
            };

            self.stack.push(IterFrame {
                path: child_path,
                nav: child_nav,
                keys: child_keys,
                key_idx: 0,
            });

            if let Some(entry) = entry {
                return Some(Ok(entry));
            }
        }
    }
}

impl<'a, S: ChunkGetter> IntoIterator for &'a mut Manifest<S> {
    type Item = Result<Entry>;
    type IntoIter = ManifestIter<'a, S>;

    fn into_iter(self) -> Self::IntoIter {
        ManifestIter::new(&mut self.trie, &self.store)
    }
}

/// Compute keccak256 hash.
pub fn keccak256(data: &[u8]) -> [u8; 32] {
    use alloy_primitives::utils::keccak256 as alloy_keccak;
    *alloy_keccak(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persist_idempotence() {
        let store = MockChunkStore::new();

        let mut m = Manifest::new(&store, false);

        let paths = &[
            "aa", "b", "aaaaaa", "aaaaab", "abbbb", "abbba", "bbbbba", "bbbaaa", "bbbaab",
        ];

        for &path in paths {
            m.save().unwrap();
            let mut v = path.as_bytes().to_vec();
            v.resize(32, 0);
            m.add(
                path,
                Entry {
                    path: path.as_bytes().to_vec(),
                    reference: v,
                    metadata: BTreeMap::new(),
                },
            )
            .unwrap();
        }

        m.save().unwrap();

        for &path in paths {
            let entry = m.lookup(path).unwrap();
            let mut v = path.as_bytes().to_vec();
            v.resize(32, 0);
            assert_eq!(entry.reference, v);
        }
    }

    #[test]
    fn manifest_entries() {
        let store = MockChunkStore::new();
        let mut m = Manifest::new(&store, false);

        let paths = &["index.html", "img/1.png", "img/2.png", "robots.txt"];
        for &path in paths {
            let mut v = path.as_bytes().to_vec();
            v.resize(32, 0);
            m.add(
                path,
                Entry {
                    path: path.as_bytes().to_vec(),
                    reference: v,
                    metadata: BTreeMap::new(),
                },
            )
            .unwrap();
        }

        let entries = m.entries().unwrap();
        assert_eq!(entries.len(), paths.len());

        for &path in paths {
            assert!(
                entries.iter().any(|e| e.path == path.as_bytes()),
                "path {path} not found in entries"
            );
        }
    }

    #[test]
    fn entry_builder() {
        let reference = vec![1u8; 32];
        let entry = Entry::new(reference.clone())
            .with_content_type("image/png")
            .with_filename("logo.png")
            .with_metadata("custom-key", "custom-value");

        assert_eq!(entry.reference, reference);
        assert!(entry.path.is_empty());
        assert_eq!(entry.content_type(), Some("image/png"));
        assert_eq!(entry.filename(), Some("logo.png"));
        assert_eq!(
            entry.metadata.get("custom-key").map(|s| s.as_str()),
            Some("custom-value")
        );
    }

    #[test]
    fn entry_builder_no_metadata() {
        let entry = Entry::new(vec![2u8; 32]);
        assert_eq!(entry.content_type(), None);
        assert_eq!(entry.filename(), None);
        assert!(entry.metadata.is_empty());
    }

    #[test]
    fn website_document_helpers() {
        let store = MockChunkStore::new();
        let mut m = Manifest::new(&store, false);

        // Add a dummy entry so the root "/" path has an entry
        m.add(
            "/",
            Entry::new(vec![0u8; 32]),
        )
        .unwrap();

        m.set_index_document("index.html").unwrap();
        m.set_error_document("404.html").unwrap();

        assert_eq!(m.index_document().unwrap(), Some("index.html".to_string()));
        assert_eq!(m.error_document().unwrap(), Some("404.html".to_string()));
    }

    #[test]
    fn website_document_helpers_merge_metadata() {
        let store = MockChunkStore::new();
        let mut m = Manifest::new(&store, false);

        // Set index first
        m.set_index_document("index.html").unwrap();
        // Set error — should merge, not replace
        m.set_error_document("404.html").unwrap();

        assert_eq!(m.index_document().unwrap(), Some("index.html".to_string()));
        assert_eq!(m.error_document().unwrap(), Some("404.html".to_string()));
    }

    #[test]
    fn website_document_helpers_none_when_missing() {
        let store = MockChunkStore::new();
        let mut m = Manifest::new(&store, false);

        assert_eq!(m.index_document().unwrap(), None);
        assert_eq!(m.error_document().unwrap(), None);
    }

    #[test]
    fn iterate_addresses_yields_all_refs() {
        let store = MockChunkStore::new();
        let mut m = Manifest::new(&store, false);

        let paths = &["index.html", "img/1.png", "img/2.png", "robots.txt"];
        for &path in paths {
            let mut v = path.as_bytes().to_vec();
            v.resize(32, 0);
            m.add(path, Entry::new(v)).unwrap();
        }

        m.save().unwrap();
        let root_ref = m.reference().to_vec();

        let mut m2 = Manifest::new_manifest_reference(&root_ref, &store);
        let mut addresses = Vec::new();
        m2.iterate_addresses(|addr| {
            addresses.push(addr.to_vec());
            Ok(())
        })
        .unwrap();

        // Should have both node refs (trie chunks) and entry refs (content chunks)
        assert!(!addresses.is_empty());

        // All entry references should be present
        for &path in paths {
            let mut v = path.as_bytes().to_vec();
            v.resize(32, 0);
            assert!(
                addresses.iter().any(|a| *a == v),
                "entry ref for {path} not found in addresses"
            );
        }
    }

    #[test]
    fn partial_update_workflow() {
        let store = MockChunkStore::new();
        let mut m = Manifest::new(&store, false);

        // Build a manifest with 100 entries
        for i in 0..100u32 {
            let path = format!("dir{}/file{}.txt", i / 10, i);
            let mut v = vec![0u8; 28];
            v.extend_from_slice(&i.to_be_bytes());
            m.add(&path, Entry::new(v)).unwrap();
        }
        m.save().unwrap();
        let root_ref_1 = m.reference().to_vec();

        // Update a single path
        let mut v = vec![0u8; 28];
        v.extend_from_slice(&999u32.to_be_bytes());
        m.add("dir0/file0.txt", Entry::new(v.clone())).unwrap();
        m.save().unwrap();
        let root_ref_2 = m.reference().to_vec();

        // Root reference should have changed
        assert_ne!(root_ref_1, root_ref_2);

        // Updated entry should have new value
        let entry = m.lookup("dir0/file0.txt").unwrap();
        assert_eq!(entry.reference, v);

        // Other entries should be intact
        for i in 1..100u32 {
            let path = format!("dir{}/file{}.txt", i / 10, i);
            let entry = m.lookup(&path).unwrap();
            let mut expected = vec![0u8; 28];
            expected.extend_from_slice(&i.to_be_bytes());
            assert_eq!(entry.reference, expected, "entry at {path} was corrupted");
        }
    }

    #[test]
    fn into_iterator() {
        let store = MockChunkStore::new();
        let mut m = Manifest::new(&store, false);

        let paths = &["index.html", "img/1.png", "img/2.png", "robots.txt"];
        for &path in paths {
            let mut v = path.as_bytes().to_vec();
            v.resize(32, 0);
            m.add(path, Entry::new(v)).unwrap();
        }

        let mut all_entries = Vec::new();
        for result in &mut m {
            all_entries.push(result.unwrap());
        }

        assert_eq!(all_entries.len(), paths.len());
        for &path in paths {
            assert!(
                all_entries.iter().any(|e| e.path == path.as_bytes()),
                "path {path} not found via IntoIterator"
            );
        }
    }

    #[test]
    fn manifest_iter_lazy() {
        let store = MockChunkStore::new();
        let mut m = Manifest::new(&store, false);

        let paths = &["index.html", "img/1.png", "img/2.png", "robots.txt"];
        for &path in paths {
            let mut v = path.as_bytes().to_vec();
            v.resize(32, 0);
            m.add(
                path,
                Entry {
                    path: path.as_bytes().to_vec(),
                    reference: v,
                    metadata: BTreeMap::new(),
                },
            )
            .unwrap();
        }

        // Save and reload to exercise lazy loading
        m.save().unwrap();
        let root_ref = m.reference().to_vec();

        let mut m2 = Manifest::new_manifest_reference(&root_ref, &store);

        let mut visited = Vec::new();
        while let Some(result) = m2.iter().next() {
            let entry = result.unwrap();
            visited.push(entry.path);
            // Break after first to show lazy behavior — we don't load everything
            break;
        }
        assert_eq!(visited.len(), 1);

        // Full iteration
        let mut m3 = Manifest::new_manifest_reference(&root_ref, &store);
        let mut all_entries = Vec::new();
        let mut iter = m3.iter();
        while let Some(result) = iter.next() {
            all_entries.push(result.unwrap());
        }

        assert_eq!(all_entries.len(), paths.len());
        for &path in paths {
            assert!(
                all_entries.iter().any(|e| e.path == path.as_bytes()),
                "path {path} not found via iterator"
            );
        }
    }
}

//! Node and Fork types for the mantaray trie.

use std::collections::BTreeMap;

use bytes::Bytes;
use nectar_primitives::chunk::{Chunk, ChunkAddress, ContentChunk};
use nectar_primitives::store::{ChunkGet, ChunkPut};
use crate::error::{MantarayError, Result};
use crate::mode::NodeEntry;
use crate::obfuscation::ObfuscationKey;
use crate::{NODE_PREFIX_MAX_SIZE, PATH_SEPARATOR};

/// Inline-only byte buffer for fork prefixes (max 30 bytes).
///
/// Always stores data inline — no heap allocation, no branching.
/// 31 bytes total (1 len + 30 data).
#[derive(Clone, PartialEq, Eq)]
pub struct Prefix {
    len: u8,
    data: [u8; NODE_PREFIX_MAX_SIZE],
}

impl Prefix {
    /// Create an empty prefix.
    #[inline]
    pub const fn new() -> Self {
        Self {
            len: 0,
            data: [0u8; NODE_PREFIX_MAX_SIZE],
        }
    }

    /// Create a prefix from a byte slice. Panics if `src.len() > 30`.
    #[inline]
    pub fn from_slice(src: &[u8]) -> Self {
        debug_assert!(src.len() <= NODE_PREFIX_MAX_SIZE);
        let mut data = [0u8; NODE_PREFIX_MAX_SIZE];
        data[..src.len()].copy_from_slice(src);
        Self {
            len: src.len() as u8,
            data,
        }
    }

    /// Returns the prefix length in bytes.
    #[inline]
    pub const fn len(&self) -> usize {
        self.len as usize
    }

    /// Returns true if the prefix is empty.
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the full 30-byte backing array (zero-padded beyond `len`).
    #[inline]
    pub const fn padded_bytes(&self) -> &[u8; NODE_PREFIX_MAX_SIZE] {
        &self.data
    }
}

impl std::ops::Deref for Prefix {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        &self.data[..self.len as usize]
    }
}

impl std::fmt::Debug for Prefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Prefix({:?})", &**self)
    }
}

bitflags::bitflags! {
    /// Bitflags encoding the kind of a mantaray node.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct NodeType: u8 {
        /// Node stores a value (has an entry).
        const VALUE = 2;
        /// Node has child forks.
        const EDGE = 4;
        /// Path contains a "/" separator.
        const PATH_SEPARATOR = 8;
        /// Node has metadata key-value pairs.
        const METADATA = 16;
    }
}

/// A node in the mantaray trie.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node<E: NodeEntry = ChunkAddress> {
    /// Bitflags encoding the node kind (value, edge, path-separator, metadata).
    pub(crate) node_type: NodeType,
    /// XOR obfuscation key for binary serialisation.
    pub(crate) obfuscation_key: ObfuscationKey,
    /// Content-addressed reference for this node (None if not yet persisted).
    pub(crate) reference: Option<ChunkAddress>,
    /// The typed entry stored at this node (the chunk reference this path maps to).
    pub(crate) entry: Option<E>,
    /// Metadata key-value pairs attached to this node.
    pub(crate) metadata: BTreeMap<String, String>,
    /// Child forks keyed by the first byte of their prefix.
    pub(crate) forks: BTreeMap<u8, Fork<E>>,
    /// Whether this node's forks have been loaded from storage.
    pub(crate) loaded: bool,
}

impl<E: NodeEntry> Default for Node<E> {
    fn default() -> Self {
        Self {
            node_type: NodeType::empty(),
            obfuscation_key: ObfuscationKey::ZERO,
            reference: None,
            entry: None,
            metadata: BTreeMap::new(),
            forks: BTreeMap::new(),
            loaded: false,
        }
    }
}

/// A fork in the mantaray trie, consisting of a prefix and a child node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fork<E: NodeEntry = ChunkAddress> {
    /// Inline-only prefix (max 30 bytes). No heap allocation, no branching.
    pub(crate) prefix: Prefix,
    /// The child node.
    pub(crate) node: Node<E>,
}

impl<E: NodeEntry> Default for Fork<E> {
    fn default() -> Self {
        Self {
            prefix: Prefix::new(),
            node: Node::default(),
        }
    }
}

impl<E: NodeEntry> Fork<E> {
    /// The prefix bytes for this fork edge.
    pub fn prefix(&self) -> &[u8] {
        &self.prefix
    }

    /// The child node.
    pub const fn node(&self) -> &Node<E> {
        &self.node
    }

    /// Mutable access to the child node.
    pub const fn node_mut(&mut self) -> &mut Node<E> {
        &mut self.node
    }
}

/// Return the length of the common prefix of two byte slices.
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

impl<E: NodeEntry> Node<E> {
    /// Create a new node with a zeroed obfuscation key (unencrypted).
    pub fn new_unencrypted() -> Self {
        Self {
            obfuscation_key: ObfuscationKey::ZERO,
            ..Default::default()
        }
    }

    /// Create a node that references persisted data.
    pub fn from_reference(reference: ChunkAddress) -> Self {
        Self {
            reference: Some(reference),
            ..Default::default()
        }
    }

    /// The typed entry stored at this node.
    pub fn entry(&self) -> Option<&E> {
        self.entry.as_ref()
    }

    /// Metadata key-value pairs attached to this node.
    pub const fn metadata(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }

    /// Mutable access to metadata for in-place modification.
    pub(crate) fn metadata_mut(&mut self) -> &mut BTreeMap<String, String> {
        &mut self.metadata
    }

    /// Content-addressed reference for this node.
    pub const fn reference(&self) -> Option<&ChunkAddress> {
        self.reference.as_ref()
    }

    /// Child forks keyed by the first byte of their prefix.
    pub const fn forks(&self) -> &BTreeMap<u8, Fork<E>> {
        &self.forks
    }

    /// Mutable access to child forks.
    pub const fn forks_mut(&mut self) -> &mut BTreeMap<u8, Fork<E>> {
        &mut self.forks
    }

    /// XOR obfuscation key for binary serialisation.
    pub fn obfuscation_key(&self) -> &ObfuscationKey {
        &self.obfuscation_key
    }

    /// Set the content-addressed reference for this node.
    pub const fn set_reference(&mut self, reference: ChunkAddress) {
        self.reference = Some(reference);
    }

    /// Check if the node has a value (entry).
    pub const fn is_value(&self) -> bool {
        self.node_type.contains(NodeType::VALUE)
    }

    /// Set the value flag.
    pub(crate) const fn make_value(&mut self) {
        self.node_type = self.node_type.union(NodeType::VALUE);
    }

    /// Check if the node has child forks.
    pub const fn is_edge(&self) -> bool {
        self.node_type.contains(NodeType::EDGE)
    }

    /// Set the edge flag.
    pub(crate) const fn make_edge(&mut self) {
        self.node_type = self.node_type.union(NodeType::EDGE);
    }

    /// Check if the path contains a separator.
    pub const fn is_with_path_separator(&self) -> bool {
        self.node_type.contains(NodeType::PATH_SEPARATOR)
    }

    /// Check if the node has metadata.
    pub const fn is_with_metadata(&self) -> bool {
        self.node_type.contains(NodeType::METADATA)
    }

    /// Set the metadata flag.
    pub(crate) const fn make_with_metadata(&mut self) {
        self.node_type = self.node_type.union(NodeType::METADATA);
    }

    #[cfg(test)]
    pub(crate) const fn make_not_value(&mut self) {
        self.node_type = self.node_type.difference(NodeType::VALUE);
    }

    fn update_is_with_path_separator(&mut self, path: &[u8]) {
        let sep = PATH_SEPARATOR.as_bytes()[0];
        if path.iter().skip(1).any(|&b| b == sep) {
            self.node_type = self.node_type.union(NodeType::PATH_SEPARATOR);
        } else {
            self.node_type = self.node_type.difference(NodeType::PATH_SEPARATOR);
        }
    }

    /// Clear persisted reference, marking this node for re-serialization on next save.
    pub(crate) fn mark_dirty(&mut self) {
        self.reference = None;
    }

    /// Load forks from storage if the node hasn't been loaded yet.
    fn ensure_loaded<S: ChunkGet<BS>, const BS: usize>(&mut self, loader: &S) -> Result<()> {
        if !self.loaded {
            self.load_from(loader)?;
        }
        Ok(())
    }

    /// Load this node from storage by its reference.
    pub fn load_from<S: ChunkGet<BS>, const BS: usize>(&mut self, loader: &S) -> Result<()> {
        let address = match self.reference {
            Some(addr) => addr,
            None => {
                self.loaded = true;
                return Ok(());
            }
        };

        let chunk = loader.get(&address).map_err(|e| MantarayError::StoreGet {
            source: std::sync::Arc::new(e),
        })?;
        let mut loaded = Node::<E>::try_from(chunk.data().as_ref())?;
        loaded.reference = Some(address);
        // Preserve fields that live in the parent's fork data, not in this node's chunk:
        // node_type flags and metadata key-value pairs.
        loaded.node_type |= self.node_type;
        loaded.metadata = core::mem::take(&mut self.metadata);
        *self = loaded;
        Ok(())
    }

    /// Look up the node at the given path, loading from storage as needed.
    pub(crate) fn lookup_node_with_loader<S: ChunkGet<BS>, const BS: usize>(
        &mut self,
        path: &[u8],
        loader: &S,
    ) -> Result<&mut Self> {
        self.ensure_loaded(loader)?;

        if path.is_empty() {
            return Ok(self);
        }

        let first = path[0];
        let fork = self.forks.get_mut(&first).ok_or_else(|| {
            MantarayError::NoForkFound {
                reference: self.reference,
            }
        })?;

        let c = common_prefix_len(&fork.prefix, path);
        if c == fork.prefix.len() {
            fork.node.lookup_node_with_loader(&path[c..], loader)
        } else {
            Err(MantarayError::NoForkFound {
                reference: self.reference,
            })
        }
    }

    /// Look up the entry at the given path, loading from storage as needed.
    pub fn lookup_with_loader<S: ChunkGet<BS>, const BS: usize>(
        &mut self,
        path: &[u8],
        loader: &S,
    ) -> Result<Option<&E>> {
        let node = self.lookup_node_with_loader(path, loader)?;
        if !node.is_value() && !path.is_empty() {
            return Err(MantarayError::NoEntryFound {
                reference: node.reference,
            });
        }
        Ok(node.entry.as_ref())
    }

    /// Add an entry at the given path with optional metadata, loading from storage as needed.
    pub(crate) fn add_with_loader<S: ChunkGet<BS>, const BS: usize>(
        &mut self,
        path: &[u8],
        entry: Option<E>,
        metadata: BTreeMap<String, String>,
        loader: &S,
    ) -> Result<()> {
        // empty path — set this node as a value
        if path.is_empty() {
            self.entry = entry;
            self.make_value();

            if !metadata.is_empty() {
                self.metadata = metadata;
                self.make_with_metadata();
            }

            self.mark_dirty();
            return Ok(());
        }

        // load forks if needed
        if !self.loaded {
            self.load_from(loader)?;
            self.mark_dirty();
        }

        if !self.forks.contains_key(&path[0]) {
            // no existing fork for this byte — create a new one
            let mut nn = Self {
                obfuscation_key: self.obfuscation_key,
                ..Default::default()
            };

            if path.len() > NODE_PREFIX_MAX_SIZE {
                let (prefix, rest) = path.split_at(NODE_PREFIX_MAX_SIZE);
                nn.add_with_loader(rest, entry, metadata, loader)?;
                nn.update_is_with_path_separator(prefix);
                self.forks.insert(
                    path[0],
                    Fork {
                        prefix: Prefix::from_slice(prefix),
                        node: nn,
                    },
                );
                self.make_edge();
                return Ok(());
            }

            nn.entry = entry;
            if !metadata.is_empty() {
                nn.metadata = metadata;
                nn.make_with_metadata();
            }
            nn.make_value();
            nn.update_is_with_path_separator(path);

            self.forks.insert(
                path[0],
                Fork {
                    prefix: Prefix::from_slice(path),
                    node: nn,
                },
            );
            self.make_edge();
            return Ok(());
        }

        // existing fork — need to split or extend
        let fork = self.forks.get(&path[0]).expect("checked above");
        let c = common_prefix_len(&fork.prefix, path);
        let rest = Prefix::from_slice(&fork.prefix[c..]);
        let common_prefix = Prefix::from_slice(&fork.prefix[..c]);

        // Take ownership — avoids cloning the entire node subtree
        let old_fork = self.forks.remove(&path[0]).expect("checked above");

        let mut nn = if rest.is_empty() {
            old_fork.node
        } else {
            // split: create intermediate node
            let mut intermediate = Self {
                obfuscation_key: self.obfuscation_key,
                ..Default::default()
            };

            let mut old_fork_node = old_fork.node;
            old_fork_node.update_is_with_path_separator(&rest);
            intermediate.forks.insert(rest[0], Fork { prefix: rest, node: old_fork_node });
            intermediate.make_edge();

            if c == path.len() {
                intermediate.make_value();
            }
            intermediate
        };

        nn.update_is_with_path_separator(path);
        nn.add_with_loader(&path[c..], entry, metadata, loader)?;

        self.forks.insert(
            path[0],
            Fork {
                prefix: common_prefix,
                node: nn,
            },
        );
        self.make_edge();

        Ok(())
    }

    /// Remove the entry at the given path, loading from storage as needed.
    pub(crate) fn remove_with_loader<S: ChunkGet<BS>, const BS: usize>(
        &mut self,
        path: &[u8],
        loader: &S,
    ) -> Result<()> {
        if path.is_empty() {
            return Err(MantarayError::EmptyPath);
        }

        self.ensure_loaded(loader)?;

        let first = path[0];

        // Clone prefix to release the borrow on self.forks
        let prefix = match self.forks.get(&first) {
            Some(f) => f.prefix.clone(),
            None => {
                return Err(MantarayError::PathPrefixNotFound {
                    prefix: String::from_utf8_lossy(&[first]).to_string(),
                });
            }
        };

        if !path.starts_with(&prefix) {
            return Err(MantarayError::PathPrefixNotFound {
                prefix: String::from_utf8_lossy(path).to_string(),
            });
        }

        let rest = &path[prefix.len()..];
        let result = if rest.is_empty() {
            self.forks.remove(&first);
            Ok(())
        } else {
            let fork = self.forks.get_mut(&first).expect("checked above");
            fork.node.remove_with_loader(rest, loader)
        };

        // Always clear reference so the node gets re-saved (matches Go's defer pattern)
        self.mark_dirty();
        result
    }

    /// Test whether a prefix exists in the trie, loading from storage as needed.
    pub(crate) fn has_prefix_with_loader<S: ChunkGet<BS>, const BS: usize>(
        &mut self,
        path: &[u8],
        loader: &S,
    ) -> Result<bool> {
        if path.is_empty() {
            return Ok(true);
        }

        self.ensure_loaded(loader)?;

        let fork = match self.forks.get_mut(&path[0]) {
            Some(f) => f,
            None => return Ok(false),
        };

        let c = common_prefix_len(&fork.prefix, path);

        if c == fork.prefix.len() {
            return fork.node.has_prefix_with_loader(&path[c..], loader);
        }

        if fork.prefix.starts_with(path) {
            return Ok(true);
        }

        Ok(false)
    }

    /// Recursively save this node and all children to storage.
    ///
    /// Uses BMT content-addressing via `ContentChunk`.
    pub fn save<S: ChunkPut<BS>, const BS: usize>(&mut self, saver: &mut S) -> Result<()> {
        if self.reference.is_some() {
            return Ok(());
        }

        for fork in self.forks.values_mut() {
            fork.node.save(saver)?;
        }

        let data = Vec::<u8>::try_from(&*self)?;
        let chunk = ContentChunk::<BS>::new(Bytes::from(data))?;
        let address = *chunk.address();
        saver.put(chunk.into()).map_err(|e| MantarayError::StorePut {
            source: std::sync::Arc::new(e),
        })?;
        self.reference = Some(address);
        self.forks.clear();
        self.loaded = false;

        Ok(())
    }

    /// Walk all nodes depth-first, calling `f` for each node with its path.
    pub(crate) fn walk_with_loader<S: ChunkGet<BS>, const BS: usize, F>(
        &mut self,
        loader: &S,
        f: &mut F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &Self) -> Result<()>,
    {
        let mut path_buf = Vec::new();
        walk_inner(&mut path_buf, self, loader, f)
    }

    /// Walk the subtree at `root`, calling `f` for each node.
    pub fn walk_node_with_loader<S: ChunkGet<BS>, const BS: usize, F>(
        &mut self,
        root: &[u8],
        loader: &S,
        f: &mut F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &Self) -> Result<()>,
    {
        let mut path_buf = root.to_vec();
        if root.is_empty() {
            return walk_inner(&mut path_buf, self, loader, f);
        }

        let target = self.lookup_node_with_loader(root, loader)?;
        walk_inner(&mut path_buf, target, loader, f)
    }
}

fn walk_inner<E: NodeEntry, S: ChunkGet<BS>, const BS: usize, F>(
    path_buf: &mut Vec<u8>,
    node: &mut Node<E>,
    loader: &S,
    f: &mut F,
) -> Result<()>
where
    F: FnMut(&[u8], &Node<E>) -> Result<()>,
{
    node.ensure_loaded(loader)?;

    f(path_buf, node)?;

    // collect keys to avoid borrow conflict
    let keys: Vec<u8> = node.forks.keys().copied().collect();
    for key in keys {
        let fork = node.forks.get_mut(&key).ok_or_else(|| MantarayError::NoForkFound {
            reference: node.reference,
        })?;
        let prev_len = path_buf.len();
        path_buf.extend_from_slice(&fork.prefix);
        walk_inner(path_buf, &mut fork.node, loader, f)?;
        path_buf.truncate(prev_len);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
    use nectar_primitives::store::{MemoryStore, NullLoader};

    struct TestCase {
        _name: &'static str,
        items: Vec<&'static str>,
    }

    #[derive(Default, Clone)]
    struct RemoveTestCaseItem {
        path: String,
        metadata: BTreeMap<String, String>,
    }

    #[derive(Clone)]
    struct RemoveTestCase {
        _name: &'static str,
        items: Vec<RemoveTestCaseItem>,
        remove: Vec<String>,
    }

    #[derive(Clone)]
    struct HasPrefixTestCase {
        _name: &'static str,
        paths: Vec<String>,
        test_paths: Vec<String>,
        should_exist: Vec<bool>,
    }

    fn test_case_data() -> [TestCase; 6] {
        [
            TestCase {
                _name: "a",
                items: vec![
                    "aaaaaa", "aaaaab", "abbbb", "abbba", "bbbbba", "bbbaaa", "bbbaab", "aa", "b",
                ],
            },
            TestCase {
                _name: "simple",
                items: vec!["/", "index.html", "img/1.png", "img/2.png", "robots.txt"],
            },
            TestCase {
                _name: "nested-value-node-is-recognized",
                items: vec![
                    "..............................@",
                    "..............................",
                ],
            },
            TestCase {
                _name: "nested-prefix-is-not-collapsed",
                items: vec![
                    "index.html",
                    "img/1.png",
                    "img/2/test1.png",
                    "img/2/test2.png",
                    "robots.txt",
                ],
            },
            TestCase {
                _name: "conflicting-path",
                items: vec!["app.js.map", "app.js"],
            },
            TestCase {
                _name: "spa-website",
                items: vec![
                    "css/",
                    "css/app.css",
                    "favicon.ico",
                    "img/",
                    "img/logo.png",
                    "index.html",
                    "js/",
                    "js/chunk-vendors.js.map",
                    "js/chunk-vendors.js",
                    "js/app.js.map",
                    "js/app.js",
                ],
            },
        ]
    }

    fn remove_test_case_data() -> Vec<RemoveTestCase> {
        vec![
            RemoveTestCase {
                _name: "simple",
                items: vec![
                    RemoveTestCaseItem {
                        path: "/".to_string(),
                        metadata: serde_json::from_str(
                            r#"{"index-document": "index.html"}"#,
                        )
                        .unwrap(),
                    },
                    RemoveTestCaseItem {
                        path: "index.html".to_string(),
                        ..Default::default()
                    },
                    RemoveTestCaseItem {
                        path: "img/1.png".to_string(),
                        ..Default::default()
                    },
                    RemoveTestCaseItem {
                        path: "img/2.png".to_string(),
                        ..Default::default()
                    },
                    RemoveTestCaseItem {
                        path: "robots.txt".to_string(),
                        ..Default::default()
                    },
                ],
                remove: vec!["img/2.png".to_string()],
            },
            RemoveTestCase {
                _name: "nested-prefix-is-not-collapsed",
                items: vec![
                    RemoveTestCaseItem {
                        path: "index.html".to_string(),
                        ..Default::default()
                    },
                    RemoveTestCaseItem {
                        path: "img/1.png".to_string(),
                        ..Default::default()
                    },
                    RemoveTestCaseItem {
                        path: "img/2/test1.png".to_string(),
                        ..Default::default()
                    },
                    RemoveTestCaseItem {
                        path: "img/2/test2.png".to_string(),
                        ..Default::default()
                    },
                    RemoveTestCaseItem {
                        path: "robots.txt".to_string(),
                        ..Default::default()
                    },
                ],
                remove: vec!["img/2/test1.png".to_string()],
            },
        ]
    }

    fn has_prefix_test_case_data() -> Vec<HasPrefixTestCase> {
        vec![
            HasPrefixTestCase {
                _name: "simple",
                paths: vec![
                    "index.html".to_string(),
                    "img/1.png".to_string(),
                    "img/2.png".to_string(),
                    "robots.txt".to_string(),
                ],
                test_paths: vec!["img/".to_string(), "images/".to_string()],
                should_exist: vec![true, false],
            },
            HasPrefixTestCase {
                _name: "nested-single",
                paths: vec!["some-path/file.ext".to_string()],
                test_paths: vec![
                    "some-path".to_string(),
                    "some-path/file".to_string(),
                    "some-other-path/".to_string(),
                ],
                should_exist: vec![true, true, false],
            },
        ]
    }

    const NL: NullLoader = NullLoader;
    const BS: usize = DEFAULT_BODY_SIZE;

    /// Create a 32-byte ChunkAddress from a string, left-padded with zeroes.
    fn make_entry(s: &str) -> ChunkAddress {
        let bytes = s.as_bytes();
        let mut buf = [0u8; 32];
        let start = 32 - bytes.len();
        buf[start..].copy_from_slice(bytes);
        ChunkAddress::from(buf)
    }

    /// In-memory add: delegates to `add_with_loader` with NullLoader.
    fn node_add(n: &mut Node, path: &[u8], entry: ChunkAddress, meta: BTreeMap<String, String>) {
        n.add_with_loader::<NullLoader, BS>(path, Some(entry), meta, &NL).unwrap();
    }

    /// In-memory lookup: delegates to `lookup_with_loader` with NullLoader.
    fn node_lookup<'n>(n: &'n mut Node, path: &[u8]) -> Result<Option<&'n ChunkAddress>> {
        n.lookup_with_loader::<NullLoader, BS>(path, &NL)
    }

    /// In-memory lookup_node: delegates to `lookup_node_with_loader` with NullLoader.
    fn node_lookup_node<'n>(n: &'n mut Node, path: &[u8]) -> Result<&'n mut Node> {
        n.lookup_node_with_loader::<NullLoader, BS>(path, &NL)
    }

    /// In-memory remove: delegates to `remove_with_loader` with NullLoader.
    fn node_remove(n: &mut Node, path: &[u8]) -> Result<()> {
        n.remove_with_loader::<NullLoader, BS>(path, &NL)
    }

    /// In-memory has_prefix: delegates to `has_prefix_with_loader` with NullLoader.
    fn node_has_prefix(n: &mut Node, path: &[u8]) -> Result<bool> {
        n.has_prefix_with_loader::<NullLoader, BS>(path, &NL)
    }

    /// In-memory walk: delegates to `walk_with_loader` with NullLoader.
    fn node_walk<F>(n: &mut Node, f: &mut F) -> Result<()>
    where
        F: FnMut(&[u8], &Node) -> Result<()>,
    {
        n.walk_with_loader::<NullLoader, BS, _>(&NL, f)
    }

    /// In-memory walk_node: delegates to `walk_node_with_loader` with NullLoader.
    fn node_walk_node<F>(n: &mut Node, root: &[u8], f: &mut F) -> Result<()>
    where
        F: FnMut(&[u8], &Node) -> Result<()>,
    {
        n.walk_node_with_loader::<NullLoader, BS, _>(root, &NL, f)
    }

    #[test]
    fn nil_path() {
        let mut n = Node::default();
        assert!(node_lookup(&mut n, b"").is_ok());
    }

    #[test]
    fn add_and_lookup() {
        let mut n = Node::default();
        let items = &test_case_data()[0].items;

        for (i, c) in items.iter().enumerate() {
            let e = make_entry(c);
            node_add(&mut n, c.as_bytes(), e, BTreeMap::new());

            for &d in items.iter().take(i) {
                let r = node_lookup(&mut n, d.as_bytes()).unwrap();
                assert_eq!(r, Some(&make_entry(d)));
            }
        }
    }

    fn run_add_and_lookup_node(items: &[&str]) {
        let mut n = Node::default();

        for (i, c) in items.iter().enumerate() {
            let e = make_entry(c);
            node_add(&mut n, c.as_bytes(), e, BTreeMap::new());

            for &d in items.iter().take(i) {
                let node = node_lookup_node(&mut n, d.as_bytes()).unwrap();
                assert!(node.is_value());
                assert_eq!(node.entry(), Some(&make_entry(d)));
            }
        }
    }

    #[test]
    fn add_and_lookup_node_a() {
        run_add_and_lookup_node(&test_case_data()[0].items);
    }

    #[test]
    fn add_and_lookup_node_simple() {
        run_add_and_lookup_node(&test_case_data()[1].items);
    }

    #[test]
    fn add_and_lookup_node_nested_value() {
        run_add_and_lookup_node(&test_case_data()[2].items);
    }

    #[test]
    fn add_and_lookup_node_nested_prefix() {
        run_add_and_lookup_node(&test_case_data()[3].items);
    }

    #[test]
    fn add_and_lookup_node_conflicting_path() {
        run_add_and_lookup_node(&test_case_data()[4].items);
    }

    #[test]
    fn add_and_lookup_node_spa_website() {
        run_add_and_lookup_node(&test_case_data()[5].items);
    }

    fn run_add_and_lookup_with_load_save(items: &[&str]) {
        let mut n = Node::default();

        for c in items {
            let e = make_entry(c);
            node_add(&mut n, c.as_bytes(), e, BTreeMap::new());
        }

        let mut store = MemoryStore::<{ DEFAULT_BODY_SIZE }>::new();
        n.save(&mut store).unwrap();

        let mut n2: Node = Node::from_reference(n.reference.unwrap());

        for &d in items {
            let node = n2
                .lookup_node_with_loader(d.as_bytes(), &store)
                .unwrap();
            assert!(node.is_value());
            assert_eq!(node.entry(), Some(&make_entry(d)));
        }
    }

    #[test]
    fn add_and_lookup_with_load_save_a() {
        run_add_and_lookup_with_load_save(&test_case_data()[0].items);
    }

    #[test]
    fn add_and_lookup_with_load_save_simple() {
        run_add_and_lookup_with_load_save(&test_case_data()[1].items);
    }

    #[test]
    fn add_and_lookup_with_load_save_nested_value() {
        run_add_and_lookup_with_load_save(&test_case_data()[2].items);
    }

    #[test]
    fn add_and_lookup_with_load_save_nested_prefix() {
        run_add_and_lookup_with_load_save(&test_case_data()[3].items);
    }

    #[test]
    fn add_and_lookup_with_load_save_conflicting_path() {
        run_add_and_lookup_with_load_save(&test_case_data()[4].items);
    }

    #[test]
    fn add_and_lookup_with_load_save_spa_website() {
        run_add_and_lookup_with_load_save(&test_case_data()[5].items);
    }

    fn run_remove(tc: RemoveTestCase) {
        let mut n = Node::default();

        for (i, c) in tc.items.iter().enumerate() {
            let e = make_entry(&c.path);
            node_add(&mut n, c.path.as_bytes(), e, c.metadata.clone());

            for item in tc.items.iter().take(i) {
                let r = node_lookup(&mut n, item.path.as_bytes()).unwrap();
                assert_eq!(r, Some(&make_entry(&item.path)));
            }
        }

        for c in &tc.remove {
            node_remove(&mut n, c.as_bytes()).unwrap();
            assert!(node_lookup(&mut n, c.as_bytes()).is_err());
        }
    }

    #[test]
    fn remove_simple() {
        run_remove(remove_test_case_data()[0].clone());
    }

    #[test]
    fn remove_nested_prefix() {
        run_remove(remove_test_case_data()[1].clone());
    }

    fn run_has_prefix(tc: HasPrefixTestCase) {
        let mut n = Node::default();

        for c in &tc.paths {
            let e = make_entry(c);
            node_add(&mut n, c.as_bytes(), e, BTreeMap::default());
        }

        for (i, test_prefix) in tc.test_paths.iter().enumerate() {
            assert_eq!(
                node_has_prefix(&mut n, test_prefix.as_bytes()).unwrap(),
                tc.should_exist[i],
            );
        }
    }

    #[test]
    fn has_prefix_simple() {
        run_has_prefix(has_prefix_test_case_data()[0].clone());
    }

    #[test]
    fn has_prefix_nested_single() {
        run_has_prefix(has_prefix_test_case_data()[1].clone());
    }

    // Tests save->reload->remove->save->reload->verify-removed cycle.

    fn run_persist_remove(tc: RemoveTestCase) {
        let mut store = MemoryStore::<{ DEFAULT_BODY_SIZE }>::new();

        // add entries and persist
        let mut n = Node::default();
        for c in &tc.items {
            let e = make_entry(&c.path);
            n.add_with_loader(c.path.as_bytes(), Some(e), c.metadata.clone(), &store)
                .unwrap();
        }
        n.save(&mut store).unwrap();
        let ref_ = n.reference.unwrap();

        // reload and remove
        let mut nn: Node = Node::from_reference(ref_);
        for path in &tc.remove {
            nn.remove_with_loader(path.as_bytes(), &store).unwrap();
        }
        nn.save(&mut store).unwrap();
        let ref2 = nn.reference.unwrap();

        // reload and verify removed paths are gone
        let mut nnn: Node = Node::from_reference(ref2);
        for path in &tc.remove {
            let result = nnn.lookup_node_with_loader(path.as_bytes(), &store);
            assert!(result.is_err(), "expected removed path '{path}' to be not found");
        }
    }

    #[test]
    fn persist_remove_simple() {
        run_persist_remove(remove_test_case_data()[0].clone());
    }

    #[test]
    fn persist_remove_nested_prefix() {
        run_persist_remove(remove_test_case_data()[1].clone());
    }

    fn make_entry_bytes(s: &[u8]) -> ChunkAddress {
        let mut buf = [0u8; 32];
        let start = 32 - s.len();
        buf[start..].copy_from_slice(s);
        ChunkAddress::from(buf)
    }

    #[test]
    fn walk_visits_all_nodes() {
        let mut root = Node::default();

        let paths = &["index.html", "img/1.png", "img/2.png", "robots.txt"];
        for &p in paths {
            let entry = make_entry_bytes(p.as_bytes());
            node_add(&mut root, p.as_bytes(), entry, BTreeMap::new());
        }

        let mut visited: Vec<(Vec<u8>, bool)> = Vec::new();
        node_walk(&mut root, &mut |path, node| {
            visited.push((path.to_vec(), node.is_value()));
            Ok(())
        })
        .unwrap();

        for &p in paths {
            assert!(
                visited
                    .iter()
                    .any(|(vp, is_val)| vp == p.as_bytes() && *is_val),
                "path {p} not visited as value"
            );
        }
    }

    #[test]
    fn walk_node_exact_order() {
        let to_add: &[&[u8]] = &[
            b"index.html.backup",
            b"index.html",
            b"img/test/oho.png",
            b"img/test/old/test.png.backup",
            b"img/test/old/test.png",
            b"img/2.png",
            b"img/1.png",
            b"robots.txt",
        ];

        let expected: &[&[u8]] = &[
            b"",
            b"i",
            b"img/",
            b"img/1.png",
            b"img/2.png",
            b"img/test/o",
            b"img/test/oho.png",
            b"img/test/old/test.png",
            b"img/test/old/test.png.backup",
            b"index.html",
            b"index.html.backup",
            b"robots.txt",
        ];

        let mut n = Node::default();
        for &path in to_add {
            let entry = make_entry_bytes(path);
            node_add(&mut n, path, entry, BTreeMap::new());
        }

        let mut walked: Vec<Vec<u8>> = Vec::new();
        node_walk_node(&mut n, b"", &mut |path, _node| {
            walked.push(path.to_vec());
            Ok(())
        })
        .unwrap();

        assert_eq!(
            walked.len(),
            expected.len(),
            "expected {} nodes, got {}",
            expected.len(),
            walked.len()
        );

        for (i, (got, &want)) in walked.iter().zip(expected.iter()).enumerate() {
            assert_eq!(
                got.as_slice(),
                want,
                "walk step {i}: expected {:?}, got {:?}",
                core::str::from_utf8(want).unwrap_or("<non-utf8>"),
                core::str::from_utf8(got).unwrap_or("<non-utf8>"),
            );
        }
    }

    #[test]
    fn walk_node_from_subtree() {
        let to_add: &[&[u8]] = &[
            b"index.html",
            b"img/1.png",
            b"img/2.png",
            b"robots.txt",
        ];

        let mut n = Node::default();
        for &path in to_add {
            let entry = make_entry_bytes(path);
            node_add(&mut n, path, entry, BTreeMap::new());
        }

        let mut walked: Vec<Vec<u8>> = Vec::new();
        node_walk_node(&mut n, b"img/", &mut |path, _node| {
            walked.push(path.to_vec());
            Ok(())
        })
        .unwrap();

        assert!(walked.iter().any(|p| p == b"img/1.png"));
        assert!(walked.iter().any(|p| p == b"img/2.png"));
        assert!(!walked.iter().any(|p| p == b"index.html"));
        assert!(!walked.iter().any(|p| p == b"robots.txt"));
    }

    #[test]
    fn walk_node_exact_order_with_load_save() {
        let to_add: &[&[u8]] = &[
            b"index.html.backup",
            b"index.html",
            b"img/test/oho.png",
            b"img/test/old/test.png.backup",
            b"img/test/old/test.png",
            b"img/2.png",
            b"img/1.png",
            b"robots.txt",
        ];

        let expected: &[&[u8]] = &[
            b"",
            b"i",
            b"img/",
            b"img/1.png",
            b"img/2.png",
            b"img/test/o",
            b"img/test/oho.png",
            b"img/test/old/test.png",
            b"img/test/old/test.png.backup",
            b"index.html",
            b"index.html.backup",
            b"robots.txt",
        ];

        let mut n = Node::default();
        for &path in to_add {
            let entry = make_entry_bytes(path);
            node_add(&mut n, path, entry, BTreeMap::new());
        }

        let mut store = MemoryStore::<{ DEFAULT_BODY_SIZE }>::new();
        n.save(&mut store).unwrap();

        let mut n2: Node = Node::from_reference(n.reference.unwrap());

        let mut walked: Vec<Vec<u8>> = Vec::new();
        n2.walk_node_with_loader(b"", &store, &mut |path: &[u8], _node: &Node| {
            walked.push(path.to_vec());
            Ok(())
        })
        .unwrap();

        assert_eq!(
            walked.len(),
            expected.len(),
            "expected {} nodes, got {}",
            expected.len(),
            walked.len()
        );

        for (i, (got, &want)) in walked.iter().zip(expected.iter()).enumerate() {
            assert_eq!(
                got.as_slice(),
                want,
                "walk step {i}: expected {:?}, got {:?}",
                core::str::from_utf8(want).unwrap_or("<non-utf8>"),
                core::str::from_utf8(got).unwrap_or("<non-utf8>"),
            );
        }
    }
}

//! Node and Fork types for the mantaray trie.

use std::collections::BTreeMap;

use alloy_primitives::hex;
use bytes::Bytes;
use nectar_primitives::chunk::{Chunk, ChunkAddress, ContentChunk};
use nectar_primitives::store::{ChunkGet, ChunkPut};

use crate::error::{MantarayError, Result};
use crate::{
    NODE_OBFUSCATION_KEY_SIZE, NODE_PREFIX_MAX_SIZE, NT_EDGE, NT_MASK, NT_VALUE, NT_WITH_METADATA,
    NT_WITH_PATH_SEPARATOR, PATH_SEPARATOR,
};

/// A node in the mantaray trie.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Node {
    /// Bitfield encoding the node kind (value, edge, path-separator, metadata).
    pub(crate) node_type: u8,
    /// Size of references in bytes (typically 32 or 64 for encrypted).
    pub(crate) ref_bytes_size: u32,
    /// XOR obfuscation key for binary serialisation.
    pub(crate) obfuscation_key: Vec<u8>,
    /// Content-addressed reference for this node (empty if not yet persisted).
    pub(crate) reference: Vec<u8>,
    /// The entry data stored at this node (the chunk reference this path maps to).
    pub(crate) entry: Vec<u8>,
    /// Metadata key-value pairs attached to this node.
    pub(crate) metadata: BTreeMap<String, String>,
    /// Child forks keyed by the first byte of their prefix.
    pub(crate) forks: BTreeMap<u8, Fork>,
}

/// A fork in the mantaray trie, consisting of a prefix and a child node.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Fork {
    /// The prefix bytes for this fork edge.
    pub(crate) prefix: Vec<u8>,
    /// The child node.
    pub(crate) node: Node,
}

impl Fork {
    /// The prefix bytes for this fork edge.
    pub fn prefix(&self) -> &[u8] {
        &self.prefix
    }

    /// The child node.
    pub const fn node(&self) -> &Node {
        &self.node
    }

    /// Mutable access to the child node.
    pub const fn node_mut(&mut self) -> &mut Node {
        &mut self.node
    }
}

/// Return the length of the common prefix of two byte slices.
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

/// Convert a reference byte slice to a `ChunkAddress`.
///
/// Takes the first 32 bytes of the reference. Panics if reference is shorter
/// than 32 bytes (caller must ensure this).
fn ref_to_address(reference: &[u8]) -> ChunkAddress {
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&reference[..32]);
    ChunkAddress::from(bytes)
}

impl Node {
    /// Create a new node with a zeroed obfuscation key (unencrypted).
    pub fn new_unencrypted() -> Self {
        Self {
            obfuscation_key: vec![0u8; NODE_OBFUSCATION_KEY_SIZE],
            ..Default::default()
        }
    }

    /// Create a node that references persisted data.
    pub fn from_reference(reference: &[u8]) -> Self {
        Self {
            reference: reference.to_vec(),
            ..Default::default()
        }
    }

    /// The entry data stored at this node.
    pub fn entry(&self) -> &[u8] {
        &self.entry
    }

    /// Metadata key-value pairs attached to this node.
    pub const fn metadata(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }

    /// Content-addressed reference for this node.
    pub fn reference(&self) -> &[u8] {
        &self.reference
    }

    /// Child forks keyed by the first byte of their prefix.
    pub const fn forks(&self) -> &BTreeMap<u8, Fork> {
        &self.forks
    }

    /// Mutable access to child forks.
    pub const fn forks_mut(&mut self) -> &mut BTreeMap<u8, Fork> {
        &mut self.forks
    }

    /// XOR obfuscation key for binary serialisation.
    pub fn obfuscation_key(&self) -> &[u8] {
        &self.obfuscation_key
    }

    /// Set the content-addressed reference for this node.
    pub fn set_reference(&mut self, reference: Vec<u8>) {
        self.reference = reference;
    }

    /// Returns true if this node contains an entry (is a value node).
    pub const fn is_value(&self) -> bool {
        (self.node_type & NT_VALUE) == NT_VALUE
    }

    /// Returns true if this node has child forks.
    pub const fn is_edge(&self) -> bool {
        (self.node_type & NT_EDGE) == NT_EDGE
    }

    /// Returns true if the node path contains a separator character.
    pub const fn is_with_path_separator(&self) -> bool {
        (self.node_type & NT_WITH_PATH_SEPARATOR) == NT_WITH_PATH_SEPARATOR
    }

    /// Returns true if this node carries metadata.
    pub const fn is_with_metadata(&self) -> bool {
        (self.node_type & NT_WITH_METADATA) == NT_WITH_METADATA
    }

    pub(crate) const fn make_value(&mut self) {
        self.node_type |= NT_VALUE;
    }

    pub(crate) const fn make_edge(&mut self) {
        self.node_type |= NT_EDGE;
    }

    const fn make_with_path_separator(&mut self) {
        self.node_type |= NT_WITH_PATH_SEPARATOR;
    }

    const fn make_with_metadata(&mut self) {
        self.node_type |= NT_WITH_METADATA;
    }

    #[allow(dead_code)]
    pub(crate) const fn make_not_value(&mut self) {
        self.node_type &= NT_MASK ^ NT_VALUE;
    }

    const fn make_not_with_path_separator(&mut self) {
        self.node_type &= NT_MASK ^ NT_WITH_PATH_SEPARATOR;
    }

    fn set_obfuscation_key(&mut self, key: &[u8]) {
        assert_eq!(key.len(), NODE_OBFUSCATION_KEY_SIZE, "invalid key length");
        self.obfuscation_key = key.to_vec();
    }

    fn update_is_with_path_separator(&mut self, path: &[u8]) {
        let sep = PATH_SEPARATOR.as_bytes()[0];
        if path.iter().skip(1).any(|&b| b == sep) {
            self.make_with_path_separator();
        } else {
            self.make_not_with_path_separator();
        }
    }

    /// Load this node from storage if it has a reference but no forks loaded.
    pub fn load<S: ChunkGet<BS>, const BS: usize>(&mut self, loader: Option<&S>) -> Result<()> {
        if self.reference.is_empty() {
            return Ok(());
        }

        let loader = loader.ok_or_else(|| MantarayError::StoreGet {
            message: "no loader provided".into(),
        })?;
        let address = ref_to_address(&self.reference);
        let chunk = loader.get(&address).map_err(|e| MantarayError::StoreGet {
            message: e.to_string(),
        })?;
        let mut data = chunk.data().to_vec();
        self.unmarshal_binary(&mut data)
    }

    /// Look up the node at the given path, loading from storage as needed.
    pub fn lookup_node<S: ChunkGet<BS>, const BS: usize>(
        &mut self,
        path: &[u8],
        loader: Option<&S>,
    ) -> Result<&mut Self> {
        if self.forks.is_empty() {
            self.load(loader)?;
        }

        if path.is_empty() {
            return Ok(self);
        }

        let first = path[0];
        let fork = self.forks.get_mut(&first).ok_or_else(|| {
            MantarayError::NoForkFound {
                ref_hex: hex::encode(&self.reference),
            }
        })?;

        let c = common_prefix_len(&fork.prefix, path);
        if c == fork.prefix.len() {
            fork.node.lookup_node(&path[c..], loader)
        } else {
            Err(MantarayError::NoForkFound {
                ref_hex: hex::encode(&self.reference),
            })
        }
    }

    /// Look up the entry bytes at the given path.
    pub fn lookup<S: ChunkGet<BS>, const BS: usize>(
        &mut self,
        path: &[u8],
        loader: Option<&S>,
    ) -> Result<&[u8]> {
        let node = self.lookup_node(path, loader)?;
        if !node.is_value() && !path.is_empty() {
            return Err(MantarayError::NoEntryFound {
                ref_hex: hex::encode(&node.reference),
            });
        }
        Ok(node.entry.as_slice())
    }

    /// Add an entry at the given path with optional metadata.
    pub fn add<S: ChunkGet<BS>, const BS: usize>(
        &mut self,
        path: &[u8],
        entry: &[u8],
        metadata: BTreeMap<String, String>,
        loader: Option<&S>,
    ) -> Result<()> {
        if self.ref_bytes_size == 0 {
            if entry.len() > 256 {
                return Err(MantarayError::EntryTooLarge {
                    size: entry.len(),
                    max: 256,
                });
            }
            if !entry.is_empty() {
                self.ref_bytes_size = entry.len() as u32;
            }
        } else if !entry.is_empty() && entry.len() != self.ref_bytes_size as usize {
            return Err(MantarayError::EntrySizeMismatch {
                expected: self.ref_bytes_size as usize,
                actual: entry.len(),
            });
        }

        // empty path — set this node as a value
        if path.is_empty() {
            self.entry = entry.to_vec();
            self.make_value();

            if !metadata.is_empty() {
                self.metadata = metadata;
                self.make_with_metadata();
            }

            self.reference = vec![];
            return Ok(());
        }

        // load forks if needed
        if self.forks.is_empty() {
            self.load(loader)?;
            self.reference = vec![];
        }

        if !self.forks.contains_key(&path[0]) {
            // no existing fork for this byte — create a new one
            let mut nn = Self::default();

            if !self.obfuscation_key.is_empty() {
                nn.set_obfuscation_key(&self.obfuscation_key);
            }
            nn.ref_bytes_size = self.ref_bytes_size;

            if path.len() > NODE_PREFIX_MAX_SIZE {
                let (prefix, rest) = path.split_at(NODE_PREFIX_MAX_SIZE);
                nn.add(rest, entry, metadata, loader)?;
                nn.update_is_with_path_separator(prefix);
                self.forks.insert(
                    path[0],
                    Fork {
                        prefix: prefix.to_vec(),
                        node: nn,
                    },
                );
                self.make_edge();
                return Ok(());
            }

            nn.entry = entry.to_vec();
            if !metadata.is_empty() {
                nn.metadata = metadata;
                nn.make_with_metadata();
            }
            nn.make_value();
            nn.update_is_with_path_separator(path);

            self.forks.insert(
                path[0],
                Fork {
                    prefix: path.to_vec(),
                    node: nn,
                },
            );
            self.make_edge();
            return Ok(());
        }

        // existing fork — need to split or extend
        let fork = self.forks.get(&path[0]).expect("checked above");
        let c = common_prefix_len(&fork.prefix, path);
        let rest = fork.prefix[c..].to_vec();
        let mut nn = fork.node.clone();

        if !rest.is_empty() {
            // split: create intermediate node
            nn = Self::default();
            if !self.obfuscation_key.is_empty() {
                nn.set_obfuscation_key(&self.obfuscation_key);
            }
            nn.ref_bytes_size = self.ref_bytes_size;

            let mut old_fork_node = self.forks[&path[0]].node.clone();
            old_fork_node.update_is_with_path_separator(&rest);

            nn.forks.insert(
                rest[0],
                Fork {
                    prefix: rest,
                    node: old_fork_node,
                },
            );
            nn.make_edge();

            if c == path.len() {
                nn.make_value();
            }
        }

        nn.update_is_with_path_separator(path);
        nn.add(&path[c..], entry, metadata, loader)?;

        self.forks.insert(
            path[0],
            Fork {
                prefix: fork.prefix[..c].to_vec(),
                node: nn,
            },
        );
        self.make_edge();

        Ok(())
    }

    /// Remove the entry at the given path.
    pub fn remove<S: ChunkGet<BS>, const BS: usize>(
        &mut self,
        path: &[u8],
        loader: Option<&S>,
    ) -> Result<()> {
        if path.is_empty() {
            return Err(MantarayError::EmptyPath);
        }

        if self.forks.is_empty() {
            self.load(loader)?;
        }

        let first = path[0];

        // Clone prefix to release the borrow on self.forks
        let prefix = match self.forks.get(&first) {
            Some(f) => f.prefix.clone(),
            None => {
                self.reference = vec![];
                return Err(MantarayError::PathPrefixNotFound {
                    prefix: String::from_utf8_lossy(&[first]).to_string(),
                });
            }
        };

        if !path.starts_with(&prefix) {
            self.reference = vec![];
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
            fork.node.remove(rest, loader)
        };

        // Always clear reference so the node gets re-saved (matches Go's defer pattern)
        self.reference = vec![];
        result
    }

    /// Test whether a prefix exists in the trie.
    pub fn has_prefix<S: ChunkGet<BS>, const BS: usize>(
        &mut self,
        path: &[u8],
        loader: Option<&S>,
    ) -> Result<bool> {
        if path.is_empty() {
            return Ok(true);
        }

        if self.forks.is_empty() {
            self.load(loader)?;
        }

        let fork = match self.forks.get_mut(&path[0]) {
            Some(f) => f,
            None => return Ok(false),
        };

        let c = common_prefix_len(&fork.prefix, path);

        if c == fork.prefix.len() {
            return fork.node.has_prefix(&path[c..], loader);
        }

        if fork.prefix.starts_with(path) {
            return Ok(true);
        }

        Ok(false)
    }

    /// Recursively save this node and all children to storage.
    ///
    /// Uses BMT content-addressing via `ContentChunk` for Go Bee compatibility.
    pub fn save<S: ChunkPut<BS>, const BS: usize>(&mut self, saver: &mut S) -> Result<()> {
        if !self.reference.is_empty() {
            return Ok(());
        }

        for fork in self.forks.values_mut() {
            fork.node.save(saver)?;
        }

        let data = self.marshal_binary()?;
        let chunk = ContentChunk::<BS>::new(Bytes::from(data)).map_err(|e| {
            MantarayError::ChunkError {
                message: e.to_string(),
            }
        })?;
        let address = *chunk.address();
        saver.put(chunk).map_err(|e| MantarayError::StorePut {
            message: e.to_string(),
        })?;
        self.reference = address.as_slice().to_vec();
        self.forks.clear();

        Ok(())
    }

    /// Walk all nodes depth-first, calling `f` for each node with its path.
    pub fn walk<S: ChunkGet<BS>, const BS: usize, F>(
        &mut self,
        loader: Option<&S>,
        f: &mut F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &Self) -> Result<()>,
    {
        walk_inner(&[], self, loader, f)
    }

    /// Walk the subtree at `root`, calling `f` for each node.
    pub fn walk_node<S: ChunkGet<BS>, const BS: usize, F>(
        &mut self,
        root: &[u8],
        loader: Option<&S>,
        f: &mut F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &Self) -> Result<()>,
    {
        if root.is_empty() {
            return walk_inner(&[], self, loader, f);
        }

        let target = self.lookup_node(root, loader)?;
        walk_inner(root, target, loader, f)
    }
}

fn walk_inner<S: ChunkGet<BS>, const BS: usize, F>(
    path: &[u8],
    node: &mut Node,
    loader: Option<&S>,
    f: &mut F,
) -> Result<()>
where
    F: FnMut(&[u8], &Node) -> Result<()>,
{
    if node.forks.is_empty() {
        node.load(loader)?;
    }

    f(path, node)?;

    // collect keys to avoid borrow conflict
    let keys: Vec<u8> = node.forks.keys().copied().collect();
    for key in keys {
        let fork = node.forks.get_mut(&key).expect("key from iterator");
        let mut next_path = path.to_vec();
        next_path.extend_from_slice(&fork.prefix);
        walk_inner(&next_path, &mut fork.node, loader, f)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
    use nectar_primitives::store::MemorySink;

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

    /// Create a 32-byte entry from a string, left-padded with zeroes.
    fn make_entry(s: &str) -> Vec<u8> {
        let bytes = s.as_bytes();
        let mut entry = vec![0u8; 32 - bytes.len()];
        entry.extend_from_slice(bytes);
        entry
    }

    // Type alias for no-loader calls — we need to specify the generic params.
    type NoLoader = MemorySink;

    #[test]
    fn nil_path() {
        let mut n = Node::default();
        assert!(n.lookup::<NoLoader, { DEFAULT_BODY_SIZE }>(b"", None).is_ok());
    }

    #[test]
    fn add_and_lookup() {
        let mut n = Node::default();
        let items = &test_case_data()[0].items;

        for (i, c) in items.iter().enumerate() {
            let e = make_entry(c);
            n.add::<NoLoader, { DEFAULT_BODY_SIZE }>(c.as_bytes(), &e, BTreeMap::new(), None)
                .unwrap();

            for &d in items.iter().take(i) {
                let r = n
                    .lookup::<NoLoader, { DEFAULT_BODY_SIZE }>(d.as_bytes(), None)
                    .unwrap();
                assert_eq!(r, make_entry(d));
            }
        }
    }

    fn run_add_and_lookup_node(items: &[&str]) {
        let mut n = Node::default();

        for (i, c) in items.iter().enumerate() {
            let e = make_entry(c);
            n.add::<NoLoader, { DEFAULT_BODY_SIZE }>(c.as_bytes(), &e, BTreeMap::new(), None)
                .unwrap();

            for &d in items.iter().take(i) {
                let node = n
                    .lookup_node::<NoLoader, { DEFAULT_BODY_SIZE }>(d.as_bytes(), None)
                    .unwrap();
                assert!(node.is_value());
                assert_eq!(node.entry(), make_entry(d));
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
            n.add::<MemorySink, { DEFAULT_BODY_SIZE }>(c.as_bytes(), &e, BTreeMap::new(), None)
                .unwrap();
        }

        let mut store = MemorySink::<{ DEFAULT_BODY_SIZE }>::new();
        n.save(&mut store).unwrap();

        let mut n2 = Node::from_reference(&n.reference);

        for &d in items {
            let node = n2
                .lookup_node(d.as_bytes(), Some(&store))
                .unwrap();
            assert!(node.is_value());
            assert_eq!(node.entry(), make_entry(d));
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
            n.add::<NoLoader, { DEFAULT_BODY_SIZE }>(
                c.path.as_bytes(),
                &e,
                c.metadata.clone(),
                None,
            )
            .unwrap();

            for item in tc.items.iter().take(i) {
                let r = n
                    .lookup::<NoLoader, { DEFAULT_BODY_SIZE }>(item.path.as_bytes(), None)
                    .unwrap();
                assert_eq!(r, make_entry(&item.path));
            }
        }

        for c in &tc.remove {
            n.remove::<NoLoader, { DEFAULT_BODY_SIZE }>(c.as_bytes(), None)
                .unwrap();
            assert!(n
                .lookup::<NoLoader, { DEFAULT_BODY_SIZE }>(c.as_bytes(), None)
                .is_err());
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
            n.add::<NoLoader, { DEFAULT_BODY_SIZE }>(c.as_bytes(), &e, BTreeMap::default(), None)
                .unwrap();
        }

        for (i, test_prefix) in tc.test_paths.iter().enumerate() {
            assert_eq!(
                n.has_prefix::<NoLoader, { DEFAULT_BODY_SIZE }>(test_prefix.as_bytes(), None)
                    .unwrap(),
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

    // --- Go bee compatibility: TestPersistRemove ---
    // Tests save->reload->remove->save->reload->verify-removed cycle.

    fn run_persist_remove(tc: RemoveTestCase) {
        let mut store = MemorySink::<{ DEFAULT_BODY_SIZE }>::new();

        // add entries and persist
        let mut n = Node::default();
        for c in &tc.items {
            let e = make_entry(&c.path);
            n.add(c.path.as_bytes(), &e, c.metadata.clone(), Some(&store))
                .unwrap();
        }
        n.save(&mut store).unwrap();
        let ref_ = n.reference.clone();

        // reload and remove
        let mut nn = Node::from_reference(&ref_);
        for path in &tc.remove {
            nn.remove(path.as_bytes(), Some(&store)).unwrap();
        }
        nn.save(&mut store).unwrap();
        let ref2 = nn.reference.clone();

        // reload and verify removed paths are gone
        let mut nnn = Node::from_reference(&ref2);
        for path in &tc.remove {
            let result = nnn.lookup_node(path.as_bytes(), Some(&store));
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
}

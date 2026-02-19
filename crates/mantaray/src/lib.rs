//! Mantaray manifest trie for Ethereum Swarm.
//!
//! Mantaray is a trie-based manifest structure that maps human-readable paths
//! (e.g. `index.html`, `img/logo.png`) to content-addressed chunk references.
//! It supports XOR obfuscation, versioned binary serialisation (v0.1 and v0.2),
//! and metadata per path.

#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;
pub mod error;
pub mod marshal;
pub mod node;
pub mod persist;
pub mod walker;

pub use error::{MantarayError, Result};
pub use node::{Fork, Node};
pub use persist::{MantarayLoader, MantaraySaver, MantarayStore, MockStoreCell};
pub use walker::{walk, walk_node};

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

/// A reference to Swarm content (typically 32 or 64 bytes for encrypted).
pub type Reference = Vec<u8>;

/// A manifest entry: a reference plus optional metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// The chunk reference.
    pub reference: Reference,
    /// Key-value metadata.
    pub metadata: BTreeMap<String, String>,
}

/// High-level mantaray manifest backed by an optional storage backend.
#[derive(Debug)]
pub struct Manifest<S> {
    /// The root trie node.
    pub trie: Node,
    store: Option<S>,
}

impl<S> Manifest<S> {
    /// Create a new manifest. If `encrypted` is false, the obfuscation key is zeroed.
    pub fn new(store: S, encrypted: bool) -> Self {
        let mut trie = Node::default();
        if !encrypted {
            trie.obfuscation_key = vec![0u8; NODE_OBFUSCATION_KEY_SIZE];
        }
        Self {
            trie,
            store: Some(store),
        }
    }

    /// Create a manifest from an existing root reference.
    pub fn new_manifest_reference(reference: &[u8], store: S) -> Self {
        Self {
            trie: Node::new_node_ref(reference),
            store: Some(store),
        }
    }
}

impl<S: MantarayLoader> Manifest<S> {
    /// Add a path and entry to the manifest.
    pub fn add(&mut self, path: &str, entry: Entry) -> Result<()> {
        self.trie.add(
            path.as_bytes(),
            &entry.reference,
            entry.metadata,
            self.store.as_ref().map(|s| s as &dyn MantarayLoader),
        )
    }

    /// Remove a path from the manifest.
    pub fn remove(&mut self, path: &str) -> Result<()> {
        self.trie.remove(
            path.as_bytes(),
            self.store.as_ref().map(|s| s as &dyn MantarayLoader),
        )
    }

    /// Look up a path in the manifest.
    pub fn lookup(&mut self, path: &str) -> Result<Entry> {
        let node = self.trie.lookup_node(
            path.as_bytes(),
            self.store.as_ref().map(|s| s as &dyn MantarayLoader),
        )?;

        if !node.is_value_type() {
            return Err(MantarayError::NotValueType);
        }

        Ok(Entry {
            reference: node.entry.clone(),
            metadata: node.metadata.clone(),
        })
    }

    /// Test whether the manifest contains a prefix.
    pub fn has_prefix(&mut self, prefix: &str) -> Result<bool> {
        self.trie.has_prefix(
            prefix.as_bytes(),
            self.store.as_ref().map(|s| s as &dyn MantarayLoader),
        )
    }
}

impl<S: MantarayLoader + MantaraySaver> Manifest<S> {
    /// Persist the manifest trie to storage.
    pub fn store(&mut self) -> Result<Vec<u8>> {
        let saver = self.store.as_ref().expect("store is set");
        self.trie.save(saver)?;
        Ok(self.trie.ref_.clone())
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
        let store = MockStoreCell::new();

        let mut m = Manifest::new(&store, false);

        let paths = &[
            "aa", "b", "aaaaaa", "aaaaab", "abbbb", "abbba", "bbbbba", "bbbaaa", "bbbaab",
        ];

        for &path in paths {
            m.store().unwrap();
            let mut v = path.as_bytes().to_vec();
            v.resize(32, 0);
            m.add(
                path,
                Entry {
                    reference: v,
                    metadata: BTreeMap::new(),
                },
            )
            .unwrap();
        }

        m.store().unwrap();

        for &path in paths {
            let entry = m.lookup(path).unwrap();
            let mut v = path.as_bytes().to_vec();
            v.resize(32, 0);
            assert_eq!(entry.reference, v);
        }
    }
}

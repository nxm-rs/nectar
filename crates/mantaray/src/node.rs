//! Node and Fork types for the mantaray trie.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use crate::error::{MantarayError, Result};
use crate::obfuscation::ObfuscationKey;
use crate::{PATH_SEPARATOR, PREFIX_MAX_LEN};
use bytes::Bytes;
use nectar_primitives::chunk::{Chunk, ChunkAddress, ChunkRef, ContentChunk, Reference};
use nectar_primitives::store::{ChunkGet, ChunkPut, MaybeSend};
use nectar_primitives::wire::{Cursor, FromCursor, ToWriter, Writer};

/// Boxed recursion future: `Send` on native, unbounded on wasm32 so `!Send`
/// browser stores stay usable. `MaybeSend` cannot appear in a `dyn` bound
/// directly (it is not an auto trait), so the auto trait is cfg-gated here.
#[cfg(not(target_arch = "wasm32"))]
type RecurseFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
#[cfg(target_arch = "wasm32")]
type RecurseFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + 'a>>;

/// Inline-only byte buffer for fork prefixes (max 30 bytes).
///
/// Always stores data inline; no heap allocation, no branching.
/// 31 bytes total (1 len + 30 data).
#[derive(Clone, PartialEq, Eq)]
pub struct Prefix {
    len: u8,
    data: [u8; PREFIX_MAX_LEN],
}

impl Default for Prefix {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Prefix {
    /// Maximum prefix length in bytes (constrained by the fork pre-reference region).
    pub const MAX_LEN: usize = PREFIX_MAX_LEN;

    /// Create an empty prefix.
    #[inline]
    pub const fn new() -> Self {
        Self {
            len: 0,
            data: [0u8; PREFIX_MAX_LEN],
        }
    }

    /// Create a prefix from a byte slice whose length is already structurally
    /// bounded (trie construction, where splits never exceed the maximum).
    ///
    /// The wire decode path reads a `Prefix` from a [`Cursor`] instead, which
    /// validates the length rather than asserting it.
    ///
    /// # Panics
    ///
    /// Panics if `src.len() > 30`.
    #[inline]
    pub fn from_slice(src: &[u8]) -> Self {
        assert!(
            src.len() <= PREFIX_MAX_LEN,
            "prefix length {} exceeds maximum {}",
            src.len(),
            PREFIX_MAX_LEN
        );
        let mut data = [0u8; PREFIX_MAX_LEN];
        #[allow(clippy::indexing_slicing)]
        // src.len() <= PREFIX_MAX_LEN asserted above (documented # Panics contract)
        data[..src.len()].copy_from_slice(src);
        #[allow(clippy::as_conversions)] // src.len() <= PREFIX_MAX_LEN (30) asserted above, fits u8
        let len = src.len() as u8;
        Self { len, data }
    }

    /// Construct a prefix from its wire form: the fixed 30-byte padded region
    /// and the declared length byte.
    ///
    /// Enforces the 1..=30 length invariant at construction, so decode never
    /// relies on a caller-side guard. Bytes past `len` are re-zeroed to keep
    /// the padding canonical for equality and re-encoding.
    #[inline]
    pub fn from_wire(padded: &[u8; PREFIX_MAX_LEN], len: u8) -> Result<Self> {
        let len_usize = usize::from(len);
        if len == 0 || len_usize > PREFIX_MAX_LEN {
            return Err(MantarayError::InvalidPrefixLength {
                max: PREFIX_MAX_LEN,
                actual: len_usize,
            });
        }
        let mut data = [0u8; PREFIX_MAX_LEN];
        #[allow(clippy::indexing_slicing)] // len_usize <= PREFIX_MAX_LEN checked above
        data[..len_usize].copy_from_slice(&padded[..len_usize]);
        Ok(Self { len, data })
    }

    /// Returns the prefix length in bytes.
    #[inline]
    #[allow(clippy::as_conversions)] // u8 -> usize widening, infallible; `usize::from` is not const-callable
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
    pub const fn padded_bytes(&self) -> &[u8; PREFIX_MAX_LEN] {
        &self.data
    }
}

/// Reads the prefix wire record: the length byte, then the padded 30-byte
/// block. The length byte never leaves this impl; callers take a validated
/// `Prefix` in one step.
impl FromCursor for Prefix {
    type Error = MantarayError;

    fn take_from(cur: &mut Cursor<'_>) -> std::result::Result<Self, Self::Error> {
        let len = cur.take::<u8>()?;
        let padded = cur.take::<[u8; PREFIX_MAX_LEN]>()?;
        Self::from_wire(&padded, len)
    }
}

/// Writes the prefix wire record: the length byte, then the padded 30-byte
/// block. The mirror of the `FromCursor` impl above, so the length byte never
/// appears at call sites on the encode side either.
impl ToWriter for Prefix {
    fn put_into(&self, w: &mut Writer<'_>) {
        w.put(&self.len);
        w.put(&self.data);
    }
}

impl std::ops::Deref for Prefix {
    type Target = [u8];

    #[inline]
    #[allow(clippy::indexing_slicing)] // invariant: self.len <= PREFIX_MAX_LEN, the backing array length
    fn deref(&self) -> &[u8] {
        &self.data[..usize::from(self.len)]
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
pub struct Node<R: Reference = ChunkRef> {
    /// Bitflags encoding the node kind (value, edge, path-separator, metadata).
    pub(crate) node_type: NodeType,
    /// XOR obfuscation key for binary serialisation.
    pub(crate) obfuscation_key: ObfuscationKey,
    /// Content-addressed reference for this node (None if not yet persisted).
    pub(crate) reference: Option<ChunkAddress>,
    /// The typed entry stored at this node (the chunk reference this path maps to).
    pub(crate) entry: Option<R>,
    /// Metadata key-value pairs attached to this node.
    pub(crate) metadata: BTreeMap<String, String>,
    /// Child forks keyed by the first byte of their prefix.
    pub(crate) forks: BTreeMap<u8, Fork<R>>,
    /// Whether this node's forks have been loaded from storage.
    pub(crate) loaded: bool,
}

impl<R: Reference> Default for Node<R> {
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
pub struct Fork<R: Reference = ChunkRef> {
    /// Inline-only prefix (max 30 bytes). No heap allocation, no branching.
    pub(crate) prefix: Prefix,
    /// The child node.
    pub(crate) node: Node<R>,
}

impl<R: Reference> Default for Fork<R> {
    fn default() -> Self {
        Self {
            prefix: Prefix::new(),
            node: Node::default(),
        }
    }
}

impl<R: Reference> Fork<R> {
    /// The prefix bytes for this fork edge.
    pub fn prefix(&self) -> &[u8] {
        &self.prefix
    }

    /// The child node.
    pub const fn node(&self) -> &Node<R> {
        &self.node
    }

    /// Mutable access to the child node.
    pub const fn node_mut(&mut self) -> &mut Node<R> {
        &mut self.node
    }
}

/// Return the length of the common prefix of two byte slices.
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

impl<R: Reference> Node<R> {
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
    pub const fn entry(&self) -> Option<&R> {
        self.entry.as_ref()
    }

    /// Metadata key-value pairs attached to this node.
    pub const fn metadata(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }

    /// Mutable access to metadata for in-place modification.
    pub(crate) const fn metadata_mut(&mut self) -> &mut BTreeMap<String, String> {
        &mut self.metadata
    }

    /// Content-addressed reference for this node.
    pub const fn reference(&self) -> Option<&ChunkAddress> {
        self.reference.as_ref()
    }

    /// Child forks keyed by the first byte of their prefix.
    pub const fn forks(&self) -> &BTreeMap<u8, Fork<R>> {
        &self.forks
    }

    /// XOR obfuscation key for binary serialisation.
    pub const fn obfuscation_key(&self) -> &ObfuscationKey {
        &self.obfuscation_key
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

    fn update_is_with_path_separator(&mut self, path: &[u8]) {
        #[allow(clippy::indexing_slicing)] // PATH_SEPARATOR is a non-empty str constant
        let sep = PATH_SEPARATOR.as_bytes()[0];
        if path.iter().skip(1).any(|&b| b == sep) {
            self.node_type = self.node_type.union(NodeType::PATH_SEPARATOR);
        } else {
            self.node_type = self.node_type.difference(NodeType::PATH_SEPARATOR);
        }
    }

    /// Clear persisted reference, marking this node for re-serialization on next save.
    pub(crate) const fn mark_dirty(&mut self) {
        self.reference = None;
    }

    /// Load forks from storage if the node hasn't been loaded yet.
    async fn ensure_loaded<S: ChunkGet<BS>, const BS: usize>(&mut self, store: &S) -> Result<()> {
        if !self.loaded {
            self.load(store).await?;
        }
        Ok(())
    }

    /// Load this node from storage by its reference.
    pub(crate) async fn load<S: ChunkGet<BS>, const BS: usize>(&mut self, store: &S) -> Result<()> {
        let address = match self.reference {
            Some(addr) => addr,
            None => {
                self.loaded = true;
                return Ok(());
            }
        };

        let chunk = store
            .get(&address)
            .await
            .map_err(|e| MantarayError::StoreGet {
                source: std::sync::Arc::new(e),
            })?;
        let mut loaded = Self::try_from(chunk.data().as_ref())?;
        loaded.reference = Some(address);
        // Preserve fields that live in the parent's fork data, not in this node's chunk:
        // node_type flags and metadata key-value pairs.
        loaded.node_type |= self.node_type;
        loaded.metadata = core::mem::take(&mut self.metadata);
        *self = loaded;
        Ok(())
    }

    /// Look up the node at the given path, loading from storage as needed.
    #[allow(clippy::indexing_slicing)] // `rest` is checked non-empty before `rest[0]`; `c <= rest.len()` from common_prefix_len
    pub(crate) async fn lookup_node<S: ChunkGet<BS>, const BS: usize>(
        &mut self,
        path: &[u8],
        store: &S,
    ) -> Result<&mut Self> {
        // Iterative descent: reborrow `current` to the chosen child each step.
        let mut current = self;
        let mut rest = path;
        loop {
            current.ensure_loaded(store).await?;

            if rest.is_empty() {
                return Ok(current);
            }

            let first = rest[0];
            let reference = current.reference;
            let fork = current
                .forks
                .get_mut(&first)
                .ok_or(MantarayError::NoForkFound { reference })?;

            let c = common_prefix_len(&fork.prefix, rest);
            if c != fork.prefix.len() {
                return Err(MantarayError::NoForkFound { reference });
            }

            current = &mut fork.node;
            rest = &rest[c..];
        }
    }

    /// Look up the entry at the given path, loading from storage as needed.
    #[cfg(test)]
    pub(crate) async fn lookup<S: ChunkGet<BS>, const BS: usize>(
        &mut self,
        path: &[u8],
        store: &S,
    ) -> Result<Option<&R>> {
        let node = self.lookup_node(path, store).await?;
        if !node.is_value() && !path.is_empty() {
            return Err(MantarayError::NoEntryFound {
                reference: node.reference,
            });
        }
        Ok(node.entry.as_ref())
    }

    /// Add an entry at the given path with optional metadata, loading from storage as needed.
    ///
    /// Returns a boxed future so the `&mut self` recursion can name its own type.
    /// The `MaybeSend` bound keeps `!Send` wasm stores usable.
    // Panic-freedom: `path` is non-empty past the empty-path guard, so
    // `path[0]` is in-bounds; `c = common_prefix_len(prefix, path)` is
    // <= min(prefix.len(), path.len()), bounding every split; the fork at
    // `path[0]` is checked present (`contains_key`) before each get/expect.
    #[allow(clippy::indexing_slicing, clippy::expect_used)]
    pub(crate) fn add<'a, S: ChunkGet<BS>, const BS: usize>(
        &'a mut self,
        path: &'a [u8],
        entry: Option<R>,
        metadata: BTreeMap<String, String>,
        store: &'a S,
    ) -> RecurseFuture<'a>
    where
        R: MaybeSend,
    {
        Box::pin(async move {
            // empty path; set this node as a value
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
                self.load(store).await?;
                self.mark_dirty();
            }

            if !self.forks.contains_key(&path[0]) {
                // no existing fork for this byte; create a new one
                let mut nn = Self {
                    obfuscation_key: self.obfuscation_key,
                    ..Default::default()
                };

                if path.len() > PREFIX_MAX_LEN {
                    let (prefix, rest) = path.split_at(PREFIX_MAX_LEN);
                    nn.add(rest, entry, metadata, store).await?;
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

            // existing fork; need to split or extend
            let fork = self.forks.get(&path[0]).expect("checked above");
            let c = common_prefix_len(&fork.prefix, path);
            let rest = Prefix::from_slice(&fork.prefix[c..]);
            let common_prefix = Prefix::from_slice(&fork.prefix[..c]);

            // Take ownership; avoids cloning the entire node subtree
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
                intermediate.forks.insert(
                    rest[0],
                    Fork {
                        prefix: rest,
                        node: old_fork_node,
                    },
                );
                intermediate.make_edge();

                if c == path.len() {
                    intermediate.make_value();
                }
                intermediate
            };

            nn.update_is_with_path_separator(path);
            nn.add(&path[c..], entry, metadata, store).await?;

            self.forks.insert(
                path[0],
                Fork {
                    prefix: common_prefix,
                    node: nn,
                },
            );
            self.make_edge();

            Ok(())
        })
    }

    /// Remove the entry at the given path, loading from storage as needed.
    ///
    /// Returns a boxed future so the `&mut self` recursion can name its own type.
    // Panic-freedom: `path` is non-empty past the EmptyPath guard;
    // `path.starts_with(&prefix)` guarantees `prefix.len() <= path.len()`;
    // the fork at `first` is checked present before the get_mut/expect.
    #[allow(clippy::indexing_slicing, clippy::expect_used)]
    pub(crate) fn remove<'a, S: ChunkGet<BS>, const BS: usize>(
        &'a mut self,
        path: &'a [u8],
        store: &'a S,
    ) -> RecurseFuture<'a>
    where
        R: MaybeSend,
    {
        Box::pin(async move {
            if path.is_empty() {
                return Err(MantarayError::EmptyPath);
            }

            self.ensure_loaded(store).await?;

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
                fork.node.remove(rest, store).await
            };

            // Always clear reference so the node gets re-saved.
            self.mark_dirty();
            result
        })
    }

    /// Test whether a prefix exists in the trie, loading from storage as needed.
    #[allow(clippy::indexing_slicing)] // `rest` is checked non-empty before `rest[0]`; `c <= rest.len()` from common_prefix_len
    pub(crate) async fn has_prefix<S: ChunkGet<BS>, const BS: usize>(
        &mut self,
        path: &[u8],
        store: &S,
    ) -> Result<bool> {
        // Iterative descent: reborrow `current` to the chosen child each step.
        let mut current = self;
        let mut rest = path;
        loop {
            if rest.is_empty() {
                return Ok(true);
            }

            current.ensure_loaded(store).await?;

            let fork = match current.forks.get_mut(&rest[0]) {
                Some(f) => f,
                None => return Ok(false),
            };

            let c = common_prefix_len(&fork.prefix, rest);

            if c == fork.prefix.len() {
                current = &mut fork.node;
                rest = &rest[c..];
                continue;
            }

            if fork.prefix.starts_with(rest) {
                return Ok(true);
            }

            return Ok(false);
        }
    }

    /// Save this node and all children to storage in post-order.
    ///
    /// Uses BMT content-addressing via `ContentChunk`. An explicit stack avoids
    /// recursion: each frame visits its forks (pushing unsaved children) before
    /// the node itself is encoded and put.
    #[allow(clippy::arithmetic_side_effects)] // the only arithmetic is the fork-cursor `key_idx += 1`, bounded by keys.len() <= 256
    pub(crate) async fn save<S: ChunkPut<BS>, const BS: usize>(&mut self, store: &S) -> Result<()> {
        if self.reference.is_some() {
            return Ok(());
        }

        struct SaveFrame<R: Reference> {
            /// Node owned by an ancestor's fork map, valid for this call.
            node: *mut Node<R>,
            /// Fork keys still to descend into.
            keys: Vec<u8>,
            /// Index into `keys`.
            key_idx: usize,
        }

        let mut stack: Vec<SaveFrame<R>> = vec![SaveFrame {
            node: std::ptr::from_mut(self),
            keys: self.forks.keys().copied().collect(),
            key_idx: 0,
        }];

        while let Some(frame) = stack.last_mut() {
            // SAFETY: every frame's node points into the exclusively borrowed
            // trie. Children are only pushed once, then their parent waits in
            // the stack below them, so no two frames alias the same node.
            let node = unsafe { &mut *frame.node };

            if frame.key_idx < frame.keys.len() {
                #[allow(clippy::indexing_slicing)] // key_idx < keys.len() checked above
                let key = frame.keys[frame.key_idx];
                frame.key_idx += 1;
                #[allow(clippy::expect_used)]
                // key was collected from this node's fork map, which is not mutated while the frame is live
                let child = node.forks.get_mut(&key).expect("key from this node");
                if child.node.reference.is_none() {
                    let child_ptr = std::ptr::from_mut(&mut child.node);
                    let child_keys = child.node.forks.keys().copied().collect();
                    stack.push(SaveFrame {
                        node: child_ptr,
                        keys: child_keys,
                        key_idx: 0,
                    });
                }
                continue;
            }

            // All children saved; encode and put this node, then pop.
            let data = Vec::<u8>::try_from(&*node)?;
            let chunk = ContentChunk::<BS>::new(Bytes::from(data))?;
            let address = *chunk.address();
            store
                .put(chunk.into())
                .await
                .map_err(|e| MantarayError::StorePut {
                    source: std::sync::Arc::new(e),
                })?;
            node.reference = Some(address);
            node.forks.clear();
            node.loaded = false;
            stack.pop();
        }

        Ok(())
    }

    /// Walk all nodes depth-first, calling `f` for each node with its path.
    pub(crate) async fn walk<S: ChunkGet<BS>, const BS: usize, F>(
        &mut self,
        store: &S,
        f: &mut F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &Self) -> Result<()>,
    {
        let mut path_buf = Vec::new();
        walk_inner(&mut path_buf, self, store, f).await
    }

    /// Walk the subtree at `root`, calling `f` for each node.
    pub(crate) async fn walk_from<S: ChunkGet<BS>, const BS: usize, F>(
        &mut self,
        root: &[u8],
        store: &S,
        f: &mut F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &Self) -> Result<()>,
    {
        let mut path_buf = root.to_vec();
        if root.is_empty() {
            return walk_inner(&mut path_buf, self, store, f).await;
        }

        let target = self.lookup_node(root, store).await?;
        walk_inner(&mut path_buf, target, store, f).await
    }
}

/// Pre-order DFS visitor over a loaded-on-demand trie via an explicit stack.
///
/// The visitor `f` only reads loaded nodes, so it stays a synchronous `FnMut`.
#[allow(clippy::arithmetic_side_effects)] // the only arithmetic is the fork-cursor `key_idx += 1`, bounded by keys.len() <= 256
async fn walk_inner<R: Reference, S: ChunkGet<BS>, const BS: usize, F>(
    path_buf: &mut Vec<u8>,
    node: &mut Node<R>,
    store: &S,
    f: &mut F,
) -> Result<()>
where
    F: FnMut(&[u8], &Node<R>) -> Result<()>,
{
    struct WalkFrame {
        /// Node visited at this level (raw pointer into the exclusive borrow).
        node: *mut (),
        /// Length of `path_buf` before this frame's prefix was appended.
        path_len_before: usize,
        /// Sorted fork keys for this node.
        keys: Vec<u8>,
        /// Index into `keys`.
        key_idx: usize,
    }

    node.ensure_loaded(store).await?;
    f(path_buf, node)?;

    let mut stack: Vec<WalkFrame> = vec![WalkFrame {
        node: std::ptr::from_mut(node).cast::<()>(),
        path_len_before: path_buf.len(),
        keys: node.forks.keys().copied().collect(),
        key_idx: 0,
    }];

    while let Some(frame) = stack.last_mut() {
        if frame.key_idx >= frame.keys.len() {
            path_buf.truncate(frame.path_len_before);
            stack.pop();
            continue;
        }

        #[allow(clippy::indexing_slicing)] // key_idx < keys.len() checked above
        let key = frame.keys[frame.key_idx];
        frame.key_idx += 1;

        // SAFETY: frame.node points into the exclusively borrowed trie. Each
        // node appears in exactly one frame and is only dereferenced while at
        // the top of the stack, so no two live references alias.
        let parent = unsafe { &mut *frame.node.cast::<Node<R>>() };
        let reference = parent.reference;
        let fork = parent
            .forks
            .get_mut(&key)
            .ok_or(MantarayError::NoForkFound { reference })?;

        let prev_len = path_buf.len();
        path_buf.extend_from_slice(&fork.prefix);

        let child = &mut fork.node;
        child.ensure_loaded(store).await?;
        f(path_buf, child)?;

        let child_ptr = std::ptr::from_mut(child).cast::<()>();
        let child_keys = child.forks.keys().copied().collect();
        stack.push(WalkFrame {
            node: child_ptr,
            path_len_before: prev_len,
            keys: child_keys,
            key_idx: 0,
        });
    }

    Ok(())
}

/// `Arbitrary` implementations that generate *valid* mantaray values: every
/// generated [`Node`] encodes successfully and survives an encode/decode
/// round trip, so structured fuzz targets can assert
/// `decode(encode(node)) == node` rather than merely "no panic".
///
/// Mirrors the manual valid-by-construction impls in `nectar-primitives`
/// (`BmtBody`, `SingleOwnerChunk`). The wire format constrains what can round
/// trip, and these impls generate only that shape:
///
/// - Fork prefixes are 1..=30 bytes (`Prefix::from_wire` rejects empty ones)
///   and each fork is keyed by its prefix's first byte, as the encoder's forks
///   index expects.
/// - Fork children carry a chunk reference (a reference-less child is not
///   encodable) plus flags, and metadata only when the METADATA flag is set.
/// - The root's own flags are not serialized; the v0.2 decoder derives EDGE
///   from a non-empty forks index and nothing else, so the root's `node_type`
///   is exactly that.
/// - An all-zero entry is the wire sentinel for "no entry", so it is mapped
///   to `None`.
#[cfg(any(test, feature = "arbitrary"))]
mod arbitrary_impls {
    use arbitrary::{Arbitrary, Result as ArbitraryResult, Unstructured};

    use super::*;

    impl<'a> Arbitrary<'a> for Prefix {
        fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
            let len = u.int_in_range(1..=PREFIX_MAX_LEN)?;
            let mut data = [0u8; PREFIX_MAX_LEN];
            // In-bounds: len is drawn from 1..=PREFIX_MAX_LEN and data is PREFIX_MAX_LEN long.
            #[allow(clippy::indexing_slicing)]
            u.fill_buffer(&mut data[..len])?;
            #[allow(clippy::as_conversions)] // len ∈ 1..=PREFIX_MAX_LEN (30), fits u8
            let len = len as u8;
            Ok(Self { len, data })
        }
    }

    impl<'a> Arbitrary<'a> for NodeType {
        fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
            Ok(Self::from_bits_truncate(u8::arbitrary(u)?))
        }
    }

    impl<'a, R> Arbitrary<'a> for Fork<R>
    where
        R: Reference + Arbitrary<'a>,
    {
        fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
            let prefix = Prefix::arbitrary(u)?;
            // On the wire a fork child is flags + a chunk reference (plus
            // optional metadata); a reference-less child is not encodable.
            let mut node =
                Node::<R>::from_reference(ChunkAddress::from(u.arbitrary::<[u8; 32]>()?));
            node.node_type = NodeType::arbitrary(u)?;
            if node.node_type.contains(NodeType::METADATA) {
                // Keep pairs small: the encoder caps the padded metadata JSON
                // at u16::MAX bytes.
                let pairs = u.int_in_range(0..=3usize)?;
                for _ in 0..pairs {
                    let key: String = u.arbitrary::<&str>()?.chars().take(8).collect();
                    let value: String = u.arbitrary::<&str>()?.chars().take(8).collect();
                    node.metadata.insert(key, value);
                }
            }
            Ok(Self { prefix, node })
        }
    }

    impl<'a, R> Arbitrary<'a> for Node<R>
    where
        R: Reference + Arbitrary<'a>,
    {
        fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
            let obfuscation_key = ObfuscationKey::arbitrary(u)?;

            // An all-zero entry encodes as the "no entry" sentinel, so it
            // cannot round-trip as `Some`; map it to `None`.
            let entry = if u.arbitrary::<bool>()? {
                let e = R::arbitrary(u)?;
                e.to_bytes().iter().any(|&b| b != 0).then_some(e)
            } else {
                None
            };

            let fork_count = u.int_in_range(0..=4usize)?;
            let mut forks = BTreeMap::new();
            for _ in 0..fork_count {
                let fork = Fork::<R>::arbitrary(u)?;
                // In-bounds: Prefix::arbitrary yields a non-empty prefix.
                #[allow(clippy::indexing_slicing)]
                forks.insert(fork.prefix[0], fork);
            }

            // The root's own flags are not serialized; the v0.2 decoder
            // derives EDGE from a non-empty forks index and nothing else.
            let node_type = if forks.is_empty() {
                NodeType::empty()
            } else {
                NodeType::EDGE
            };

            Ok(Self {
                node_type,
                obfuscation_key,
                // Decoding yields an unpersisted, loaded node.
                reference: None,
                entry,
                metadata: BTreeMap::new(),
                forks,
                loaded: true,
            })
        }
    }
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
                        metadata: serde_json::from_str(r#"{"index-document": "index.html"}"#)
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

    use futures::executor::block_on;

    const NL: NullLoader = NullLoader;
    const BS: usize = DEFAULT_BODY_SIZE;

    /// Create a plain reference from a string, left-padded with zeroes.
    fn make_entry(s: &str) -> ChunkRef {
        let bytes = s.as_bytes();
        let mut buf = [0u8; 32];
        let start = 32 - bytes.len();
        buf[start..].copy_from_slice(bytes);
        ChunkRef::from(ChunkAddress::from(buf))
    }

    /// In-memory add: delegates to `add` with NullLoader.
    fn node_add(n: &mut Node, path: &[u8], entry: ChunkRef, meta: BTreeMap<String, String>) {
        block_on(n.add::<NullLoader, BS>(path, Some(entry), meta, &NL)).unwrap();
    }

    /// In-memory lookup: delegates to `lookup` with NullLoader.
    fn node_lookup<'n>(n: &'n mut Node, path: &[u8]) -> Result<Option<&'n ChunkRef>> {
        block_on(n.lookup::<NullLoader, BS>(path, &NL))
    }

    /// In-memory lookup_node: delegates to `lookup_node` with NullLoader.
    fn node_lookup_node<'n>(n: &'n mut Node, path: &[u8]) -> Result<&'n mut Node> {
        block_on(n.lookup_node::<NullLoader, BS>(path, &NL))
    }

    /// In-memory remove: delegates to `remove` with NullLoader.
    fn node_remove(n: &mut Node, path: &[u8]) -> Result<()> {
        block_on(n.remove::<NullLoader, BS>(path, &NL))
    }

    /// In-memory has_prefix: delegates to `has_prefix` with NullLoader.
    fn node_has_prefix(n: &mut Node, path: &[u8]) -> Result<bool> {
        block_on(n.has_prefix::<NullLoader, BS>(path, &NL))
    }

    /// In-memory walk: delegates to `walk` with NullLoader.
    fn node_walk<F>(n: &mut Node, f: &mut F) -> Result<()>
    where
        F: FnMut(&[u8], &Node) -> Result<()>,
    {
        block_on(n.walk::<NullLoader, BS, _>(&NL, f))
    }

    /// In-memory walk_node: delegates to `walk_from` with NullLoader.
    fn node_walk_node<F>(n: &mut Node, root: &[u8], f: &mut F) -> Result<()>
    where
        F: FnMut(&[u8], &Node) -> Result<()>,
    {
        block_on(n.walk_from::<NullLoader, BS, _>(root, &NL, f))
    }

    #[test]
    fn nil_path() {
        let mut n = Node::default();
        assert!(node_lookup(&mut n, b"").is_ok());
    }

    #[test]
    fn prefix_from_wire_valid() {
        let mut padded = [0u8; PREFIX_MAX_LEN];
        padded[..3].copy_from_slice(b"abc");
        let prefix = Prefix::from_wire(&padded, 3).unwrap();
        assert_eq!(prefix.len(), 3);
        assert_eq!(&*prefix, b"abc");
    }

    #[test]
    fn prefix_from_wire_zeroes_padding() {
        // Non-zero bytes past `len` must be dropped so equality and re-encode
        // stay canonical.
        let padded = [0xffu8; PREFIX_MAX_LEN];
        let prefix = Prefix::from_wire(&padded, 2).unwrap();
        assert_eq!(&*prefix, &[0xff, 0xff]);
        assert_eq!(prefix, Prefix::from_slice(&[0xff, 0xff]));
        assert!(prefix.padded_bytes()[2..].iter().all(|&b| b == 0));
    }

    #[test]
    fn prefix_from_wire_rejects_zero_length() {
        let padded = [0u8; PREFIX_MAX_LEN];
        let err = Prefix::from_wire(&padded, 0).unwrap_err();
        assert!(matches!(
            err,
            MantarayError::InvalidPrefixLength { max, actual } if max == PREFIX_MAX_LEN && actual == 0
        ));
    }

    #[test]
    fn prefix_from_wire_rejects_oversized() {
        let padded = [0u8; PREFIX_MAX_LEN];
        #[allow(clippy::as_conversions)] // test literal within u8 range
        let over = (PREFIX_MAX_LEN + 1) as u8;
        let err = Prefix::from_wire(&padded, over).unwrap_err();
        assert!(matches!(
            err,
            MantarayError::InvalidPrefixLength { max, actual }
                if max == PREFIX_MAX_LEN && actual == usize::from(over)
        ));
    }

    #[test]
    fn prefix_from_wire_accepts_max_length() {
        let padded = [7u8; PREFIX_MAX_LEN];
        #[allow(clippy::as_conversions)] // PREFIX_MAX_LEN (30) fits u8
        let prefix = Prefix::from_wire(&padded, PREFIX_MAX_LEN as u8).unwrap();
        assert_eq!(prefix.len(), PREFIX_MAX_LEN);
        assert_eq!(&*prefix, &padded[..]);
    }

    #[test]
    fn prefix_take_consumes_len_byte_and_padded_block() {
        let mut wire = vec![3u8];
        wire.extend_from_slice(b"abc");
        wire.resize(1 + PREFIX_MAX_LEN, 0);
        let mut cur = Cursor::new(&wire);
        let prefix = cur.take::<Prefix>().unwrap();
        assert_eq!(&*prefix, b"abc");
        assert!(cur.is_empty());
    }

    #[test]
    fn prefix_wire_round_trips_through_put_and_take() {
        let prefix = Prefix::from_slice(b"abc");
        let mut buf = Vec::new();
        Writer::new(&mut buf).put(&prefix);
        assert_eq!(buf.len(), 1 + PREFIX_MAX_LEN);

        let mut cur = Cursor::new(&buf);
        assert_eq!(cur.take::<Prefix>().unwrap(), prefix);
        assert!(cur.is_empty());
    }

    #[test]
    fn prefix_take_rejects_invalid_length() {
        let mut wire = vec![0u8];
        wire.resize(1 + PREFIX_MAX_LEN, 0);
        let mut cur = Cursor::new(&wire);
        assert!(matches!(
            cur.take::<Prefix>().unwrap_err(),
            MantarayError::InvalidPrefixLength { actual: 0, .. }
        ));
    }

    #[test]
    fn prefix_take_underrun_is_data_too_short() {
        let wire = [3u8, b'a'];
        let mut cur = Cursor::new(&wire);
        assert!(matches!(
            cur.take::<Prefix>().unwrap_err(),
            MantarayError::DataTooShort
        ));
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

        let store = MemoryStore::<{ DEFAULT_BODY_SIZE }>::new();
        block_on(n.save(&store)).unwrap();

        let mut n2: Node = Node::from_reference(n.reference.unwrap());

        for &d in items {
            let node = block_on(n2.lookup_node(d.as_bytes(), &store)).unwrap();
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
        let store = MemoryStore::<{ DEFAULT_BODY_SIZE }>::new();

        // add entries and persist
        let mut n = Node::default();
        for c in &tc.items {
            let e = make_entry(&c.path);
            block_on(n.add(c.path.as_bytes(), Some(e), c.metadata.clone(), &store)).unwrap();
        }
        block_on(n.save(&store)).unwrap();
        let ref_ = n.reference.unwrap();

        // reload and remove
        let mut nn: Node = Node::from_reference(ref_);
        for path in &tc.remove {
            block_on(nn.remove(path.as_bytes(), &store)).unwrap();
        }
        block_on(nn.save(&store)).unwrap();
        let ref2 = nn.reference.unwrap();

        // reload and verify removed paths are gone
        let mut nnn: Node = Node::from_reference(ref2);
        for path in &tc.remove {
            let result = block_on(nnn.lookup_node(path.as_bytes(), &store));
            assert!(
                result.is_err(),
                "expected removed path '{path}' to be not found"
            );
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

    fn make_entry_bytes(s: &[u8]) -> ChunkRef {
        let mut buf = [0u8; 32];
        let start = 32 - s.len();
        buf[start..].copy_from_slice(s);
        ChunkRef::from(ChunkAddress::from(buf))
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
        let to_add: &[&[u8]] = &[b"index.html", b"img/1.png", b"img/2.png", b"robots.txt"];

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

        let store = MemoryStore::<{ DEFAULT_BODY_SIZE }>::new();
        block_on(n.save(&store)).unwrap();

        let mut n2: Node = Node::from_reference(n.reference.unwrap());

        let mut walked: Vec<Vec<u8>> = Vec::new();
        block_on(n2.walk_from(b"", &store, &mut |path: &[u8], _node: &Node| {
            walked.push(path.to_vec());
            Ok(())
        }))
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

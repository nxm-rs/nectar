//! Trie traversal and save machinery serving only the deprecated
//! [`Manifest`](super::manifest::Manifest) surface; removed with it.

use bytes::Bytes;
use nectar_primitives::chunk::{ChunkOps, ContentChunk, Reference};
use nectar_primitives::store::{ChunkPut, TrustedGet};
use nectar_primitives::{AnyChunkSet, Chunk};

use crate::error::{MantarayError, Result};
use crate::node::{Node, NodeState, StoredReference, common_prefix_len};

impl<R: Reference> Node<R> {
    /// Look up the node at the given path, loading from storage as needed.
    #[allow(clippy::indexing_slicing)] // `rest` is checked non-empty before `rest[0]`; `c <= rest.len()` from common_prefix_len
    pub(crate) async fn lookup_node<S: TrustedGet<AnyChunkSet<BS>>, const BS: usize>(
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
            let reference = current.reference().map(|r| *r.address());
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

    /// Resolve the node at `path` over owned clones, loading on demand.
    ///
    /// Shared-read counterpart to [`lookup_node`](Self::lookup_node): borrows
    /// `&self` and clones each descended fork, so reading a persisted manifest
    /// leaves the trie untouched. Returns `None` for an absent path.
    #[allow(clippy::indexing_slicing)] // `rest` is checked non-empty before `rest[0]`; `c <= rest.len()` from common_prefix_len
    pub(crate) async fn get_node<S: TrustedGet<AnyChunkSet<BS>>, const BS: usize>(
        &self,
        path: &[u8],
        store: &S,
    ) -> Result<Option<Self>> {
        let mut current = self.clone();
        let mut rest = path;
        loop {
            current.ensure_loaded(store).await?;

            if rest.is_empty() {
                return Ok(Some(current));
            }

            let first = rest[0];
            let Some(fork) = current.forks.get(&first) else {
                return Ok(None);
            };

            let c = common_prefix_len(&fork.prefix, rest);
            if c != fork.prefix.len() {
                return Ok(None);
            }

            let child = fork.node.clone();
            rest = &rest[c..];
            current = child;
        }
    }

    /// Look up the entry at the given path, loading from storage as needed.
    #[cfg(test)]
    pub(crate) async fn lookup<S: TrustedGet<AnyChunkSet<BS>>, const BS: usize>(
        &mut self,
        path: &[u8],
        store: &S,
    ) -> Result<Option<&R>> {
        let node = self.lookup_node(path, store).await?;
        if !node.is_value() && !path.is_empty() {
            return Err(MantarayError::NoEntryFound {
                reference: node.reference().map(|r| *r.address()),
            });
        }
        Ok(node.entry.as_ref())
    }

    /// Test whether a prefix exists in the trie, loading from storage as needed.
    #[allow(clippy::indexing_slicing)] // `rest` is checked non-empty before `rest[0]`; `c <= rest.len()` from common_prefix_len
    pub(crate) async fn has_prefix<S: TrustedGet<AnyChunkSet<BS>>, const BS: usize>(
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
    pub(crate) async fn save<S: ChunkPut<AnyChunkSet<BS>>, const BS: usize>(
        &mut self,
        store: &S,
    ) -> Result<()>
    where
        R: StoredReference,
    {
        if self.reference().is_some() {
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
            node: core::ptr::from_mut(self),
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
                if child.node.reference().is_none() {
                    let child_ptr = core::ptr::from_mut(&mut child.node);
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
            let data = node.encode()?;
            let chunk = ContentChunk::<BS>::new(Bytes::from(data))?;
            let address = *chunk.address();
            let sealed: Chunk<_, AnyChunkSet<BS>> = Chunk::from_envelope(chunk.into())?;
            store
                .put(sealed)
                .await
                .map_err(|e| MantarayError::StorePut {
                    source: alloc::sync::Arc::new(e),
                })?;
            // Persist the reference and drop the now-redundant forks: the node
            // becomes a stub, reloaded on demand.
            node.state = NodeState::Stub(R::from_stored(address));
            node.forks.clear();
            stack.pop();
        }

        Ok(())
    }

    /// Walk all nodes depth-first, calling `f` for each node with its path.
    pub(crate) async fn walk<S: TrustedGet<AnyChunkSet<BS>>, const BS: usize, F>(
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
    pub(crate) async fn walk_from<S: TrustedGet<AnyChunkSet<BS>>, const BS: usize, F>(
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
async fn walk_inner<R: Reference, S: TrustedGet<AnyChunkSet<BS>>, const BS: usize, F>(
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
        node: core::ptr::from_mut(node).cast::<()>(),
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
        let reference = parent.reference().map(|r| *r.address());
        let fork = parent
            .forks
            .get_mut(&key)
            .ok_or(MantarayError::NoForkFound { reference })?;

        let prev_len = path_buf.len();
        path_buf.extend_from_slice(&fork.prefix);

        let child = &mut fork.node;
        child.ensure_loaded(store).await?;
        f(path_buf, child)?;

        let child_ptr = core::ptr::from_mut(child).cast::<()>();
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

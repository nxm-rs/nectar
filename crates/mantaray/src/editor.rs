//! Submission-order manifest editor over the node persistence seam.
//!
//! Ops are recorded synchronously into a `(path, op)` log and applied one at
//! a time at commit, in submission order. The committed root is defined as
//! the root the reference mutation path produces for the same sequence
//! (pinned by the registry-crate differential gate), shape quirks included;
//! ops are never reordered or batched. Nodes persist through a
//! [`NodeSaver`], so the storage layout and any put concurrency are the
//! adapter's.
//!
//! Commit cost is O(touched trie), not O(whole trie): a deliberate trade for
//! the submission-order pin. Bulk construction should use the manifest 1.0
//! builder instead.

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::collections::btree_map;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::future::poll_fn;
use core::pin::Pin;
use core::task::{Context, Poll};

use futures_util::stream::{FuturesUnordered, Stream};
use nectar_governor::{Admission, BoxFuture, Window};
use nectar_primitives::chunk::{ChunkAddress, ChunkRef, Reference};
use nectar_primitives::{EncryptedChunkRef, EntryRef};

use crate::error::EditorError;
use crate::node::{Fork, Node, NodeState, Prefix};
use crate::persist::{NodeLoader, NodeSaver};
use crate::{MantarayError, metadata};

/// One recorded manifest mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Op<R: Reference = ChunkRef> {
    /// Insert the entry at the path, replacing the whole binding. An empty
    /// `metadata` clears the node's.
    Insert {
        /// Entry reference, or `None` for a metadata-only value node.
        reference: Option<R>,
        /// Metadata to attach; empty clears the node's.
        metadata: BTreeMap<String, String>,
    },
    /// Clear the binding at exactly the path: its value and its metadata, and
    /// nothing below it. An absent path is a no-op.
    Remove,
    /// Prune the fork whose boundary the path names, taking every key under it.
    /// An absent path fails the commit.
    ///
    /// The legacy boundary op; [`Remove`](Self::Remove) is the map verb.
    RemoveSubtree,
    /// Merge one metadata key into the node at the path, creating the node
    /// when absent.
    SetRootMetadata {
        /// Metadata key to merge.
        key: String,
        /// Metadata value to set under the key.
        value: String,
    },
    /// Remove one metadata key from the node at the path, pruning a node the
    /// removal leaves with neither a binding nor a child. An absent key is a
    /// no-op.
    ClearRootMetadata {
        /// Metadata key to remove.
        key: String,
    },
}

/// Submission-order manifest editor.
///
/// Records `(path, op)` pairs without touching storage; [`commit`] applies
/// them sequentially and persists every rewritten node through the
/// loadsaver. Commit consumes the editor: reopen from the returned root to
/// edit further.
///
/// [`commit`]: Self::commit
///
/// ```
/// # use nectar_mantaray::{ManifestEditor, DefaultMemoryStore};
/// # use nectar_primitives::chunk::ChunkAddress;
/// let mut editor: ManifestEditor<_> = ManifestEditor::new(DefaultMemoryStore::new());
/// editor.insert("index.html", ChunkAddress::from([7u8; 32]));
/// editor.set_index_document("index.html");
/// assert_eq!(editor.ops().len(), 2);
/// ```
#[derive(Debug)]
pub struct ManifestEditor<S, R: Reference = ChunkRef> {
    trie: Node<R>,
    ops: Vec<(Vec<u8>, Op<R>)>,
    store: S,
    commit_window: Window,
}

impl<S> ManifestEditor<S, ChunkRef> {
    /// Editor over an empty plain manifest.
    pub fn new(store: S) -> Self {
        Self::with_root(Node::new_unencrypted(), store)
    }

    /// Editor over the persisted plain manifest rooted at `root`.
    pub fn open(root: ChunkAddress, store: S) -> Self {
        Self::with_root(Node::from_reference(ChunkRef::from(root)), store)
    }
}

impl<S> ManifestEditor<S, EncryptedChunkRef> {
    /// Editor over an empty encrypted manifest with a random obfuscation key.
    #[cfg(feature = "rand")]
    #[cfg_attr(docsrs, doc(cfg(feature = "rand")))]
    pub fn new_encrypted(store: S) -> Self {
        let trie = Node {
            obfuscation_key: crate::obfuscation::ObfuscationKey::generate(),
            ..Node::default()
        };
        Self::with_root(trie, store)
    }

    /// Editor over the persisted encrypted manifest at `root`; the
    /// obfuscation key rides the root node's own bytes.
    pub fn open_encrypted(root: EncryptedChunkRef, store: S) -> Self {
        Self::with_root(Node::from_reference(root), store)
    }
}

impl<S, R: Reference> ManifestEditor<S, R> {
    /// Editor over the persisted manifest `root` reaches, at whatever width
    /// the reference carries.
    ///
    /// The width-generic twin of [`open`](Self::open) and
    /// [`open_encrypted`](Self::open_encrypted), for callers that are
    /// themselves generic over the reference.
    pub fn open_reference(root: R, store: S) -> Self {
        Self::with_root(Node::from_reference(root), store)
    }

    /// Editor over an empty manifest at whatever width `R` carries.
    #[cfg(feature = "manifest")]
    pub(crate) fn empty_reference(store: S) -> Self {
        Self::with_root(Node::default(), store)
    }

    const fn with_root(trie: Node<R>, store: S) -> Self {
        Self {
            trie,
            ops: Vec::new(),
            store,
            commit_window: Window::DEFAULT,
        }
    }

    /// The backing loadsaver.
    #[must_use]
    pub const fn store(&self) -> &S {
        &self.store
    }

    /// Replace the commit save window: the cap on node saves in flight while
    /// the dirty trie is persisted post-order.
    #[must_use]
    pub const fn with_commit_window(mut self, window: Window) -> Self {
        self.commit_window = window;
        self
    }

    /// The recorded ops in submission order.
    #[must_use]
    pub fn ops(&self) -> &[(Vec<u8>, Op<R>)] {
        &self.ops
    }

    /// Record inserting the entry at `path` with empty metadata.
    ///
    /// An insert replaces the whole binding, clearing existing metadata unless
    /// [`insert_with`](Self::insert_with) carries some.
    ///
    /// Format limitations, both rejected at commit: an all-zero reference is
    /// the wire's absent-entry sentinel, and metadata on the empty path (the
    /// trie root) has no wire slot.
    pub fn insert(&mut self, path: impl AsRef<[u8]>, reference: impl Into<R>) -> &mut Self {
        self.insert_with(path, reference, BTreeMap::new())
    }

    /// Record inserting the entry at `path`, carrying `metadata`.
    pub fn insert_with(
        &mut self,
        path: impl AsRef<[u8]>,
        reference: impl Into<R>,
        metadata: BTreeMap<String, String>,
    ) -> &mut Self {
        self.push(
            path,
            Op::Insert {
                reference: Some(reference.into()),
                metadata,
            },
        )
    }

    /// Record clearing the binding at exactly `path`.
    ///
    /// Exact-key: the path's own value and metadata go, and the paths below it
    /// stay. A childless leaf is pruned. An absent path is a no-op.
    pub fn remove(&mut self, path: impl AsRef<[u8]>) -> &mut Self {
        self.push(path, Op::Remove)
    }

    /// Record pruning the whole subtree the fork at `path` reaches.
    ///
    /// The legacy boundary remove: it takes keys the caller never named. Use
    /// [`remove`](Self::remove) for the map verb. An absent path fails the
    /// commit.
    pub fn remove_subtree(&mut self, path: impl AsRef<[u8]>) -> &mut Self {
        self.push(path, Op::RemoveSubtree)
    }

    /// Record merging one metadata key into the manifest's root path node.
    ///
    /// The root path node is [`metadata::ROOT_PATH`], where the reference
    /// client keeps the site-level documents. A merge, not a replace: only the
    /// named key moves.
    pub fn set_root_metadata(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> &mut Self {
        self.push(
            metadata::ROOT_PATH,
            Op::SetRootMetadata {
                key: key.into(),
                value: value.into(),
            },
        )
    }

    /// Record removing one metadata key from the manifest's root path node.
    ///
    /// The other keys stay, and a node left carrying nothing is pruned. An
    /// absent key is a no-op.
    pub fn clear_root_metadata(&mut self, key: impl Into<String>) -> &mut Self {
        self.push(
            metadata::ROOT_PATH,
            Op::ClearRootMetadata { key: key.into() },
        )
    }

    /// Record setting the website index document.
    pub fn set_index_document(&mut self, filename: &str) -> &mut Self {
        self.set_root_metadata(metadata::WEBSITE_INDEX_DOCUMENT, filename)
    }

    /// Record setting the website error document.
    pub fn set_error_document(&mut self, path: &str) -> &mut Self {
        self.set_root_metadata(metadata::WEBSITE_ERROR_DOCUMENT, path)
    }

    fn push(&mut self, path: impl AsRef<[u8]>, op: Op<R>) -> &mut Self {
        self.ops.push((path.as_ref().to_vec(), op));
        self
    }
}

impl<S: NodeLoader, R: Reference> ManifestEditor<S, R> {
    /// Apply the recorded ops to the trie, one at a time, in submission order.
    async fn apply_ops(&mut self) -> Result<(), EditorError> {
        let ops = core::mem::take(&mut self.ops);
        for (index, (path, op)) in ops.into_iter().enumerate() {
            let result = match op {
                Op::Insert {
                    reference,
                    metadata,
                } => {
                    // The wire reads an all-zero entry slot as absent, so
                    // storing the all-zero reference would silently drop it.
                    if reference.as_ref().is_some_and(is_zero_reference) {
                        Err(MantarayError::ZeroReference)
                    } else {
                        self.trie.add(&path, reference, metadata, &self.store).await
                    }
                }
                Op::Remove => self.trie.clear(&path, &self.store).await.map(|_| ()),
                Op::RemoveSubtree => self.trie.remove(&path, &self.store).await,
                Op::SetRootMetadata { key, value } => {
                    apply_metadata_merge::<S, R>(&mut self.trie, &path, key, value, &self.store)
                        .await
                }
                Op::ClearRootMetadata { key } => {
                    apply_metadata_clear::<S, R>(&mut self.trie, &path, &key, &self.store).await
                }
            };
            result.map_err(|source| EditorError::Apply {
                index,
                path,
                source,
            })?;
        }
        Ok(())
    }
}

impl<S: NodeLoader + NodeSaver<R>, R: Reference> ManifestEditor<S, R> {
    /// Apply the log and persist the trie, returning the root's full-width
    /// reference and the loadsaver.
    ///
    /// The width-generic commit the two typed commits delegate to; an
    /// encrypted root's reference carries its decryption key.
    pub async fn commit_reference(mut self) -> Result<(R, S), EditorError> {
        self.apply_ops().await?;
        let window = self.commit_window;
        let committed = commit_trie::<S, R>(self.trie, &self.store, window)
            .await
            .map_err(EditorError::Commit)?;
        let reference = committed
            .reference()
            .cloned()
            .ok_or(EditorError::Commit(MantarayError::MissingReference))?;
        Ok((reference, self.store))
    }
}

impl<S: NodeLoader + NodeSaver<ChunkRef>> ManifestEditor<S, ChunkRef> {
    /// Apply the log and persist the trie, returning the root chunk address
    /// and the loadsaver.
    pub async fn commit(self) -> Result<(ChunkAddress, S), EditorError> {
        let (reference, store) = self.commit_reference().await?;
        Ok((*reference.address(), store))
    }
}

impl<S: NodeLoader + NodeSaver<EncryptedChunkRef>> ManifestEditor<S, EncryptedChunkRef> {
    /// Apply the log and persist the trie, returning the root's full-width
    /// reference (address plus decryption key) and the loadsaver.
    pub async fn commit(self) -> Result<(EncryptedChunkRef, S), EditorError> {
        self.commit_reference().await
    }
}

/// Outcome of a metadata-merge descent.
enum MergeOutcome {
    /// The node exists and its metadata was merged in place.
    Applied,
    /// No node at the path; the caller creates it.
    Missing,
}

/// Merge one metadata key into the node at `path`, creating it when absent.
///
/// Shape-exact twin of the reference root-metadata merge: an existing node keeps
/// its entry and gains the key; an absent one is created as a metadata-only
/// value. Every node on the descent is marked dirty so a clean ancestor can
/// never shadow the merged metadata at commit.
async fn apply_metadata_merge<S, R>(
    trie: &mut Node<R>,
    path: &[u8],
    key: String,
    value: String,
    store: &S,
) -> Result<(), MantarayError>
where
    S: NodeLoader,
    R: Reference,
{
    match merge_descent(trie, path, &key, &value, store).await? {
        MergeOutcome::Applied => Ok(()),
        MergeOutcome::Missing => {
            let mut meta = BTreeMap::new();
            meta.insert(key, value);
            trie.add(path, None, meta, store).await
        }
    }
}

/// Remove one metadata key from the node at `path`.
///
/// What the node keeps is rebound with [`Node::add`], and a node left carrying
/// nothing is cleared, which prunes it as a childless leaf.
async fn apply_metadata_clear<S, R>(
    trie: &mut Node<R>,
    path: &[u8],
    key: &str,
    store: &S,
) -> Result<(), MantarayError>
where
    S: NodeLoader,
    R: Reference,
{
    let Some((entry, mut metadata)) = binding_at(trie, path, store).await? else {
        return Ok(());
    };
    if metadata.remove(key).is_none() {
        return Ok(());
    }
    if metadata.is_empty() && entry.is_none() {
        return trie.clear(path, store).await.map(|_| ());
    }
    trie.add(path, entry, metadata, store).await
}

/// The binding the node at `path` carries, or `None` when no node is there.
///
/// Every visited node is dirtied: a clean node would keep its persisted
/// reference and shadow the rebind at commit.
async fn binding_at<S, R>(
    trie: &mut Node<R>,
    path: &[u8],
    store: &S,
) -> Result<Option<(Option<R>, BTreeMap<String, String>)>, MantarayError>
where
    S: NodeLoader,
    R: Reference,
{
    let mut current = trie;
    let mut rest = path;
    loop {
        if !current.is_loaded() {
            current.load(store).await?;
        }
        current.mark_dirty();
        let Some((first, _)) = rest.split_first() else {
            return Ok(Some((
                current.reference().cloned(),
                current.metadata().clone(),
            )));
        };
        let Some(fork) = current.forks.get_mut(first) else {
            return Ok(None);
        };
        let prefix: &[u8] = &fork.prefix;
        let Some(next) = rest.strip_prefix(prefix) else {
            return Ok(None);
        };
        current = &mut fork.node;
        rest = next;
    }
}

/// Descend to `path`, dirtying every visited node, and merge the key there.
async fn merge_descent<S, R>(
    trie: &mut Node<R>,
    path: &[u8],
    key: &str,
    value: &str,
    store: &S,
) -> Result<MergeOutcome, MantarayError>
where
    S: NodeLoader,
    R: Reference,
{
    let mut current = trie;
    let mut rest = path;
    loop {
        if !current.is_loaded() {
            current.load(store).await?;
        }
        // Dirtying an unchanged node is safe: it re-encodes to the same
        // address, so a divergent descent never moves the root.
        current.mark_dirty();
        let Some((first, _)) = rest.split_first() else {
            current.metadata_mut().insert(key.into(), value.into());
            current.make_with_metadata();
            return Ok(MergeOutcome::Applied);
        };
        let Some(fork) = current.forks.get_mut(first) else {
            return Ok(MergeOutcome::Missing);
        };
        let prefix: &[u8] = &fork.prefix;
        let Some(next) = rest.strip_prefix(prefix) else {
            return Ok(MergeOutcome::Missing);
        };
        current = &mut fork.node;
        rest = next;
    }
}

/// Persist the dirty subtree post-order through the saver and return the
/// root as a persisted stub.
///
/// Independent subtrees save concurrently up to `window`; a parent is
/// admitted to save only once every child save has completed, so it embeds
/// each child's saver-issued reference and the committed root is byte- and
/// address-identical to a serial save. A node's encoded image is held only
/// while its own save is in flight and dropped when it collapses to a stub,
/// so peak encoded bytes beyond the dirty trie stay within the window.
async fn commit_trie<S, R>(
    root: Node<R>,
    saver: &S,
    window: Window,
) -> Result<Node<R>, MantarayError>
where
    S: NodeSaver<R>,
    R: Reference,
{
    if root.reference().is_some() {
        return Ok(root);
    }
    // Only fork records carry metadata on the wire; a root's own would be
    // silently dropped, so fail loud instead.
    if !root.metadata.is_empty() {
        return Err(MantarayError::RootMetadata);
    }
    let mut walk = CommitWalk::new(saver, window, root);
    poll_fn(|cx| walk.poll(cx)).await
}

/// Save completion: the dispatch id and the saver's reference outcome.
type SaveDone<R> = (u64, Result<R, MantarayError>);

/// One node mid-commit: its parent frame, its reattachment slot, the children
/// still to descend and the ones already saved, and the count of its own
/// children whose saves are outstanding.
struct CommitFrame<R: Reference> {
    /// Routes a child completion back to this frame; the root's is unused.
    id: u64,
    /// Parent frame id; `None` only for the root frame.
    parent: Option<u64>,
    /// Fork slot (key and prefix) this node reattaches to in its parent;
    /// `None` only for the root frame.
    slot: Option<(u8, Prefix)>,
    node: Node<R>,
    /// Children still to visit, drained from the node's fork map.
    todo: btree_map::IntoIter<u8, Fork<R>>,
    /// Children already persisted, keyed for reattachment.
    done: BTreeMap<u8, Fork<R>>,
    /// This node's children whose saves are dispatched but not yet folded
    /// into `done`.
    saving: usize,
}

/// A node whose own save is in flight: held only until its reference arrives,
/// then collapsed to a stub and reattached to its parent.
struct Saving<R: Reference> {
    parent: Option<u64>,
    slot: Option<(u8, Prefix)>,
    node: Node<R>,
}

/// A frame over `node`, its forks drained into the visit queue.
fn commit_frame<R: Reference>(
    id: u64,
    parent: Option<u64>,
    slot: Option<(u8, Prefix)>,
    mut node: Node<R>,
) -> CommitFrame<R> {
    let todo = core::mem::take(&mut node.forks).into_iter();
    CommitFrame {
        id,
        parent,
        slot,
        node,
        todo,
        done: BTreeMap::new(),
        saving: 0,
    }
}

/// The commit walk: a depth-first descent whose ready frames dispatch their
/// saves into a window, folding completions back into their parents until
/// the root is persisted last and delivered as the walk's one outcome.
///
/// The frontier shape mirrors the read walk's: a window of saves runs ahead
/// through [`Admission`], their futures borrowing the saver so the writes
/// land in the store the commit returns.
struct CommitWalk<'s, S, R: Reference> {
    saver: &'s S,
    admission: Admission,
    /// Depth-first path of open frames, root at the base.
    stack: Vec<CommitFrame<R>>,
    /// Nodes whose own save is in flight, keyed by dispatch id.
    inflight_nodes: BTreeMap<u64, Saving<R>>,
    in_flight: FuturesUnordered<BoxFuture<'s, SaveDone<R>>>,
    /// The persisted root, set when the root's own save completes.
    root: Option<Node<R>>,
    next_id: u64,
    /// A descent fault, surfaced ahead of any persisted root.
    fault: Option<MantarayError>,
}

impl<'s, S, R> CommitWalk<'s, S, R>
where
    S: NodeSaver<R>,
    R: Reference,
{
    fn new(saver: &'s S, window: Window, root: Node<R>) -> Self {
        Self {
            saver,
            admission: Admission::new(window),
            stack: alloc::vec![commit_frame(0, None, None, root)],
            inflight_nodes: BTreeMap::new(),
            in_flight: FuturesUnordered::new(),
            root: None,
            next_id: 1,
            fault: None,
        }
    }

    /// Drive the descent to the persisted root.
    ///
    /// Cancel-safe: all progress lives in `self`. A fault is terminal at the
    /// turn it is folded, and an in-flight set that empties with no root is
    /// a stalled commit.
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<Node<R>, MantarayError>> {
        loop {
            self.admit();
            if let Some(error) = self.fault.take() {
                return Poll::Ready(Err(error));
            }
            if let Some(root) = self.root.take() {
                return Poll::Ready(Ok(root));
            }
            match Pin::new(&mut self.in_flight).poll_next(cx) {
                Poll::Ready(Some(done)) => {
                    if let Err(error) = self.absorb(done) {
                        return Poll::Ready(Err(error));
                    }
                }
                Poll::Ready(None) => return Poll::Ready(Err(MantarayError::MissingReference)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    /// Walk the frontier: queue persisted children, descend dirty ones, and
    /// dispatch a frame's own save once its children are all saved and the
    /// window has room. Stops when the deepest frame waits on its children,
    /// the window is full, or a dispatch faults.
    #[inline]
    fn admit(&mut self) {
        loop {
            let Some(top) = self.stack.last_mut() else {
                return;
            };
            if let Some((key, fork)) = top.todo.next() {
                if fork.node.reference().is_some() {
                    // Already persisted; nothing below it changed.
                    top.done.insert(key, fork);
                } else {
                    let parent = top.id;
                    let id = self.next_id;
                    self.next_id = self.next_id.wrapping_add(1);
                    self.stack.push(commit_frame(
                        id,
                        Some(parent),
                        Some((key, fork.prefix)),
                        fork.node,
                    ));
                }
                continue;
            }
            if top.saving > 0 {
                // Children still saving; this deepest frame, and so the whole
                // stack, waits on their completions.
                return;
            }
            // Saves are order-independent, so there is no serial-drain head to
            // reserve a slot for: the whole window admits.
            if !self.admission.admits(self.in_flight.len(), true) {
                // The window is full; wait for a save to complete.
                return;
            }
            if let Err(error) = self.dispatch() {
                self.fault = Some(error);
                return;
            }
        }
    }

    /// Fold one completed save in: collapse its node to a stub and reattach
    /// it to its parent, or crown the root.
    #[inline]
    fn absorb(&mut self, (id, result): SaveDone<R>) -> Result<(), MantarayError> {
        let reference = result?;
        let Some(Saving {
            parent,
            slot,
            mut node,
        }) = self.inflight_nodes.remove(&id)
        else {
            // Every completion matches a dispatched node.
            return Err(MantarayError::MissingReference);
        };
        // The persisted node collapses to a stub, reloaded on demand.
        node.state = NodeState::Stub(reference);
        node.forks.clear();
        match (parent, slot) {
            (Some(parent), Some((key, prefix))) => {
                let Some(parent_frame) = self.stack.iter_mut().rev().find(|f| f.id == parent)
                else {
                    return Err(MantarayError::MissingReference);
                };
                parent_frame.done.insert(key, Fork { prefix, node });
                parent_frame.saving = parent_frame.saving.saturating_sub(1);
            }
            // A parentless, slotless node is the root.
            _ => self.root = Some(node),
        }
        Ok(())
    }

    /// Encode the top frame's node, dispatch its save into the window, and
    /// pop it; its parent's outstanding-child count rises until the save
    /// completes.
    fn dispatch(&mut self) -> Result<(), MantarayError> {
        let Some(mut frame) = self.stack.pop() else {
            return Ok(());
        };
        // Fold the saved children back in, then encode the node's image.
        frame.node.forks = core::mem::take(&mut frame.done);
        let data = frame.node.encode()?;
        let id = frame.id;
        let saver = self.saver;
        let future: BoxFuture<'s, SaveDone<R>> = Box::pin(async move {
            let outcome = saver.save(data).await.map_err(|e| MantarayError::StorePut {
                source: Arc::new(e),
            });
            (id, outcome)
        });
        if let Some(parent) = frame.parent {
            let Some(parent_frame) = self.stack.iter_mut().rev().find(|f| f.id == parent) else {
                // A dispatched child's parent is always still on the stack.
                return Err(MantarayError::MissingReference);
            };
            parent_frame.saving = parent_frame.saving.saturating_add(1);
        }
        self.inflight_nodes.insert(
            id,
            Saving {
                parent: frame.parent,
                slot: frame.slot,
                node: frame.node,
            },
        );
        self.in_flight.push(future);
        Ok(())
    }
}

/// True when the reference would occupy the wire's absent-entry slot.
fn is_zero_reference<R: Reference>(reference: &R) -> bool {
    match reference.clone().into_entry_ref() {
        EntryRef::Plain(r) => r.address().is_zero(),
        EntryRef::Encrypted(r) => {
            r.address().is_zero() && r.key().as_bytes().iter().all(|b| *b == 0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::sync::atomic::{AtomicUsize, Ordering};

    use nectar_primitives::store::MemoryStore;
    use nectar_primitives::{EncryptionKey, StandardChunkSet};
    use nectar_testing::run;

    use crate::persist::single_chunk::{SingleChunkError, SingleChunkLoadSaver};

    type Store = MemoryStore<StandardChunkSet>;
    type LoadSaver = SingleChunkLoadSaver<Store>;
    type Editor = ManifestEditor<LoadSaver>;

    /// A ChunkAddress from a string, right-padded with zeroes.
    fn make_addr(s: &str) -> ChunkAddress {
        let bytes = s.as_bytes();
        let mut buf = [0u8; 32];
        let len = bytes.len().min(32);
        buf[..len].copy_from_slice(&bytes[..len]);
        ChunkAddress::from(buf)
    }

    /// One scripted mutation, replayable on the editor.
    #[derive(Clone, Copy)]
    enum Script {
        Add(&'static str, &'static str),
        AddMeta(&'static str, &'static str, &'static str, &'static str),
        Rm(&'static str),
        SetIndex(&'static str),
        SetError(&'static str),
    }

    /// Record a script into an editor.
    fn record(editor: &mut Editor, script: &[Script]) {
        for op in script {
            match *op {
                Script::Add(p, seed) => {
                    editor.insert(p, make_addr(seed));
                }
                Script::AddMeta(p, seed, k, v) => {
                    let meta = [(k.to_string(), v.to_string())].into();
                    editor.insert_with(p, make_addr(seed), meta);
                }
                Script::Rm(p) => {
                    editor.remove(p);
                }
                Script::SetIndex(v) => {
                    editor.set_index_document(v);
                }
                Script::SetError(v) => {
                    editor.set_error_document(v);
                }
            }
        }
    }

    /// Editor replay of a full script from an empty manifest.
    fn editor_replay(script: &[Script]) -> (ChunkAddress, LoadSaver) {
        let mut editor = Editor::new(LoadSaver::new(Store::new()));
        record(&mut editor, script);
        run(editor.commit()).unwrap()
    }

    /// Editor replay with a commit boundary after `split` ops, continuing
    /// from the persisted intermediate root.
    fn editor_replay_split(script: &[Script], split: usize) -> (ChunkAddress, LoadSaver) {
        let (head, tail) = script.split_at(split.min(script.len()));
        let mut editor = Editor::new(LoadSaver::new(Store::new()));
        record(&mut editor, head);
        let (root, store) = run(editor.commit()).unwrap();
        let mut editor = Editor::open(root, store);
        record(&mut editor, tail);
        run(editor.commit()).unwrap()
    }

    /// Hostile shapes: prefix splits at and around values, removes that
    /// leave non-canonical edges, re-adds, overwrites, long edges, and root
    /// metadata interleavings.
    fn corpora() -> Vec<Vec<Script>> {
        use Script::*;
        vec![
            vec![Add("app.js.map", "m"), Add("app.js", "j")],
            vec![Add("app.js", "j"), Add("app.js.map", "m")],
            vec![
                Add("abcdef", "1"),
                Add("abc", "2"),
                Rm("abcdef"),
                Add("abcxyz", "3"),
            ],
            vec![
                Add("a", "1"),
                Add("ab", "2"),
                Add("abc", "3"),
                Rm("ab"),
                Rm("a"),
            ],
            vec![
                Add("img/1.png", "1"),
                Add("img/2.png", "2"),
                Add("index.html", "i"),
                Rm("img/1.png"),
                Add("img/1.png", "1v2"),
            ],
            vec![
                Add("d/x", "x"),
                Add("d/y", "y"),
                Rm("d/x"),
                Rm("d/y"),
                Add("da", "da"),
            ],
            vec![Add("same", "old"), Add("same", "new")],
            vec![
                Add(
                    "oneverylongpathsegmentthatexceedsthethirtybyteprefixlimitforsure",
                    "l1",
                ),
                Add(
                    "oneverylongpathsegmentthatexceedsthethirtybyteprefixlimitforsurely",
                    "l2",
                ),
                Rm("oneverylongpathsegmentthatexceedsthethirtybyteprefixlimitforsure"),
            ],
            vec![
                Add("/", "root"),
                SetIndex("index.html"),
                SetError("404.html"),
                SetIndex("start.html"),
                Add("index.html", "i"),
            ],
            vec![
                SetIndex("index.html"),
                Add("a/b/c/d/e/f/g/h/file00.dat", "f0"),
                Add("a/b/c/d/e/f/g/h/file01.dat", "f1"),
                Add("a/b/c/x.txt", "x"),
                Rm("a/b/c/d/e/f/g/h/file00.dat"),
            ],
            vec![
                AddMeta("logo.png", "logo", "Content-Type", "image/png"),
                Add("logo.png", "logo2"),
                AddMeta("logo.png", "logo3", "Filename", "logo.png"),
            ],
        ]
    }

    #[test]
    fn split_commit_matches_the_fresh_replay() {
        for (i, script) in corpora().iter().enumerate() {
            let (want, _) = editor_replay(script);
            for split in 0..=script.len() {
                let (got, _) = editor_replay_split(script, split);
                assert_eq!(
                    got, want,
                    "corpus {i} split {split} diverges from the fresh replay"
                );
            }
        }
    }

    #[test]
    fn committed_root_is_readable() {
        let script = corpora().swap_remove(4);
        let (root, loadsaver) = editor_replay(&script);
        let reader = crate::Reader::new(loadsaver);
        let entry = run(reader.get(root, b"img/1.png")).unwrap().unwrap();
        assert_eq!(
            entry.reference().map(|r| *r.address()),
            Some(make_addr("1v2"))
        );
        assert!(run(reader.get(root, b"img/2.png")).unwrap().is_some());
        assert!(run(reader.get(root, b"absent")).unwrap().is_none());
    }

    #[test]
    fn root_documents_readable_on_an_edge_node() {
        let mut editor = Editor::new(LoadSaver::new(Store::new()));
        editor.insert("/c", make_addr("c"));
        editor.insert("//", make_addr("s"));
        editor.set_index_document("doc");
        let (root, loadsaver) = run(editor.commit()).unwrap();
        let entry = run(crate::Reader::new(loadsaver).get(root, b"/"))
            .unwrap()
            .expect("metadata-carrying edge reads back");
        assert!(entry.reference().is_none());
        assert_eq!(
            entry
                .metadata()
                .get("website-index-document")
                .map(String::as_str),
            Some("doc")
        );
    }

    /// Metadata on the empty path lands on the trie root, which has no wire
    /// slot for it; commit fails loud instead of dropping it silently.
    #[test]
    fn root_metadata_put_fails_commit() {
        let mut editor = Editor::new(LoadSaver::new(Store::new()));
        let meta = [("k".to_string(), "v".to_string())].into();
        editor.insert_with("", make_addr("r"), meta);
        let err = run(editor.commit()).unwrap_err();
        assert!(matches!(
            err,
            EditorError::Commit(MantarayError::RootMetadata)
        ));
    }

    #[test]
    fn zero_reference_put_fails_commit() {
        let mut editor = Editor::new(LoadSaver::new(Store::new()));
        editor.insert("a", ChunkAddress::from([0u8; 32]));
        let err = run(editor.commit()).unwrap_err();
        assert!(matches!(
            err,
            EditorError::Apply {
                index: 0,
                source: MantarayError::ZeroReference,
                ..
            }
        ));
    }

    #[test]
    fn apply_error_names_op_index_and_path() {
        let mut editor = Editor::new(LoadSaver::new(Store::new()));
        editor.insert("present", make_addr("p"));
        // The legacy boundary remove is the one that fails on an absent path.
        editor.remove_subtree("absent");
        let err = run(editor.commit()).unwrap_err();
        assert!(matches!(
            err,
            EditorError::Apply { index: 1, ref path, .. } if path == b"absent"
        ));
    }

    /// The map removal is exact-key: a key with children keeps them, a
    /// childless leaf is pruned, and an absent key changes nothing.
    #[test]
    fn remove_is_exact_key_and_absence_is_a_noop() {
        let (root, loadsaver) = editor_replay(&[
            Script::Add("a", "1"),
            Script::Add("ab", "2"),
            Script::Add("ac", "3"),
        ]);

        let mut editor = Editor::open(root, loadsaver);
        editor.remove("a");
        let (pruned, loadsaver) = run(editor.commit()).unwrap();
        let reader = crate::Reader::new(loadsaver);
        assert!(run(reader.get(pruned, b"a")).unwrap().is_none());
        assert!(run(reader.get(pruned, b"ab")).unwrap().is_some());
        assert!(run(reader.get(pruned, b"ac")).unwrap().is_some());

        // Absent, and unbound: neither moves the root.
        let mut editor = Editor::open(pruned, reader.into_store());
        editor.remove("zzz");
        editor.remove("a");
        let (again, _) = run(editor.commit()).unwrap();
        assert_eq!(again, pruned, "a removal of nothing removes nothing");
    }

    /// A removal that empties the fork a split created leaves a trie that reads
    /// exactly the surviving keys.
    ///
    /// The root is not the root a replay of the surviving keys writes: this
    /// format is order-dependent by design. Only the key set is contracted.
    #[test]
    fn remove_leaves_a_trie_that_reads_the_surviving_keys() {
        let (root, loadsaver) = editor_replay(&[
            Script::Add("alpha", "1"),
            Script::Add("alpine", "2"),
            Script::Add("beta", "3"),
        ]);
        let mut editor = Editor::open(root, loadsaver);
        editor.remove("alpine");
        let (emptied, loadsaver) = run(editor.commit()).unwrap();

        let reader = crate::Reader::new(loadsaver);
        assert!(run(reader.get(emptied, b"alpine")).unwrap().is_none());
        assert!(run(reader.get(emptied, b"alpha")).unwrap().is_some());
        assert!(run(reader.get(emptied, b"beta")).unwrap().is_some());
    }

    /// The same past the 30-byte prefix bound, where the surviving key chains
    /// through more than one edge.
    #[test]
    fn remove_past_the_prefix_bound_reads_the_surviving_key() {
        const ONE: &str = "deep/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaone";
        const TWO: &str = "deep/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaatwo";
        assert!(ONE.len() > Prefix::MAX_LEN, "the key outruns one edge");

        let (root, loadsaver) = editor_replay(&[Script::Add(ONE, "1"), Script::Add(TWO, "2")]);
        let mut editor = Editor::open(root, loadsaver);
        editor.remove(TWO);
        let (emptied, loadsaver) = run(editor.commit()).unwrap();

        let reader = crate::Reader::new(loadsaver);
        assert!(run(reader.get(emptied, TWO.as_bytes())).unwrap().is_none());
        assert!(run(reader.get(emptied, ONE.as_bytes())).unwrap().is_some());
    }

    /// The legacy boundary remove keeps taking the whole subtree.
    #[test]
    fn remove_subtree_still_takes_the_whole_subtree() {
        let (root, loadsaver) = editor_replay(&[
            Script::Add("a", "1"),
            Script::Add("ab", "2"),
            Script::Add("ac", "3"),
        ]);
        let mut editor = Editor::open(root, loadsaver);
        editor.remove_subtree("a");
        let (pruned, loadsaver) = run(editor.commit()).unwrap();
        let reader = crate::Reader::new(loadsaver);
        for path in [&b"a"[..], &b"ab"[..], &b"ac"[..]] {
            assert!(run(reader.get(pruned, path)).unwrap().is_none());
        }
    }

    /// The clean-ancestor hazard: root metadata set after a persist boundary
    /// must not be shadowed by the loaded-but-clean root at the next commit.
    #[test]
    fn clean_ancestor_hazard_regression() {
        // The well-defined root for the sequence, from a fresh replay.
        let (want, _) = editor_replay(&[
            Script::Add("index.html", "i"),
            Script::SetIndex("index.html"),
        ]);

        // The editor commits the metadata across a reopen boundary.
        let mut editor = Editor::new(LoadSaver::new(Store::new()));
        editor.insert("index.html", make_addr("i"));
        let (root, loadsaver) = run(editor.commit()).unwrap();
        assert_ne!(root, want, "the metadata must change the root");
        let mut editor = Editor::open(root, loadsaver);
        editor.set_index_document("index.html");
        let (got, loadsaver) = run(editor.commit()).unwrap();
        assert_eq!(got, want);

        let reader = crate::Reader::new(loadsaver);
        let entry = run(reader.get(got, b"/")).unwrap().unwrap();
        assert_eq!(
            entry.metadata().get("website-index-document").cloned(),
            Some("index.html".to_string())
        );
    }

    /// A no-op removal loads the node it walks without dirtying it, so an add
    /// over that node must dirty it itself or the commit splices the old
    /// reference back and drops the write.
    #[test]
    fn an_add_over_a_clean_loaded_node_is_not_dropped() {
        let (want, _) = editor_replay(&[Script::Add("a", "1"), Script::Add("b/c", "2")]);

        let (root, loadsaver) = editor_replay(&[Script::Add("a", "1")]);
        let mut editor = Editor::open(root, loadsaver);
        // Absent at the root and absent below an existing edge: neither op
        // changes the trie, and both leave the node they walked loaded.
        editor.remove("zzz");
        editor.remove("ax");
        editor.insert("b/c", make_addr("2"));
        let (got, loadsaver) = run(editor.commit()).unwrap();
        assert_eq!(got, want, "the add after a no-op removal was dropped");

        let reader = crate::Reader::new(loadsaver);
        assert!(run(reader.get(got, b"b/c")).unwrap().is_some());
        assert!(run(reader.get(got, b"a")).unwrap().is_some());
    }

    #[test]
    fn noop_commit_on_opened_root_is_stable_and_save_free() {
        let (root, loadsaver) = editor_replay(&[Script::Add("a", "1"), Script::Add("b", "2")]);
        let counting = CountingSaver::new(loadsaver);
        let editor: ManifestEditor<_> = ManifestEditor::open(root, counting.clone());
        let (again, _) = run(editor.commit()).unwrap();
        assert_eq!(again, root);
        assert_eq!(counting.saves(), 0);
    }

    #[test]
    fn encrypted_split_commit_matches_the_fresh_replay() {
        // Seed a persisted empty encrypted manifest so both replays share
        // one obfuscation key.
        let seed: ManifestEditor<_, EncryptedChunkRef> =
            ManifestEditor::new_encrypted(LoadSaver::new(Store::new()));
        let (seed_ref, store) = run(seed.commit()).unwrap();
        let enc = |s: &str| EncryptedChunkRef::new(make_addr(s), EncryptionKey::from([0x5a; 32]));

        // Single-session replay from the seed.
        let mut single: ManifestEditor<_, EncryptedChunkRef> =
            ManifestEditor::open_encrypted(seed_ref.clone(), store);
        single.insert("secret/a.txt", enc("a"));
        single.insert("secret/b.txt", enc("b"));
        single.remove("secret/a.txt");
        let (want, store) = run(single.commit()).unwrap();

        // The same ops across a commit boundary land on the same root.
        let mut editor: ManifestEditor<_, EncryptedChunkRef> =
            ManifestEditor::open_encrypted(seed_ref, store);
        editor.insert("secret/a.txt", enc("a"));
        editor.insert("secret/b.txt", enc("b"));
        let (mid, store) = run(editor.commit()).unwrap();
        let mut editor: ManifestEditor<_, EncryptedChunkRef> =
            ManifestEditor::open_encrypted(mid, store);
        editor.remove("secret/a.txt");
        let (got, _) = run(editor.commit()).unwrap();
        assert_eq!(got, want);
    }

    /// A loadsaver wrapper counting save calls; `Clone` shares one count.
    #[derive(Clone)]
    struct CountingSaver {
        inner: LoadSaver,
        saves: Arc<AtomicUsize>,
    }

    impl CountingSaver {
        fn new(inner: LoadSaver) -> Self {
            Self {
                inner,
                saves: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn saves(&self) -> usize {
            self.saves.load(Ordering::SeqCst)
        }
    }

    impl NodeLoader for CountingSaver {
        type Error = SingleChunkError;

        async fn load(&self, reference: &EntryRef) -> Result<Vec<u8>, Self::Error> {
            self.inner.load(reference).await
        }
    }

    impl NodeSaver<ChunkRef> for CountingSaver {
        type Error = SingleChunkError;

        async fn save(&self, data: Vec<u8>) -> Result<ChunkRef, Self::Error> {
            self.saves.fetch_add(1, Ordering::SeqCst);
            NodeSaver::<ChunkRef>::save(&self.inner, data).await
        }
    }

    /// A saver that fails the `fail_at`-th save, in dispatch order, and
    /// completes the rest; `Clone` shares one counter.
    #[derive(Debug, Clone)]
    struct FailingSaver {
        inner: LoadSaver,
        fail_at: usize,
        dispatched: Arc<AtomicUsize>,
    }

    impl FailingSaver {
        fn new(inner: LoadSaver, fail_at: usize) -> Self {
            Self {
                inner,
                fail_at,
                dispatched: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl NodeLoader for FailingSaver {
        type Error = SingleChunkError;

        async fn load(&self, reference: &EntryRef) -> Result<Vec<u8>, Self::Error> {
            self.inner.load(reference).await
        }
    }

    impl NodeSaver<ChunkRef> for FailingSaver {
        type Error = SingleChunkError;

        async fn save(&self, data: Vec<u8>) -> Result<ChunkRef, Self::Error> {
            let seen = self.dispatched.fetch_add(1, Ordering::SeqCst);
            // An image far past one chunk is the loadsaver's own failure.
            let data = if seen == self.fail_at {
                alloc::vec![0u8; 1 << 20]
            } else {
                data
            };
            NodeSaver::<ChunkRef>::save(&self.inner, data).await
        }
    }

    /// A failing save is terminal at the turn it folds back: the commit
    /// surfaces the wrapped saver fault rather than crowning a root or
    /// stalling on the frames still owed a save. The four dirty nodes are
    /// three leaves and the root, so the last index fails the root's own
    /// save.
    #[test]
    fn a_failing_save_fails_the_commit() {
        // Pin that shape, so a trie change fails here rather than silently
        // dropping the root from the indices below.
        let control = FailingSaver::new(LoadSaver::new(Store::new()), usize::MAX);
        let mut editor = ManifestEditor::new(control.clone());
        for p in ["a", "b", "c"] {
            editor.insert(p, make_addr(p));
        }
        run(editor.commit()).unwrap();
        assert_eq!(control.dispatched.load(Ordering::SeqCst), 4, "dirty nodes");

        for fail_at in [0usize, 2, 3] {
            let saver = FailingSaver::new(LoadSaver::new(Store::new()), fail_at);
            let mut editor = ManifestEditor::new(saver);
            for p in ["a", "b", "c"] {
                editor.insert(p, make_addr(p));
            }
            let err = run(editor.commit()).unwrap_err();
            assert!(
                matches!(err, EditorError::Commit(MantarayError::StorePut { .. })),
                "fail_at {fail_at}: {err:?}"
            );
        }
    }

    /// A saver that parks each save once before completing and records the
    /// concurrency and the resident encoded bytes it observes, so a test can
    /// witness sibling overlap and the window bound directly. `Clone` shares
    /// one recording.
    #[derive(Clone)]
    struct WindowSaver {
        inner: LoadSaver,
        probe: Arc<SaveProbe>,
    }

    struct SaveProbe {
        saves: AtomicUsize,
        in_flight: AtomicUsize,
        peak_in_flight: AtomicUsize,
        resident: AtomicUsize,
        peak_resident: AtomicUsize,
        total: AtomicUsize,
    }

    impl WindowSaver {
        fn new(inner: LoadSaver) -> Self {
            Self {
                inner,
                probe: Arc::new(SaveProbe {
                    saves: AtomicUsize::new(0),
                    in_flight: AtomicUsize::new(0),
                    peak_in_flight: AtomicUsize::new(0),
                    resident: AtomicUsize::new(0),
                    peak_resident: AtomicUsize::new(0),
                    total: AtomicUsize::new(0),
                }),
            }
        }

        fn saves(&self) -> usize {
            self.probe.saves.load(Ordering::SeqCst)
        }

        fn peak_in_flight(&self) -> usize {
            self.probe.peak_in_flight.load(Ordering::SeqCst)
        }

        fn peak_resident(&self) -> usize {
            self.probe.peak_resident.load(Ordering::SeqCst)
        }

        fn total(&self) -> usize {
            self.probe.total.load(Ordering::SeqCst)
        }
    }

    impl NodeLoader for WindowSaver {
        type Error = SingleChunkError;

        async fn load(&self, reference: &EntryRef) -> Result<Vec<u8>, Self::Error> {
            self.inner.load(reference).await
        }
    }

    impl NodeSaver<ChunkRef> for WindowSaver {
        type Error = SingleChunkError;

        async fn save(&self, data: Vec<u8>) -> Result<ChunkRef, Self::Error> {
            let bytes = data.len();
            self.probe.saves.fetch_add(1, Ordering::SeqCst);
            self.probe.total.fetch_add(bytes, Ordering::SeqCst);
            let level = self.probe.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.probe.peak_in_flight.fetch_max(level, Ordering::SeqCst);
            let held = self.probe.resident.fetch_add(bytes, Ordering::SeqCst) + bytes;
            self.probe.peak_resident.fetch_max(held, Ordering::SeqCst);
            // Park once so queued siblings ramp their in-flight count before
            // any single save resolves.
            yield_once().await;
            let result = NodeSaver::<ChunkRef>::save(&self.inner, data).await;
            self.probe.resident.fetch_sub(bytes, Ordering::SeqCst);
            self.probe.in_flight.fetch_sub(1, Ordering::SeqCst);
            result
        }
    }

    /// Yield once, waking immediately, so pending saves accumulate.
    async fn yield_once() {
        let mut yielded = false;
        core::future::poll_fn(|cx| {
            if yielded {
                Poll::Ready(())
            } else {
                yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;
    }

    /// A wide, two-level dirty manifest: `groups` first-byte fan-outs, each an
    /// intermediate node with `leaves` value children keyed `[group, leaf]`.
    /// Independent siblings under one intermediate are what a bounded window
    /// overlaps; two levels keep the node count in the thousands without a
    /// single node exceeding one chunk.
    fn wide_editor(groups: u8, leaves: u8) -> ManifestEditor<WindowSaver> {
        let mut editor = ManifestEditor::new(WindowSaver::new(LoadSaver::new(Store::new())));
        for g in 0..groups {
            for l in 0..leaves {
                editor.insert([g, l], make_addr(&format!("{g}-{l}")));
            }
        }
        editor
    }

    /// A window of `slots`.
    fn win(slots: u16) -> Window {
        Window::new(slots).unwrap()
    }

    /// Independent siblings save concurrently and exactly fill the window: the
    /// descent dispatches leaf saves until the window is full, so the peak is
    /// the window whenever a group has at least that many leaves.
    #[test]
    fn commit_overlaps_siblings_within_the_window() {
        for slots in [1u16, 4, 16] {
            let editor = wide_editor(4, 24).with_commit_window(win(slots));
            let saver = editor.store().clone();
            let (_root, _store) = run(editor.commit()).unwrap();
            let peak = saver.peak_in_flight();
            assert!(
                peak <= usize::from(slots),
                "window {slots}: peak in-flight {peak} exceeds the window"
            );
            assert_eq!(
                peak,
                usize::from(slots),
                "window {slots}: independent siblings must fill the window (peak {peak})"
            );
        }
    }

    /// The commit is memory-bounded: peak resident encoded bytes stay within a
    /// window of node images, never the whole tree, even as the tree grows far
    /// past the window.
    #[test]
    fn commit_holds_at_most_a_window_of_encoded_images() {
        let slots = 8u16;
        let editor = wide_editor(32, 32).with_commit_window(win(slots));
        let saver = editor.store().clone();
        run(editor.commit()).unwrap();

        let saves = saver.saves();
        assert!(
            saves > 1000,
            "expected thousands of dirty nodes, saved {saves}"
        );
        assert!(
            saver.peak_in_flight() <= usize::from(slots),
            "in-flight saves exceeded the window"
        );
        // Resident encoded bytes never approach the whole tree's: a window of
        // images is a small fraction of every node's image summed.
        let peak = saver.peak_resident();
        let total = saver.total();
        let mean = total / saves.max(1);
        assert!(
            peak <= usize::from(slots) * mean * 4,
            "resident bytes {peak} exceed a window of images (mean {mean})"
        );
        assert!(
            peak * 4 < total,
            "resident bytes {peak} are not bounded below the whole tree {total}"
        );
    }

    /// Replay the committed seed corpus of the `mantaray_editor_differential`
    /// fuzz target: the seed bytes decode into an op log through the shared
    /// `EditorOp` grammar and run the exact differential oracle the fuzzer
    /// drives. This keeps the curated op-log seeds meaningful on stable
    /// without running the fuzzer itself.
    #[test]
    fn seed_replay_mantaray_editor_differential() {
        use arbitrary::{Arbitrary, Unstructured};

        nectar_testing::SeedReplay::corpus(
            env!("CARGO_MANIFEST_DIR"),
            "mantaray_editor_differential",
        )
        .each(|name, data| {
            let ops = Vec::<crate::oracles::EditorOp>::arbitrary_take_rest(Unstructured::new(data))
                .unwrap_or_else(|e| panic!("seed {name} must decode an op log: {e}"));
            assert!(!ops.is_empty(), "seed {name} must carry at least one op");
            run(crate::oracles::editor_differential(&ops))
                .unwrap_or_else(|v| panic!("seed {name}: {v}"));
        })
        .covers("prefix-")
        .covers("root-")
        .covers("zero-")
        .floor(4)
        .run();
    }
}

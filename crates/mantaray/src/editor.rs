//! Submission-order manifest editor with a streaming, bounded-put commit.
//!
//! Ops are recorded synchronously into a `(path, op)` log and applied one at
//! a time at commit, in submission order. The committed root is defined as
//! the root the legacy mutation path produces for the same sequence, shape
//! quirks included; ops are never reordered or batched.

use alloc::collections::BTreeMap;
use alloc::collections::btree_map;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::{ChunkAddress, ChunkOps, ChunkRef, ContentChunk, Reference};
use nectar_primitives::store::{ChunkPut, MaybeSend, TrustedGet};
use nectar_primitives::{AnyChunkSet, Chunk};

use crate::error::EditorError;
use crate::node::{Fork, Node, NodeState, Prefix, StoredReference};
use crate::{MantarayError, metadata};

/// Default bound on in-flight commit puts.
///
/// Matches the listing fan-out width, balancing round-trip overlap against
/// peak in-flight store load.
pub const DEFAULT_PUT_WIDTH: usize = 8;

/// One recorded manifest mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Op<R: Reference = ChunkRef> {
    /// Set the entry at the path; a non-empty metadata map replaces the
    /// node's metadata, an empty one leaves existing metadata in place.
    Put {
        /// Entry reference, or `None` for a metadata-only value node.
        reference: Option<R>,
        /// Metadata to attach; empty means keep what is there.
        metadata: BTreeMap<String, String>,
    },
    /// Remove the value at the path; an absent path fails the commit.
    Remove,
    /// Merge one metadata key into the node at the path, creating the node
    /// when absent.
    SetRootMetadata {
        /// Metadata key to merge.
        key: String,
        /// Metadata value to set under the key.
        value: String,
    },
}

/// Submission-order manifest editor.
///
/// Records `(path, op)` pairs without touching the store; [`commit`] applies
/// them sequentially and persists every rewritten node with a bounded number
/// of puts in flight. Commit consumes the editor: reopen from the returned
/// root to edit further.
///
/// [`commit`]: Self::commit
///
/// ```
/// # use nectar_mantaray::{ManifestEditor, DefaultMemoryStore};
/// # use nectar_primitives::chunk::ChunkAddress;
/// # futures::executor::block_on(async {
/// let mut editor = ManifestEditor::new(DefaultMemoryStore::new());
/// editor.put("index.html", ChunkAddress::from([7u8; 32]));
/// editor.set_index_document("index.html");
/// let (root, _store) = editor.commit().await.unwrap();
/// # let _ = root;
/// # });
/// ```
#[derive(Debug)]
pub struct ManifestEditor<S, R: Reference = ChunkRef, const BS: usize = DEFAULT_BODY_SIZE> {
    trie: Node<R>,
    ops: Vec<(Vec<u8>, Op<R>)>,
    store: S,
    put_width: usize,
}

impl<S, const BS: usize> ManifestEditor<S, ChunkRef, BS> {
    /// Editor over an empty plain manifest.
    pub fn new(store: S) -> Self {
        Self::with_root(Node::new_unencrypted(), store)
    }

    /// Editor over the persisted plain manifest rooted at `root`.
    pub fn open(root: ChunkAddress, store: S) -> Self {
        Self::with_root(Node::from_reference(ChunkRef::from(root)), store)
    }
}

impl<S, const BS: usize> ManifestEditor<S, nectar_primitives::EncryptedChunkRef, BS> {
    /// Editor over an empty encrypted manifest with a random obfuscation key.
    #[cfg(feature = "std")]
    pub fn new_encrypted(store: S) -> Self {
        let trie = Node {
            obfuscation_key: crate::obfuscation::ObfuscationKey::generate(),
            ..Node::default()
        };
        Self::with_root(trie, store)
    }

    /// Editor over the persisted encrypted manifest at `root`.
    pub fn open_encrypted(root: crate::ManifestRef, store: S) -> Self {
        let (addr, key) = root.into_parts();
        let mut trie =
            Node::from_reference(nectar_primitives::EncryptedChunkRef::from_stored(addr));
        trie.obfuscation_key = key;
        Self::with_root(trie, store)
    }
}

impl<S, R: Reference, const BS: usize> ManifestEditor<S, R, BS> {
    const fn with_root(trie: Node<R>, store: S) -> Self {
        Self {
            trie,
            ops: Vec::new(),
            store,
            put_width: DEFAULT_PUT_WIDTH,
        }
    }

    /// Replace the in-flight put bound; clamped to at least 1.
    #[must_use]
    pub fn with_put_width(mut self, width: usize) -> Self {
        self.put_width = width.max(1);
        self
    }

    /// The in-flight put bound used by commit.
    #[must_use]
    pub const fn put_width(&self) -> usize {
        self.put_width
    }

    /// The backing store.
    #[must_use]
    pub const fn store(&self) -> &S {
        &self.store
    }

    /// The recorded ops in submission order.
    #[must_use]
    pub fn ops(&self) -> &[(Vec<u8>, Op<R>)] {
        &self.ops
    }

    /// Record setting the entry at `path`.
    pub fn put(&mut self, path: impl AsRef<[u8]>, reference: impl Into<R>) -> &mut Self {
        self.push(
            path,
            Op::Put {
                reference: Some(reference.into()),
                metadata: BTreeMap::new(),
            },
        )
    }

    /// Record setting the entry at `path` with metadata.
    pub fn put_with_metadata(
        &mut self,
        path: impl AsRef<[u8]>,
        reference: impl Into<R>,
        metadata: BTreeMap<String, String>,
    ) -> &mut Self {
        self.push(
            path,
            Op::Put {
                reference: Some(reference.into()),
                metadata,
            },
        )
    }

    /// Record removing the value at `path`.
    pub fn remove(&mut self, path: impl AsRef<[u8]>) -> &mut Self {
        self.push(path, Op::Remove)
    }

    /// Record merging one metadata key into the manifest's root path node.
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

impl<S: TrustedGet<AnyChunkSet<BS>>, R: Reference + MaybeSend, const BS: usize>
    ManifestEditor<S, R, BS>
{
    /// Apply the recorded ops to the trie, one at a time, in submission order.
    async fn apply_ops(&mut self) -> Result<(), EditorError> {
        let ops = core::mem::take(&mut self.ops);
        for (index, (path, op)) in ops.into_iter().enumerate() {
            let result = match op {
                Op::Put {
                    reference,
                    metadata,
                } => {
                    self.trie
                        .add::<S, BS>(&path, reference, metadata, &self.store)
                        .await
                }
                Op::Remove => self.trie.remove::<S, BS>(&path, &self.store).await,
                Op::SetRootMetadata { key, value } => {
                    apply_metadata_merge::<S, R, BS>(&mut self.trie, &path, key, value, &self.store)
                        .await
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

impl<S: TrustedGet<AnyChunkSet<BS>> + ChunkPut<AnyChunkSet<BS>>, const BS: usize>
    ManifestEditor<S, ChunkRef, BS>
{
    /// Apply the log and persist the trie, returning the root chunk address
    /// and the store.
    pub async fn commit(mut self) -> Result<(ChunkAddress, S), EditorError> {
        self.apply_ops().await?;
        let committed = commit_trie::<S, ChunkRef, BS>(self.trie, &self.store, self.put_width)
            .await
            .map_err(EditorError::Commit)?;
        let address = *committed
            .reference()
            .ok_or(EditorError::Commit(MantarayError::MissingReference))?
            .address();
        Ok((address, self.store))
    }
}

impl<S: TrustedGet<AnyChunkSet<BS>> + ChunkPut<AnyChunkSet<BS>>, const BS: usize>
    ManifestEditor<S, nectar_primitives::EncryptedChunkRef, BS>
{
    /// Apply the log and persist the trie, returning the manifest reference
    /// and the store.
    pub async fn commit(mut self) -> Result<(crate::ManifestRef, S), EditorError> {
        self.apply_ops().await?;
        let committed = commit_trie::<S, nectar_primitives::EncryptedChunkRef, BS>(
            self.trie,
            &self.store,
            self.put_width,
        )
        .await
        .map_err(EditorError::Commit)?;
        let address = *committed
            .reference()
            .ok_or(EditorError::Commit(MantarayError::MissingReference))?
            .address();
        Ok((
            crate::ManifestRef::new(address, *committed.obfuscation_key()),
            self.store,
        ))
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
/// Shape-exact twin of the legacy root-metadata merge: an existing node keeps
/// its entry and gains the key; an absent one is created as a metadata-only
/// value. Every node on the descent is marked dirty so a clean ancestor can
/// never shadow the merged metadata at commit.
async fn apply_metadata_merge<S, R, const BS: usize>(
    trie: &mut Node<R>,
    path: &[u8],
    key: String,
    value: String,
    store: &S,
) -> Result<(), MantarayError>
where
    S: TrustedGet<AnyChunkSet<BS>>,
    R: Reference + MaybeSend,
{
    match merge_descent(trie, path, &key, &value, store).await? {
        MergeOutcome::Applied => Ok(()),
        MergeOutcome::Missing => {
            let mut meta = BTreeMap::new();
            meta.insert(key, value);
            trie.add::<S, BS>(path, None, meta, store).await
        }
    }
}

/// Descend to `path`, dirtying every visited node, and merge the key there.
async fn merge_descent<S, R, const BS: usize>(
    trie: &mut Node<R>,
    path: &[u8],
    key: &str,
    value: &str,
    store: &S,
) -> Result<MergeOutcome, MantarayError>
where
    S: TrustedGet<AnyChunkSet<BS>>,
    R: Reference,
{
    let mut current = trie;
    let mut rest = path;
    loop {
        if !current.is_loaded() {
            current.load::<S, BS>(store).await?;
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

/// Persist the dirty subtree post-order, keeping at most `width` puts in
/// flight, and return the root as a persisted stub.
///
/// A child's address is content-derived at encode time, so parents encode
/// without waiting for the child's put to land; only completion is awaited.
async fn commit_trie<S, R, const BS: usize>(
    root: Node<R>,
    store: &S,
    width: usize,
) -> Result<Node<R>, MantarayError>
where
    S: ChunkPut<AnyChunkSet<BS>>,
    R: StoredReference,
{
    if root.reference().is_some() {
        return Ok(root);
    }

    struct CommitFrame<R: Reference> {
        /// Fork slot (key and prefix) this node re-attaches to in its
        /// parent; `None` only for the root frame.
        slot: Option<(u8, Prefix)>,
        node: Node<R>,
        /// Children still to visit, drained from the node's fork map.
        todo: btree_map::IntoIter<u8, Fork<R>>,
        /// Children already persisted, keyed for re-attachment.
        done: BTreeMap<u8, Fork<R>>,
    }

    fn frame<R: Reference>(slot: Option<(u8, Prefix)>, mut node: Node<R>) -> CommitFrame<R> {
        let todo = core::mem::take(&mut node.forks).into_iter();
        CommitFrame {
            slot,
            node,
            todo,
            done: BTreeMap::new(),
        }
    }

    let width = width.max(1);
    let mut pending = FuturesUnordered::new();
    let mut stack = alloc::vec![frame(None, root)];
    let mut committed_root = None;

    while let Some(top) = stack.last_mut() {
        if let Some((key, fork)) = top.todo.next() {
            if fork.node.reference().is_some() {
                // Already persisted; nothing below it changed.
                top.done.insert(key, fork);
            } else {
                stack.push(frame(Some((key, fork.prefix)), fork.node));
            }
            continue;
        }

        let Some(mut finished) = stack.pop() else {
            break;
        };
        finished.node.forks = core::mem::take(&mut finished.done);
        let data = finished.node.encode()?;
        let chunk = ContentChunk::<BS>::new(Bytes::from(data))?;
        let address = *chunk.address();
        let sealed: Chunk<_, AnyChunkSet<BS>> = Chunk::from_envelope(chunk.into())?;
        pending.push(store.put(sealed));
        if pending.len() >= width
            && let Some(result) = pending.next().await
        {
            result.map_err(|e| MantarayError::StorePut {
                source: Arc::new(e),
            })?;
        }

        // The persisted node collapses to a stub, reloaded on demand.
        finished.node.state = NodeState::Stub(R::from_stored(address));
        finished.node.forks.clear();
        match stack.last_mut() {
            Some(parent) => {
                if let Some((key, prefix)) = finished.slot {
                    parent.done.insert(
                        key,
                        Fork {
                            prefix,
                            node: finished.node,
                        },
                    );
                }
                // A slotless frame is the root, which never has a parent.
            }
            None => committed_root = Some(finished.node),
        }
    }

    while let Some(result) = pending.next().await {
        result.map_err(|e| MantarayError::StorePut {
            source: Arc::new(e),
        })?;
    }

    committed_root.ok_or(MantarayError::MissingReference)
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::sync::atomic::{AtomicUsize, Ordering};

    use futures::executor::block_on;
    use nectar_primitives::store::{ChunkGet, MemoryStore};
    use nectar_primitives::{EncryptedChunkRef, EncryptionKey, StandardChunkSet, Verified};

    use crate::{EncryptedManifest, PlainManifest};

    type Store = MemoryStore<StandardChunkSet>;
    type Legacy = PlainManifest<Store>;
    type Editor = ManifestEditor<Store>;

    /// A ChunkAddress from a string, right-padded with zeroes.
    fn make_addr(s: &str) -> ChunkAddress {
        let bytes = s.as_bytes();
        let mut buf = [0u8; 32];
        let len = bytes.len().min(32);
        buf[..len].copy_from_slice(&bytes[..len]);
        ChunkAddress::from(buf)
    }

    /// One scripted mutation, replayable on the legacy manifest and the editor.
    #[derive(Clone, Copy)]
    enum Script {
        Add(&'static str, &'static str),
        AddMeta(&'static str, &'static str, &'static str, &'static str),
        Rm(&'static str),
        SetIndex(&'static str),
        SetError(&'static str),
    }

    /// Fresh single-session legacy replay: build in memory, save once. All
    /// nodes are dirty at save, so the legacy root is the well-defined one.
    fn legacy_replay(script: &[Script]) -> (ChunkAddress, Store) {
        let mut m = Legacy::new(Store::new());
        for op in script {
            match *op {
                Script::Add(p, seed) => block_on(m.add(p, make_addr(seed))).unwrap(),
                Script::AddMeta(p, seed, k, v) => {
                    let meta = [(k.to_string(), v.to_string())].into();
                    block_on(m.add_with_metadata(p, make_addr(seed), meta)).unwrap();
                }
                Script::Rm(p) => block_on(m.remove(p)).unwrap(),
                Script::SetIndex(v) => block_on(m.set_index_document(v)).unwrap(),
                Script::SetError(v) => block_on(m.set_error_document(v)).unwrap(),
            }
        }
        let root = block_on(m.save()).unwrap();
        let (_, store) = m.into_parts();
        (root, store)
    }

    /// Record a script into an editor.
    fn record(editor: &mut Editor, script: &[Script]) {
        for op in script {
            match *op {
                Script::Add(p, seed) => {
                    editor.put(p, make_addr(seed));
                }
                Script::AddMeta(p, seed, k, v) => {
                    let meta = [(k.to_string(), v.to_string())].into();
                    editor.put_with_metadata(p, make_addr(seed), meta);
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
    fn editor_replay(script: &[Script]) -> (ChunkAddress, Store) {
        let mut editor = Editor::new(Store::new());
        record(&mut editor, script);
        block_on(editor.commit()).unwrap()
    }

    /// Editor replay with a commit boundary after `split` ops, continuing
    /// from the persisted intermediate root.
    fn editor_replay_split(script: &[Script], split: usize) -> (ChunkAddress, Store) {
        let (head, tail) = script.split_at(split.min(script.len()));
        let mut editor = Editor::new(Store::new());
        record(&mut editor, head);
        let (root, store) = block_on(editor.commit()).unwrap();
        let mut editor = Editor::open(root, store);
        record(&mut editor, tail);
        block_on(editor.commit()).unwrap()
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
    fn commit_matches_legacy_fresh_replay() {
        for (i, script) in corpora().iter().enumerate() {
            let (want, _) = legacy_replay(script);
            let (got, _) = editor_replay(script);
            assert_eq!(got, want, "corpus {i} diverges from the legacy root");
        }
    }

    #[test]
    fn split_commit_matches_legacy_fresh_replay() {
        for (i, script) in corpora().iter().enumerate() {
            let (want, _) = legacy_replay(script);
            for split in 0..=script.len() {
                let (got, _) = editor_replay_split(script, split);
                assert_eq!(
                    got, want,
                    "corpus {i} split {split} diverges from the legacy root"
                );
            }
        }
    }

    #[test]
    fn committed_root_is_readable_by_the_legacy_manifest() {
        let script = corpora().swap_remove(4);
        let (root, store) = editor_replay(&script);
        let m = Legacy::open(root, store);
        let entry = block_on(m.get("img/1.png")).unwrap().unwrap();
        assert_eq!(entry.address(), Some(&make_addr("1v2")));
        assert!(block_on(m.get("img/2.png")).unwrap().is_some());
        assert!(block_on(m.get("absent")).unwrap().is_none());
    }

    #[test]
    fn apply_error_names_op_index_and_path() {
        let mut editor = Editor::new(Store::new());
        editor.put("present", make_addr("p"));
        editor.remove("absent");
        let err = block_on(editor.commit()).unwrap_err();
        assert!(matches!(
            err,
            EditorError::Apply { index: 1, ref path, .. } if path == b"absent"
        ));
    }

    /// The clean-ancestor hazard: legacy root metadata set after a save is
    /// dropped, because the loaded-but-clean root shadows the dirty child at
    /// the next save. The editor commit must not reproduce it.
    #[test]
    fn clean_ancestor_hazard_regression() {
        // Legacy exhibits the hazard: the second save returns the stale root.
        let mut legacy = Legacy::new(Store::new());
        block_on(legacy.add("index.html", make_addr("i"))).unwrap();
        let stale = block_on(legacy.save()).unwrap();
        block_on(legacy.set_index_document("index.html")).unwrap();
        assert_eq!(block_on(legacy.save()).unwrap(), stale);

        // The well-defined root for the same sequence, from a fresh replay.
        let (want, _) = legacy_replay(&[
            Script::Add("index.html", "i"),
            Script::SetIndex("index.html"),
        ]);
        assert_ne!(want, stale);

        // The editor commits the metadata across the same boundary.
        let mut editor = Editor::new(Store::new());
        editor.put("index.html", make_addr("i"));
        let (root, store) = block_on(editor.commit()).unwrap();
        assert_eq!(root, stale);
        let mut editor = Editor::open(root, store);
        editor.set_index_document("index.html");
        let (got, store) = block_on(editor.commit()).unwrap();
        assert_eq!(got, want);

        let m = Legacy::open(got, store);
        assert_eq!(
            block_on(m.index_document()).unwrap(),
            Some("index.html".to_string())
        );
    }

    #[test]
    fn noop_commit_on_opened_root_is_stable_and_put_free() {
        let (root, store) = editor_replay(&[Script::Add("a", "1"), Script::Add("b", "2")]);
        let counting = CountingPutStore::new(store);
        let editor: ManifestEditor<_> = ManifestEditor::open(root, counting);
        let (again, counting) = block_on(editor.commit()).unwrap();
        assert_eq!(again, root);
        assert_eq!(counting.puts.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn encrypted_commit_matches_legacy_from_shared_seed() {
        // Seed a persisted encrypted manifest so both sides share one
        // obfuscation key; the legacy continuation is safe (no lookups, so
        // no clean ancestors) and defines the expected root.
        let mut legacy = EncryptedManifest::new_encrypted(Store::new());
        let seed_ref = block_on(legacy.save()).unwrap();
        let enc = |s: &str| EncryptedChunkRef::new(make_addr(s), EncryptionKey::from([0x5a; 32]));
        block_on(legacy.add("secret/a.txt", enc("a"))).unwrap();
        block_on(legacy.add("secret/b.txt", enc("b"))).unwrap();
        block_on(legacy.remove("secret/a.txt")).unwrap();
        let want = block_on(legacy.save()).unwrap();
        let (_, store) = legacy.into_parts();

        let mut editor: ManifestEditor<_, EncryptedChunkRef> =
            ManifestEditor::open_encrypted(seed_ref, store);
        editor.put("secret/a.txt", enc("a"));
        editor.put("secret/b.txt", enc("b"));
        editor.remove("secret/a.txt");
        let (got, _) = block_on(editor.commit()).unwrap();
        assert_eq!(got, want);
    }

    /// A `ChunkPut` wrapper recording total and peak-concurrent puts.
    struct CountingPutStore {
        inner: Store,
        puts: AtomicUsize,
        inflight: AtomicUsize,
        max_inflight: AtomicUsize,
    }

    impl CountingPutStore {
        fn new(inner: Store) -> Self {
            Self {
                inner,
                puts: AtomicUsize::new(0),
                inflight: AtomicUsize::new(0),
                max_inflight: AtomicUsize::new(0),
            }
        }
    }

    /// Yield once so queued sibling puts can ramp their in-flight count
    /// before any single put resolves.
    async fn yield_once() {
        use core::task::Poll;
        let mut yielded = false;
        futures::future::poll_fn(|cx| {
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

    impl ChunkGet<StandardChunkSet> for CountingPutStore {
        type Trust = Verified;
        type Error = <Store as ChunkGet<StandardChunkSet>>::Error;

        async fn get(&self, address: &ChunkAddress) -> Result<Chunk, Self::Error> {
            ChunkGet::get(&self.inner, address).await
        }
    }

    impl ChunkPut<StandardChunkSet> for CountingPutStore {
        type Error = <Store as ChunkPut<StandardChunkSet>>::Error;

        async fn put(&self, chunk: Chunk) -> Result<(), Self::Error> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            let cur = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_inflight.fetch_max(cur, Ordering::SeqCst);
            yield_once().await;
            let r = ChunkPut::put(&self.inner, chunk).await;
            self.inflight.fetch_sub(1, Ordering::SeqCst);
            r
        }
    }

    #[test]
    fn commit_puts_stay_bounded_and_overlap() {
        // Twenty sibling files: one root and twenty leaves to put.
        let mut editor: ManifestEditor<_> =
            ManifestEditor::new(CountingPutStore::new(Store::new()));
        for i in 0..20 {
            let path = format!("file{i:02}.dat");
            editor.put(path.as_str(), make_addr(&path));
        }
        let width = 4;
        let (_, store) = block_on(editor.with_put_width(width).commit()).unwrap();
        assert!(store.puts.load(Ordering::SeqCst) > 1);
        let peak = store.max_inflight.load(Ordering::SeqCst);
        assert!(peak > 1, "commit puts must overlap (peak {peak})");
        assert!(peak <= width, "peak {peak} exceeds put width {width}");
    }

    #[test]
    fn commit_width_one_is_serial() {
        let mut editor: ManifestEditor<_> =
            ManifestEditor::new(CountingPutStore::new(Store::new()));
        for i in 0..8 {
            let path = format!("file{i:02}.dat");
            editor.put(path.as_str(), make_addr(&path));
        }
        let (_, store) = block_on(editor.with_put_width(0).commit()).unwrap();
        assert_eq!(store.max_inflight.load(Ordering::SeqCst), 1);
    }
}

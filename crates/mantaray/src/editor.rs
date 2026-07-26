//! Submission-order manifest editor over the node persistence seam.
//!
//! Ops are recorded synchronously into a `(path, op)` log and applied one at
//! a time at commit, in submission order. The committed root is defined as
//! the root the reference mutation path produces for the same sequence
//! (pinned by the registry-crate differential gate), shape quirks included;
//! ops are never reordered or batched. Nodes persist through a
//! [`NodeSaver`], so the storage layout and any put concurrency are the
//! adapter's.

use alloc::collections::BTreeMap;
use alloc::collections::btree_map;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use nectar_primitives::chunk::{ChunkAddress, ChunkRef, Reference};
use nectar_primitives::store::MaybeSend;
use nectar_primitives::{EncryptedChunkRef, EntryRef};

use crate::error::EditorError;
use crate::node::{Fork, Node, NodeState, Prefix};
use crate::persist::{NodeLoader, NodeSaver};
use crate::{MantarayError, metadata};

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
/// editor.put("index.html", ChunkAddress::from([7u8; 32]));
/// editor.set_index_document("index.html");
/// assert_eq!(editor.ops().len(), 2);
/// ```
#[derive(Debug)]
pub struct ManifestEditor<S, R: Reference = ChunkRef> {
    trie: Node<R>,
    ops: Vec<(Vec<u8>, Op<R>)>,
    store: S,
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
    const fn with_root(trie: Node<R>, store: S) -> Self {
        Self {
            trie,
            ops: Vec::new(),
            store,
        }
    }

    /// The backing loadsaver.
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
    ///
    /// Format limitations, both rejected at commit: an all-zero reference is
    /// the wire's absent-entry sentinel, and metadata on the empty path (the
    /// trie root) has no wire slot.
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

impl<S: NodeLoader, R: Reference + MaybeSend> ManifestEditor<S, R> {
    /// Apply the recorded ops to the trie, one at a time, in submission order.
    async fn apply_ops(&mut self) -> Result<(), EditorError> {
        let ops = core::mem::take(&mut self.ops);
        for (index, (path, op)) in ops.into_iter().enumerate() {
            let result = match op {
                Op::Put {
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
                Op::Remove => self.trie.remove(&path, &self.store).await,
                Op::SetRootMetadata { key, value } => {
                    apply_metadata_merge::<S, R>(&mut self.trie, &path, key, value, &self.store)
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

impl<S: NodeLoader + NodeSaver<ChunkRef>> ManifestEditor<S, ChunkRef> {
    /// Apply the log and persist the trie, returning the root chunk address
    /// and the loadsaver.
    pub async fn commit(mut self) -> Result<(ChunkAddress, S), EditorError> {
        self.apply_ops().await?;
        let committed = commit_trie::<S, ChunkRef>(self.trie, &self.store)
            .await
            .map_err(EditorError::Commit)?;
        let address = *committed
            .reference()
            .ok_or(EditorError::Commit(MantarayError::MissingReference))?
            .address();
        Ok((address, self.store))
    }
}

impl<S: NodeLoader + NodeSaver<EncryptedChunkRef>> ManifestEditor<S, EncryptedChunkRef> {
    /// Apply the log and persist the trie, returning the root's full-width
    /// reference (address plus decryption key) and the loadsaver.
    pub async fn commit(mut self) -> Result<(EncryptedChunkRef, S), EditorError> {
        self.apply_ops().await?;
        let committed = commit_trie::<S, EncryptedChunkRef>(self.trie, &self.store)
            .await
            .map_err(EditorError::Commit)?;
        let reference = committed
            .reference()
            .cloned()
            .ok_or(EditorError::Commit(MantarayError::MissingReference))?;
        Ok((reference, self.store))
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
    R: Reference + MaybeSend,
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
/// A parent embeds each child's saver-issued reference, so every child save
/// is awaited before its parent encodes; put concurrency is the saver's.
async fn commit_trie<S, R>(root: Node<R>, saver: &S) -> Result<Node<R>, MantarayError>
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
        let reference = saver
            .save(data)
            .await
            .map_err(|e| MantarayError::StorePut {
                source: Arc::new(e),
            })?;

        // The persisted node collapses to a stub, reloaded on demand.
        finished.node.state = NodeState::Stub(reference);
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

    committed_root.ok_or(MantarayError::MissingReference)
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
        editor.put("/c", make_addr("c"));
        editor.put("//", make_addr("s"));
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
        editor.put_with_metadata("", make_addr("r"), meta);
        let err = run(editor.commit()).unwrap_err();
        assert!(matches!(
            err,
            EditorError::Commit(MantarayError::RootMetadata)
        ));
    }

    #[test]
    fn zero_reference_put_fails_commit() {
        let mut editor = Editor::new(LoadSaver::new(Store::new()));
        editor.put("a", ChunkAddress::from([0u8; 32]));
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
        editor.put("present", make_addr("p"));
        editor.remove("absent");
        let err = run(editor.commit()).unwrap_err();
        assert!(matches!(
            err,
            EditorError::Apply { index: 1, ref path, .. } if path == b"absent"
        ));
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
        editor.put("index.html", make_addr("i"));
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

    #[test]
    fn noop_commit_on_opened_root_is_stable_and_save_free() {
        let (root, loadsaver) = editor_replay(&[Script::Add("a", "1"), Script::Add("b", "2")]);
        let counting = CountingSaver::new(loadsaver);
        let editor: ManifestEditor<_> = ManifestEditor::open(root, counting);
        let (again, counting) = run(editor.commit()).unwrap();
        assert_eq!(again, root);
        assert_eq!(counting.saves.load(Ordering::SeqCst), 0);
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
        single.put("secret/a.txt", enc("a"));
        single.put("secret/b.txt", enc("b"));
        single.remove("secret/a.txt");
        let (want, store) = run(single.commit()).unwrap();

        // The same ops across a commit boundary land on the same root.
        let mut editor: ManifestEditor<_, EncryptedChunkRef> =
            ManifestEditor::open_encrypted(seed_ref, store);
        editor.put("secret/a.txt", enc("a"));
        editor.put("secret/b.txt", enc("b"));
        let (mid, store) = run(editor.commit()).unwrap();
        let mut editor: ManifestEditor<_, EncryptedChunkRef> =
            ManifestEditor::open_encrypted(mid, store);
        editor.remove("secret/a.txt");
        let (got, _) = run(editor.commit()).unwrap();
        assert_eq!(got, want);
    }

    /// A loadsaver wrapper counting save calls.
    struct CountingSaver {
        inner: LoadSaver,
        saves: AtomicUsize,
    }

    impl CountingSaver {
        fn new(inner: LoadSaver) -> Self {
            Self {
                inner,
                saves: AtomicUsize::new(0),
            }
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

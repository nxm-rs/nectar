//! The [`Manifest`] seam over the trie: paths to references, batched writes,
//! and data loads through the file pipeline under the adapter's own policy.
//!
//! Two seams meet here and stay separate: nodes persist through the trie's own
//! [`NodeLoader`]/[`NodeSaver`] adapter, while an entry's data is joined
//! straight from a chunk store. A manifest whose nodes live behind one layout
//! and whose data lives behind another is therefore expressible, and the
//! common case passes the same store twice.
//!
//! [`TrieView`] reads one root through the depth-guarded reader; the walks are
//! the raw [`TrieCursor`] under the seam's [`PathCursor`]. [`Manifest::apply`]
//! replays the checked batch through [`ManifestEditor`].

use alloc::collections::BTreeMap;
use alloc::string::String;
use core::future::Future;
use core::ops::Bound;
use core::pin::Pin;
use core::task::{Context, Poll};

use futures_util::Stream;
use nectar_file::{Policy, load_reference};
use nectar_manifest::{
    Batch, Listing, Manifest, ManifestError, ManifestOp, ManifestPath, ManifestView, MapEntry,
    NodeLoader, NodeSaver, PathCursor, RawCursor, RawItem, SiteConfig, collapse_dir,
};
use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::{ContentOnlyChunkSet, Reference};
use nectar_primitives::store::{ContentGet, MaybeSend, MaybeSync, TrustedGet, WriteAt};

use crate::cursor::TrieListing;
use crate::editor::ManifestEditor;
use crate::error::{CursorError, EditorError, ReaderError};
use crate::persist::NodeLoadSaver;
use crate::reader::Reader;
use crate::{constants::metadata, entry::Entry};

/// The trie's own failures behind [`ManifestError::Format`].
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum TrieFormatError {
    /// A path lookup failed.
    #[error(transparent)]
    Read(#[from] ReaderError),
    /// A listing walk failed.
    #[error(transparent)]
    List(#[from] CursorError),
    /// Applying the batch failed.
    #[error(transparent)]
    Edit(#[from] EditorError),
}

nectar_manifest::format_error_from!(TrieFormatError: ReaderError, CursorError, EditorError);

/// The trie as a [`Manifest`]: a node adapter for the trie itself and a chunk
/// store for entry data.
///
/// Cheap to clone when both seams are: a handle clones them once per call.
#[derive(Clone, Copy, Debug)]
pub struct MantarayManifest<L, S, const B: usize = DEFAULT_BODY_SIZE> {
    nodes: L,
    data: S,
    policy: Policy,
}

impl<L, S, const B: usize> MantarayManifest<L, S, B> {
    /// A manifest whose nodes persist through `nodes` and whose entry data is
    /// joined from `data`, under [`Policy::DEFAULT`].
    pub const fn new(nodes: L, data: S) -> Self {
        Self {
            nodes,
            data,
            policy: Policy::DEFAULT,
        }
    }

    /// The same manifest with entry-data loads running under `policy`.
    #[must_use]
    pub const fn with_policy(mut self, policy: Policy) -> Self {
        self.policy = policy;
        self
    }

    /// The node persistence adapter.
    pub const fn nodes(&self) -> &L {
        &self.nodes
    }

    /// The entry-data store.
    pub const fn data(&self) -> &S {
        &self.data
    }

    /// The retrieval budgets entry-data loads run under.
    #[must_use]
    pub const fn policy(&self) -> Policy {
        self.policy
    }
}

impl<S, const B: usize> MantarayManifest<NodeLoadSaver<S, B>, ContentGet<S>, B>
where
    S: Clone,
{
    /// Both seams over one chunk store: nodes persist through the file
    /// pipeline and entry data is joined from the same store.
    ///
    /// The store is cloned once, so pass a handle whose clones share state
    /// (an `Arc`, say). A store that clones its contents leaves the two
    /// seams over separate copies, and every data load then misses.
    pub fn over(store: S) -> Self {
        Self::new(NodeLoadSaver::new(store.clone()), ContentGet::new(store))
    }
}

impl<L, S, R, const B: usize> Manifest<R> for MantarayManifest<L, S, B>
where
    L: NodeLoader<Vec<u8>> + NodeSaver<[u8], R> + Clone + Unpin + 'static,
    S: TrustedGet<ContentOnlyChunkSet<B>> + Clone + MaybeSend + MaybeSync + 'static,
    R: Reference + Unpin,
{
    /// The trie's metadata: a string map, stored verbatim on the fork record.
    type Metadata = BTreeMap<String, String>;

    type FormatError = TrieFormatError;

    type View<'a>
        = TrieView<L, S, R, B>
    where
        Self: 'a;

    /// The empty trie persisted at width `R` with a zero obfuscation key.
    fn empty(&self) -> impl Future<Output = Result<R, ManifestError<TrieFormatError>>> + MaybeSend {
        let editor = ManifestEditor::empty_reference(self.nodes.clone());
        async move {
            let (root, _) = editor.commit_reference().await?;
            Ok(root)
        }
    }

    fn at(&self, root: R) -> Self::View<'_> {
        TrieView {
            nodes: self.nodes.clone(),
            data: self.data.clone(),
            policy: self.policy,
            root,
        }
    }

    /// The checked batch replayed through the submission-order editor: ops in
    /// order, then the site-document delta, one commit.
    fn apply(
        &self,
        base: R,
        batch: Batch<R, Self::Metadata>,
    ) -> impl Future<Output = Result<R, ManifestError<TrieFormatError>>> + MaybeSend {
        let nodes = self.nodes.clone();
        async move {
            let checked = batch.into_checked().map_err(ManifestError::Reserved)?;
            let mut editor = ManifestEditor::open_reference(base, nodes);
            for op in checked.ops {
                match op {
                    ManifestOp::Insert {
                        path,
                        reference,
                        meta,
                    } => editor.insert_with(path.into_bytes(), reference, meta),
                    ManifestOp::Remove { path } => editor.remove(path.into_bytes()),
                };
            }
            // The site documents are a merge: untouched records nothing, and
            // `Some(None)` clears the key. The trie stores metadata values as
            // text, so invalid UTF-8 in a path is replaced.
            for (key, delta) in [
                (metadata::WEBSITE_INDEX_DOCUMENT, checked.index_document),
                (metadata::WEBSITE_ERROR_DOCUMENT, checked.error_document),
            ] {
                match delta {
                    Some(Some(path)) => {
                        let value = String::from_utf8_lossy(path.as_bytes()).into_owned();
                        editor.set_root_metadata(key, value);
                    }
                    Some(None) => {
                        editor.clear_root_metadata(key);
                    }
                    None => {}
                }
            }
            let (root, _) = editor.commit_reference().await?;
            Ok(root)
        }
    }
}

/// The seam's read view over one trie root. Owns its two store handles rather
/// than borrowing the manifest.
#[derive(Clone, Copy, Debug)]
pub struct TrieView<L, S, R: Reference, const B: usize = DEFAULT_BODY_SIZE> {
    nodes: L,
    data: S,
    policy: Policy,
    root: R,
}

impl<L, S, R, const B: usize> TrieView<L, S, R, B>
where
    L: NodeLoader<Vec<u8>> + Clone,
    R: Reference + Unpin,
{
    /// The entry at `path`, which is the trie key verbatim. The structural
    /// root and the site-config node read as absent.
    async fn entry(
        &self,
        path: &ManifestPath,
    ) -> Result<Option<Entry>, ManifestError<TrieFormatError>> {
        let Some(key) = path.content_key() else {
            return Ok(None);
        };
        let reader = Reader::new(self.nodes.clone());
        Ok(reader.get(self.root.clone().into_entry_ref(), key).await?)
    }

    /// The trie's site-config node, which the reference client keys at `"/"`.
    async fn root_node(&self) -> Result<Option<Entry>, ManifestError<TrieFormatError>> {
        let reader = Reader::new(self.nodes.clone());
        Ok(reader
            .get(
                self.root.clone().into_entry_ref(),
                metadata::ROOT_PATH.as_bytes(),
            )
            .await?)
    }

    /// The raw ordered walk of the keys under `prefix`, which the trie prunes
    /// to. An empty prefix walks the whole trie.
    fn walk(&self, prefix: &[u8]) -> TrieCursor<L, R> {
        TrieCursor {
            cursor: TrieListing::new(self.nodes.clone(), self.root.clone().into_entry_ref())
                .with_prefix(prefix),
            _reference: core::marker::PhantomData,
        }
    }
}

impl<L, S, R, const B: usize> ManifestView<R> for TrieView<L, S, R, B>
where
    L: NodeLoader<Vec<u8>> + Clone + Unpin + 'static,
    S: TrustedGet<ContentOnlyChunkSet<B>> + Clone + MaybeSend + MaybeSync + 'static,
    R: Reference + Unpin,
{
    type Metadata = BTreeMap<String, String>;

    type Error = ManifestError<TrieFormatError>;

    type Cursor = PathCursor<TrieCursor<L, R>, R>;

    fn get(
        &self,
        path: &ManifestPath,
    ) -> impl Future<Output = Result<Option<MapEntry<R>>, Self::Error>> + MaybeSend {
        let path = path.clone();
        async move { Ok(self.entry(&path).await?.map(|entry| seam_entry(&entry))) }
    }

    async fn site_config(&self) -> Result<SiteConfig, Self::Error> {
        let Some(entry) = self.root_node().await? else {
            return Ok(SiteConfig::new());
        };
        let document = |key: &str| {
            entry
                .metadata()
                .get(key)
                .map(|value| ManifestPath::from(value.as_str()))
        };
        Ok(SiteConfig::new()
            .with_index_document(document(metadata::WEBSITE_INDEX_DOCUMENT))
            .with_error_document(document(metadata::WEBSITE_ERROR_DOCUMENT)))
    }

    fn metadata(
        &self,
        path: &ManifestPath,
    ) -> impl Future<Output = Result<Self::Metadata, Self::Error>> + MaybeSend {
        let path = path.clone();
        async move {
            Ok(self
                .entry(&path)
                .await?
                .map(|entry| entry.metadata().clone())
                .unwrap_or_default())
        }
    }

    /// The trie prunes to the listed prefix, so a level costs its own subtree
    /// rather than every key before it.
    fn dir(
        &self,
        dir: &ManifestPath,
    ) -> impl Future<Output = Result<Listing<R>, Self::Error>> + MaybeSend {
        collapse_dir(dir.clone(), PathCursor::new(self.walk(dir.as_bytes())))
    }

    fn load<K: WriteAt + ?Sized>(
        &self,
        path: &ManifestPath,
        sink: &mut K,
    ) -> impl Future<Output = Result<u64, Self::Error>> + MaybeSend {
        let path = path.clone();
        let store = self.data.clone();
        let policy = self.policy;
        async move {
            let entry = self
                .entry(&path)
                .await?
                .ok_or_else(|| ManifestError::NotFound(path.clone()))?;
            // Load success tracks `get`: only a reference of the caller's
            // width names data here.
            let reference = entry
                .reference()
                .cloned()
                .and_then(|reference| R::from_entry_ref(reference).ok())
                .ok_or(ManifestError::NoData(path))?;
            load_reference::<_, _, _, B>(store, policy, reference.into_entry_ref(), sink).await
        }
    }

    fn iter(&self) -> impl Future<Output = Result<Self::Cursor, Self::Error>> + MaybeSend {
        let cursor = PathCursor::new(self.walk(&[]));
        async move { Ok(cursor) }
    }

    fn range(
        &self,
        bounds: (Bound<ManifestPath>, Bound<ManifestPath>),
    ) -> impl Future<Output = Result<Self::Cursor, Self::Error>> + MaybeSend {
        let cursor = PathCursor::bounded(self.walk(&[]), bounds);
        async move { Ok(cursor) }
    }
}

/// The trie's raw ordered walk: every stored key under the walk's prefix,
/// the site-config node included. The trie has no ordered seek, so a bounded
/// walk is the seam's filter over a full one.
#[derive(Debug)]
pub struct TrieCursor<L, R: Reference> {
    cursor: TrieListing<L>,
    _reference: core::marker::PhantomData<R>,
}

impl<L, R> RawCursor<R> for TrieCursor<L, R>
where
    L: NodeLoader<Vec<u8>> + Clone + MaybeSend + Unpin + 'static,
    R: Reference + Unpin,
{
    type Error = ManifestError<TrieFormatError>;
}

impl<L, R> Stream for TrieCursor<L, R>
where
    L: NodeLoader<Vec<u8>> + Clone + MaybeSend + Unpin + 'static,
    R: Reference + Unpin,
{
    type Item = Result<RawItem<R>, ManifestError<TrieFormatError>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.cursor).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok(entry))) => {
                Poll::Ready(Some(Ok((entry.path().to_vec(), seam_entry(&entry)))))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error.into()))),
        }
    }
}

/// One trie entry at the caller's width: a metadata-only node, or a reference
/// of the other width, is opaque.
fn seam_entry<R: Reference>(entry: &Entry) -> MapEntry<R> {
    entry
        .reference()
        .cloned()
        .map_or(MapEntry::Opaque, MapEntry::from_entry_ref)
}

/// The seam reserves the lone separator, where the trie keys its site-config
/// node, so the shared reserved skip hides it from every walk.
const _: () = assert!(matches!(
    metadata::ROOT_PATH.as_bytes(),
    [ManifestPath::SEPARATOR]
));

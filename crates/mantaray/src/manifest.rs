//! The [`Manifest`] seam over the trie: paths to references, batched writes,
//! and data loads through the file pipeline.
//!
//! Two seams meet here and stay separate: nodes persist through the trie's own
//! [`NodeLoader`]/[`NodeSaver`] adapter, while an entry's data is joined
//! straight from a chunk store. A manifest whose nodes live behind one layout
//! and whose data lives behind another is therefore expressible, and the
//! common case passes the same store twice.
//!
//! The handles are the trie's own: [`TrieView`] reads one root through the
//! depth-guarded reader and the ordered cursor, and [`TrieWriter`] records the
//! batch through the submission-order [`ManifestEditor`].

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;
use core::future::Future;
use core::ops::{Bound, RangeBounds};

use nectar_file::{File, Policy};
use nectar_manifest::{
    DataSink, ListEntry, Listing, Manifest, ManifestMetadata, ManifestOp, ManifestPath, MapCursor,
    MapEntry, MapView, MapWriter, SinkError, SiteConfig, WellKnownKey,
};
use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::{ContentOnlyChunkSet, Reference};
use nectar_primitives::store::{BoxedError, MaybeSend, MaybeSync, TrustedGet};

use crate::cursor::Cursor;
use crate::editor::ManifestEditor;
use crate::error::{CursorError, EditorError, ReaderError};
use crate::persist::{NodeLoader, NodeSaver};
use crate::reader::Reader;
use crate::{constants::metadata, entry::Entry};

/// A failure crossing the manifest seam.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    /// A path lookup failed.
    #[error(transparent)]
    Read(#[from] ReaderError),
    /// A listing walk failed.
    #[error(transparent)]
    List(#[from] CursorError),
    /// Applying the batch failed.
    #[error(transparent)]
    Edit(#[from] EditorError),
    /// No entry is bound at the requested path.
    #[error("no entry at {path:?}")]
    NotFound {
        /// The path that resolved to nothing.
        path: ManifestPath,
    },
    /// The entry at the path carries metadata but no reference, so it names
    /// no data.
    #[error("the entry at {path:?} carries no reference")]
    NoReference {
        /// The path whose entry names no data.
        path: ManifestPath,
    },
    /// Reading the entry's data through the file pipeline failed.
    #[error("load entry data")]
    Data(#[source] BoxedError),
}

impl ManifestError {
    /// Box a data-side failure behind the seam.
    fn data<E: core::error::Error + MaybeSend + MaybeSync + 'static>(error: E) -> Self {
        Self::Data(Box::new(error))
    }
}

/// The trie as a [`Manifest`]: a node adapter for the trie itself and a chunk
/// store for entry data.
///
/// Cheap to clone when both seams are; a handle clones them once per call,
/// because the reader, the cursor and the editor each own their store.
#[derive(Clone, Copy, Debug)]
pub struct MantarayManifest<L, S, const B: usize = DEFAULT_BODY_SIZE> {
    nodes: L,
    data: S,
}

impl<L, S, const B: usize> MantarayManifest<L, S, B> {
    /// A manifest whose nodes persist through `nodes` and whose entry data is
    /// joined from `data`.
    pub const fn new(nodes: L, data: S) -> Self {
        Self { nodes, data }
    }

    /// The node persistence adapter.
    pub const fn nodes(&self) -> &L {
        &self.nodes
    }

    /// The entry-data store.
    pub const fn data(&self) -> &S {
        &self.data
    }
}

impl<L, S, R, const B: usize> Manifest<R> for MantarayManifest<L, S, B>
where
    L: NodeLoader + NodeSaver<R> + Clone + 'static,
    S: TrustedGet<ContentOnlyChunkSet<B>> + Clone + MaybeSend + MaybeSync + 'static,
    R: Reference + MaybeSend + MaybeSync,
{
    /// The trie's metadata: a string map, stored verbatim on the fork record.
    type Metadata = BTreeMap<String, String>;

    type Error = ManifestError;

    type View<'a>
        = TrieView<L, S, R, B>
    where
        Self: 'a;

    type Writer<'a>
        = TrieWriter<L, R>
    where
        Self: 'a;

    fn at(&self, root: &R) -> Self::View<'_> {
        TrieView {
            nodes: self.nodes.clone(),
            data: self.data.clone(),
            root: root.clone(),
        }
    }

    fn edit(&self, base: &R) -> Self::Writer<'_> {
        TrieWriter {
            editor: ManifestEditor::open_reference(base.clone(), self.nodes.clone()),
        }
    }

    fn metadata_from_view(
        &self,
        view: &dyn ManifestMetadata,
    ) -> Result<Self::Metadata, Self::Error> {
        let mut map = BTreeMap::new();
        for (key, name) in [
            (WellKnownKey::ContentType, metadata::CONTENT_TYPE),
            (
                WellKnownKey::IndexDocument,
                metadata::WEBSITE_INDEX_DOCUMENT,
            ),
            (
                WellKnownKey::ErrorDocument,
                metadata::WEBSITE_ERROR_DOCUMENT,
            ),
        ] {
            if let Some(value) = view.get(&key) {
                map.insert(String::from(name), String::from(value));
            }
        }
        Ok(map)
    }
}

/// The seam's read view over one trie root.
///
/// Owns its two store handles rather than borrowing the manifest, because the
/// reader and the cursor own theirs; a view is therefore as cheap as the two
/// clones are.
#[derive(Clone, Copy, Debug)]
pub struct TrieView<L, S, R: Reference, const B: usize = DEFAULT_BODY_SIZE> {
    nodes: L,
    data: S,
    root: R,
}

impl<L, S, R, const B: usize> TrieView<L, S, R, B>
where
    L: NodeLoader + Clone,
    R: Reference,
{
    /// The entry at `path`, which is the trie key verbatim.
    ///
    /// A path that names no content key is absent rather than mapped: the trie's
    /// structural root and its site-config node are not entries in the map.
    async fn entry(&self, path: &ManifestPath) -> Result<Option<Entry>, ManifestError> {
        let Some(key) = content_key(path) else {
            return Ok(None);
        };
        let reader = Reader::new(self.nodes.clone());
        Ok(reader.get(self.root.clone().into_entry_ref(), key).await?)
    }

    /// The trie's site-config node, which the reference client keys at `"/"`.
    async fn root_node(&self) -> Result<Option<Entry>, ManifestError> {
        let reader = Reader::new(self.nodes.clone());
        Ok(reader
            .get(
                self.root.clone().into_entry_ref(),
                metadata::ROOT_PATH.as_bytes(),
            )
            .await?)
    }

    /// An ordered walk of the whole trie, bounded to `bounds`.
    fn walk(&self, bounds: (Bound<Vec<u8>>, Bound<Vec<u8>>)) -> TrieCursor<L, R> {
        TrieCursor {
            cursor: Cursor::new(self.nodes.clone(), self.root.clone().into_entry_ref()),
            bounds,
            _reference: core::marker::PhantomData,
        }
    }
}

impl<L, S, R, const B: usize> MapView<R> for TrieView<L, S, R, B>
where
    L: NodeLoader + Clone + 'static,
    S: TrustedGet<ContentOnlyChunkSet<B>> + Clone + MaybeSend + MaybeSync + 'static,
    R: Reference + MaybeSend + MaybeSync,
{
    type Metadata = BTreeMap<String, String>;

    type Error = ManifestError;

    type Cursor = TrieCursor<L, R>;

    fn get(
        &self,
        path: &ManifestPath,
    ) -> impl Future<Output = Result<Option<MapEntry<R>>, Self::Error>> + MaybeSend {
        let path = path.clone();
        async move { Ok(self.entry(&path).await?.map(|entry| mapped(&entry))) }
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

    fn dir(
        &self,
        dir: &ManifestPath,
    ) -> impl Future<Output = Result<Listing<R>, Self::Error>> + MaybeSend {
        let mut cursor = Cursor::new(self.nodes.clone(), self.root.clone().into_entry_ref())
            .with_prefix(dir.as_bytes());
        let prefix = dir.as_bytes().to_vec();
        async move {
            let mut entries = Vec::new();
            let mut last_dir: Option<Vec<u8>> = None;
            while let Some(entry) = cursor.next().await.transpose()? {
                let Some(listed) = collapse(&prefix, &entry, &mut last_dir) else {
                    continue;
                };
                entries.push(listed);
            }
            Ok(Listing::new(entries))
        }
    }

    fn load<K: DataSink<Error: SinkError> + MaybeSend>(
        &self,
        path: &ManifestPath,
        sink: &mut K,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend {
        let path = path.clone();
        let store = self.data.clone();
        async move {
            let entry = self
                .entry(&path)
                .await?
                .ok_or_else(|| ManifestError::NotFound { path: path.clone() })?;
            let reference = entry
                .reference()
                .cloned()
                .ok_or(ManifestError::NoReference { path })?;
            File::<S, B>::new(store, Policy::DEFAULT)
                .load(reference, sink)
                .await
                .map_err(ManifestError::data)?;
            Ok(())
        }
    }

    fn iter(&self) -> impl Future<Output = Result<Self::Cursor, Self::Error>> + MaybeSend {
        let cursor = self.walk((Bound::Unbounded, Bound::Unbounded));
        async move { Ok(cursor) }
    }

    fn range(
        &self,
        bounds: impl RangeBounds<ManifestPath> + MaybeSend,
    ) -> impl Future<Output = Result<Self::Cursor, Self::Error>> + MaybeSend {
        let cursor = self.walk((owned(bounds.start_bound()), owned(bounds.end_bound())));
        async move { Ok(cursor) }
    }
}

/// The seam's ordered walk over a trie.
///
/// The trie has no ordered seek, so a bounded walk filters an ordered full
/// walk: the paths arrive in order, so the bounds are exact, and the cost of a
/// lower bound is the nodes before it.
#[derive(Debug)]
pub struct TrieCursor<L, R: Reference> {
    cursor: Cursor<L>,
    bounds: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    _reference: core::marker::PhantomData<R>,
}

impl<L, R> MapCursor<R> for TrieCursor<L, R>
where
    L: NodeLoader + Clone + MaybeSend + 'static,
    R: Reference + MaybeSend + MaybeSync,
{
    type Error = ManifestError;

    fn next(
        &mut self,
    ) -> impl Future<Output = Result<Option<(ManifestPath, MapEntry<R>)>, Self::Error>> + MaybeSend
    {
        let (start, end) = (&self.bounds.0, &self.bounds.1);
        let cursor = &mut self.cursor;
        async move {
            while let Some(entry) = cursor.next().await.transpose()? {
                let path = entry.path();
                if past_end(end, path) {
                    return Ok(None);
                }
                if before_start(start, path) {
                    continue;
                }
                // The site-config node is not a content key, so a walk of the
                // map steps over it.
                if is_site_config(path) {
                    continue;
                }
                return Ok(Some((ManifestPath::new(path.to_vec()), mapped(&entry))));
            }
            Ok(None)
        }
    }
}

/// The seam's write handle over one base root.
#[derive(Debug)]
pub struct TrieWriter<L, R: Reference> {
    editor: ManifestEditor<L, R>,
}

impl<L, R: Reference> TrieWriter<L, R> {
    /// Record one site document on the trie's site-config node, or clear it.
    ///
    /// A merge either way, so the two documents are independent: setting one
    /// leaves the other exactly as it was, and clearing the last one prunes the
    /// node, which is how the site config leaves no trace on the wire.
    ///
    /// The trie stores metadata values as text, so a path that is not valid
    /// UTF-8 cannot be a site document; its invalid bytes are replaced rather
    /// than failing a staging call that cannot report an error.
    fn document(&mut self, key: &str, path: Option<ManifestPath>) -> &mut Self {
        match path {
            Some(path) => {
                let value = String::from_utf8_lossy(path.as_bytes()).into_owned();
                self.editor.set_root_metadata(key, value);
            }
            None => {
                self.editor.clear_root_metadata(key);
            }
        }
        self
    }
}

impl<L, R> MapWriter<R> for TrieWriter<L, R>
where
    L: NodeLoader + NodeSaver<R> + MaybeSend,
    R: Reference + MaybeSend + MaybeSync,
{
    type Metadata = BTreeMap<String, String>;

    type Error = ManifestError;

    /// An insert replaces the whole binding; existing metadata is cleared
    /// unless `meta` carries some, because the op's metadata is the path's
    /// metadata from then on.
    fn stage(&mut self, op: ManifestOp<R, Self::Metadata>) {
        match op {
            ManifestOp::Insert {
                path,
                reference,
                meta,
            } => {
                if let Some(key) = content_key(&path) {
                    self.editor.insert(key, reference).meta(meta);
                }
            }
            ManifestOp::Remove { path } => {
                if let Some(key) = content_key(&path) {
                    self.editor.remove(key);
                }
            }
        }
    }

    fn with_index_document(&mut self, path: impl Into<Option<ManifestPath>>) -> &mut Self {
        self.document(metadata::WEBSITE_INDEX_DOCUMENT, path.into())
    }

    fn with_error_document(&mut self, path: impl Into<Option<ManifestPath>>) -> &mut Self {
        self.document(metadata::WEBSITE_ERROR_DOCUMENT, path.into())
    }

    fn commit(self) -> impl Future<Output = Result<R, Self::Error>> + MaybeSend {
        let editor = self.editor;
        async move {
            let (root, _) = editor.commit_reference().await?;
            Ok(root)
        }
    }
}

/// One path bound, owned for the walk that filters on it.
fn owned(edge: Bound<&ManifestPath>) -> Bound<Vec<u8>> {
    match edge {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(path) => Bound::Included(path.as_bytes().to_vec()),
        Bound::Excluded(path) => Bound::Excluded(path.as_bytes().to_vec()),
    }
}

/// Whether `path` falls short of the walk's lower bound.
fn before_start(start: &Bound<Vec<u8>>, path: &[u8]) -> bool {
    match start {
        Bound::Unbounded => false,
        Bound::Included(bound) => path < bound.as_slice(),
        Bound::Excluded(bound) => path <= bound.as_slice(),
    }
}

/// Whether `path` reaches the walk's upper bound, which ends it.
fn past_end(end: &Bound<Vec<u8>>, path: &[u8]) -> bool {
    match end {
        Bound::Unbounded => false,
        Bound::Included(bound) => path > bound.as_slice(),
        Bound::Excluded(bound) => path >= bound.as_slice(),
    }
}

/// One trie entry as a seam entry: a reference of the caller's width, or an
/// opaque value.
///
/// A metadata-only node, or a reference of the other width, is bound but names
/// no reference the caller can read on its own.
fn mapped<R: Reference>(entry: &Entry) -> MapEntry<R> {
    match entry.reference().cloned().map(R::from_entry_ref) {
        Some(Ok(reference)) => MapEntry::Reference(reference),
        Some(Err(_)) | None => MapEntry::Opaque,
    }
}

/// The trie key `path` addresses, or `None` when it names no content key.
///
/// A content key is the path bytes verbatim, which is what keeps the image
/// byte-identical to the reference client's. Two byte strings are not content
/// keys: the empty one, which is the trie's structural root, and
/// [`metadata::ROOT_PATH`], which is the site-config node the site documents
/// live on. Neither is read, written or walked as a key.
fn content_key(path: &ManifestPath) -> Option<&[u8]> {
    let bytes = path.as_bytes();
    if bytes.is_empty() || is_site_config(bytes) {
        return None;
    }
    Some(bytes)
}

/// Whether `key` is the trie's site-config node rather than content.
fn is_site_config(key: &[u8]) -> bool {
    key == metadata::ROOT_PATH.as_bytes()
}

/// Fold one listed entry into the directory level below `prefix`, collapsing
/// deeper paths at the next separator.
///
/// `last_dir` carries the previously collapsed subdirectory: the walk is in
/// path order, so every path under one subdirectory arrives consecutively and
/// a single comparison deduplicates it.
fn collapse<R: Reference>(
    prefix: &[u8],
    entry: &Entry,
    last_dir: &mut Option<Vec<u8>>,
) -> Option<ListEntry<R>> {
    let path = entry.path();
    let suffix = path.strip_prefix(prefix)?;
    // The directory itself is not one of its own children, and the trie's
    // site-config node is not content, so neither is listed.
    if suffix.is_empty() || is_site_config(path) {
        return None;
    }
    let Some(cut) = suffix
        .iter()
        .position(|&byte| byte == ManifestPath::SEPARATOR)
    else {
        let path = ManifestPath::new(path.to_vec());
        return Some(match mapped::<R>(entry) {
            // A width the caller did not ask for still names a path; it is
            // listed as an opaque value rather than failing the listing.
            MapEntry::Reference(reference) => ListEntry::File { path, reference },
            MapEntry::Opaque => ListEntry::Value { path },
        });
    };
    let through = prefix.len().saturating_add(cut).saturating_add(1);
    let dir = path.get(..through).unwrap_or(path).to_vec();
    if last_dir.as_deref() == Some(dir.as_slice()) {
        return None;
    }
    *last_dir = Some(dir.clone());
    Some(ListEntry::Dir {
        path: ManifestPath::new(dir),
    })
}

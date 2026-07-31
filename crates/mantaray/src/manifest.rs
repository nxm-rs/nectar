//! The [`Manifest`] seam over the trie: paths to references, batched writes,
//! and data loads through the file pipeline.
//!
//! Two seams meet here and stay separate: nodes persist through the trie's own
//! [`NodeLoader`]/[`NodeSaver`] adapter, while an entry's data is joined
//! straight from a chunk store. A manifest whose nodes live behind one layout
//! and whose data lives behind another is therefore expressible, and the
//! common case passes the same store twice.
//!
//! [`TrieView`] reads one root through the depth-guarded reader and the ordered
//! cursor; [`TrieWriter`] records the batch through [`ManifestEditor`].

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;
use core::future::Future;
use core::ops::{Bound, RangeBounds};

use nectar_file::{File, LoadError, Policy};
use nectar_manifest::{
    DataSink, ListEntry, Listing, Manifest, ManifestError, ManifestMetadata, ManifestOp,
    ManifestPath, MapCursor, MapEntry, MapView, MapWriter, ReservedKey, SinkError, SiteConfig,
    WellKnownKey,
};
use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::{ContentOnlyChunkSet, Reference};
use nectar_primitives::store::{MaybeSend, MaybeSync, TrustedGet};

use crate::cursor::Cursor;
use crate::editor::ManifestEditor;
use crate::error::{CursorError, EditorError, ReaderError};
use crate::persist::{NodeLoader, NodeSaver};
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

    type FormatError = TrieFormatError;

    type View<'a>
        = TrieView<L, S, R, B>
    where
        Self: 'a;

    type Writer<'a>
        = TrieWriter<L, R>
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
            reserved: None,
        }
    }

    fn metadata_from_view(
        &self,
        view: &dyn ManifestMetadata,
    ) -> Result<Self::Metadata, ManifestError<TrieFormatError>> {
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

/// The seam's read view over one trie root. Owns its two store handles rather
/// than borrowing the manifest.
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
    /// The entry at `path`, which is the trie key verbatim. The structural
    /// root and the site-config node read as absent.
    async fn entry(
        &self,
        path: &ManifestPath,
    ) -> Result<Option<Entry>, ManifestError<TrieFormatError>> {
        let Some(key) = content_key(path) else {
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

    type Error = ManifestError<TrieFormatError>;

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
                .ok_or_else(|| ManifestError::NotFound(path.clone()))?;
            let reference = entry
                .reference()
                .cloned()
                .ok_or(ManifestError::NoData(path))?;
            File::<S, B>::new(store, Policy::DEFAULT)
                .load(reference, sink)
                .await
                .map_err(|error| match error {
                    LoadError::Sink { source, .. } => ManifestError::sink(source),
                    data => ManifestError::data(data),
                })?;
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
/// walk: the cost of a lower bound is the nodes before it.
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
    type Error = ManifestError<TrieFormatError>;

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
                // The site-config node is not a content key.
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
    /// The first reserved path the batch staged. Staging is infallible, so the
    /// refusal is held until the commit.
    reserved: Option<ReservedKey>,
}

impl<L, R: Reference> TrieWriter<L, R> {
    /// Record one site document on the trie's site-config node, or clear it.
    ///
    /// A merge, so the two documents are independent. Clearing the last one
    /// prunes the node. The trie stores metadata values as text, so invalid
    /// UTF-8 in a path is replaced.
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

    type Error = ManifestError<TrieFormatError>;

    /// An insert replaces the whole binding, clearing existing metadata unless
    /// `meta` carries some. A reserved path stages no trie op and refuses the
    /// commit instead.
    fn stage(&mut self, op: ManifestOp<R, Self::Metadata>) {
        if content_key(op.path()).is_none() {
            self.reserved
                .get_or_insert_with(|| ReservedKey::new(op.path().clone()));
            return;
        }
        match op {
            ManifestOp::Insert {
                path,
                reference,
                meta,
            } => {
                self.editor.insert(path.into_bytes(), reference).meta(meta);
            }
            ManifestOp::Remove { path } => {
                self.editor.remove(path.into_bytes());
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
        let Self { editor, reserved } = self;
        async move {
            if let Some(reserved) = reserved {
                return Err(ManifestError::Reserved(reserved));
            }
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
/// A metadata-only node, or a reference of the other width, names no reference
/// the caller can read on its own.
fn mapped<R: Reference>(entry: &Entry) -> MapEntry<R> {
    match entry.reference().cloned().map(R::from_entry_ref) {
        Some(Ok(reference)) => MapEntry::Reference(reference),
        Some(Err(_)) | None => MapEntry::Opaque,
    }
}

/// The trie key `path` addresses, or `None` when it names no content key.
///
/// A content key is the path bytes verbatim. The empty path and
/// [`metadata::ROOT_PATH`] are reserved: neither is read, written or walked as
/// a key.
fn content_key(path: &ManifestPath) -> Option<&[u8]> {
    (!path.is_reserved()).then(|| path.as_bytes())
}

/// Whether `key` is the trie's site-config node rather than content.
fn is_site_config(key: &[u8]) -> bool {
    key == metadata::ROOT_PATH.as_bytes()
}

/// The seam reserves the lone separator, where the trie keys its site-config
/// node.
const _: () = assert!(matches!(
    metadata::ROOT_PATH.as_bytes(),
    [ManifestPath::SEPARATOR]
));

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
            // A width the caller did not ask for lists as an opaque value.
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

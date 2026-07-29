//! The [`Manifest`] seam over the trie: paths to references, batched writes,
//! and data loads through the file pipeline.
//!
//! Two seams meet here and stay separate: nodes persist through the trie's own
//! [`NodeLoader`]/[`NodeSaver`] adapter, while an entry's data is joined
//! straight from a chunk store. A manifest whose nodes live behind one layout
//! and whose data lives behind another is therefore expressible, and the
//! common case passes the same store twice.

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;
use core::future::Future;

use nectar_file::{File, Policy};
use nectar_manifest::{
    DataSink, ListEntry, Listing, Manifest, ManifestMetadata, ManifestOp, ManifestPath, SinkError,
    WellKnownKey,
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
/// Cheap to clone when both seams are; a listing or a batch clones them once
/// per call, because both the cursor and the editor own their store.
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

    fn list(
        &self,
        root: &R,
        dir: &ManifestPath,
    ) -> impl Future<Output = Result<Listing<R>, Self::Error>> + MaybeSend {
        let mut cursor = Cursor::new(self.nodes.clone(), root.clone().into_entry_ref())
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
        root: &R,
        path: &ManifestPath,
        sink: &mut K,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend {
        let reader = Reader::new(self.nodes.clone());
        let root = root.clone().into_entry_ref();
        let store = self.data.clone();
        let path = path.clone();
        let key = edit_path(&path);
        async move {
            let entry = reader
                .get(root, &key)
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

    fn apply(
        &self,
        base: &R,
        ops: impl IntoIterator<Item = ManifestOp<R, Self::Metadata>> + MaybeSend,
    ) -> impl Future<Output = Result<R, Self::Error>> + MaybeSend {
        // Recorded before the first await: the editor's op log is the batch,
        // and the source iterator never crosses an await point.
        let mut editor = ManifestEditor::open_reference(base.clone(), self.nodes.clone());
        for op in ops {
            match op {
                ManifestOp::Put {
                    path,
                    reference,
                    meta,
                } => {
                    editor.put_with_metadata(edit_path(&path), reference, meta);
                }
                ManifestOp::Remove { path } => {
                    editor.remove(edit_path(&path));
                }
            }
        }
        async move {
            let (root, _) = editor.commit_reference().await?;
            Ok(root)
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

/// The trie key a path addresses.
///
/// The manifest root has no metadata slot of its own on the wire; the trie
/// keeps the site-level documents on the `/` node instead, so a root-scope op
/// lands there. Reads apply the same mapping, or a root put would never load
/// back.
fn edit_path(path: &ManifestPath) -> Vec<u8> {
    if path.is_root() {
        metadata::ROOT_PATH.as_bytes().to_vec()
    } else {
        path.as_bytes().to_vec()
    }
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
    // root path node is its metadata slot rather than a directory.
    if suffix.is_empty() || path == metadata::ROOT_PATH.as_bytes() {
        return None;
    }
    let Some(cut) = suffix
        .iter()
        .position(|&byte| byte == ManifestPath::SEPARATOR)
    else {
        let path = ManifestPath::new(path.to_vec());
        return Some(match entry.reference() {
            Some(reference) => match R::from_entry_ref(reference.clone()) {
                // A width the caller did not ask for still names a path; it is
                // listed as an opaque value rather than failing the listing.
                Err(_) => ListEntry::Value { path },
                Ok(reference) => ListEntry::File { path, reference },
            },
            None => ListEntry::Value { path },
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

//! The [`Manifest`] seam over the key-value database, read through its folder
//! view.
//!
//! One store serves both roles: the trie nodes and the entry data behind a
//! reference. A key bound to inline bytes carries its own data, so a load of
//! it never reaches the file pipeline.
//!
//! The seam's handles are the database's own: [`LdbView`] wraps
//! [`Database::at`] and [`LdbWriter`] wraps [`Database::edit`], so the trait's
//! map vocabulary and the crate's map vocabulary are the same code path with
//! paths in place of keys.

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::future::Future;
use core::ops::{Bound, RangeBounds};

use bytes::Bytes;
use nectar_file::{File, Policy};
use nectar_manifest::{
    DataSink, ListEntry, Listing, Manifest, ManifestMetadata, ManifestOp, ManifestPath, MapCursor,
    MapEntry, MapView, MapWriter, SinkError, WellKnownKey,
};
use nectar_primitives::EntryRef;
use nectar_primitives::chunk::ContentOnlyChunkSet;
use nectar_primitives::store::{BoxedError, ChunkPut, MaybeSend, MaybeSync, TrustedGet};

use crate::apply::ApplyError;
use crate::db::{Database, Editor, View};
use crate::error::{MetadataTooLong, NotAReference};
use crate::folder::DirEntry;
use crate::format::{Format, V1};
use crate::meta::{KeyId, Metadata};
use crate::node::NodeRef;
use crate::reader::ReaderError;
use crate::scan::Cursor;
use crate::store::{Plaintext, Seal};
use crate::value::{Entry, Key};

/// A failure crossing the manifest seam.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    /// A lookup or a listing walk failed.
    #[error(transparent)]
    Read(#[from] ReaderError),
    /// Folding the batch into a new root failed.
    #[error(transparent)]
    Apply(#[from] ApplyError),
    /// The rebuilt metadata block exceeded the format's bound.
    #[error(transparent)]
    Metadata(#[from] MetadataTooLong),
    /// The entry is bound to inline bytes where a reference was required.
    #[error(transparent)]
    NotAReference(#[from] NotAReference),
    /// No entry is bound at the requested path.
    #[error("no entry at {path:?}")]
    NotFound {
        /// The path that resolved to nothing.
        path: ManifestPath,
    },
    /// Reading the entry's data through the file pipeline failed.
    #[error("load entry data")]
    Data(#[source] BoxedError),
    /// Writing into the sink failed.
    #[error("write into the sink")]
    Sink(#[source] BoxedError),
}

impl ManifestError {
    /// Box a data-side failure behind the seam.
    fn data<E: core::error::Error + MaybeSend + MaybeSync + 'static>(error: E) -> Self {
        Self::Data(Box::new(error))
    }

    /// Box a sink failure behind the seam.
    fn sink<E: core::error::Error + MaybeSend + MaybeSync + 'static>(error: E) -> Self {
        Self::Sink(Box::new(error))
    }
}

/// The key-value database as a [`Manifest`], keyed by path.
///
/// The seal is the write-side secret: a plaintext database seals with
/// [`Plaintext`], an encrypted one with the sealer that carries the secret the
/// base tree was built under. Reads need no such state, because an encrypted
/// reference carries its own key.
#[derive(Clone, Copy, Debug)]
pub struct LdbManifest<S, K = Plaintext> {
    db: Database<S, K, V1>,
}

impl<S, K> LdbManifest<S, K> {
    /// A manifest over `store`, publishing rewritten nodes through `seal`.
    pub const fn new(store: S, seal: K) -> Self {
        Self {
            db: Database::new(store, seal),
        }
    }

    /// The backing database.
    pub const fn db(&self) -> &Database<S, K, V1> {
        &self.db
    }

    /// The backing store.
    pub const fn store(&self) -> &S {
        self.db.store()
    }

    /// The write-side sealer.
    pub const fn seal(&self) -> &K {
        self.db.seal()
    }
}

impl<S> LdbManifest<S, Plaintext> {
    /// A plaintext manifest over `store`.
    pub const fn plain(store: S) -> Self {
        Self::new(store, Plaintext)
    }
}

impl<S, K, R> Manifest<R> for LdbManifest<S, K>
where
    S: TrustedGet<ContentOnlyChunkSet> + ChunkPut + Clone + MaybeSend + MaybeSync + 'static,
    K: Seal<R> + MaybeSend + MaybeSync,
    R: NodeRef,
{
    /// The database's metadata: the typed key registry, absent as `None`.
    type Metadata = Option<Metadata<V1>>;

    type Error = ManifestError;

    type View<'a>
        = LdbView<'a, S, R>
    where
        Self: 'a;

    type Writer<'a>
        = LdbWriter<'a, S, K, R>
    where
        Self: 'a;

    fn at(&self, root: &R) -> Self::View<'_> {
        LdbView {
            view: self.db.at(root),
        }
    }

    fn edit(&self, base: &R) -> Self::Writer<'_> {
        LdbWriter {
            editor: self.db.edit(base),
        }
    }

    fn metadata_from_view(
        &self,
        view: &dyn ManifestMetadata,
    ) -> Result<Self::Metadata, Self::Error> {
        let mut meta: Option<Metadata<V1>> = None;
        for (key, id) in [
            (WellKnownKey::ContentType, KeyId::ContentType),
            (WellKnownKey::IndexDocument, KeyId::WebsiteIndexDocument),
            (WellKnownKey::ErrorDocument, KeyId::WebsiteErrorDocument),
        ] {
            let Some(value) = view.get(&key) else {
                continue;
            };
            let value = Bytes::copy_from_slice(value.as_bytes());
            match meta.as_mut() {
                Some(block) => {
                    block.insert(id, value)?;
                }
                None => meta = Some(Metadata::new(id, value)?),
            }
        }
        Ok(meta)
    }
}

/// The seam's read view: the database's own view, keyed by path.
#[derive(Debug)]
pub struct LdbView<'a, S, R: NodeRef> {
    view: View<'a, S, V1, R>,
}

impl<'a, S, R> MapView<R> for LdbView<'a, S, R>
where
    S: TrustedGet<ContentOnlyChunkSet> + Clone + MaybeSend + MaybeSync + 'static,
    R: NodeRef,
{
    type Metadata = Option<Metadata<V1>>;

    type Error = ManifestError;

    type Cursor = LdbCursor<'a, S, R>;

    fn get(
        &self,
        path: &ManifestPath,
    ) -> impl Future<Output = Result<Option<MapEntry<R>>, Self::Error>> + MaybeSend {
        let key = Key::from(path.as_bytes());
        async move { Ok(self.view.get(&key).await?.map(mapped)) }
    }

    fn metadata(
        &self,
        path: &ManifestPath,
    ) -> impl Future<Output = Result<Self::Metadata, Self::Error>> + MaybeSend {
        let key = Key::from(path.as_bytes());
        async move { Ok(self.view.metadata(&key).await?) }
    }

    fn contains_key(
        &self,
        path: &ManifestPath,
    ) -> impl Future<Output = Result<bool, Self::Error>> + MaybeSend {
        let key = Key::from(path.as_bytes());
        async move { Ok(self.view.contains_key(&key).await?) }
    }

    fn dir(
        &self,
        dir: &ManifestPath,
    ) -> impl Future<Output = Result<Listing<R>, Self::Error>> + MaybeSend {
        let key = Key::from(dir.as_bytes());
        async move {
            let mut listing = self.view.dir(&key).await?;
            let mut entries = Vec::new();
            while let Some(item) = listing.next().await? {
                entries.push(listed(item));
            }
            Ok(Listing::new(entries))
        }
    }

    fn load<T: DataSink<Error: SinkError> + MaybeSend>(
        &self,
        path: &ManifestPath,
        sink: &mut T,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend {
        let path = path.clone();
        let store = self.view.store().clone();
        async move {
            let entry = self
                .view
                .get(&Key::from(path.as_bytes()))
                .await?
                .ok_or_else(|| ManifestError::NotFound { path })?;
            match entry {
                Entry::Inline(value) => sink
                    .write_at(0, value.as_bytes())
                    .map_err(ManifestError::sink)?,
                bound => {
                    let reference = EntryRef::try_from(bound)?;
                    File::new(store, Policy::DEFAULT)
                        .load(reference, sink)
                        .await
                        .map_err(ManifestError::data)?;
                }
            }
            Ok(())
        }
    }

    fn iter(&self) -> impl Future<Output = Result<Self::Cursor, Self::Error>> + MaybeSend {
        // The view is a store reference and a root, so the walk takes its own
        // copy rather than borrowing the handle it was opened through.
        let view = self.view.clone();
        async move {
            Ok(LdbCursor {
                cursor: view.iter().await?,
            })
        }
    }

    fn range(
        &self,
        bounds: impl RangeBounds<ManifestPath> + MaybeSend,
    ) -> impl Future<Output = Result<Self::Cursor, Self::Error>> + MaybeSend {
        let bounds = key_bounds(&bounds);
        let view = self.view.clone();
        async move {
            Ok(LdbCursor {
                cursor: view.range(bounds).await?,
            })
        }
    }
}

/// The seam's ordered walk: the database's own cursor, keyed by path.
#[derive(Debug)]
pub struct LdbCursor<'a, S, R: NodeRef> {
    cursor: Cursor<'a, S, V1, R>,
}

impl<S, R> MapCursor<R> for LdbCursor<'_, S, R>
where
    S: TrustedGet<ContentOnlyChunkSet> + MaybeSend + MaybeSync,
    R: NodeRef,
{
    type Error = ManifestError;

    fn next(
        &mut self,
    ) -> impl Future<Output = Result<Option<(ManifestPath, MapEntry<R>)>, Self::Error>> + MaybeSend
    {
        let cursor = &mut self.cursor;
        async move {
            Ok(cursor
                .next()
                .await?
                .map(|(key, entry)| (ManifestPath::new(key.as_bytes().to_vec()), mapped(entry))))
        }
    }
}

/// The seam's write handle: the database's own editor, keyed by path.
#[derive(Debug)]
pub struct LdbWriter<'a, S, K, R: NodeRef> {
    editor: Editor<'a, S, K, V1, R>,
}

impl<S, K, R> MapWriter<R> for LdbWriter<'_, S, K, R>
where
    S: TrustedGet<ContentOnlyChunkSet> + ChunkPut + MaybeSend + MaybeSync,
    K: Seal<R> + MaybeSend + MaybeSync,
    R: NodeRef,
{
    type Metadata = Option<Metadata<V1>>;

    type Error = ManifestError;

    fn stage(&mut self, op: ManifestOp<R, Self::Metadata>) {
        match op {
            ManifestOp::Insert {
                path,
                reference,
                meta,
            } => {
                let mut staged = self.editor.insert(
                    Key::from(path.as_bytes()),
                    Entry::from(reference.into_entry_ref()),
                );
                if let Some(meta) = meta {
                    staged.meta(meta);
                }
            }
            ManifestOp::Remove { path } => {
                self.editor.remove(Key::from(path.as_bytes()));
            }
        }
    }

    fn commit(self) -> impl Future<Output = Result<R, Self::Error>> + MaybeSend {
        let editor = self.editor;
        async move { Ok(editor.commit().await?) }
    }
}

/// The key bounds a path range selects, in the database's own key type.
fn key_bounds(bounds: &impl RangeBounds<ManifestPath>) -> (Bound<Key>, Bound<Key>) {
    (bound(bounds.start_bound()), bound(bounds.end_bound()))
}

/// One path bound as a key bound.
fn bound(edge: Bound<&ManifestPath>) -> Bound<Key> {
    match edge {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(path) => Bound::Included(Key::from(path.as_bytes())),
        Bound::Excluded(path) => Bound::Excluded(Key::from(path.as_bytes())),
    }
}

/// One bound value as a seam entry: a reference of the caller's width, or an
/// opaque value.
///
/// An inline value, or a reference of the other width, is bound but names no
/// reference the caller can read on its own; a load still reaches its bytes.
fn mapped<R: NodeRef>(entry: Entry<V1>) -> MapEntry<R> {
    match EntryRef::try_from(entry).map(R::from_entry_ref) {
        Ok(Ok(reference)) => MapEntry::Reference(reference),
        Ok(Err(_)) | Err(_) => MapEntry::Opaque,
    }
}

/// One folder-view child as a seam listing entry.
///
/// A key bound to inline bytes, or to a reference of the other width, still
/// names a path: it lists as a value rather than failing the listing.
fn listed<R: NodeRef>(entry: DirEntry<V1>) -> ListEntry<R> {
    match entry {
        DirEntry::Dir { key } => ListEntry::Dir {
            path: ManifestPath::new(key.as_bytes().to_vec()),
        },
        DirEntry::File { key, entry } => {
            let path = ManifestPath::new(key.as_bytes().to_vec());
            match mapped::<R>(entry) {
                MapEntry::Reference(reference) => ListEntry::File { path, reference },
                MapEntry::Opaque => ListEntry::Value { path },
            }
        }
    }
}

/// The seam's separator is the format's own, so a path splits into the same
/// segments on either side of the trait.
const _: () = assert!(ManifestPath::SEPARATOR == <V1 as Format>::SEPARATOR);

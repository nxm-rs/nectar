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
    MapEntry, MapView, MapWriter, ReservedKey, SinkError, SiteConfig, WellKnownKey,
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
    /// The batch staged a write at a key the map reserves, so none of it
    /// landed.
    #[error("the batch named a reserved key")]
    Reserved(#[from] ReservedKey),
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
            reserved: None,
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
        let key = content_key(path);
        async move {
            let Some(key) = key else { return Ok(None) };
            Ok(self.view.get(&key).await?.map(mapped))
        }
    }

    async fn site_config(&self) -> Result<SiteConfig, Self::Error> {
        let site = self.view.website().await?;
        let document = |bytes: Option<&[u8]>| bytes.map(ManifestPath::from);
        Ok(SiteConfig::new()
            .with_index_document(document(site.index()))
            .with_error_document(document(site.error())))
    }

    fn metadata(
        &self,
        path: &ManifestPath,
    ) -> impl Future<Output = Result<Self::Metadata, Self::Error>> + MaybeSend {
        let key = content_key(path);
        async move {
            let Some(key) = key else {
                return Ok(None);
            };
            Ok(self.view.metadata(&key).await?)
        }
    }

    fn contains_key(
        &self,
        path: &ManifestPath,
    ) -> impl Future<Output = Result<bool, Self::Error>> + MaybeSend {
        let key = content_key(path);
        async move {
            let Some(key) = key else { return Ok(false) };
            Ok(self.view.contains_key(&key).await?)
        }
    }

    /// The database seeks the floor natively, so the walk the default would
    /// pay is replaced by one O(depth) descent.
    fn floor(
        &self,
        path: &ManifestPath,
    ) -> impl Future<Output = Result<Option<(ManifestPath, MapEntry<R>)>, Self::Error>> + MaybeSend
    {
        let key = Key::from(path.as_bytes());
        let view = self.view.clone();
        async move {
            let found = view.floor(&key).await?;
            match found {
                None => Ok(None),
                Some((found, entry)) if !is_reserved(&found) => {
                    Ok(Some((floored(found), mapped(entry))))
                }
                // The seek landed on a reserved key, which is no content key
                // at all, so the greatest content key below it is the answer.
                // Only a database written past the seam holds one, so the walk
                // this costs is one no seam write can provoke.
                Some(_) => {
                    let mut cursor = view.range((Bound::Unbounded, Bound::Included(key))).await?;
                    let mut last = None;
                    while let Some((found, entry)) = cursor.next().await? {
                        if !is_reserved(&found) {
                            last = Some((floored(found), mapped(entry)));
                        }
                    }
                    Ok(last)
                }
            }
        }
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
                // A reserved key is not content, so the listing never surfaces
                // it, whatever put it in the database. The folder view gives
                // one name to two different things when the key is exactly the
                // separator: the reserved key itself, and the directory that
                // stands for the content below it. Only what is bound strictly
                // under it tells them apart, so the bare slot is hidden and a
                // directory of content is listed like any other, which is what
                // the trie lists too.
                let hidden = is_reserved(item.key())
                    && !(item.is_dir() && bound_below(&self.view, item.key()).await?);
                if hidden {
                    continue;
                }
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
            let entry = match content_key(&path) {
                Some(key) => self.view.get(&key).await?,
                None => None,
            }
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
            // The root slot leads the walk when the root binds a value, and no
            // reserved key is content, so the map steps over both.
            while let Some((key, entry)) = cursor.next().await? {
                if is_reserved(&key) {
                    continue;
                }
                return Ok(Some((
                    ManifestPath::new(key.as_bytes().to_vec()),
                    mapped(entry),
                )));
            }
            Ok(None)
        }
    }
}

/// The seam's write handle: the database's own editor, keyed by path.
#[derive(Debug)]
pub struct LdbWriter<'a, S, K, R: NodeRef> {
    editor: Editor<'a, S, K, V1, R>,
    /// The first reserved path the batch staged, which fails the commit.
    ///
    /// Staging is infallible, so the refusal is held here and reported once,
    /// at the commit that would otherwise write the batch.
    reserved: Option<ReservedKey>,
}

impl<S, K, R: NodeRef> LdbWriter<'_, S, K, R> {
    /// Stage one site document into the database's root manifest metadata, or
    /// clear it.
    ///
    /// A merge either way, so the two documents are independent: setting one
    /// leaves the other where it was, and clearing the last one leaves the
    /// manifest carrying no metadata at all.
    fn document(&mut self, id: KeyId, path: Option<ManifestPath>) -> &mut Self {
        let value = path.map(|path| Bytes::copy_from_slice(path.as_bytes()));
        self.editor.set_root_metadata(id, value);
        self
    }
}

impl<S, K, R> MapWriter<R> for LdbWriter<'_, S, K, R>
where
    S: TrustedGet<ContentOnlyChunkSet> + ChunkPut + MaybeSend + MaybeSync,
    K: Seal<R> + MaybeSend + MaybeSync,
    R: NodeRef,
{
    type Metadata = Option<Metadata<V1>>;

    type Error = ManifestError;

    /// An insert replaces the whole binding; existing metadata is cleared
    /// unless `meta` carries some, because the op's metadata is the key's
    /// metadata from then on.
    ///
    /// A reserved path stages no database op and refuses the commit instead,
    /// because it is no key: the site documents are written through the
    /// option-typed setters below.
    fn stage(&mut self, op: ManifestOp<R, Self::Metadata>) {
        let Some(key) = content_key(op.path()) else {
            self.reserved
                .get_or_insert_with(|| ReservedKey::new(op.path().clone()));
            return;
        };
        match op {
            ManifestOp::Insert {
                path: _,
                reference,
                meta,
            } => {
                let mut staged = self
                    .editor
                    .insert(key, Entry::from(reference.into_entry_ref()));
                if let Some(meta) = meta {
                    staged.meta(meta);
                }
            }
            ManifestOp::Remove { path: _ } => {
                self.editor.remove(key);
            }
        }
    }

    fn with_index_document(&mut self, path: impl Into<Option<ManifestPath>>) -> &mut Self {
        self.document(KeyId::WebsiteIndexDocument, path.into())
    }

    fn with_error_document(&mut self, path: impl Into<Option<ManifestPath>>) -> &mut Self {
        self.document(KeyId::WebsiteErrorDocument, path.into())
    }

    fn commit(self) -> impl Future<Output = Result<R, Self::Error>> + MaybeSend {
        let Self { editor, reserved } = self;
        async move {
            // The whole batch is refused, so a reserved path writes nothing at
            // all rather than landing the ops around it.
            if let Some(reserved) = reserved {
                return Err(ManifestError::Reserved(reserved));
            }
            Ok(editor.commit().await?)
        }
    }
}

/// The database key `path` addresses, or `None` when it names no content key.
///
/// The key bytes are the path bytes verbatim. The reserved paths are the two
/// the seam names on either format: the empty one, which is the database's own
/// root slot holding the manifest metadata the site-level documents live in,
/// and the lone separator, which is the slot the trie keys them at. Neither is
/// read, written or walked as a key here, so the two formats answer alike.
fn content_key(path: &ManifestPath) -> Option<Key> {
    (!path.is_reserved()).then(|| Key::from(path.as_bytes()))
}

/// One database key as the path a read answers with.
fn floored(key: Key) -> ManifestPath {
    ManifestPath::new(key.as_bytes().to_vec())
}

/// Whether `key` is one the map reserves rather than content.
///
/// The read-side twin of [`content_key`], over the database's own key type: a
/// walk, a listing and a floor each step over what a lookup answers absent.
fn is_reserved(key: &Key) -> bool {
    matches!(key.as_bytes(), [] | [ManifestPath::SEPARATOR])
}

/// Whether the database binds anything strictly below `key`.
///
/// What tells a listed reserved key apart from the directory of content the
/// folder view gives the same name to. The probe stops at the first key past
/// `key` itself, and `key` sorts first in its own prefix range, so it costs one
/// seek and at most two steps.
async fn bound_below<S, R>(view: &View<'_, S, V1, R>, key: &Key) -> Result<bool, ManifestError>
where
    S: TrustedGet<ContentOnlyChunkSet> + MaybeSync,
    R: NodeRef,
{
    let mut cursor = view.prefix(key).await?;
    while let Some((found, _)) = cursor.next().await? {
        if found.as_bytes() != key.as_bytes() {
            return Ok(true);
        }
    }
    Ok(false)
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

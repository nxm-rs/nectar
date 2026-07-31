//! The [`Manifest`] seam, implemented directly on [`Database`], [`View`] and
//! [`Cursor`]: keyed by path, reserved keys filtered, a checked batch folded
//! through one [`Database::edit`] changeset. Inherent methods win on the
//! concrete types, so a seam call names the trait: `Manifest::at(&db, root)`.

use alloc::vec::Vec;
use core::ops::{Bound, RangeBounds};

use bytes::Bytes;
use nectar_file::{File, LoadError, Policy};
use nectar_manifest::{
    Batch, DataSink, ListEntry, Listing, Manifest, ManifestError, ManifestOp, ManifestPath,
    MapCursor, MapEntry, MapView, SinkError, SiteConfig,
};
use nectar_primitives::EntryRef;
use nectar_primitives::chunk::ContentOnlyChunkSet;
use nectar_primitives::store::{ChunkPut, MaybeSend, MaybeSync, TrustedGet};

use crate::apply::ApplyError;
use crate::builder::Builder;
use crate::db::{Database, View};
use crate::folder::DirEntry;
use crate::format::{Format, V1};
use crate::meta::{KeyId, Metadata};
use crate::node::NodeRef;
use crate::reader::ReaderError;
use crate::scan::Cursor;
use crate::store::Seal;
use crate::value::{Entry, Key};

/// The database's own failures behind [`ManifestError::Format`].
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum LdbFormatError {
    /// A lookup or a listing walk failed.
    #[error(transparent)]
    Read(#[from] ReaderError),
    /// Folding the batch into a new root failed.
    #[error(transparent)]
    Apply(#[from] ApplyError),
}

nectar_manifest::format_error_from!(LdbFormatError: ReaderError, ApplyError);

/// The database as a [`Manifest`], keyed by path.
impl<S, K, R> Manifest<R> for Database<S, K, V1>
where
    S: TrustedGet<ContentOnlyChunkSet> + ChunkPut + Clone + MaybeSend + MaybeSync + 'static,
    K: Seal<R> + MaybeSend + MaybeSync,
    R: NodeRef,
{
    /// The typed key registry, absent as `None`.
    type Metadata = Option<Metadata<V1>>;

    type FormatError = LdbFormatError;

    type View<'a>
        = View<'a, S, V1, R>
    where
        Self: 'a;

    /// The empty database persisted through the native builder.
    async fn empty(&self) -> Result<R, ManifestError<LdbFormatError>> {
        let built = Builder::<V1>::new()
            .build(self.store(), self.seal())
            .await
            .map_err(ApplyError::from)?;
        Ok(built.root().clone())
    }

    fn at(&self, root: &R) -> Self::View<'_> {
        // Inherent resolution wins, so this is the native `Database::at`.
        Self::at(self, root)
    }

    /// The checked batch folded through one changeset: order never reaches
    /// the produced root.
    async fn apply(
        &self,
        base: R,
        batch: Batch<R, Self::Metadata>,
    ) -> Result<R, ManifestError<LdbFormatError>> {
        let checked = batch.into_checked().map_err(ManifestError::Reserved)?;
        let mut editor = self.edit(&base);
        for op in checked.ops {
            match op {
                ManifestOp::Insert {
                    path,
                    reference,
                    meta,
                } => {
                    let entry = Entry::from(reference.into_entry_ref());
                    editor.insert_with(Key::from(path.as_bytes()), entry, meta)
                }
                ManifestOp::Remove { path } => editor.remove(Key::from(path.as_bytes())),
            };
        }
        // Site documents: untouched stages nothing, `Some(None)` clears.
        for (id, delta) in [
            (KeyId::WebsiteIndexDocument, checked.index_document),
            (KeyId::WebsiteErrorDocument, checked.error_document),
        ] {
            if let Some(path) = delta {
                editor.set_root_metadata(id, path.map(|p| Bytes::copy_from_slice(p.as_bytes())));
            }
        }
        Ok(editor.commit().await?)
    }
}

/// The native view as the seam's read handle, keyed by path. Each body's
/// key-typed call resolves to the inherent method.
impl<'a, S, R> MapView<R> for View<'a, S, V1, R>
where
    S: TrustedGet<ContentOnlyChunkSet> + Clone + MaybeSend + MaybeSync + 'static,
    R: NodeRef,
{
    type Metadata = Option<Metadata<V1>>;

    type Error = ManifestError<LdbFormatError>;

    type Cursor = Cursor<'a, S, V1, R>;

    async fn get(&self, path: &ManifestPath) -> Result<Option<MapEntry<R>>, Self::Error> {
        let Some(key) = content_key(path) else {
            return Ok(None);
        };
        Ok(self.get(&key).await?.map(mapped))
    }

    async fn site_config(&self) -> Result<SiteConfig, Self::Error> {
        let site = self.website().await?;
        let document = |bytes: Option<&[u8]>| bytes.map(ManifestPath::from);
        Ok(SiteConfig::new()
            .with_index_document(document(site.index()))
            .with_error_document(document(site.error())))
    }

    async fn metadata(&self, path: &ManifestPath) -> Result<Self::Metadata, Self::Error> {
        let Some(key) = content_key(path) else {
            return Ok(None);
        };
        Ok(self.metadata(&key).await?)
    }

    /// One O(depth) descent, in place of the default's walk.
    async fn floor(
        &self,
        path: &ManifestPath,
    ) -> Result<Option<(ManifestPath, MapEntry<R>)>, Self::Error> {
        let key = Key::from(path.as_bytes());
        match self.floor(&key).await? {
            None => Ok(None),
            Some((found, entry)) if !is_reserved(&found) => {
                Ok(Some((path_of(&found), mapped(entry))))
            }
            // The seek landed on a reserved key: the greatest content key
            // below it answers.
            Some(_) => {
                let mut cursor = self.range((Bound::Unbounded, Bound::Included(key))).await?;
                let mut last = None;
                while let Some(pair) = MapCursor::next(&mut cursor).await? {
                    last = Some(pair);
                }
                Ok(last)
            }
        }
    }

    async fn dir(&self, dir: &ManifestPath) -> Result<Listing<R>, Self::Error> {
        let mut listing = self.dir(&Key::from(dir.as_bytes())).await?;
        let mut entries = Vec::new();
        while let Some(item) = listing.next().await? {
            // At the bare separator the folder view names both the reserved
            // key and the directory of content below it: hide the slot, list
            // the directory.
            if is_reserved(item.key()) {
                let mut probe = self.prefix(item.key()).await?;
                let mut below = false;
                while let Some((found, _)) = probe.next().await? {
                    if found.as_bytes() != item.key().as_bytes() {
                        below = true;
                        break;
                    }
                }
                if !(item.is_dir() && below) {
                    continue;
                }
            }
            entries.push(listed(item));
        }
        Ok(Listing::new(entries))
    }

    async fn load<T: DataSink<Error: SinkError> + MaybeSend>(
        &self,
        path: &ManifestPath,
        sink: &mut T,
    ) -> Result<(), Self::Error> {
        let entry = match content_key(path) {
            Some(key) => self.get(&key).await?,
            None => None,
        }
        .ok_or_else(|| ManifestError::NotFound(path.clone()))?;
        // An inline value is its own data; references take the file walk.
        let reference = match entry {
            Entry::Inline(value) => {
                return sink
                    .write_at(0, value.as_bytes())
                    .map_err(ManifestError::sink);
            }
            Entry::Ref32(reference) => EntryRef::Plain(reference),
            Entry::Ref64(reference) => EntryRef::Encrypted(reference),
        };
        File::new(self.store().clone(), Policy::DEFAULT)
            .load(reference, sink)
            .await
            .map(drop)
            .map_err(|error| match error {
                LoadError::Sink { source, .. } => ManifestError::sink(source),
                data => ManifestError::data(data),
            })
    }

    async fn iter(&self) -> Result<Self::Cursor, Self::Error> {
        Ok(self.iter().await?)
    }

    async fn range(
        &self,
        bounds: impl RangeBounds<ManifestPath> + MaybeSend,
    ) -> Result<Self::Cursor, Self::Error> {
        let key = |edge: Bound<&ManifestPath>| edge.map(|path| Key::from(path.as_bytes()));
        Ok(self
            .range((key(bounds.start_bound()), key(bounds.end_bound())))
            .await?)
    }
}

/// The native cursor as the seam's ordered walk; reserved keys step over.
impl<S, R> MapCursor<R> for Cursor<'_, S, V1, R>
where
    S: TrustedGet<ContentOnlyChunkSet> + MaybeSend + MaybeSync,
    R: NodeRef,
{
    type Error = ManifestError<LdbFormatError>;

    async fn next(&mut self) -> Result<Option<(ManifestPath, MapEntry<R>)>, Self::Error> {
        while let Some((key, entry)) = Cursor::next(self).await? {
            if !is_reserved(&key) {
                return Ok(Some((path_of(&key), mapped(entry))));
            }
        }
        Ok(None)
    }
}

/// The database key `path` addresses verbatim, or `None` on a reserved path.
fn content_key(path: &ManifestPath) -> Option<Key> {
    (!path.is_reserved()).then(|| Key::from(path.as_bytes()))
}

/// One database key as the path a read answers with.
fn path_of(key: &Key) -> ManifestPath {
    ManifestPath::from(key.as_bytes())
}

/// Whether `key` is one the map reserves rather than content.
fn is_reserved(key: &Key) -> bool {
    matches!(key.as_bytes(), [] | [ManifestPath::SEPARATOR])
}

/// One bound value as a seam entry; a load still reaches an opaque value.
fn mapped<R: NodeRef>(entry: Entry<V1>) -> MapEntry<R> {
    match EntryRef::try_from(entry).map(R::from_entry_ref) {
        Ok(Ok(reference)) => MapEntry::Reference(reference),
        Ok(Err(_)) | Err(_) => MapEntry::Opaque,
    }
}

/// One folder-view child as a seam listing entry; an inline or other-width
/// binding lists as a value.
fn listed<R: NodeRef>(entry: DirEntry<V1>) -> ListEntry<R> {
    match entry {
        DirEntry::Dir { key } => ListEntry::Dir {
            path: path_of(&key),
        },
        DirEntry::File { key, entry } => {
            let path = path_of(&key);
            match mapped::<R>(entry) {
                MapEntry::Reference(reference) => ListEntry::File { path, reference },
                MapEntry::Opaque => ListEntry::Value { path },
            }
        }
    }
}

/// The seam's separator is the format's own.
const _: () = assert!(ManifestPath::SEPARATOR == <V1 as Format>::SEPARATOR);

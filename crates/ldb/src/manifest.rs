//! The [`Manifest`] seam, implemented directly on [`Database`], [`View`] and
//! [`ScanRawCursor`]: keyed by path, reserved keys filtered, a checked batch
//! folded through one [`Database::edit`] changeset. Inherent methods win on
//! the concrete types, so a seam call names the trait:
//! `Manifest::at(&db, root)`.

use alloc::vec::Vec;
use core::future::Future;
use core::ops::Bound;
use core::pin::Pin;
use core::task::{Context, Poll};

use bytes::Bytes;
use futures_util::Stream;
use futures_util::StreamExt;
use nectar_file::load_reference;
use nectar_manifest::{
    Batch, ListEntry, Listing, Manifest, ManifestError, ManifestOp, ManifestPath, ManifestView,
    MapEntry, NodeLoader, NodeSaver, PathCursor, RawCursor, RawItem, Served, SiteConfig,
    serve_fallback,
};
use nectar_primitives::chunk::{ChunkAddress, ChunkRef, ContentOnlyChunkSet};
use nectar_primitives::store::{ChunkPut, MaybeSend, MaybeSync, TrustedGet};
use nectar_primitives::{Chunk, EntryRef};
use positioned_io::WriteAt;

use crate::apply::ApplyError;
use crate::builder::Builder;
use crate::db::{Database, View};
use crate::folder::{DirEntry, FolderServed};
use crate::format::{Format, V1};
use crate::meta::{KeyId, Metadata};
use crate::node::{Node, NodeRef};
use crate::reader::ReaderError;
use crate::scan::ScanCursor;
use crate::store::{Seal, StoreError, load_node, materialize_traced, save_node};
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
    S: TrustedGet<ContentOnlyChunkSet> + ChunkPut<Chunk> + Clone + MaybeSend + MaybeSync + 'static,
    K: Seal<R> + MaybeSend + MaybeSync,
    R: NodeRef + Unpin,
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

    fn at(&self, root: R) -> Self::View<'_> {
        // Inherent resolution wins, so this is the native `Database::at`.
        Self::at(self, &root)
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

/// The seam at the decoded node: a spilled node has no single stored image,
/// so bytes are not a unit this format can offer.
impl<S, K, F, R> NodeLoader<Node<F, R>> for Database<S, K, F>
where
    S: TrustedGet<ContentOnlyChunkSet> + MaybeSend + MaybeSync,
    K: MaybeSend + MaybeSync,
    F: Format,
    R: NodeRef,
{
    type Error = StoreError;

    async fn load(&self, reference: &EntryRef) -> Result<Node<F, R>, StoreError> {
        let reference = R::from_entry_ref(reference.clone())?;
        load_node(self.store(), &reference).await
    }

    async fn load_traced(
        &self,
        reference: &EntryRef,
    ) -> Result<(Node<F, R>, Vec<ChunkAddress>), StoreError> {
        let reference = R::from_entry_ref(reference.clone())?;
        let (node, segments) = materialize_traced::<S, F, R>(self.store(), &reference).await?;
        let mut addresses = Vec::with_capacity(segments.len().saturating_add(1));
        addresses.push(*reference.address());
        addresses.extend(segments);
        Ok((node, addresses))
    }
}

/// The seam's write half, sealing through the database's own secret.
impl<S, K, F, R> NodeSaver<Node<F, R>, R> for Database<S, K, F>
where
    S: ChunkPut<Chunk> + MaybeSend + MaybeSync,
    K: Seal<R> + MaybeSend + MaybeSync,
    F: Format,
    R: NodeRef,
{
    type Error = StoreError;

    fn save(&self, node: &Node<F, R>) -> impl Future<Output = Result<R, StoreError>> + MaybeSend {
        save_node(self.store(), node, self.seal())
    }
}

/// The native view as the seam's read handle, keyed by path. Each body's
/// key-typed call resolves to the inherent method.
impl<'a, S, R> ManifestView<R> for View<'a, S, V1, R>
where
    S: TrustedGet<ContentOnlyChunkSet> + Clone + MaybeSend + MaybeSync + 'static,
    R: NodeRef + Unpin,
{
    type Metadata = Option<Metadata<V1>>;

    type Error = ManifestError<LdbFormatError>;

    type Cursor = PathCursor<ScanRawCursor<'a, S, R>, R>;

    async fn get(&self, path: &ManifestPath) -> Result<Option<MapEntry<R>>, Self::Error> {
        let Some(key) = path.content_key().map(Key::from) else {
            return Ok(None);
        };
        Ok(self.get(&key).await?.map(seam_entry))
    }

    async fn site_config(&self) -> Result<SiteConfig, Self::Error> {
        let site = self.website().await?;
        let document = |bytes: Option<&[u8]>| bytes.map(ManifestPath::from);
        Ok(SiteConfig::new()
            .with_index_document(document(site.index()))
            .with_error_document(document(site.error())))
    }

    async fn metadata(&self, path: &ManifestPath) -> Result<Self::Metadata, Self::Error> {
        let Some(key) = path.content_key().map(Key::from) else {
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
            Some((found, entry)) if !ManifestPath::is_reserved_bytes(found.as_bytes()) => {
                Ok(Some((path_of(&found), seam_entry(entry))))
            }
            // The seek landed on a reserved key: the greatest content key
            // below it answers.
            Some(_) => {
                let mut cursor = PathCursor::new(ScanRawCursor::new(
                    self.range((Bound::Unbounded, Bound::Included(key))).await?,
                ));
                let mut last = None;
                while let Some(pair) = cursor.next().await.transpose()? {
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
            if ManifestPath::is_reserved_bytes(item.key().as_bytes()) {
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

    /// One native resolution in place of the default's three seam probes.
    async fn serve(&self, path: &ManifestPath) -> Result<Served<R>, Self::Error> {
        // The seam serves content paths alone: a reserved request path must
        // not answer from the root slot the native exact probe reads.
        if path.is_reserved() {
            return serve_fallback(self, path).await;
        }
        let served = match self.serve(&Key::from(path.as_bytes())).await? {
            FolderServed::Exact { key, entry } => Served::Exact {
                path: path_of(&key),
                entry: seam_entry(entry),
            },
            FolderServed::Index { key, entry } => Served::Index {
                path: path_of(&key),
                entry: seam_entry(entry),
            },
            // An error document set to a reserved path names no content,
            // exactly as the seam's own probe would read it.
            FolderServed::Error { key, entry }
                if !ManifestPath::is_reserved_bytes(key.as_bytes()) =>
            {
                Served::Error {
                    path: path_of(&key),
                    entry: seam_entry(entry),
                }
            }
            FolderServed::Error { .. } | FolderServed::Missing => Served::Missing,
        };
        Ok(served)
    }

    async fn load<T: WriteAt + MaybeSend + ?Sized>(
        &self,
        path: &ManifestPath,
        sink: &mut T,
    ) -> Result<(), Self::Error> {
        let entry = match path.content_key().map(Key::from) {
            Some(key) => self.get(&key).await?,
            None => None,
        }
        .ok_or_else(|| ManifestError::NotFound(path.clone()))?;
        // An inline value is its own data; references take the file walk.
        let reference = match entry {
            Entry::Inline(value) => {
                return sink
                    .write_all_at(0, value.as_bytes())
                    .map_err(ManifestError::sink);
            }
            Entry::Ref32(reference) => EntryRef::Plain(reference),
            Entry::Ref64(reference) => EntryRef::Encrypted(reference),
        };
        // Load success tracks `get`: a reference the caller's width cannot
        // hold reads as opaque and loads as no data.
        let reference =
            R::from_entry_ref(reference).map_err(|_| ManifestError::NoData(path.clone()))?;
        load_reference(
            self.store().clone(),
            self.policy,
            reference.into_entry_ref(),
            sink,
        )
        .await
    }

    async fn iter(&self) -> Result<Self::Cursor, Self::Error> {
        Ok(PathCursor::new(ScanRawCursor::new(self.iter().await?)))
    }

    async fn range(
        &self,
        bounds: (Bound<ManifestPath>, Bound<ManifestPath>),
    ) -> Result<Self::Cursor, Self::Error> {
        let key = |path: ManifestPath| Key::from(path.into_bytes());
        // The native walk seeks to the bounds, so the seam wrapper only skips
        // the reserved keys.
        Ok(PathCursor::new(ScanRawCursor::new(
            self.range((bounds.0.map(key), bounds.1.map(key))).await?,
        )))
    }
}

/// The database's raw ordered walk for the seam: every stored key, the
/// reserved slots included, as byte keys with their seam entries.
#[derive(Debug)]
pub struct ScanRawCursor<'a, S, R: NodeRef = ChunkRef> {
    walk: ScanCursor<'a, S, V1, R>,
}

impl<'a, S, R> ScanRawCursor<'a, S, R>
where
    S: TrustedGet<ContentOnlyChunkSet> + MaybeSend + MaybeSync,
    R: NodeRef,
{
    /// A raw walk over `walk`.
    #[must_use]
    pub const fn new(walk: ScanCursor<'a, S, V1, R>) -> Self {
        Self { walk }
    }
}

impl<'a, S, R> Stream for ScanRawCursor<'a, S, R>
where
    S: TrustedGet<ContentOnlyChunkSet> + MaybeSend + MaybeSync,
    R: NodeRef + Unpin,
{
    type Item = Result<RawItem<R>, ManifestError<LdbFormatError>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.walk).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error.into()))),
            Poll::Ready(Some(Ok((key, entry)))) => {
                Poll::Ready(Some(Ok((key.as_bytes().to_vec(), seam_entry(entry)))))
            }
        }
    }
}

impl<'a, S, R> RawCursor<R> for ScanRawCursor<'a, S, R>
where
    S: TrustedGet<ContentOnlyChunkSet> + MaybeSend + MaybeSync,
    R: NodeRef + Unpin,
{
    type Error = ManifestError<LdbFormatError>;
}

/// One database key as the path a read answers with.
fn path_of(key: &Key) -> ManifestPath {
    ManifestPath::from(key.as_bytes())
}

/// One bound value as a seam entry: a reference at the caller's width, the
/// manifest-carried inline value, or opaque when the width does not fit.
fn seam_entry<R: NodeRef>(entry: Entry<V1>) -> MapEntry<R> {
    EntryRef::try_from(entry).map_or(MapEntry::Value, MapEntry::from_entry_ref)
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
            match seam_entry::<R>(entry) {
                MapEntry::Reference(reference) => ListEntry::File { path, reference },
                MapEntry::Value | MapEntry::Opaque => ListEntry::Value { path },
            }
        }
    }
}

/// The seam's separator is the format's own.
const _: () = assert!(ManifestPath::SEPARATOR == <V1 as Format>::SEPARATOR);

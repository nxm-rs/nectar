//! The object-safe wrapper: one handle over a manifest format chosen at
//! runtime.
//!
//! Erasure costs four things and states each: the futures box, the reference
//! width is fixed to [`ChunkRef`], metadata crosses as a [`MetadataSource`]
//! rather than the format's own type, and an ordered walk stays on the static
//! path. Every erased call takes the root it reads or writes against, and a
//! failure crosses as [`ErasedManifestError`] with the seam variants intact.

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::ops::ControlFlow;

use futures_util::StreamExt;
use nectar_marker::{MaybeSend, MaybeSync};
use nectar_primitives::chunk::ChunkRef;
use nectar_tasks::BoxFuture;
use positioned_io::WriteAt;

use crate::Manifest;
use crate::batch::Batch;
use crate::error::{ErasedFormat, ErasedManifestError, ManifestError};
use crate::listing::Listing;
use crate::meta::{ManifestMeta, MetadataSource};
use crate::op::ManifestOp;
use crate::path::ManifestPath;
use crate::site::SiteConfig;
use crate::view::{ManifestView, MapEntry};

/// A typed seam failure with its format union boxed.
fn erase<F: core::error::Error + MaybeSend + MaybeSync + 'static>(
    error: ManifestError<F>,
) -> ErasedManifestError {
    error.map_format(|format| ErasedFormat(Box::new(format)))
}

/// The object-safe positional target of [`ErasedManifest::dyn_load`].
///
/// An object name carries its marker bounds only through a principal trait,
/// so the erased sink borrows one over [`WriteAt`] with no behaviour of its
/// own. Blanket-implemented, so any positional target is usable.
pub trait DynWriteAt: WriteAt + MaybeSend {}

impl<T: WriteAt + MaybeSend + ?Sized> DynWriteAt for T {}

/// Object-safe visitor for [`ErasedManifest::dyn_for_each`]:
/// blanket-implemented for closures, so a caller passes
/// `&mut |path, entry| ...` directly.
pub trait DynVisit: MaybeSend {
    /// Fold one `(path, entry)`; [`ControlFlow::Break`] stops the walk.
    fn visit(&mut self, path: ManifestPath, entry: MapEntry) -> ControlFlow<()>;
}

impl<F: FnMut(ManifestPath, MapEntry) -> ControlFlow<()> + MaybeSend> DynVisit for F {
    fn visit(&mut self, path: ManifestPath, entry: MapEntry) -> ControlFlow<()> {
        self(path, entry)
    }
}

/// Object-safe [`Manifest`]: a manifest whose format is decided at runtime.
///
/// Blanket-implemented for every `Manifest<ChunkRef>`, so a format implements
/// the static trait once and is held as `Box<dyn ErasedManifest>` for free.
pub trait ErasedManifest: MaybeSend + MaybeSync {
    /// The root of the empty manifest, freshly persisted.
    fn dyn_empty(&self) -> BoxFuture<'_, Result<ChunkRef, ErasedManifestError>>;

    /// The entry bound to `path`, or `None` when the path is absent.
    fn dyn_get<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: &'a ManifestPath,
    ) -> BoxFuture<'a, Result<Option<MapEntry>, ErasedManifestError>>;

    /// Whether `path` is bound.
    fn dyn_contains_key<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: &'a ManifestPath,
    ) -> BoxFuture<'a, Result<bool, ErasedManifestError>>;

    /// The greatest bound path `<= path`, with its entry.
    fn dyn_floor<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: &'a ManifestPath,
    ) -> BoxFuture<'a, Result<Option<(ManifestPath, MapEntry)>, ErasedManifestError>>;

    /// The immediate children of the directory `dir` names, in path order.
    fn dyn_dir<'a>(
        &'a self,
        root: &'a ChunkRef,
        dir: &'a ManifestPath,
    ) -> BoxFuture<'a, Result<Listing, ErasedManifestError>>;

    /// Fold every bound `(path, entry)` in path order into `visit`, stopping
    /// early on [`ControlFlow::Break`]. Internal iteration: an object-safe
    /// cursor would box every step, so the erased walk inverts control.
    fn dyn_for_each<'a>(
        &'a self,
        root: &'a ChunkRef,
        visit: &'a mut dyn DynVisit,
    ) -> BoxFuture<'a, Result<(), ErasedManifestError>>;

    /// Write the data bound to `path` into `sink`, starting at offset zero.
    fn dyn_load<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: &'a ManifestPath,
        sink: &'a mut (dyn DynWriteAt + 'a),
    ) -> BoxFuture<'a, Result<(), ErasedManifestError>>;

    /// The site-level documents the manifest declares, each absent as `None`.
    fn dyn_site_config<'a>(
        &'a self,
        root: &'a ChunkRef,
    ) -> BoxFuture<'a, Result<SiteConfig, ErasedManifestError>>;

    /// Replace the site-level documents, returning the new root.
    ///
    /// A replace, not a merge: a document left `None` is cleared.
    fn dyn_set_site_config<'a>(
        &'a self,
        root: &'a ChunkRef,
        config: SiteConfig,
    ) -> BoxFuture<'a, Result<ChunkRef, ErasedManifestError>>;

    /// The metadata bound to `path`; an absent path reads back empty.
    fn dyn_metadata<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: &'a ManifestPath,
    ) -> BoxFuture<'a, Result<Box<dyn MetadataSource>, ErasedManifestError>>;

    /// Insert one path, returning the new root. The format rebuilds `meta`
    /// into its native type; what it cannot carry is its stated limit.
    fn dyn_insert<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: ManifestPath,
        reference: ChunkRef,
        meta: &'a dyn MetadataSource,
    ) -> BoxFuture<'a, Result<ChunkRef, ErasedManifestError>>;

    /// Remove one path, returning the new root.
    fn dyn_remove<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: ManifestPath,
    ) -> BoxFuture<'a, Result<ChunkRef, ErasedManifestError>>;

    /// Fold `ops` into the manifest rooted at `base`, returning the new root;
    /// each op's metadata crosses through [`ManifestMeta::from_source`].
    fn dyn_apply<'a>(
        &'a self,
        base: &'a ChunkRef,
        ops: Vec<ManifestOp<ChunkRef, Box<dyn MetadataSource>>>,
    ) -> BoxFuture<'a, Result<ChunkRef, ErasedManifestError>>;
}

impl<T: Manifest<ChunkRef>> ErasedManifest for T
where
    T::Metadata: 'static,
{
    fn dyn_empty(&self) -> BoxFuture<'_, Result<ChunkRef, ErasedManifestError>> {
        Box::pin(async move { self.empty().await.map_err(erase) })
    }

    fn dyn_get<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: &'a ManifestPath,
    ) -> BoxFuture<'a, Result<Option<MapEntry>, ErasedManifestError>> {
        Box::pin(async move { self.at(*root).get(path).await.map_err(erase) })
    }

    fn dyn_contains_key<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: &'a ManifestPath,
    ) -> BoxFuture<'a, Result<bool, ErasedManifestError>> {
        Box::pin(async move { self.at(*root).contains_key(path).await.map_err(erase) })
    }

    fn dyn_floor<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: &'a ManifestPath,
    ) -> BoxFuture<'a, Result<Option<(ManifestPath, MapEntry)>, ErasedManifestError>> {
        Box::pin(async move { self.at(*root).floor(path).await.map_err(erase) })
    }

    fn dyn_dir<'a>(
        &'a self,
        root: &'a ChunkRef,
        dir: &'a ManifestPath,
    ) -> BoxFuture<'a, Result<Listing, ErasedManifestError>> {
        Box::pin(async move { self.at(*root).dir(dir).await.map_err(erase) })
    }

    fn dyn_for_each<'a>(
        &'a self,
        root: &'a ChunkRef,
        visit: &'a mut dyn DynVisit,
    ) -> BoxFuture<'a, Result<(), ErasedManifestError>> {
        Box::pin(async move {
            let mut cursor = self.at(*root).iter().await.map_err(erase)?;
            while let Some((path, entry)) = cursor.next().await.transpose().map_err(erase)? {
                if visit.visit(path, entry).is_break() {
                    return Ok(());
                }
            }
            Ok(())
        })
    }

    fn dyn_load<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: &'a ManifestPath,
        sink: &'a mut (dyn DynWriteAt + 'a),
    ) -> BoxFuture<'a, Result<(), ErasedManifestError>> {
        Box::pin(async move { self.at(*root).load(path, sink).await.map_err(erase) })
    }

    fn dyn_site_config<'a>(
        &'a self,
        root: &'a ChunkRef,
    ) -> BoxFuture<'a, Result<SiteConfig, ErasedManifestError>> {
        Box::pin(async move { self.at(*root).site_config().await.map_err(erase) })
    }

    fn dyn_set_site_config<'a>(
        &'a self,
        root: &'a ChunkRef,
        config: SiteConfig,
    ) -> BoxFuture<'a, Result<ChunkRef, ErasedManifestError>> {
        Box::pin(async move {
            let (index, error) = config.into_parts();
            let mut batch = Batch::new();
            batch.set_index_document(index).set_error_document(error);
            self.apply(*root, batch).await.map_err(erase)
        })
    }

    fn dyn_metadata<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: &'a ManifestPath,
    ) -> BoxFuture<'a, Result<Box<dyn MetadataSource>, ErasedManifestError>> {
        Box::pin(async move {
            let meta = self.at(*root).metadata(path).await.map_err(erase)?;
            let boxed: Box<dyn MetadataSource> = Box::new(meta);
            Ok(boxed)
        })
    }

    fn dyn_insert<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: ManifestPath,
        reference: ChunkRef,
        meta: &'a dyn MetadataSource,
    ) -> BoxFuture<'a, Result<ChunkRef, ErasedManifestError>> {
        Box::pin(async move {
            let mut batch = Batch::new();
            batch.insert_with(path, reference, T::Metadata::from_source(meta));
            self.apply(*root, batch).await.map_err(erase)
        })
    }

    fn dyn_remove<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: ManifestPath,
    ) -> BoxFuture<'a, Result<ChunkRef, ErasedManifestError>> {
        Box::pin(async move { self.remove(*root, path).await.map_err(erase) })
    }

    fn dyn_apply<'a>(
        &'a self,
        base: &'a ChunkRef,
        ops: Vec<ManifestOp<ChunkRef, Box<dyn MetadataSource>>>,
    ) -> BoxFuture<'a, Result<ChunkRef, ErasedManifestError>> {
        Box::pin(async move {
            let mut batch = Batch::new();
            for op in ops {
                match op {
                    ManifestOp::Insert {
                        path,
                        reference,
                        meta,
                    } => batch.insert_with(path, reference, T::Metadata::from_source(&*meta)),
                    ManifestOp::Remove { path } => batch.remove(path),
                };
            }
            self.apply(*base, batch).await.map_err(erase)
        })
    }
}

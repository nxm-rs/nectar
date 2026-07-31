//! The object-safe wrapper: one handle over a manifest format chosen at
//! runtime.
//!
//! Erasure costs four things and states each: the futures box, the reference
//! width is fixed to [`ChunkRef`], metadata crosses as [`ManifestMetadata`]
//! rather than the format's own type, and an ordered walk stays on the static
//! path. Every erased call takes the root it reads or writes against, and a
//! failure crosses as [`ErasedManifestError`] with the seam variants intact.

use alloc::boxed::Box;
use alloc::vec::Vec;

use nectar_file::sink::DataSink;
use nectar_marker::{MaybeSend, MaybeSync};
use nectar_primitives::chunk::ChunkRef;
use nectar_primitives::store::BoxedError;
use nectar_tasks::BoxFuture;

use crate::batch::Batch;
use crate::error::{ErasedFormat, ErasedManifestError, ManifestError};
use crate::listing::Listing;
use crate::meta::ManifestMetadata;
use crate::op::ManifestOp;
use crate::path::ManifestPath;
use crate::site::SiteConfig;
use crate::view::{MapEntry, MapView};
use crate::{Manifest, SinkError};

/// A sink write that failed behind the erased seam; the concrete error
/// survives as the source.
#[derive(Debug, thiserror::Error)]
#[error("erased sink write failed")]
pub struct DynSinkError(#[source] BoxedError);

/// Object-safe [`DataSink`]: the same positional, idempotent write with the
/// error boxed.
///
/// Blanket-implemented, so any sink is usable through the erased seam.
pub trait DynSink: MaybeSend {
    /// Write `data` at absolute byte `offset`, growing the sink as needed.
    fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<(), DynSinkError>;
}

impl<K: DataSink<Error: SinkError> + MaybeSend> DynSink for K {
    fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<(), DynSinkError> {
        DataSink::write_at(self, offset, data).map_err(|source| DynSinkError(Box::new(source)))
    }
}

/// An erased sink borrowed back into the static [`DataSink`] the format's own
/// load expects.
struct SinkBridge<'a>(&'a mut dyn DynSink);

impl DataSink for SinkBridge<'_> {
    type Error = DynSinkError;

    fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<(), Self::Error> {
        self.0.write_at(offset, data)
    }
}

/// A typed seam failure with its format union boxed.
fn erase<F: core::error::Error + MaybeSend + MaybeSync + 'static>(
    error: ManifestError<F>,
) -> ErasedManifestError {
    error.map_format(|format| ErasedFormat(Box::new(format)))
}

/// Object-safe [`Manifest`]: a manifest whose format is decided at runtime.
///
/// Blanket-implemented for every `Manifest<ChunkRef>`, so a format implements
/// the static trait once and is held as `Box<dyn DynManifest>` for free.
pub trait DynManifest: MaybeSend + MaybeSync {
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

    /// Write the data bound to `path` into `sink`, starting at offset zero.
    fn dyn_load<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: &'a ManifestPath,
        sink: &'a mut dyn DynSink,
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

    /// Insert one path, returning the new root.
    fn dyn_insert<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: ManifestPath,
        reference: ChunkRef,
        meta: Box<dyn ManifestMetadata>,
    ) -> BoxFuture<'a, Result<ChunkRef, ErasedManifestError>>;

    /// Remove one path, returning the new root.
    fn dyn_remove<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: ManifestPath,
    ) -> BoxFuture<'a, Result<ChunkRef, ErasedManifestError>>;

    /// Fold `ops` into the manifest rooted at `base`, returning the new root.
    ///
    /// Each op's metadata is rebuilt into the format's native type from the
    /// registered keys of the well-known-key view, so a custom key, or one the
    /// format cannot carry, is dropped here.
    fn dyn_apply<'a>(
        &'a self,
        base: &'a ChunkRef,
        ops: Vec<ManifestOp<ChunkRef, Box<dyn ManifestMetadata>>>,
    ) -> BoxFuture<'a, Result<ChunkRef, ErasedManifestError>>;
}

impl<T: Manifest<ChunkRef>> DynManifest for T {
    fn dyn_empty(&self) -> BoxFuture<'_, Result<ChunkRef, ErasedManifestError>> {
        Box::pin(async move { self.empty().await.map_err(erase) })
    }

    fn dyn_get<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: &'a ManifestPath,
    ) -> BoxFuture<'a, Result<Option<MapEntry>, ErasedManifestError>> {
        Box::pin(async move { self.at(root).get(path).await.map_err(erase) })
    }

    fn dyn_contains_key<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: &'a ManifestPath,
    ) -> BoxFuture<'a, Result<bool, ErasedManifestError>> {
        Box::pin(async move { self.at(root).contains_key(path).await.map_err(erase) })
    }

    fn dyn_floor<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: &'a ManifestPath,
    ) -> BoxFuture<'a, Result<Option<(ManifestPath, MapEntry)>, ErasedManifestError>> {
        Box::pin(async move { self.at(root).floor(path).await.map_err(erase) })
    }

    fn dyn_dir<'a>(
        &'a self,
        root: &'a ChunkRef,
        dir: &'a ManifestPath,
    ) -> BoxFuture<'a, Result<Listing, ErasedManifestError>> {
        Box::pin(async move { self.at(root).dir(dir).await.map_err(erase) })
    }

    fn dyn_load<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: &'a ManifestPath,
        sink: &'a mut dyn DynSink,
    ) -> BoxFuture<'a, Result<(), ErasedManifestError>> {
        Box::pin(async move {
            let mut bridge = SinkBridge(sink);
            self.at(root).load(path, &mut bridge).await.map_err(erase)
        })
    }

    fn dyn_site_config<'a>(
        &'a self,
        root: &'a ChunkRef,
    ) -> BoxFuture<'a, Result<SiteConfig, ErasedManifestError>> {
        Box::pin(async move { self.at(root).site_config().await.map_err(erase) })
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

    fn dyn_insert<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: ManifestPath,
        reference: ChunkRef,
        meta: Box<dyn ManifestMetadata>,
    ) -> BoxFuture<'a, Result<ChunkRef, ErasedManifestError>> {
        Box::pin(async move {
            let meta = self.metadata_from_view(&*meta).map_err(erase)?;
            let mut batch = Batch::new();
            batch.insert_with(path, reference, meta);
            self.apply(*root, batch).await.map_err(erase)
        })
    }

    fn dyn_remove<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: ManifestPath,
    ) -> BoxFuture<'a, Result<ChunkRef, ErasedManifestError>> {
        Box::pin(async move { self.remove(root, path).await.map_err(erase) })
    }

    fn dyn_apply<'a>(
        &'a self,
        base: &'a ChunkRef,
        ops: Vec<ManifestOp<ChunkRef, Box<dyn ManifestMetadata>>>,
    ) -> BoxFuture<'a, Result<ChunkRef, ErasedManifestError>> {
        Box::pin(async move {
            let mut batch = Batch::new();
            for op in ops {
                match op {
                    ManifestOp::Insert {
                        path,
                        reference,
                        meta,
                    } => {
                        let meta = self.metadata_from_view(&*meta).map_err(erase)?;
                        batch.insert_with(path, reference, meta)
                    }
                    ManifestOp::Remove { path } => batch.remove(path),
                };
            }
            self.apply(*base, batch).await.map_err(erase)
        })
    }
}

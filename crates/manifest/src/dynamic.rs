//! The object-safe wrapper: one handle over a manifest format chosen at
//! runtime.
//!
//! Erasure costs four things and states each: the futures box, the reference
//! width is fixed to [`ChunkRef`], metadata crosses as [`ManifestMetadata`]
//! rather than the format's own type, and an ordered walk stays on the static
//! path, because a cursor borrows the view it came from. Everything else is
//! the static path verbatim, root argument included: an erased call cannot
//! hold a handle, so it takes the root it reads or writes against.

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::fmt;

use nectar_file::sink::DataSink;
use nectar_marker::{MaybeSend, MaybeSync};
use nectar_primitives::chunk::ChunkRef;
use nectar_primitives::store::BoxedError;
use nectar_tasks::BoxFuture;

use crate::listing::Listing;
use crate::meta::ManifestMetadata;
use crate::op::ManifestOp;
use crate::path::ManifestPath;
use crate::view::{MapEntry, MapView};
use crate::writer::MapWriter;
use crate::{Manifest, SinkError};

/// A sink write that failed behind the erased seam; the concrete error
/// survives as the source.
#[derive(Debug)]
pub struct DynSinkError(BoxedError);

impl fmt::Display for DynSinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("erased sink write failed")
    }
}

impl core::error::Error for DynSinkError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        Some(&*self.0)
    }
}

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

/// Box a typed error behind the erased seam.
fn erase<E: core::error::Error + MaybeSend + MaybeSync + 'static>(error: E) -> BoxedError {
    Box::new(error)
}

/// Object-safe [`Manifest`]: a manifest whose format is decided at runtime.
///
/// Blanket-implemented for every `Manifest<ChunkRef>`, so a format implements
/// the static trait once and is held as `Box<dyn DynManifest>` for free.
pub trait DynManifest: MaybeSend + MaybeSync {
    /// The entry bound to `path`, or `None` when the path is absent.
    fn dyn_get<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: &'a ManifestPath,
    ) -> BoxFuture<'a, Result<Option<MapEntry>, BoxedError>>;

    /// Whether `path` is bound.
    fn dyn_contains_key<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: &'a ManifestPath,
    ) -> BoxFuture<'a, Result<bool, BoxedError>>;

    /// The immediate children of the directory `dir` names, in path order.
    fn dyn_dir<'a>(
        &'a self,
        root: &'a ChunkRef,
        dir: &'a ManifestPath,
    ) -> BoxFuture<'a, Result<Listing, BoxedError>>;

    /// Write the data bound to `path` into `sink`, starting at offset zero.
    fn dyn_load<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: &'a ManifestPath,
        sink: &'a mut dyn DynSink,
    ) -> BoxFuture<'a, Result<(), BoxedError>>;

    /// Insert one path into the manifest rooted at `root`, returning the new
    /// root.
    fn dyn_insert<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: ManifestPath,
        reference: ChunkRef,
        meta: Box<dyn ManifestMetadata>,
    ) -> BoxFuture<'a, Result<ChunkRef, BoxedError>>;

    /// Remove one path from the manifest rooted at `root`, returning the new
    /// root.
    fn dyn_remove<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: ManifestPath,
    ) -> BoxFuture<'a, Result<ChunkRef, BoxedError>>;

    /// Fold `ops` into the manifest rooted at `base`, returning the new root.
    ///
    /// Each op's metadata is rebuilt into the format's native type from the
    /// registered keys of the well-known-key view, so a custom key, or one the
    /// format cannot carry, is dropped here.
    fn dyn_apply<'a>(
        &'a self,
        base: &'a ChunkRef,
        ops: Vec<ManifestOp<ChunkRef, Box<dyn ManifestMetadata>>>,
    ) -> BoxFuture<'a, Result<ChunkRef, BoxedError>>;
}

impl<T: Manifest<ChunkRef>> DynManifest for T {
    fn dyn_get<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: &'a ManifestPath,
    ) -> BoxFuture<'a, Result<Option<MapEntry>, BoxedError>> {
        Box::pin(async move { self.at(root).get(path).await.map_err(erase) })
    }

    fn dyn_contains_key<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: &'a ManifestPath,
    ) -> BoxFuture<'a, Result<bool, BoxedError>> {
        Box::pin(async move { self.at(root).contains_key(path).await.map_err(erase) })
    }

    fn dyn_dir<'a>(
        &'a self,
        root: &'a ChunkRef,
        dir: &'a ManifestPath,
    ) -> BoxFuture<'a, Result<Listing, BoxedError>> {
        Box::pin(async move { self.at(root).dir(dir).await.map_err(erase) })
    }

    fn dyn_load<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: &'a ManifestPath,
        sink: &'a mut dyn DynSink,
    ) -> BoxFuture<'a, Result<(), BoxedError>> {
        Box::pin(async move {
            let mut bridge = SinkBridge(sink);
            self.at(root).load(path, &mut bridge).await.map_err(erase)
        })
    }

    fn dyn_insert<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: ManifestPath,
        reference: ChunkRef,
        meta: Box<dyn ManifestMetadata>,
    ) -> BoxFuture<'a, Result<ChunkRef, BoxedError>> {
        Box::pin(async move {
            let meta = self.metadata_from_view(&*meta).map_err(erase)?;
            let mut writer = self.edit(root);
            writer.insert(path, reference).meta(meta);
            writer.commit().await.map_err(erase)
        })
    }

    fn dyn_remove<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: ManifestPath,
    ) -> BoxFuture<'a, Result<ChunkRef, BoxedError>> {
        Box::pin(async move { self.remove(root, path).await.map_err(erase) })
    }

    fn dyn_apply<'a>(
        &'a self,
        base: &'a ChunkRef,
        ops: Vec<ManifestOp<ChunkRef, Box<dyn ManifestMetadata>>>,
    ) -> BoxFuture<'a, Result<ChunkRef, BoxedError>> {
        Box::pin(async move {
            let mut native = Vec::with_capacity(ops.len());
            for op in ops {
                native.push(match op {
                    ManifestOp::Insert {
                        path,
                        reference,
                        meta,
                    } => ManifestOp::Insert {
                        path,
                        reference,
                        meta: self.metadata_from_view(&*meta).map_err(erase)?,
                    },
                    ManifestOp::Remove { path } => ManifestOp::Remove { path },
                });
            }
            self.apply(base, native).await.map_err(erase)
        })
    }
}

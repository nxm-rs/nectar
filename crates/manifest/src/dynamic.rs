//! The object-safe wrapper: one handle over a manifest format chosen at
//! runtime.
//!
//! Erasure costs three things and states each: the futures box, the reference
//! width is fixed to [`ChunkRef`], and metadata crosses as
//! [`ManifestMetadata`] rather than the format's own type. Everything else is
//! the static path verbatim.

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
    /// The immediate children of the directory `dir` names, in path order.
    fn dyn_list<'a>(
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

    /// Fold `ops` into the manifest rooted at `base`, returning the new root.
    ///
    /// Each op's metadata is rebuilt into the format's native type through the
    /// well-known-key view, so a key the format cannot carry is dropped here.
    fn dyn_apply<'a>(
        &'a self,
        base: &'a ChunkRef,
        ops: Vec<ManifestOp<ChunkRef, Box<dyn ManifestMetadata>>>,
    ) -> BoxFuture<'a, Result<ChunkRef, BoxedError>>;
}

impl<T: Manifest<ChunkRef>> DynManifest for T {
    fn dyn_list<'a>(
        &'a self,
        root: &'a ChunkRef,
        dir: &'a ManifestPath,
    ) -> BoxFuture<'a, Result<Listing, BoxedError>> {
        Box::pin(async move { self.list(root, dir).await.map_err(erase) })
    }

    fn dyn_load<'a>(
        &'a self,
        root: &'a ChunkRef,
        path: &'a ManifestPath,
        sink: &'a mut dyn DynSink,
    ) -> BoxFuture<'a, Result<(), BoxedError>> {
        Box::pin(async move {
            let mut bridge = SinkBridge(sink);
            self.load(root, path, &mut bridge).await.map_err(erase)
        })
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
                    ManifestOp::Put {
                        path,
                        reference,
                        meta,
                    } => ManifestOp::Put {
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

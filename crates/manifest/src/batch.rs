//! The seam-owned write batch [`Manifest::apply`](crate::Manifest::apply) folds in.

use alloc::vec::Vec;

use nectar_primitives::chunk::{ChunkRef, Reference};

use crate::op::ManifestOp;
use crate::path::ManifestPath;
use crate::reserved::ReservedKey;

/// Staged manifest writes, applied atomically against one base root.
///
/// Staging is infallible and touches no storage; the first reserved path is
/// recorded and [`into_checked`](Self::into_checked) refuses the whole batch
/// with it. The site documents are a delta: untouched means unchanged.
#[derive(Debug)]
pub struct Batch<R: Reference = ChunkRef, M = ()> {
    ops: Vec<ManifestOp<R, M>>,
    index_document: Option<Option<ManifestPath>>,
    error_document: Option<Option<ManifestPath>>,
    reserved: Option<ReservedKey>,
}

impl<R: Reference, M> Default for Batch<R, M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R: Reference, M> Batch<R, M> {
    /// An empty batch.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            ops: Vec::new(),
            index_document: None,
            error_document: None,
            reserved: None,
        }
    }

    /// Whether nothing at all was submitted; a recorded refusal counts.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.ops.is_empty()
            && self.index_document.is_none()
            && self.error_document.is_none()
            && self.reserved.is_none()
    }

    fn stage(&mut self, op: ManifestOp<R, M>) -> &mut Self {
        if op.path().is_reserved() {
            self.reserved
                .get_or_insert_with(|| ReservedKey::new(op.path().clone()));
        } else {
            self.ops.push(op);
        }
        self
    }

    /// Stage `path` bound to `reference`; an insert replaces the whole
    /// binding, metadata included.
    pub fn insert(&mut self, path: ManifestPath, reference: R) -> &mut Self
    where
        M: Default,
    {
        self.insert_with(path, reference, M::default())
    }

    /// Stage `path` bound to `reference`, carrying `meta`.
    pub fn insert_with(&mut self, path: ManifestPath, reference: R, meta: M) -> &mut Self {
        self.stage(ManifestOp::Insert {
            path,
            reference,
            meta,
        })
    }

    /// Stage the exact-key removal of `path`; an absent path is a no-op.
    pub fn remove(&mut self, path: ManifestPath) -> &mut Self {
        self.stage(ManifestOp::Remove { path })
    }

    /// Stage a ready-made op list, in order.
    pub fn extend(&mut self, ops: impl IntoIterator<Item = ManifestOp<R, M>>) -> &mut Self {
        ops.into_iter().fold(self, |batch, op| batch.stage(op))
    }

    /// Set the site index document (a bare filename, `index.html` not
    /// `/index.html`), or clear it with `None`.
    pub fn set_index_document(&mut self, path: impl Into<Option<ManifestPath>>) -> &mut Self {
        self.index_document = Some(path.into());
        self
    }

    /// Set the site error document (one whole content path), or clear it with
    /// `None`.
    pub fn set_error_document(&mut self, path: impl Into<Option<ManifestPath>>) -> &mut Self {
        self.error_document = Some(path.into());
        self
    }

    /// The batch a format applies, or the recorded reserved refusal: a
    /// format's apply only ever sees content keys.
    pub fn into_checked(self) -> Result<CheckedBatch<R, M>, ReservedKey> {
        match self.reserved {
            Some(reserved) => Err(reserved),
            None => Ok(CheckedBatch {
                ops: self.ops,
                index_document: self.index_document,
                error_document: self.error_document,
            }),
        }
    }
}

/// A batch past the reserved check: every op names a content key. Built by
/// [`Batch::into_checked`] alone.
#[derive(Debug)]
#[non_exhaustive]
pub struct CheckedBatch<R: Reference = ChunkRef, M = ()> {
    /// The content ops, in submission order.
    pub ops: Vec<ManifestOp<R, M>>,
    /// The staged index document: untouched as `None`, cleared as `Some(None)`.
    pub index_document: Option<Option<ManifestPath>>,
    /// The staged error document, with the same shape.
    pub error_document: Option<Option<ManifestPath>>,
}

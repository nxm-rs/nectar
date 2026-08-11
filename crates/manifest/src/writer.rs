//! The write handle: stage a batch against one base root, commit to a new one.

use core::fmt;
use core::future::Future;

use nectar_marker::{MaybeSend, MaybeSync};
use nectar_primitives::chunk::{ChunkRef, Reference};

use crate::op::ManifestOp;
use crate::path::ManifestPath;

/// A staged insert, awaiting the metadata it may carry.
///
/// The op is staged when the guard drops: `writer.insert(path, r).meta(m);`.
pub struct Insert<'w, W: MapWriter<R>, R: Reference + MaybeSend> {
    writer: &'w mut W,
    pending: Option<(ManifestPath, R)>,
    /// The format's default when none is given.
    meta: Option<W::Metadata>,
}

impl<'w, W: MapWriter<R>, R: Reference + MaybeSend> Insert<'w, W, R> {
    const fn new(writer: &'w mut W, path: ManifestPath, reference: R) -> Self {
        Self {
            writer,
            pending: Some((path, reference)),
            meta: None,
        }
    }

    /// Attach `meta`, in the format's own vocabulary.
    pub fn meta(&mut self, meta: W::Metadata) -> &mut Self {
        self.meta = Some(meta);
        self
    }
}

impl<W: MapWriter<R>, R: Reference + MaybeSend> fmt::Debug for Insert<'_, W, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Insert")
            .field("path", &self.pending.as_ref().map(|(path, _)| path))
            .finish_non_exhaustive()
    }
}

impl<W: MapWriter<R>, R: Reference + MaybeSend> Drop for Insert<'_, W, R> {
    fn drop(&mut self) {
        if let Some((path, reference)) = self.pending.take() {
            self.writer.stage(ManifestOp::Insert {
                path,
                reference,
                meta: self.meta.take().unwrap_or_default(),
            });
        }
    }
}

/// The write handle of a manifest, bound to one base root.
///
/// Staging touches no storage: the ops accumulate, and
/// [`commit`](Self::commit) writes them in one pass.
pub trait MapWriter<R: Reference + MaybeSend = ChunkRef>: MaybeSend + Sized {
    /// The format's own metadata for one entry.
    type Metadata: MaybeSend + Default;

    /// Error type a commit fails with.
    type Error: core::error::Error + MaybeSend + MaybeSync + 'static;

    /// Record one op into the batch.
    fn stage(&mut self, op: ManifestOp<R, Self::Metadata>);

    /// Stage `path` bound to `reference`, with metadata as a suffix.
    ///
    /// An insert replaces the whole binding: it clears existing metadata
    /// unless [`meta`](Insert::meta) is given. A reserved path fails the whole
    /// batch at commit with [`ReservedKey`](crate::ReservedKey).
    fn insert(&mut self, path: ManifestPath, reference: R) -> Insert<'_, Self, R> {
        Insert::new(self, path, reference)
    }

    /// Set the site index document, or clear it with `None`.
    ///
    /// It lands in the format's own root slot, not under a path. The value is
    /// a filename joined below each directory: `index.html`, not
    /// `/index.html`.
    fn with_index_document(&mut self, path: impl Into<Option<ManifestPath>>) -> &mut Self;

    /// Set the site error document, one whole content path, or clear it with
    /// `None`.
    fn with_error_document(&mut self, path: impl Into<Option<ManifestPath>>) -> &mut Self;

    /// Stage the removal of `path`.
    ///
    /// Exact-key: the path's own value and metadata go, and no other path
    /// does. An absent path is a no-op. A reserved path fails the batch at
    /// commit with [`ReservedKey`](crate::ReservedKey).
    fn remove(&mut self, path: ManifestPath) -> &mut Self {
        self.stage(ManifestOp::Remove { path });
        self
    }

    /// Stage a ready-made batch.
    fn extend(
        &mut self,
        ops: impl IntoIterator<Item = ManifestOp<R, Self::Metadata>>,
    ) -> &mut Self {
        for op in ops {
            self.stage(op);
        }
        self
    }

    /// Write the batch, returning the root it produced.
    ///
    /// The whole batch lands or none of it does.
    fn commit(self) -> impl Future<Output = Result<R, Self::Error>> + MaybeSend;
}

//! The write handle: stage a batch against one base root, commit to a new one.
//!
//! A manifest is immutable, so a write never mutates the base: the batch lands
//! as a whole and [`MapWriter::commit`] hands back the root it produced. The
//! base root stays readable for as long as its chunks do.

use core::fmt;
use core::future::Future;

use nectar_marker::{MaybeSend, MaybeSync};
use nectar_primitives::chunk::{ChunkRef, Reference};

use crate::op::ManifestOp;
use crate::path::ManifestPath;

/// A staged insert, awaiting the metadata it may carry.
///
/// [`MapWriter::insert`] hands one back so metadata reads as a suffix on the
/// insert it belongs to: `writer.insert(path, reference).meta(meta);`. The op
/// is staged when the guard is dropped, which is the end of that statement,
/// so an insert with no metadata needs nothing extra.
pub struct Insert<'w, W: MapWriter<R>, R: Reference + MaybeSend> {
    writer: &'w mut W,
    /// The staged path and reference, taken by the drop that records them.
    pending: Option<(ManifestPath, R)>,
    /// Metadata to attach; the format's default when none is given.
    meta: Option<W::Metadata>,
}

impl<'w, W: MapWriter<R>, R: Reference + MaybeSend> Insert<'w, W, R> {
    /// Stage `path` bound to `reference` on `writer`.
    const fn new(writer: &'w mut W, path: ManifestPath, reference: R) -> Self {
        Self {
            writer,
            pending: Some((path, reference)),
            meta: None,
        }
    }

    /// Attach `meta` to the insert, in the format's own vocabulary.
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
    ///
    /// The primitive [`insert`](Self::insert) and [`remove`](Self::remove) are
    /// written in terms of; a caller with a ready-made batch stages it with
    /// [`extend`](Self::extend).
    fn stage(&mut self, op: ManifestOp<R, Self::Metadata>);

    /// Stage `path` bound to `reference`, with metadata as a suffix.
    ///
    /// An insert replaces the whole binding, so it clears existing metadata
    /// unless [`meta`](Insert::meta) is given. A caller that means to keep
    /// metadata restates it.
    ///
    /// A reserved path binds nothing: staging is infallible, so the commit fails
    /// with [`ReservedKey`] and refuses the whole batch. The format's root slot
    /// is reached through [`with_index_document`](Self::with_index_document) and
    /// [`with_error_document`](Self::with_error_document) alone.
    ///
    /// [`ReservedKey`]: crate::ReservedKey
    fn insert(&mut self, path: ManifestPath, reference: R) -> Insert<'_, Self, R> {
        Insert::new(self, path, reference)
    }

    /// Set the site index document, or clear it with `None`.
    ///
    /// Chainable, and it lands in the format's own root slot rather than under a
    /// path, so the map surfaces no key for it.
    ///
    /// The value is a filename joined below each directory, so it stays
    /// relative: `index.html`, not `/index.html`.
    fn with_index_document(&mut self, path: impl Into<Option<ManifestPath>>) -> &mut Self;

    /// Set the site error document, or clear it with `None`.
    ///
    /// Chainable, and it lands in the same root slot
    /// [`with_index_document`](Self::with_index_document) does. The value is one
    /// whole content path.
    fn with_error_document(&mut self, path: impl Into<Option<ManifestPath>>) -> &mut Self;

    /// Stage the removal of `path`.
    ///
    /// Exact-key on either format, as `HashMap::remove` is: the path's own value
    /// and metadata go, and no other path does. A path with children keeps every
    /// one of them, and a childless leaf is pruned.
    ///
    /// Removing an unbound or absent path is a no-op, so the commit hands the
    /// base root back. A reserved path is no key at all, so the commit fails
    /// with [`ReservedKey`]; clear the site documents through their own setters.
    ///
    /// [`ReservedKey`]: crate::ReservedKey
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
    /// The whole batch lands or none of it does, so a caller never observes a
    /// half-applied root. A batch that staged a reserved path fails here with
    /// [`ReservedKey`](crate::ReservedKey) and writes nothing at all.
    fn commit(self) -> impl Future<Output = Result<R, Self::Error>> + MaybeSend;
}

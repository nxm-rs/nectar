//! The write handle: stage a batch against one base root, commit to a new one.
//!
//! A manifest is immutable, so a write never mutates the base: the batch lands
//! as a whole and [`MapWriter::commit`] hands back the root it produced. The
//! base root stays readable for as long as its chunks do.

use alloc::string::String;
use core::fmt;
use core::future::Future;

use nectar_marker::{MaybeSend, MaybeSync};
use nectar_primitives::chunk::{ChunkRef, Reference};

use crate::meta::{MetadataView, WellKnownKey};
use crate::op::ManifestOp;
use crate::path::ManifestPath;

/// A staged insert, awaiting the metadata it may carry.
///
/// [`MapWriter::insert`] hands one back so metadata reads as a suffix on the
/// insert it belongs to: `writer.insert(path, reference).meta(meta);`. The op
/// is staged when the guard is dropped, which is the end of that statement,
/// so an insert with no metadata needs nothing extra.
///
/// The `with_*` builders are the typed spelling of the same suffix, and chain:
/// `writer.insert(path, reference).with_content_type("text/html");`. On the
/// root path they are how the site documents are set, because those are
/// well-known metadata on the root entry and nothing else:
/// `writer.insert(ManifestPath::root(), root).with_index_document("index.html");`
pub struct Insert<'w, W: MapWriter<R>, R: Reference + MaybeSend> {
    writer: &'w mut W,
    /// The staged path and reference, taken by the drop that records them.
    pending: Option<(ManifestPath, R)>,
    /// Metadata to attach; the format's default when none is given.
    meta: Option<W::Metadata>,
    /// Well-known keys the `with_*` builders set, converted on drop.
    view: MetadataView,
}

impl<'w, W: MapWriter<R>, R: Reference + MaybeSend> Insert<'w, W, R> {
    /// Stage `path` bound to `reference` on `writer`.
    const fn new(writer: &'w mut W, path: ManifestPath, reference: R) -> Self {
        Self {
            writer,
            pending: Some((path, reference)),
            meta: None,
            view: MetadataView::new(),
        }
    }

    /// Attach `meta` to the insert, in the format's own vocabulary.
    ///
    /// A `with_*` builder on the same insert wins, because it names the keys the
    /// format then rebuilds the metadata from.
    pub fn meta(&mut self, meta: W::Metadata) -> &mut Self {
        self.meta = Some(meta);
        self
    }

    /// Set the well-known `key` on the insert's metadata.
    pub fn with(&mut self, key: WellKnownKey<'_>, value: impl Into<String>) -> &mut Self {
        self.view.set(key, value);
        self
    }

    /// Set the entry's content type.
    pub fn with_content_type(&mut self, value: impl Into<String>) -> &mut Self {
        self.with(WellKnownKey::ContentType, value)
    }

    /// Set the site index document, served for a directory path.
    ///
    /// Root-scope metadata: set it on the insert at [`ManifestPath::root`].
    pub fn with_index_document(&mut self, value: impl Into<String>) -> &mut Self {
        self.with(WellKnownKey::IndexDocument, value)
    }

    /// Set the site error document, served for an unresolved path.
    ///
    /// Root-scope metadata: set it on the insert at [`ManifestPath::root`].
    pub fn with_error_document(&mut self, value: impl Into<String>) -> &mut Self {
        self.with(WellKnownKey::ErrorDocument, value)
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
            let meta = if self.view.is_empty() {
                self.meta.take().unwrap_or_default()
            } else {
                self.writer.native(&self.view)
            };
            self.writer.stage(ManifestOp::Insert {
                path,
                reference,
                meta,
            });
        }
    }
}

/// The write handle of a manifest, bound to one base root.
///
/// Staging touches no storage: the ops accumulate, and
/// [`commit`](Self::commit) writes them in one pass. What a repeated path
/// stages is the format's own business, so the batch is built the way the
/// caller means it to land.
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

    /// The format's own metadata for the well-known keys `view` carries.
    ///
    /// What the `with_*` builders on [`Insert`] convert through, and lossy the
    /// same way [`Manifest::metadata_from_view`] is: a key or a value the format
    /// cannot represent is dropped rather than failing, because the metadata
    /// rides a guard that stages when it is dropped.
    ///
    /// [`Manifest::metadata_from_view`]: crate::Manifest::metadata_from_view
    fn native(&self, view: &MetadataView) -> Self::Metadata;

    /// Stage `path` bound to `reference`, with metadata as a suffix.
    ///
    /// An insert replaces the whole binding; existing metadata is cleared
    /// unless [`meta`](Insert::meta) is given. This is the map contract: a
    /// bare insert is the value the path holds from then on, so a caller that
    /// means to keep metadata restates it.
    fn insert(&mut self, path: ManifestPath, reference: R) -> Insert<'_, Self, R> {
        Insert::new(self, path, reference)
    }

    /// Stage the removal of `path`.
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
    /// The whole batch lands or none of it does: a caller never observes a
    /// half-applied root, because the root only exists once the batch is
    /// written.
    fn commit(self) -> impl Future<Output = Result<R, Self::Error>> + MaybeSend;
}

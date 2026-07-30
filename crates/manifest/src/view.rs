//! The read handle: one immutable root, the map verbs over it.
//!
//! A view is bound to a root at construction, so no read method carries a root
//! argument. It holds a reference to the store and a clone of the root, which
//! makes it cheap enough to build per lookup.

use core::future::Future;
use core::ops::RangeBounds;

use nectar_marker::{MaybeSend, MaybeSync};
use nectar_primitives::chunk::{ChunkRef, Reference};

use crate::listing::Listing;
use crate::path::ManifestPath;
use crate::site::SiteConfig;
use crate::{DataSink, SinkError};

/// What a bound path resolves to.
///
/// A reference names a chunk the caller can read on its own. Everything else
/// is opaque: the manifest carries the value itself, or names it at a
/// reference width the caller did not ask for. Either way the bytes are
/// reachable with [`MapView::load`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MapEntry<R: Reference = ChunkRef> {
    /// The path resolves to this chunk reference.
    Reference(R),
    /// The path is bound, but not to a reference of the requested width.
    Opaque,
}

impl<R: Reference> MapEntry<R> {
    /// The bound reference, or `None` when the entry is opaque.
    #[must_use]
    pub const fn reference(&self) -> Option<&R> {
        match self {
            Self::Reference(reference) => Some(reference),
            Self::Opaque => None,
        }
    }

    /// Whether the entry names no reference of the requested width.
    #[must_use]
    pub const fn is_opaque(&self) -> bool {
        matches!(self, Self::Opaque)
    }
}

/// An ordered walk over a view, one `(path, entry)` at a time.
///
/// Pulled rather than streamed: a walk fetches the manifest nodes on its
/// frontier, so peak retained state is a function of depth, not of key count.
pub trait MapCursor<R: Reference + MaybeSend = ChunkRef>: MaybeSend {
    /// Error type a walk fails with.
    type Error: core::error::Error + MaybeSend + MaybeSync + 'static;

    /// The next `(path, entry)` in path order, or `None` at the end.
    fn next(
        &mut self,
    ) -> impl Future<Output = Result<Option<(ManifestPath, MapEntry<R>)>, Self::Error>> + MaybeSend;
}

/// The read view of a manifest, bound to one immutable root.
///
/// The vocabulary is the standard map one: [`get`](Self::get),
/// [`contains_key`](Self::contains_key), [`range`](Self::range),
/// [`iter`](Self::iter) and the ordered-map [`floor`](Self::floor).
/// [`dir`](Self::dir) and [`load`](Self::load) are the two manifest additions:
/// a path is read as a directory of paths, and an entry's bytes are joined into
/// a sink.
///
/// Every one of those reads is over content paths alone. The manifest's own
/// configuration is read through [`index_document`](Self::index_document) and
/// [`error_document`](Self::error_document), which answer `None` when the
/// manifest declares nothing: no walk yields the slot they live in, and no read
/// reaches a reserved path, which is a listing prefix or a root slot rather
/// than a key.
pub trait MapView<R: Reference + MaybeSend = ChunkRef>: MaybeSend {
    /// The format's own metadata for one entry.
    type Metadata: MaybeSend + Default;

    /// Error type for every read on the view.
    type Error: core::error::Error + MaybeSend + MaybeSync + 'static;

    /// The ordered walk [`iter`](Self::iter) and [`range`](Self::range) hand
    /// back.
    type Cursor: MapCursor<R, Error = Self::Error>;

    /// The entry bound to `path`, or `None` when the path is absent.
    ///
    /// A reserved path is absent by definition: it is a prefix or a root slot,
    /// not a key. See [`ManifestPath::is_reserved`].
    fn get(
        &self,
        path: &ManifestPath,
    ) -> impl Future<Output = Result<Option<MapEntry<R>>, Self::Error>> + MaybeSend;

    /// The site-level documents the manifest declares, read from the format's
    /// own root slot.
    ///
    /// One read answers both, which is why it is the primitive
    /// [`index_document`](Self::index_document) and
    /// [`error_document`](Self::error_document) are written in terms of.
    fn site_config(&self) -> impl Future<Output = Result<SiteConfig, Self::Error>> + MaybeSend;

    /// The index document the manifest declares, or `None` when it declares
    /// none.
    ///
    /// A gateway serves it for a directory path. It is a filename joined below
    /// each directory rather than one whole path, which is the convention the
    /// reference client reads.
    fn index_document(
        &self,
    ) -> impl Future<Output = Result<Option<ManifestPath>, Self::Error>> + MaybeSend {
        let config = self.site_config();
        async move { Ok(config.await?.into_parts().0) }
    }

    /// The error document the manifest declares, or `None` when it declares
    /// none.
    ///
    /// A gateway serves it for a path that resolves to nothing. It is one whole
    /// content path.
    fn error_document(
        &self,
    ) -> impl Future<Output = Result<Option<ManifestPath>, Self::Error>> + MaybeSend {
        let config = self.site_config();
        async move { Ok(config.await?.into_parts().1) }
    }

    /// The metadata bound to `path`, in the format's own vocabulary.
    ///
    /// A path that carries none reads back as the format's empty metadata, and
    /// so does an absent path: this is not a presence answer, so ask
    /// [`contains_key`](Self::contains_key) for that.
    fn metadata(
        &self,
        path: &ManifestPath,
    ) -> impl Future<Output = Result<Self::Metadata, Self::Error>> + MaybeSend;

    /// Whether `path` is bound.
    fn contains_key(
        &self,
        path: &ManifestPath,
    ) -> impl Future<Output = Result<bool, Self::Error>> + MaybeSend {
        let found = self.get(path);
        async move { Ok(found.await?.is_some()) }
    }

    /// The immediate children of the directory `dir` names, in path order.
    ///
    /// Deeper paths collapse into one [`ListEntry::Dir`] at the next
    /// separator; the referenced chunks are never fetched. `dir` is matched as
    /// a byte prefix, so end it in the separator to mean the directory: `img`
    /// also lists `imgx.png`, where `img/` does not. The empty path is the
    /// prefix every content path carries, so it lists the top level.
    ///
    /// [`ListEntry::Dir`]: crate::ListEntry::Dir
    fn dir(
        &self,
        dir: &ManifestPath,
    ) -> impl Future<Output = Result<Listing<R>, Self::Error>> + MaybeSend;

    /// The greatest bound path `<= path`, with its entry, or `None` when every
    /// bound path is greater.
    ///
    /// The default walks [`range`](Self::range) up to and including `path` and
    /// keeps the last item, which every format can serve. A format with an
    /// ordered seek overrides it and pays O(depth) instead.
    fn floor(
        &self,
        path: &ManifestPath,
    ) -> impl Future<Output = Result<Option<(ManifestPath, MapEntry<R>)>, Self::Error>> + MaybeSend
    {
        let walk = self.range(..=path.clone());
        async move {
            let mut cursor = walk.await?;
            let mut last = None;
            while let Some(item) = cursor.next().await? {
                last = Some(item);
            }
            Ok(last)
        }
    }

    /// Write the data bound to `path` into `sink`, starting at offset zero.
    ///
    /// The sink's writes are idempotent overwrites, so a failed load is
    /// recovered by running it again in full.
    ///
    /// Feature-gated: the bytes are joined through the file pipeline, so a
    /// format's view carries `load` only with the `nectar-file` dependency its
    /// `manifest` feature pulls in.
    fn load<K: DataSink<Error: SinkError> + MaybeSend>(
        &self,
        path: &ManifestPath,
        sink: &mut K,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend;

    /// Every bound content path, with its entry, in path order.
    ///
    /// Content only: the format's root slot is not a path, so a walk never
    /// yields it or the empty path.
    fn iter(&self) -> impl Future<Output = Result<Self::Cursor, Self::Error>> + MaybeSend;

    /// Every `(path, entry)` within `bounds`, in path order.
    ///
    /// Paths order as byte strings, so every bound is exact: an excluded bound
    /// is the included one with a zero byte appended.
    fn range(
        &self,
        bounds: impl RangeBounds<ManifestPath> + MaybeSend,
    ) -> impl Future<Output = Result<Self::Cursor, Self::Error>> + MaybeSend;
}

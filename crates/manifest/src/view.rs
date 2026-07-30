//! The read handle: one immutable root, the map verbs over it.

use core::future::Future;
use core::ops::RangeBounds;

use nectar_marker::{MaybeSend, MaybeSync};
use nectar_primitives::chunk::{ChunkRef, Reference};

use crate::listing::Listing;
use crate::path::ManifestPath;
use crate::site::SiteConfig;
use crate::{DataSink, SinkError};

/// What a bound path resolves to. Opaque bytes stay reachable through
/// [`MapView::load`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MapEntry<R: Reference = ChunkRef> {
    /// A chunk reference of the requested width.
    Reference(R),
    /// The path is bound, but not to a reference of the requested width.
    Opaque,
}

impl<R: Reference> MapEntry<R> {
    /// The bound reference.
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

/// An ordered walk over a view. Peak retained state is a function of depth,
/// not of key count.
pub trait MapCursor<R: Reference + MaybeSend = ChunkRef>: MaybeSend {
    /// Error type a walk fails with.
    type Error: core::error::Error + MaybeSend + MaybeSync + 'static;

    /// The next `(path, entry)` in path order.
    fn next(
        &mut self,
    ) -> impl Future<Output = Result<Option<(ManifestPath, MapEntry<R>)>, Self::Error>> + MaybeSend;
}

/// The read view of a manifest, bound to one immutable root.
///
/// Every read is over content paths alone. The site-level documents are read
/// through [`index_document`](Self::index_document) and
/// [`error_document`](Self::error_document), and no walk yields the slot they
/// live in.
pub trait MapView<R: Reference + MaybeSend = ChunkRef>: MaybeSend {
    /// The format's own metadata for one entry.
    type Metadata: MaybeSend + Default;

    /// Error type for every read on the view.
    type Error: core::error::Error + MaybeSend + MaybeSync + 'static;

    /// The ordered walk [`iter`](Self::iter) and [`range`](Self::range) hand
    /// back.
    type Cursor: MapCursor<R, Error = Self::Error>;

    /// A reserved path reads as absent; see [`ManifestPath::is_reserved`].
    fn get(
        &self,
        path: &ManifestPath,
    ) -> impl Future<Output = Result<Option<MapEntry<R>>, Self::Error>> + MaybeSend;

    /// The site-level documents, read from the format's own root slot.
    fn site_config(&self) -> impl Future<Output = Result<SiteConfig, Self::Error>> + MaybeSend;

    /// The index document, a filename joined below each directory rather than
    /// one whole path.
    fn index_document(
        &self,
    ) -> impl Future<Output = Result<Option<ManifestPath>, Self::Error>> + MaybeSend {
        let config = self.site_config();
        async move { Ok(config.await?.into_parts().0) }
    }

    /// The error document, one whole content path.
    fn error_document(
        &self,
    ) -> impl Future<Output = Result<Option<ManifestPath>, Self::Error>> + MaybeSend {
        let config = self.site_config();
        async move { Ok(config.await?.into_parts().1) }
    }

    /// The metadata bound to `path`. An absent path reads back as the empty
    /// metadata, so this is no presence answer; ask
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
    /// separator; the referenced chunks are never fetched. `dir` matches as a
    /// byte prefix, so end it in the separator to mean the directory: `img`
    /// also lists `imgx.png`, where `img/` does not. The empty path lists the
    /// top level.
    ///
    /// [`ListEntry::Dir`]: crate::ListEntry::Dir
    fn dir(
        &self,
        dir: &ManifestPath,
    ) -> impl Future<Output = Result<Listing<R>, Self::Error>> + MaybeSend;

    /// The greatest bound path `<= path`, with its entry.
    ///
    /// The default walks [`range`](Self::range); a format with an ordered seek
    /// overrides it and pays O(depth).
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
    /// The writes are idempotent overwrites: rerun a failed load in full.
    fn load<K: DataSink<Error: SinkError> + MaybeSend>(
        &self,
        path: &ManifestPath,
        sink: &mut K,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend;

    /// Every bound content path, with its entry, in path order. The format's
    /// root slot never appears.
    fn iter(&self) -> impl Future<Output = Result<Self::Cursor, Self::Error>> + MaybeSend;

    /// Every `(path, entry)` within `bounds`, in path order. Paths order as
    /// byte strings.
    fn range(
        &self,
        bounds: impl RangeBounds<ManifestPath> + MaybeSend,
    ) -> impl Future<Output = Result<Self::Cursor, Self::Error>> + MaybeSend;
}

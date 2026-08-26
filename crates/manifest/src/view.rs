//! The read handle: one immutable root, the map verbs over it.

use core::future::Future;
use core::ops::Bound;

use futures_util::Stream;
use futures_util::StreamExt;
use nectar_marker::{MaybeSend, MaybeSync};
use nectar_primitives::EntryRef;
use nectar_primitives::chunk::{ChunkRef, Reference};

use crate::listing::{Listing, collapse_dir};
use crate::path::ManifestPath;
use crate::site::SiteConfig;
use crate::{DataSink, SinkError};

/// What a bound path resolves to. [`load`](ManifestView::load) success is
/// predictable from it: exactly a loadable entry loads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MapEntry<R: Reference = ChunkRef> {
    /// A chunk reference of the requested width; a load joins it from the store.
    Reference(R),
    /// The manifest itself carries the bytes; a load serves them, no store read.
    Value,
    /// The path is bound, but to nothing this caller can read; a load fails
    /// as [`NoData`](crate::ManifestError::NoData).
    Opaque,
}

impl<R: Reference> MapEntry<R> {
    /// The entry a raw reference resolves to at width `R`: the reference when
    /// the width fits, opaque otherwise.
    #[must_use]
    pub fn from_entry_ref(entry: EntryRef) -> Self {
        R::from_entry_ref(entry).map_or(Self::Opaque, Self::Reference)
    }

    /// The bound reference.
    #[must_use]
    pub const fn reference(&self) -> Option<&R> {
        match self {
            Self::Reference(reference) => Some(reference),
            Self::Value | Self::Opaque => None,
        }
    }

    /// Whether [`load`](ManifestView::load) serves this entry.
    #[must_use]
    pub const fn is_loadable(&self) -> bool {
        matches!(self, Self::Reference(_) | Self::Value)
    }

    /// Whether the entry names nothing this caller can read.
    #[must_use]
    pub const fn is_opaque(&self) -> bool {
        matches!(self, Self::Opaque)
    }
}

/// An ordered walk over a view, as a [`Stream`] of `(path, entry)` in path
/// order. Peak retained state is a function of depth, not of key count.
pub trait ManifestCursor<R: Reference = ChunkRef>:
    MaybeSend + Unpin + Stream<Item = Result<(ManifestPath, MapEntry<R>), Self::Error>>
{
    /// Error type a walk fails with.
    type Error: core::error::Error + MaybeSend + MaybeSync + 'static;
}

/// The read view of a manifest, bound to one immutable root.
///
/// Every read is over content paths alone. The site-level documents are read
/// through [`index_document`](Self::index_document) and
/// [`error_document`](Self::error_document), and no walk yields the slot they
/// live in.
pub trait ManifestView<R: Reference = ChunkRef>: MaybeSend + MaybeSync {
    /// The format's own metadata for one entry.
    type Metadata: MaybeSend + Default;

    /// Error type for every read on the view.
    type Error: core::error::Error + MaybeSend + MaybeSync + 'static;

    /// The ordered walk [`iter`](Self::iter) and [`range`](Self::range) hand
    /// back.
    type Cursor: ManifestCursor<R, Error = Self::Error>;

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
    /// The default runs [`collapse_dir`] over [`range`](Self::range); a
    /// format with a native folder view or a prefix-pruned walk overrides it.
    ///
    /// [`ListEntry::Dir`]: crate::ListEntry::Dir
    fn dir(
        &self,
        dir: &ManifestPath,
    ) -> impl Future<Output = Result<Listing<R>, Self::Error>> + MaybeSend {
        let prefix = dir.clone();
        let walk = self.range((Bound::Included(dir.clone()), Bound::Unbounded));
        async move { collapse_dir(prefix, walk.await?).await }
    }

    /// The greatest bound path `<= path`, with its entry.
    ///
    /// The default walks [`range`](Self::range); a format with an ordered seek
    /// overrides it and pays O(depth).
    fn floor(
        &self,
        path: &ManifestPath,
    ) -> impl Future<Output = Result<Option<(ManifestPath, MapEntry<R>)>, Self::Error>> + MaybeSend
    {
        let walk = self.range((Bound::Unbounded, Bound::Included(path.clone())));
        async move {
            let mut cursor = walk.await?;
            let mut last = None;
            while let Some(item) = cursor.next().await.transpose()? {
                last = Some(item);
            }
            Ok(last)
        }
    }

    /// Resolve a request path to the entry a website server would return:
    /// exact content path first, then the index document joined below the
    /// request path, then the whole error document. Every probe is
    /// [`get`](Self::get), so a reserved path never answers.
    ///
    /// A format with a native resolver overrides this and keeps the order.
    fn serve(
        &self,
        path: &ManifestPath,
    ) -> impl Future<Output = Result<Served<R>, Self::Error>> + MaybeSend
    where
        Self: Sized,
    {
        let path = path.clone();
        async move {
            if let Some(entry) = self.get(&path).await? {
                return Ok(Served::Exact { path, entry });
            }
            serve_fallback(self, &path).await
        }
    }

    /// Write the data bound to `path` into `sink`, starting at offset zero.
    ///
    /// Serves exactly what [`get`](Self::get) reports loadable; an opaque
    /// entry fails as [`NoData`](crate::ManifestError::NoData). The writes
    /// are idempotent overwrites: rerun a failed load in full.
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
        bounds: (Bound<ManifestPath>, Bound<ManifestPath>),
    ) -> impl Future<Output = Result<Self::Cursor, Self::Error>> + MaybeSend;
}

/// What serving a request path resolves to; the path is the one whose entry
/// answered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Served<R: Reference = ChunkRef> {
    /// The request path matched a content path exactly.
    Exact {
        /// The matched path.
        path: ManifestPath,
        /// The bound entry.
        entry: MapEntry<R>,
    },
    /// No exact path matched; the index document joined below the request
    /// path did.
    Index {
        /// The joined index-document path that matched.
        path: ManifestPath,
        /// The bound entry.
        entry: MapEntry<R>,
    },
    /// Neither the path nor its index document matched; the error document
    /// did.
    Error {
        /// The error-document path that matched.
        path: ManifestPath,
        /// The bound entry.
        entry: MapEntry<R>,
    },
    /// No path, index document, or error document matched.
    Missing,
}

impl<R: Reference> Served<R> {
    /// The resolved entry, or `None` when nothing matched.
    #[must_use]
    pub const fn entry(&self) -> Option<&MapEntry<R>> {
        match self {
            Self::Exact { entry, .. } | Self::Index { entry, .. } | Self::Error { entry, .. } => {
                Some(entry)
            }
            Self::Missing => None,
        }
    }

    /// The resolved path, or `None` when nothing matched.
    #[must_use]
    pub const fn path(&self) -> Option<&ManifestPath> {
        match self {
            Self::Exact { path, .. } | Self::Index { path, .. } | Self::Error { path, .. } => {
                Some(path)
            }
            Self::Missing => None,
        }
    }

    /// Whether nothing matched.
    #[must_use]
    pub const fn is_missing(&self) -> bool {
        matches!(self, Self::Missing)
    }
}

/// Resolve `path` under the site fallbacks alone: the index-document join,
/// then the error document. The exact probe is the caller's, so a format
/// overriding [`ManifestView::serve`] keeps the fallback law here.
pub async fn serve_fallback<R, V>(view: &V, path: &ManifestPath) -> Result<Served<R>, V::Error>
where
    R: Reference,
    V: ManifestView<R>,
{
    let (index, error) = view.site_config().await?.into_parts();
    if let Some(index) = index {
        // The index document is a filename joined below the directory the
        // request path names.
        let joined = path.join(index.as_bytes());
        if let Some(entry) = view.get(&joined).await? {
            return Ok(Served::Index {
                path: joined,
                entry,
            });
        }
    }
    if let Some(error) = error {
        // One whole content path; a reserved one reads as absent like any
        // other get.
        if let Some(entry) = view.get(&error).await? {
            return Ok(Served::Error { path: error, entry });
        }
    }
    Ok(Served::Missing)
}

#[cfg(test)]
mod tests {
    use nectar_primitives::chunk::ChunkAddress;
    use nectar_primitives::{EncryptedChunkRef, EncryptionKey};

    use super::*;

    #[test]
    fn exactly_the_matched_kinds_carry_a_path_and_entry() {
        let entry = MapEntry::Reference(ChunkRef::new(ChunkAddress::new([1; 32])));
        let exact = Served::Exact {
            path: ManifestPath::from("a.html"),
            entry: entry.clone(),
        };
        assert_eq!(exact.path(), Some(&ManifestPath::from("a.html")));
        assert_eq!(exact.entry(), Some(&entry));
        assert!(!exact.is_missing());
        let missing = Served::<ChunkRef>::Missing;
        assert!(missing.is_missing());
        assert_eq!(missing.path(), None);
        assert_eq!(missing.entry(), None);
    }

    #[test]
    fn from_entry_ref_keeps_the_width_and_exactly_reference_and_value_load() {
        let plain = EntryRef::Plain(ChunkRef::new(ChunkAddress::new([1; 32])));
        let wide = EntryRef::Encrypted(EncryptedChunkRef::new(
            ChunkAddress::new([2; 32]),
            EncryptionKey::from([3; 32]),
        ));
        let narrow = MapEntry::<ChunkRef>::from_entry_ref(plain.clone());
        assert!(narrow.reference().is_some());
        assert!(narrow.is_loadable() && !narrow.is_opaque());
        assert!(MapEntry::<ChunkRef>::from_entry_ref(wide.clone()).is_opaque());
        assert!(matches!(
            MapEntry::<EncryptedChunkRef>::from_entry_ref(wide),
            MapEntry::Reference(_)
        ));
        assert!(MapEntry::<EncryptedChunkRef>::from_entry_ref(plain).is_opaque());
        assert!(MapEntry::<ChunkRef>::Value.is_loadable());
        assert!(MapEntry::<ChunkRef>::Value.reference().is_none());
        assert!(!MapEntry::<ChunkRef>::Opaque.is_loadable());
        assert!(MapEntry::<ChunkRef>::Opaque.is_opaque());
    }
}

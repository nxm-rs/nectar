//! The read handle: one immutable root, the map verbs over it.

use core::future::Future;
use core::ops::Bound;

use nectar_file::{File, LoadError, Policy};
use nectar_marker::{MaybeSend, MaybeSync};
use nectar_primitives::EntryRef;
use nectar_primitives::chunk::{ChunkRef, ContentOnlyChunkSet, Reference};
use nectar_primitives::store::TrustedGet;

use crate::error::ManifestError;
use crate::listing::{Listing, collapse_dir};
use crate::path::ManifestPath;
use crate::site::SiteConfig;
use crate::{DataSink, SinkError};

/// What a bound path resolves to. [`load`](MapView::load) success is
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

    /// Whether [`load`](MapView::load) serves this entry.
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
            while let Some(item) = cursor.next().await? {
                last = Some(item);
            }
            Ok(last)
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

/// Drain the file at `reference` into `sink`: the one data-load lane every
/// format shares, reporting through the seam's taxonomy.
pub async fn load_reference<S, K, F, const B: usize>(
    store: S,
    reference: EntryRef,
    sink: &mut K,
) -> Result<(), ManifestError<F>>
where
    S: TrustedGet<ContentOnlyChunkSet<B>> + Clone + MaybeSend + MaybeSync + 'static,
    K: DataSink<Error: SinkError> + MaybeSend,
{
    File::<S, B>::new(store, Policy::DEFAULT)
        .load(reference, sink)
        .await
        .map(drop)
        .map_err(|error| match error {
            LoadError::Sink { source, .. } => ManifestError::sink(source),
            data => ManifestError::data(data),
        })
}

#[cfg(test)]
mod tests {
    use nectar_primitives::chunk::ChunkAddress;
    use nectar_primitives::{EncryptedChunkRef, EncryptionKey};

    use super::*;

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

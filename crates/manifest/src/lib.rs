//! The manifest seam: one trait over every manifest format.
//!
//! A manifest binds paths to chunk references. Swarm has two formats that do
//! it: the mantaray trie and the `nectar-ldb` key-value database read through
//! its folder view. [`Manifest`] is the common surface over both, and mirrors
//! the L1 store traits: static dispatch, RPITIT futures carrying
//! [`MaybeSend`], and generic over the sealed [`Reference`] width, so an
//! encrypted manifest is the same code path as a plain one.
//!
//! [`Manifest::at`] binds a root and hands back a [`MapView`];
//! [`Manifest::apply`] folds one [`Batch`] into a base root and yields the new
//! one. [`Manifest::empty`] bootstraps the empty manifest, and every operation
//! fails with the seam-owned [`ManifestError`].
//!
//! The map holds content paths alone. The site index and error documents are
//! not keys: [`MapView`] reads them as options, and [`Batch`] sets them with
//! `set_index_document` and `set_error_document`. Each lands in the format's
//! own root slot. The two paths those slots are keyed at, the empty one and
//! `"/"`, are reserved on both formats: a read at either is absent and a write
//! at either refuses the whole batch as [`ReservedKey`], in the seam, before
//! the format runs.
//!
//! Each format keeps its own metadata type: the static path erases nothing.
//! [`DynManifest`] is the object-safe wrapper for a runtime-detected format,
//! and unifies metadata behind [`ManifestMetadata`] - the one lossy point in
//! the design.
//!
//! ```
//! use nectar_manifest::{Batch, ManifestPath, MetadataView, WellKnownKey};
//! use nectar_primitives::{ChunkAddress, ChunkRef};
//!
//! // The write vocabulary is shared: a caller stages a batch without naming
//! // a format, and `Manifest::apply` folds it in atomically.
//! let mut batch: Batch<ChunkRef, MetadataView> = Batch::new();
//! let meta = MetadataView::new().with(WellKnownKey::ContentType, "text/html");
//! let file = ChunkRef::new(ChunkAddress::new([7; 32]));
//! batch.insert_with(ManifestPath::from("index.html"), file, meta);
//! batch.remove(ManifestPath::from("stale.html"));
//! assert!(!batch.is_empty());
//! ```
//!
//! The site configuration is a value, not an op:
//!
//! ```
//! use nectar_manifest::{ManifestPath, SiteConfig};
//!
//! let config = SiteConfig::new()
//!     .with_index_document(ManifestPath::from("index.html"))
//!     .with_error_document(ManifestPath::from("404.html"));
//! assert_eq!(
//!     config.index_document().map(ManifestPath::as_bytes),
//!     Some(&b"index.html"[..])
//! );
//! // The same setter clears.
//! assert!(config.with_index_document(None).with_error_document(None).is_empty());
//! ```

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
// Test code may freely unwrap/index/panic; the runtime-safety restriction
// lints target production code paths.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::get_unwrap,
        clippy::indexing_slicing,
        clippy::string_slice,
        clippy::arithmetic_side_effects,
        clippy::panic,
        clippy::unreachable,
        clippy::panic_in_result_fn,
        clippy::as_conversions
    )
)]

extern crate alloc;

mod batch;
mod dynamic;
mod error;
mod listing;
mod meta;
mod op;
mod path;
mod reserved;
mod site;
mod view;

pub use batch::{Batch, CheckedBatch};
pub use dynamic::{DynManifest, DynSink, DynSinkError};
pub use error::{ErasedFormat, ErasedManifestError, ManifestError};
pub use listing::{ListEntry, Listing};
pub use meta::{ManifestMetadata, MetadataView, WellKnownKey};
pub use op::ManifestOp;
pub use path::ManifestPath;
pub use reserved::{ReservedKey, reserved_key};
pub use site::SiteConfig;
pub use view::{MapCursor, MapEntry, MapView};

// The positional sink a load writes into is the file crate's, re-exported so
// a manifest consumer needs no second dependency to name it.
pub use nectar_file::sink::{DataSink, MemSink};

use core::future::Future;

use nectar_marker::{MaybeSend, MaybeSync};
use nectar_primitives::chunk::{ChunkRef, Reference};

/// Bounds a sink error must carry to cross the seam.
///
/// A load reports the sink's failure through the manifest's own error type, so
/// the sink error has to box.
pub trait SinkError: core::error::Error + MaybeSend + MaybeSync + 'static {}

impl<T: core::error::Error + MaybeSend + MaybeSync + 'static> SinkError for T {}

/// A path-to-reference map, read through a root-bound view and written by
/// applying a [`Batch`] against a base root.
///
/// Static dispatch only, exactly like the L1 store traits: the futures are
/// RPITIT and the reference width is a type parameter, so nothing here costs
/// an allocation. Use [`DynManifest`] where the format is a runtime choice.
///
/// Content paths are stored bare and verbatim, byte-identical to what the
/// reference client writes. The map holds content paths alone. The site index
/// and error documents are not keys: read them with
/// [`MapView::index_document`] and [`MapView::error_document`], and write them
/// with [`Batch::set_index_document`] and [`Batch::set_error_document`]. Each
/// lands in the format's own root slot, at a path
/// [`ManifestPath::is_reserved`] names.
pub trait Manifest<R: Reference + MaybeSend = ChunkRef>: MaybeSend + MaybeSync {
    /// The format's own metadata for one entry.
    type Metadata: MaybeSend + Default;

    /// The format's own failure union, carried in [`ManifestError::Format`].
    type FormatError: core::error::Error + MaybeSend + MaybeSync + 'static;

    /// The read handle [`at`](Self::at) hands back.
    type View<'a>: MapView<R, Metadata = Self::Metadata, Error = ManifestError<Self::FormatError>>
    where
        Self: 'a;

    /// The root of the empty manifest, freshly persisted.
    fn empty(
        &self,
    ) -> impl Future<Output = Result<R, ManifestError<Self::FormatError>>> + MaybeSend;

    /// The read view of the manifest rooted at `root`. Reaches storage only
    /// when a read is awaited.
    fn at(&self, root: &R) -> Self::View<'_>;

    /// Fold `batch` into the manifest rooted at `base`, returning the new
    /// root. The whole batch lands or none of it does.
    ///
    /// A batch that staged a reserved path fails as
    /// [`ManifestError::Reserved`] before the format runs; the format checks
    /// the batch through [`Batch::into_checked`] first.
    fn apply(
        &self,
        base: R,
        batch: Batch<R, Self::Metadata>,
    ) -> impl Future<Output = Result<R, ManifestError<Self::FormatError>>> + MaybeSend;

    /// Native metadata rebuilt from the erased view, reading the registered
    /// keys and any custom key the format can carry.
    ///
    /// The seam's only lossy step, and the reason it is a method: the format
    /// decides what it can represent.
    fn metadata_from_view(
        &self,
        view: &dyn ManifestMetadata,
    ) -> Result<Self::Metadata, ManifestError<Self::FormatError>>;

    /// Insert one path, clearing any metadata bound at it; a one-op [`Batch`]
    /// through [`apply`](Self::apply).
    fn insert(
        &self,
        root: &R,
        path: ManifestPath,
        reference: R,
    ) -> impl Future<Output = Result<R, ManifestError<Self::FormatError>>> + MaybeSend {
        let mut batch = Batch::new();
        batch.insert(path, reference);
        self.apply(root.clone(), batch)
    }

    /// Remove one path. Exact-key: nothing below `path` goes with it, and an
    /// absent `path` returns `root` unchanged.
    fn remove(
        &self,
        root: &R,
        path: ManifestPath,
    ) -> impl Future<Output = Result<R, ManifestError<Self::FormatError>>> + MaybeSend {
        let mut batch = Batch::new();
        batch.remove(path);
        self.apply(root.clone(), batch)
    }
}

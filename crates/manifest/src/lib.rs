//! The manifest seam: one trait over every manifest format.
//!
//! A manifest binds paths to chunk references. Swarm has two formats that do
//! it: the mantaray trie and the `nectar-ldb` key-value database read through
//! its folder view. [`Manifest`] is the common surface over both, and mirrors
//! the L1 store traits: static dispatch, RPITIT futures carrying
//! [`MaybeSend`], and generic over the sealed [`Reference`] width, so an
//! encrypted manifest is the same code path as a plain one.
//!
//! A manifest is a map, so it speaks the map vocabulary. [`Manifest::at`]
//! binds a root and hands back a [`MapView`] to read it: `get`,
//! `contains_key`, `range`, `iter`, plus the two manifest additions `dir` and
//! `load`. [`Manifest::edit`] binds a base root and hands back a
//! [`MapWriter`]: `insert`, `remove`, and a `commit` that yields the new root,
//! because the map itself is immutable. [`Manifest::insert`] and
//! [`Manifest::remove`] are the one-shot sugar over an edit of one op.
//!
//! The map is over content paths alone. A manifest's own configuration, the
//! site index and error documents, is not a key in it: [`MapView`] answers
//! `index_document` and `error_document` as options, and [`MapWriter`] sets
//! them with the chainable `with_index_document` and `with_error_document`.
//! Each lands in the format's own root slot, so no empty key and no magic path
//! ever crosses the seam. The two paths those slots are keyed at, the empty
//! one and `"/"`, are reserved on both formats: a read at either is absent and
//! a write at either is [`ReservedKey`].
//!
//! Each format keeps its own metadata type and its own batch type: the static
//! path erases nothing. [`DynManifest`] is the object-safe wrapper for a
//! runtime-detected format, and unifies metadata behind
//! [`ManifestMetadata`] - the one lossy point in the design.
//!
//! ```
//! use nectar_manifest::{ManifestOp, ManifestPath, MetadataView, WellKnownKey};
//! use nectar_primitives::{ChunkAddress, ChunkRef};
//!
//! // The write vocabulary is shared: a caller builds ops without naming a
//! // format, and each format folds them into its own batch.
//! let ops: [ManifestOp<ChunkRef, MetadataView>; 2] = [
//!     ManifestOp::Insert {
//!         path: ManifestPath::from("index.html"),
//!         reference: ChunkRef::new(ChunkAddress::new([7; 32])),
//!         meta: MetadataView::new().with(WellKnownKey::ContentType, "text/html"),
//!     },
//!     ManifestOp::Remove {
//!         path: ManifestPath::from("stale.html"),
//!     },
//! ];
//! // A content path is stored bare and verbatim, which is what keeps the
//! // mantaray image byte-identical to the reference client's.
//! assert_eq!(ops[0].path().as_bytes(), b"index.html");
//! ```
//!
//! The manifest's own configuration is a value rather than an op, because it is
//! not a path:
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
//! // The same setter clears, so nothing needs a second verb.
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

mod dynamic;
mod listing;
mod meta;
mod op;
mod path;
mod reserved;
mod site;
mod view;
mod writer;

pub use dynamic::{DynManifest, DynSink, DynSinkError};
pub use listing::{ListEntry, Listing};
pub use meta::{ManifestMetadata, MetadataView, WellKnownKey};
pub use op::ManifestOp;
pub use path::ManifestPath;
pub use reserved::{ReservedKey, reserved_key};
pub use site::SiteConfig;
pub use view::{MapCursor, MapEntry, MapView};
pub use writer::{Insert, MapWriter};

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

/// A path-to-reference map, read through a root-bound view and written through
/// a base-bound writer.
///
/// Static dispatch only, exactly like the L1 store traits: the futures are
/// RPITIT and the reference width is a type parameter, so nothing here costs
/// an allocation. The two handles are generic associated types, so a view
/// borrows the store rather than cloning it. Use [`DynManifest`] where the
/// format is a runtime choice.
///
/// Content paths are stored bare and verbatim: `index.html` is the bytes
/// `index.html` on both formats, byte-identical to what the reference client
/// writes. The map is over content paths alone. The manifest's own
/// configuration, the site index and error documents, is not a key in it: read
/// it with [`MapView::index_document`] and [`MapView::error_document`], and
/// write it with [`MapWriter::with_index_document`] and
/// [`MapWriter::with_error_document`], each of which lands in the format's own
/// root slot. The paths those slots are keyed at are reserved: see
/// [`ManifestPath::is_reserved`].
pub trait Manifest<R: Reference + MaybeSend = ChunkRef>: MaybeSend + MaybeSync {
    /// The format's own metadata for one entry.
    type Metadata: MaybeSend + Default;

    /// Error type for every operation on the manifest.
    type Error: core::error::Error + MaybeSend + MaybeSync + 'static;

    /// The read handle [`at`](Self::at) hands back.
    type View<'a>: MapView<R, Metadata = Self::Metadata, Error = Self::Error>
    where
        Self: 'a;

    /// The write handle [`edit`](Self::edit) hands back.
    type Writer<'a>: MapWriter<R, Metadata = Self::Metadata, Error = Self::Error>
    where
        Self: 'a;

    /// The read view of the manifest rooted at `root`.
    ///
    /// Cheap: the view holds the store and a clone of the root, and reaches
    /// storage only when a read is awaited.
    fn at(&self, root: &R) -> Self::View<'_>;

    /// A writer staging a batch against the manifest rooted at `base`.
    ///
    /// Staging touches no storage; [`MapWriter::commit`] writes the batch and
    /// returns the new root.
    fn edit(&self, base: &R) -> Self::Writer<'_>;

    /// Native metadata rebuilt from the erased view, reading the registered
    /// keys and any custom key the format can carry.
    ///
    /// The seam's only lossy step, and the reason it is a method: the format
    /// decides what it can represent.
    fn metadata_from_view(
        &self,
        view: &dyn ManifestMetadata,
    ) -> Result<Self::Metadata, Self::Error>;

    /// Insert one path into the manifest rooted at `root`, returning the new
    /// root.
    ///
    /// Sugar over an [`edit`](Self::edit) of one op. Metadata rides the
    /// writer, so an insert that carries it goes through the handle.
    fn insert(
        &self,
        root: &R,
        path: ManifestPath,
        reference: R,
    ) -> impl Future<Output = Result<R, Self::Error>> + MaybeSend {
        let mut writer = self.edit(root);
        writer.insert(path, reference);
        writer.commit()
    }

    /// Remove one path from the manifest rooted at `root`, returning the new
    /// root.
    ///
    /// Exact-key, as [`MapWriter::remove`] is: nothing below `path` goes with
    /// it, and removing what is not bound returns `root` unchanged.
    fn remove(
        &self,
        root: &R,
        path: ManifestPath,
    ) -> impl Future<Output = Result<R, Self::Error>> + MaybeSend {
        let mut writer = self.edit(root);
        writer.remove(path);
        writer.commit()
    }

    /// Fold `ops` into the manifest rooted at `base`, returning the new root.
    ///
    /// The whole batch lands or none of it does: a caller never observes a
    /// half-applied root, because the root only exists once the batch is
    /// written.
    fn apply(
        &self,
        base: &R,
        ops: impl IntoIterator<Item = ManifestOp<R, Self::Metadata>> + MaybeSend,
    ) -> impl Future<Output = Result<R, Self::Error>> + MaybeSend {
        let mut writer = self.edit(base);
        writer.extend(ops);
        writer.commit()
    }
}

//! The manifest seam: one trait over every manifest format.
//!
//! A manifest binds paths to chunk references. Swarm has two formats that do
//! it: the mantaray trie and the `nectar-ldb` key-value database read through
//! its folder view. [`Manifest`] is the common surface over both, and mirrors
//! the L1 store traits: static dispatch, RPITIT futures carrying
//! [`MaybeSend`], and generic over the sealed [`Reference`] width, so an
//! encrypted manifest is the same code path as a plain one.
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
//!     ManifestOp::Put {
//!         path: ManifestPath::from("index.html"),
//!         reference: ChunkRef::new(ChunkAddress::new([7; 32])),
//!         meta: MetadataView::new().with(WellKnownKey::ContentType, "text/html"),
//!     },
//!     ManifestOp::Remove {
//!         path: ManifestPath::from("stale.html"),
//!     },
//! ];
//! assert_eq!(ops[0].path().as_bytes(), b"index.html");
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

pub use dynamic::{DynManifest, DynSink};
pub use listing::{ListEntry, Listing};
pub use meta::{ManifestMetadata, MetadataView, WellKnownKey};
pub use op::ManifestOp;
pub use path::ManifestPath;

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

/// A path-to-reference manifest, read and written whole-batch.
///
/// Static dispatch only, exactly like the L1 store traits: the futures are
/// RPITIT and the reference width is a type parameter, so nothing here costs
/// an allocation. Use [`DynManifest`] where the format is a runtime choice.
pub trait Manifest<R: Reference + MaybeSend = ChunkRef>: MaybeSend + MaybeSync {
    /// The format's own metadata for one entry.
    type Metadata: MaybeSend;

    /// Error type for every operation on the manifest.
    type Error: core::error::Error + MaybeSend + MaybeSync + 'static;

    /// The immediate children of the directory `dir` names, in path order.
    ///
    /// Deeper paths collapse into one [`ListEntry::Dir`] at the next
    /// separator; the referenced chunks are never fetched.
    fn list(
        &self,
        root: &R,
        dir: &ManifestPath,
    ) -> impl Future<Output = Result<Listing<R>, Self::Error>> + MaybeSend;

    /// Write the data bound to `path` into `sink`, starting at offset zero.
    ///
    /// The sink's writes are idempotent overwrites, so a failed load is
    /// recovered by running it again in full.
    fn load<K: DataSink<Error: SinkError> + MaybeSend>(
        &self,
        root: &R,
        path: &ManifestPath,
        sink: &mut K,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend;

    /// Fold `ops` into the manifest rooted at `base`, returning the new root.
    ///
    /// The whole batch lands or none of it does: a caller never observes a
    /// half-applied root, because the root only exists once the batch is
    /// written.
    fn apply(
        &self,
        base: &R,
        ops: impl IntoIterator<Item = ManifestOp<R, Self::Metadata>> + MaybeSend,
    ) -> impl Future<Output = Result<R, Self::Error>> + MaybeSend;

    /// Native metadata rebuilt from the erased view, reading the registered
    /// keys and any custom key the format can carry.
    ///
    /// The seam's only lossy step, and the reason it is a method: the format
    /// decides what it can represent.
    fn metadata_from_view(&self, view: &dyn ManifestMetadata) -> Result<Self::Metadata, Self::Error>;

    /// Bind one path, sugar over a one-op [`apply`](Self::apply).
    fn save(
        &self,
        base: &R,
        path: ManifestPath,
        reference: R,
        meta: Self::Metadata,
    ) -> impl Future<Output = Result<R, Self::Error>> + MaybeSend {
        self.apply(base, [ManifestOp::Put {
            path,
            reference,
            meta,
        }])
    }
}

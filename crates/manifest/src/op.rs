//! The shared write vocabulary: one op list, translated by each format into
//! its own batch.

use nectar_primitives::chunk::{ChunkRef, Reference};

use crate::path::ManifestPath;

/// One update to fold into a manifest.
///
/// A batch is applied atomically against a base root, so a caller builds the
/// list generically and the format decides how the batch reaches storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ManifestOp<R: Reference = ChunkRef, M = ()> {
    /// Bind `path` to `reference`, carrying `meta`.
    Put {
        /// The path to bind.
        path: ManifestPath,
        /// The reference the path resolves to.
        reference: R,
        /// Metadata to attach, in the format's own vocabulary.
        meta: M,
    },
    /// Unbind `path`.
    Remove {
        /// The path to unbind.
        path: ManifestPath,
    },
}

impl<R: Reference, M> ManifestOp<R, M> {
    /// The path the op acts on.
    #[must_use]
    pub const fn path(&self) -> &ManifestPath {
        match self {
            Self::Put { path, .. } | Self::Remove { path } => path,
        }
    }

    /// Whether the op removes a path.
    #[must_use]
    pub const fn is_remove(&self) -> bool {
        matches!(self, Self::Remove { .. })
    }
}

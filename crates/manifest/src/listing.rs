//! Directory listings: one level of a manifest, files and collapsed
//! subdirectories.

use alloc::vec::Vec;

use nectar_primitives::chunk::{ChunkRef, Reference};

use crate::path::ManifestPath;

/// One immediate child of a listed directory.
///
/// A subdirectory collapses every path beneath it into a single entry; a file
/// carries the reference its path is bound to. An inline value, which the
/// key-value format may store instead of a reference, lists as
/// [`Value`](Self::Value) rather than being fetched.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ListEntry<R: Reference = ChunkRef> {
    /// A file directly in the directory, and the reference it is bound to.
    File {
        /// The file's full path.
        path: ManifestPath,
        /// The reference the path resolves to.
        reference: R,
    },
    /// A path bound to bytes the manifest carries itself, not to a reference.
    Value {
        /// The value's full path.
        path: ManifestPath,
    },
    /// A subdirectory: its full path, ending in the separator.
    Dir {
        /// The subdirectory path.
        path: ManifestPath,
    },
}

impl<R: Reference> ListEntry<R> {
    /// The entry's full path.
    #[must_use]
    pub const fn path(&self) -> &ManifestPath {
        match self {
            Self::File { path, .. } | Self::Value { path } | Self::Dir { path } => path,
        }
    }

    /// Whether the entry names a subdirectory.
    #[must_use]
    pub const fn is_dir(&self) -> bool {
        matches!(self, Self::Dir { .. })
    }

    /// The bound reference, or `None` for a directory or an inline value.
    #[must_use]
    pub const fn reference(&self) -> Option<&R> {
        match self {
            Self::File { reference, .. } => Some(reference),
            Self::Dir { .. } | Self::Value { .. } => None,
        }
    }
}

/// One directory level, in path order.
///
/// Materialized rather than streamed: the seam is object-safe, and the
/// formats keep their own bounded cursors for a walk of the whole trie.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Listing<R: Reference = ChunkRef> {
    entries: Vec<ListEntry<R>>,
}

impl<R: Reference> Listing<R> {
    /// A listing over `entries`, taken in the order given.
    #[must_use]
    pub const fn new(entries: Vec<ListEntry<R>>) -> Self {
        Self { entries }
    }

    /// The listed children.
    #[must_use]
    pub fn entries(&self) -> &[ListEntry<R>] {
        &self.entries
    }

    /// Consume into the listed children.
    #[must_use]
    pub fn into_entries(self) -> Vec<ListEntry<R>> {
        self.entries
    }

    /// Number of listed children.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the directory has no children.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl<R: Reference> Default for Listing<R> {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl<R: Reference> IntoIterator for Listing<R> {
    type Item = ListEntry<R>;
    type IntoIter = alloc::vec::IntoIter<ListEntry<R>>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

impl<R: Reference> FromIterator<ListEntry<R>> for Listing<R> {
    fn from_iter<I: IntoIterator<Item = ListEntry<R>>>(entries: I) -> Self {
        Self::new(entries.into_iter().collect())
    }
}

//! Directory listings: one level of a manifest, files and collapsed
//! subdirectories.

use alloc::vec::Vec;

use futures_util::StreamExt;
use nectar_primitives::chunk::{ChunkRef, Reference};

use crate::path::ManifestPath;
use crate::view::{ManifestCursor, MapEntry};

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
    /// A path bound to bytes the manifest carries itself, or to nothing the
    /// caller's reference width can hold.
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

/// Collapse the ordered walk `cursor`, which starts at `prefix`, into the
/// directory level below `prefix`; the first path outside the prefix ends it.
pub async fn collapse_dir<R, C>(prefix: ManifestPath, mut cursor: C) -> Result<Listing<R>, C::Error>
where
    R: Reference,
    C: ManifestCursor<R>,
{
    let mut entries = Vec::new();
    let mut last_dir = None;
    while let Some((path, entry)) = cursor.next().await.transpose()? {
        if !path.as_bytes().starts_with(prefix.as_bytes()) {
            break;
        }
        if let Some(listed) = collapse_level(prefix.as_bytes(), path, entry, &mut last_dir) {
            entries.push(listed);
        }
    }
    Ok(Listing::new(entries))
}

/// Fold one walked `(path, entry)` into the level below `prefix`, collapsing
/// deeper paths at the next separator; the walk is in path order, so
/// `last_dir` deduplicates consecutive subdirectory paths.
fn collapse_level<R: Reference>(
    prefix: &[u8],
    path: ManifestPath,
    entry: MapEntry<R>,
    last_dir: &mut Option<Vec<u8>>,
) -> Option<ListEntry<R>> {
    let bytes = path.as_bytes();
    let suffix = bytes.strip_prefix(prefix)?;
    // The directory itself is not one of its own children.
    if suffix.is_empty() {
        return None;
    }
    let Some(cut) = suffix
        .iter()
        .position(|&byte| byte == ManifestPath::SEPARATOR)
    else {
        return Some(match entry {
            MapEntry::Reference(reference) => ListEntry::File { path, reference },
            MapEntry::Value | MapEntry::Opaque => ListEntry::Value { path },
        });
    };
    let through = prefix.len().saturating_add(cut).saturating_add(1);
    let dir = bytes.get(..through).unwrap_or(bytes).to_vec();
    if last_dir.as_deref() == Some(dir.as_slice()) {
        return None;
    }
    *last_dir = Some(dir.clone());
    Some(ListEntry::Dir {
        path: ManifestPath::new(dir),
    })
}

#[cfg(test)]
mod tests {
    use alloc::format;
    use alloc::string::String;
    use alloc::vec::Vec;

    use nectar_primitives::chunk::ChunkAddress;

    use super::*;

    fn reference(byte: u8) -> ChunkRef {
        ChunkRef::new(ChunkAddress::new([byte; 32]))
    }

    /// Fold a scripted level; a listed entry reads `kind:path`, a file also
    /// carrying its reference byte.
    fn fold(prefix: &[u8], walked: &[(&str, MapEntry<ChunkRef>)]) -> Vec<String> {
        let mut last_dir = None;
        walked
            .iter()
            .filter_map(|(path, entry)| {
                collapse_level(
                    prefix,
                    ManifestPath::from(*path),
                    entry.clone(),
                    &mut last_dir,
                )
            })
            .map(|listed| {
                let path = String::from_utf8_lossy(listed.path().as_bytes());
                match &listed {
                    ListEntry::File { reference, .. } => {
                        format!("F{}:{path}", reference.address().as_ref()[0])
                    }
                    ListEntry::Value { .. } => format!("V:{path}"),
                    ListEntry::Dir { .. } => format!("D:{path}"),
                }
            })
            .collect()
    }

    #[test]
    fn one_level_collapses_and_deduplicates() {
        let got = fold(
            b"a/",
            &[
                ("a/", MapEntry::Reference(reference(0))),
                ("a/file", MapEntry::Reference(reference(1))),
                ("a/sub/x", MapEntry::Reference(reference(2))),
                ("a/sub/y", MapEntry::Reference(reference(3))),
                ("a/value", MapEntry::Value),
                ("a/wide", MapEntry::Opaque),
                ("b", MapEntry::Reference(reference(4))),
            ],
        );
        assert_eq!(got, ["F1:a/file", "D:a/sub/", "V:a/value", "V:a/wide"]);
    }

    #[test]
    fn a_bare_prefix_matches_bytes_not_directories() {
        let got = fold(
            b"img",
            &[
                ("img/logo.png", MapEntry::Reference(reference(1))),
                ("imgx.png", MapEntry::Reference(reference(2))),
            ],
        );
        assert_eq!(got, ["D:img/", "F2:imgx.png"]);
    }
}

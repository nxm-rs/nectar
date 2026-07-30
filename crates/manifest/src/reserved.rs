//! The two reserved keys, and the error a write at one fails with.
//!
//! A manifest maps content paths to references. Two byte strings are not
//! content paths on either format: the empty one, which is the structural root
//! every path hangs below, and `"/"`, which is the slot the reference client
//! keeps the site-level documents in. Both formats answer the same way at
//! both: a read is absent and a write is [`ReservedKey`].
//!
//! The site documents stay reachable through the option-typed API alone:
//! [`MapView::index_document`], [`MapView::error_document`],
//! [`MapWriter::with_index_document`] and [`MapWriter::with_error_document`].
//!
//! [`MapView::index_document`]: crate::MapView::index_document
//! [`MapView::error_document`]: crate::MapView::error_document
//! [`MapWriter::with_index_document`]: crate::MapWriter::with_index_document
//! [`MapWriter::with_error_document`]: crate::MapWriter::with_error_document

use core::fmt;

use crate::path::ManifestPath;

/// A write named a key the map reserves, so the batch did not land.
///
/// Reported by [`MapWriter::commit`](crate::MapWriter::commit) on either
/// format, because staging cannot fail: the whole batch is refused, so a
/// caller never observes a half-applied root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReservedKey {
    path: ManifestPath,
}

impl ReservedKey {
    /// Report `path` as reserved.
    #[must_use]
    pub const fn new(path: ManifestPath) -> Self {
        Self { path }
    }

    /// The path the write named.
    #[must_use]
    pub const fn path(&self) -> &ManifestPath {
        &self.path
    }
}

impl fmt::Display for ReservedKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} is reserved and binds no content", self.path)
    }
}

impl core::error::Error for ReservedKey {}

/// The reserved key `error` reports, or `None` when it reports something else.
///
/// Walks the source chain, so one matcher answers for every format: a caller
/// asks the seam's question without naming the format's error type.
///
/// ```
/// use nectar_manifest::{ManifestPath, ReservedKey, reserved_key};
///
/// let error = ReservedKey::new(ManifestPath::from("/"));
/// assert_eq!(
///     reserved_key(&error).map(ReservedKey::path),
///     Some(&ManifestPath::from("/"))
/// );
/// ```
#[must_use]
pub fn reserved_key<'a>(error: &'a (dyn core::error::Error + 'static)) -> Option<&'a ReservedKey> {
    let mut step = Some(error);
    while let Some(current) = step {
        if let Some(found) = current.downcast_ref::<ReservedKey>() {
            return Some(found);
        }
        step = current.source();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A wrapper standing in for a format's own error, so the matcher is tested
    /// through a source chain rather than at the top level alone.
    #[derive(Debug)]
    struct Wrapped(ReservedKey);

    impl fmt::Display for Wrapped {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("wrapped")
        }
    }

    impl core::error::Error for Wrapped {
        fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn the_two_reserved_paths_are_the_root_and_the_separator() {
        assert!(ManifestPath::default().is_reserved());
        assert!(ManifestPath::from("/").is_reserved());
        assert!(!ManifestPath::from("index.html").is_reserved());
        assert!(!ManifestPath::from("//").is_reserved());
        assert!(!ManifestPath::from("/a").is_reserved());
        assert!(!ManifestPath::from("a/").is_reserved());
    }

    #[test]
    fn the_matcher_walks_the_source_chain() {
        let wrapped = Wrapped(ReservedKey::new(ManifestPath::from("/")));
        assert_eq!(
            reserved_key(&wrapped).map(ReservedKey::path),
            Some(&ManifestPath::from("/"))
        );
        assert!(reserved_key(&Wrapped(ReservedKey::new(ManifestPath::default()))).is_some());
    }
}

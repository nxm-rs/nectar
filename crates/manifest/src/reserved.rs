//! The two reserved keys, and the error a write at one fails with.
//!
//! The empty path and `"/"` are no content paths on either format: a read is
//! absent, a write is [`ReservedKey`], and the site documents stay reachable
//! through the option-typed API alone.

use crate::path::ManifestPath;

/// A write named a key the map reserves, so the whole batch was refused.
///
/// Reported by [`MapWriter::commit`](crate::MapWriter::commit).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{path:?} is reserved and binds no content")]
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

/// The reserved key `error` reports. Walks the source chain.
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
    core::iter::successors(Some(error), |step| step.source()).find_map(|step| step.downcast_ref())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, thiserror::Error)]
    #[error("wrapped")]
    struct Wrapped(#[source] ReservedKey);

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

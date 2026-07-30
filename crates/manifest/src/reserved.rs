//! The two reserved keys, and the error a write at one fails with.
//!
//! The empty path and `"/"` are no content paths on either format. At both, a
//! read is absent and a write is [`ReservedKey`]. The site documents stay
//! reachable through the option-typed API alone.

use core::fmt;

use crate::path::ManifestPath;

/// A write named a key the map reserves, so the whole batch was refused.
///
/// Reported by [`MapWriter::commit`](crate::MapWriter::commit).
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

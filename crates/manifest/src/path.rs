//! Manifest paths: separator-joined bytes, exactly as both formats key them.

use alloc::vec::Vec;
use core::fmt;

/// A path into a manifest.
///
/// The bytes are the format's own key: nothing is normalized, so a trailing
/// separator marks a directory and survives a round trip.
#[derive(Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ManifestPath(Vec<u8>);

impl ManifestPath {
    /// The separator both formats read a directory boundary at.
    pub const SEPARATOR: u8 = b'/';

    /// The manifest root: the empty path.
    #[must_use]
    pub const fn root() -> Self {
        Self(Vec::new())
    }

    /// Wrap `bytes` as a path.
    #[must_use]
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    /// Join `segments` with the separator; empty segments are dropped, so no
    /// join ever emits a doubled separator.
    pub fn from_segments<I, S>(segments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<[u8]>,
    {
        let mut bytes = Vec::new();
        for segment in segments {
            let segment = segment.as_ref();
            if segment.is_empty() {
                continue;
            }
            if !bytes.is_empty() {
                bytes.push(Self::SEPARATOR);
            }
            bytes.extend_from_slice(segment);
        }
        Self(bytes)
    }

    /// The non-empty segments, in order.
    pub fn segments(&self) -> impl Iterator<Item = &[u8]> {
        self.0
            .split(|&byte| byte == Self::SEPARATOR)
            .filter(|segment| !segment.is_empty())
    }

    /// The path bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consume into the path bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    /// Whether this is the manifest root.
    #[must_use]
    pub const fn is_root(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether the path names a directory: the root, or a trailing separator.
    #[must_use]
    pub fn is_dir(&self) -> bool {
        self.0.last().is_none_or(|&byte| byte == Self::SEPARATOR)
    }

    /// This path with `segment` appended below it, separated unless the path
    /// already ends in a separator or is the root.
    #[must_use]
    pub fn join(&self, segment: impl AsRef<[u8]>) -> Self {
        let segment = segment.as_ref();
        let mut bytes = Vec::with_capacity(
            self.0
                .len()
                .saturating_add(segment.len())
                .saturating_add(1),
        );
        bytes.extend_from_slice(&self.0);
        if !self.is_dir() {
            bytes.push(Self::SEPARATOR);
        }
        bytes.extend_from_slice(segment);
        Self(bytes)
    }
}

impl AsRef<[u8]> for ManifestPath {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<&[u8]> for ManifestPath {
    fn from(bytes: &[u8]) -> Self {
        Self(bytes.to_vec())
    }
}

impl From<&str> for ManifestPath {
    fn from(path: &str) -> Self {
        Self(path.as_bytes().to_vec())
    }
}

impl From<Vec<u8>> for ManifestPath {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

/// Prints the path as text, escaping any byte that is not valid UTF-8.
impl fmt::Debug for ManifestPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match core::str::from_utf8(&self.0) {
            Ok(path) => write!(f, "ManifestPath({path:?})"),
            Err(_) => write!(f, "ManifestPath({:?})", &self.0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn segments_drop_empty_runs() {
        let path = ManifestPath::from("img//logo.png");
        assert_eq!(path.segments().collect::<Vec<_>>(), vec![
            &b"img"[..],
            &b"logo.png"[..]
        ]);
    }

    #[test]
    fn from_segments_never_doubles_the_separator() {
        let path = ManifestPath::from_segments(["img", "", "logo.png"]);
        assert_eq!(path.as_bytes(), b"img/logo.png");
    }

    #[test]
    fn join_separates_only_below_a_file_path() {
        assert_eq!(ManifestPath::root().join("a").as_bytes(), b"a");
        assert_eq!(ManifestPath::from("img/").join("a").as_bytes(), b"img/a");
        assert_eq!(ManifestPath::from("img").join("a").as_bytes(), b"img/a");
    }

    #[test]
    fn a_trailing_separator_survives() {
        let dir = ManifestPath::from("img/");
        assert!(dir.is_dir());
        assert!(!ManifestPath::from("img").is_dir());
        assert_eq!(dir.as_bytes(), b"img/");
    }
}

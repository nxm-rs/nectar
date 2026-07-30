//! Manifest paths: separator-joined bytes, exactly as both formats key them.

use alloc::vec::Vec;
use core::fmt;

/// A path into a manifest.
///
/// A path is absolute: it starts at the separator, so `"/"` names the manifest
/// itself and `"/index.html"` a file directly below it. Every constructor
/// enforces that, because the leading separator is in the stored bytes of both
/// formats and a relative key would address a different slot.
///
/// Nothing else is normalized: a trailing separator marks a directory and
/// survives a round trip, and `"/"` is an ordinary key that reads, lists and
/// writes like any other.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ManifestPath(Vec<u8>);

impl ManifestPath {
    /// The separator both formats read a directory boundary at.
    pub const SEPARATOR: u8 = b'/';

    /// The manifest root: the separator alone, the least of all paths.
    #[must_use]
    pub fn root() -> Self {
        Self(alloc::vec![Self::SEPARATOR])
    }

    /// Wrap `bytes` as a path, rooting it at the separator when it is relative.
    #[must_use]
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(rooted(bytes.into()))
    }

    /// Join `segments` below the root with the separator; empty segments are
    /// dropped, so no join ever emits a doubled separator.
    pub fn from_segments<I, S>(segments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<[u8]>,
    {
        let mut bytes = alloc::vec![Self::SEPARATOR];
        for segment in segments {
            let segment = segment.as_ref();
            if segment.is_empty() {
                continue;
            }
            if bytes.last() != Some(&Self::SEPARATOR) {
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

    /// The path bytes, as both formats store them.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consume into the path bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    /// Whether this is the manifest root, the separator alone.
    #[must_use]
    pub const fn is_root(&self) -> bool {
        matches!(self.0.as_slice(), [Self::SEPARATOR])
    }

    /// Whether the path names a directory: a trailing separator, the root
    /// included.
    #[must_use]
    pub fn is_dir(&self) -> bool {
        self.0.last().is_none_or(|&byte| byte == Self::SEPARATOR)
    }

    /// This path with `segment` appended below it, separated unless the path
    /// already ends in a separator.
    #[must_use]
    pub fn join(&self, segment: impl AsRef<[u8]>) -> Self {
        let segment = segment.as_ref();
        let mut bytes =
            Vec::with_capacity(self.0.len().saturating_add(segment.len()).saturating_add(1));
        bytes.extend_from_slice(&self.0);
        if !self.is_dir() {
            bytes.push(Self::SEPARATOR);
        }
        bytes.extend_from_slice(segment);
        Self(bytes)
    }
}

/// `bytes` rooted at the separator: prepended unless it already starts there.
fn rooted(bytes: Vec<u8>) -> Vec<u8> {
    if bytes.first() == Some(&ManifestPath::SEPARATOR) {
        return bytes;
    }
    let mut out = Vec::with_capacity(bytes.len().saturating_add(1));
    out.push(ManifestPath::SEPARATOR);
    out.extend_from_slice(&bytes);
    out
}

/// The root, so a defaulted path addresses the manifest itself.
impl Default for ManifestPath {
    fn default() -> Self {
        Self::root()
    }
}

impl AsRef<[u8]> for ManifestPath {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<&[u8]> for ManifestPath {
    fn from(bytes: &[u8]) -> Self {
        Self::new(bytes.to_vec())
    }
}

impl From<&str> for ManifestPath {
    fn from(path: &str) -> Self {
        Self::new(path.as_bytes().to_vec())
    }
}

impl From<Vec<u8>> for ManifestPath {
    fn from(bytes: Vec<u8>) -> Self {
        Self::new(bytes)
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
    fn every_path_is_rooted_at_the_separator() {
        assert_eq!(ManifestPath::root().as_bytes(), b"/");
        assert_eq!(ManifestPath::from("index.html").as_bytes(), b"/index.html");
        assert_eq!(ManifestPath::from("/index.html").as_bytes(), b"/index.html");
        assert_eq!(ManifestPath::default().as_bytes(), b"/");
        assert_eq!(ManifestPath::new(vec![]).as_bytes(), b"/");
    }

    #[test]
    fn rooting_a_path_is_idempotent() {
        let once = ManifestPath::from("img/logo.png");
        let twice = ManifestPath::new(once.as_bytes().to_vec());
        assert_eq!(once, twice);
    }

    #[test]
    fn the_root_is_the_least_path() {
        let root = ManifestPath::root();
        assert!(root < ManifestPath::from("index.html"));
        assert!(root < ManifestPath::from("!bang"));
        assert!(root.is_root());
        assert!(!ManifestPath::from("/x").is_root());
    }

    #[test]
    fn segments_drop_empty_runs() {
        let path = ManifestPath::from("/img//logo.png");
        assert_eq!(
            path.segments().collect::<Vec<_>>(),
            vec![&b"img"[..], &b"logo.png"[..]]
        );
        assert_eq!(ManifestPath::root().segments().count(), 0);
    }

    #[test]
    fn from_segments_never_doubles_the_separator() {
        let path = ManifestPath::from_segments(["img", "", "logo.png"]);
        assert_eq!(path.as_bytes(), b"/img/logo.png");
        assert_eq!(
            ManifestPath::from_segments::<[&str; 0], _>([]),
            ManifestPath::root()
        );
    }

    #[test]
    fn join_separates_only_below_a_file_path() {
        assert_eq!(ManifestPath::root().join("a").as_bytes(), b"/a");
        assert_eq!(ManifestPath::from("/img/").join("a").as_bytes(), b"/img/a");
        assert_eq!(ManifestPath::from("/img").join("a").as_bytes(), b"/img/a");
    }

    #[test]
    fn a_trailing_separator_survives() {
        let dir = ManifestPath::from("img/");
        assert!(dir.is_dir());
        assert!(!ManifestPath::from("img").is_dir());
        assert_eq!(dir.as_bytes(), b"/img/");
    }
}

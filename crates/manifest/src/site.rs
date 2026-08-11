//! The manifest's own configuration: the two site-level documents.
//!
//! Neither document is a key in the map. Each format stores the pair in its own
//! root slot: the trie as metadata on its `"/"` node, the key-value database in
//! its root manifest metadata.

use crate::path::ManifestPath;

/// The site-level documents a manifest declares.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SiteConfig {
    index_document: Option<ManifestPath>,
    error_document: Option<ManifestPath>,
}

impl SiteConfig {
    /// A configuration declaring neither document.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            index_document: None,
            error_document: None,
        }
    }

    /// The index document, served for a directory path.
    #[must_use]
    pub const fn index_document(&self) -> Option<&ManifestPath> {
        self.index_document.as_ref()
    }

    /// The error document, served for a path that resolves to nothing.
    #[must_use]
    pub const fn error_document(&self) -> Option<&ManifestPath> {
        self.error_document.as_ref()
    }

    /// Set the index document, or clear it with `None`.
    #[must_use]
    pub fn with_index_document(mut self, path: impl Into<Option<ManifestPath>>) -> Self {
        self.index_document = path.into();
        self
    }

    /// Set the error document, or clear it with `None`.
    #[must_use]
    pub fn with_error_document(mut self, path: impl Into<Option<ManifestPath>>) -> Self {
        self.error_document = path.into();
        self
    }

    /// Whether neither document is declared.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.index_document.is_none() && self.error_document.is_none()
    }

    /// Unwrap into the two documents.
    #[must_use]
    pub fn into_parts(self) -> (Option<ManifestPath>, Option<ManifestPath>) {
        (self.index_document, self.error_document)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_document_sets_and_clears_through_the_option() {
        let config = SiteConfig::new().with_index_document(ManifestPath::from("index.html"));
        assert_eq!(
            config.index_document().map(ManifestPath::as_bytes),
            Some(&b"index.html"[..])
        );
        assert!(config.error_document().is_none());
        assert!(!config.is_empty());

        let cleared = config.with_index_document(None);
        assert!(cleared.is_empty());
    }
}

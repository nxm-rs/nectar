//! Internal constants and well-known metadata keys.

/// Well-known metadata keys for manifest entries.
pub mod metadata {
    /// Root path for manifest-level metadata.
    pub const ROOT_PATH: &str = "/";

    /// Website index document suffix (e.g., "index.html").
    pub const WEBSITE_INDEX_DOCUMENT: &str = "website-index-document";

    /// Website error document path (e.g., "404.html").
    pub const WEBSITE_ERROR_DOCUMENT: &str = "website-error-document";

    /// Content type (MIME type) of an entry.
    pub const CONTENT_TYPE: &str = "Content-Type";

    /// Original filename of an entry.
    pub const FILENAME: &str = "Filename";
}

// Path separator used in Swarm manifests.
#[cfg(feature = "std")]
pub(crate) const PATH_SEPARATOR: &str = "/";

// Maximum prefix length in a fork (constrained by the 32-byte pre-reference region).
#[cfg(feature = "std")]
pub(crate) const PREFIX_MAX_LEN: usize = 30;

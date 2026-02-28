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
pub(crate) const PATH_SEPARATOR: &str = "/";

// Node header field sizes.
pub(crate) const NODE_OBFUSCATION_KEY_SIZE: usize = 32;
pub(crate) const VERSION_HASH_SIZE: usize = 31;
pub(crate) const NODE_REF_BYTES_SIZE: usize = 1;
pub(crate) const NODE_HEADER_SIZE: usize =
    NODE_OBFUSCATION_KEY_SIZE + VERSION_HASH_SIZE + NODE_REF_BYTES_SIZE;

// Fork layout constants.
pub(crate) const NODE_FORK_TYPE_BYTES_SIZE: usize = 1;
pub(crate) const NODE_FORK_PREFIX_BYTES_SIZE: usize = 1;
pub(crate) const NODE_FORK_HEADER_SIZE: usize =
    NODE_FORK_TYPE_BYTES_SIZE + NODE_FORK_PREFIX_BYTES_SIZE;
pub(crate) const NODE_FORK_PRE_REFERENCE_SIZE: usize = 32;
pub(crate) const NODE_PREFIX_MAX_SIZE: usize =
    NODE_FORK_PRE_REFERENCE_SIZE - NODE_FORK_HEADER_SIZE;
pub(crate) const NODE_FORK_METADATA_BYTES_SIZE: usize = 2;

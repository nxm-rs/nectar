//! Error types for mantaray operations.

use crate::ChunkStoreError;

/// Result type alias for mantaray operations.
pub type Result<T> = std::result::Result<T, MantarayError>;

/// Errors that can occur during mantaray trie operations.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum MantarayError {
    /// Node is not a value type (has no entry).
    #[error("not a value type")]
    NotValueType,
    /// No fork found for node with the given reference.
    #[error("no fork found for node: {ref_hex}")]
    NoForkFound {
        /// Hex-encoded reference of the node.
        ref_hex: String,
    },
    /// No entry found for node with the given reference.
    #[error("no entry found for node: {ref_hex}")]
    NoEntryFound {
        /// Hex-encoded reference of the node.
        ref_hex: String,
    },
    /// Entry exceeds maximum allowed size.
    #[error("entry too large: {size} > {max}")]
    EntryTooLarge {
        /// Actual size of the entry.
        size: usize,
        /// Maximum allowed size.
        max: usize,
    },
    /// Entry size does not match expected reference byte size.
    #[error("entry size mismatch: expected {expected}, got {actual}")]
    EntrySizeMismatch {
        /// Expected size.
        expected: usize,
        /// Actual size.
        actual: usize,
    },
    /// Path cannot be empty for this operation.
    #[error("empty path")]
    EmptyPath,
    /// Prefix not found in path.
    #[error("path prefix not found: {prefix}")]
    PathPrefixNotFound {
        /// The prefix that was not found.
        prefix: String,
    },
    /// Data is too short to contain a valid header.
    #[error("data too short for header")]
    DataTooShort,
    /// Version hash does not match any known version.
    #[error("invalid version hash")]
    InvalidVersionHash,
    /// Fork data has insufficient bytes.
    #[error("insufficient fork bytes: expected {expected}, got {actual} at byte {byte_index}")]
    InsufficientForkBytes {
        /// Expected number of bytes.
        expected: usize,
        /// Actual number of bytes.
        actual: usize,
        /// Byte index of the fork.
        byte_index: usize,
    },
    /// Reference is too long.
    #[error("reference too long: max {max}, got {actual}")]
    RefTooLong {
        /// Maximum allowed length.
        max: usize,
        /// Actual length.
        actual: usize,
    },
    /// Metadata exceeds maximum allowed size.
    #[error("metadata too large: max {max}, got {actual}")]
    MetadataTooLarge {
        /// Maximum allowed size.
        max: usize,
        /// Actual size.
        actual: usize,
    },
    /// Prefix length is invalid.
    #[error("invalid prefix length: max {max}, got {actual}")]
    InvalidPrefixLength {
        /// Maximum allowed length.
        max: usize,
        /// Actual length.
        actual: usize,
    },
    /// Metadata could not be parsed.
    #[error("invalid metadata: {message}")]
    InvalidMetadata {
        /// Description of the error.
        message: String,
    },
    /// Node has not been saved yet (reference is empty).
    #[error("missing reference")]
    MissingReference,
    /// Chunk store error.
    #[error(transparent)]
    Store(#[from] ChunkStoreError),
}

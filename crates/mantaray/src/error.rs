//! Error types for mantaray operations.

use std::sync::Arc;

use nectar_primitives::chunk::ChunkAddress;
use nectar_primitives::error::PrimitivesError;

/// Result type alias for mantaray operations.
pub type Result<T> = std::result::Result<T, MantarayError>;

/// Errors that can occur during mantaray trie operations.
#[derive(thiserror::Error, Debug)]
pub enum MantarayError {
    /// Node is not a value type (has no entry).
    #[error("not a value type")]
    NotValueType,
    /// No fork found for node with the given reference.
    #[error("no fork found for node: {}", reference.map_or_else(|| "<none>".to_string(), |a| a.to_string()))]
    NoForkFound {
        /// Reference of the node.
        reference: Option<ChunkAddress>,
    },
    /// No entry found for node with the given reference.
    #[error("no entry found for node: {}", reference.map_or_else(|| "<none>".to_string(), |a| a.to_string()))]
    NoEntryFound {
        /// Reference of the node.
        reference: Option<ChunkAddress>,
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
    #[error("invalid metadata")]
    Metadata(#[from] serde_json::Error),
    /// Node has not been saved yet (reference is empty).
    #[error("missing reference")]
    MissingReference,
    /// Error from primitives (chunk creation, BMT, etc.).
    #[error(transparent)]
    Primitives(#[from] PrimitivesError),
    /// Error from the typed chunk store during get operations.
    #[error("store get error: {source}")]
    StoreGet {
        /// Original store error, preserved for downcasting.
        source: Arc<dyn std::error::Error + Send + Sync>,
    },
    /// Error from the typed chunk store during put operations.
    #[error("store put error: {source}")]
    StorePut {
        /// Original store error, preserved for downcasting.
        source: Arc<dyn std::error::Error + Send + Sync>,
    },
}

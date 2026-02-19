//! Error types for mantaray operations.

extern crate alloc;

use alloc::string::String;
use core::fmt;

/// Result type alias for mantaray operations.
pub type Result<T> = core::result::Result<T, MantarayError>;

/// Errors that can occur during mantaray trie operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MantarayError {
    /// Node is not a value type (has no entry).
    NotValueType,
    /// No fork found for node with the given reference.
    NoForkFound {
        /// Hex-encoded reference of the node.
        ref_hex: String,
    },
    /// No entry found for node with the given reference.
    NoEntryFound {
        /// Hex-encoded reference of the node.
        ref_hex: String,
    },
    /// Entry exceeds maximum allowed size.
    EntryTooLarge {
        /// Actual size of the entry.
        size: usize,
        /// Maximum allowed size.
        max: usize,
    },
    /// Entry size does not match expected reference byte size.
    EntrySizeMismatch {
        /// Expected size.
        expected: usize,
        /// Actual size.
        actual: usize,
    },
    /// Path cannot be empty for this operation.
    EmptyPath,
    /// Prefix not found in path.
    PathPrefixNotFound {
        /// The prefix that was not found.
        prefix: String,
    },
    /// Data is too short to contain a valid header.
    DataTooShort,
    /// Version hash does not match any known version.
    InvalidVersionHash,
    /// Fork data has insufficient bytes.
    InsufficientForkBytes {
        /// Expected number of bytes.
        expected: usize,
        /// Actual number of bytes.
        actual: usize,
        /// Byte index of the fork.
        byte_index: usize,
    },
    /// Reference is too long.
    RefTooLong {
        /// Maximum allowed length.
        max: usize,
        /// Actual length.
        actual: usize,
    },
    /// Metadata exceeds maximum allowed size.
    MetadataTooLarge {
        /// Maximum allowed size.
        max: usize,
        /// Actual size.
        actual: usize,
    },
    /// Prefix length is invalid.
    InvalidPrefixLength {
        /// Maximum allowed length.
        max: usize,
        /// Actual length.
        actual: usize,
    },
    /// Metadata could not be parsed.
    InvalidMetadata {
        /// Description of the error.
        message: String,
    },
    /// No loader was provided for a load operation.
    NoLoader,
}

impl fmt::Display for MantarayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotValueType => write!(f, "not a value type"),
            Self::NoForkFound { ref_hex } => write!(f, "no fork found for node: {ref_hex}"),
            Self::NoEntryFound { ref_hex } => write!(f, "no entry found for node: {ref_hex}"),
            Self::EntryTooLarge { size, max } => {
                write!(f, "entry too large: {size} > {max}")
            }
            Self::EntrySizeMismatch { expected, actual } => {
                write!(f, "entry size mismatch: expected {expected}, got {actual}")
            }
            Self::EmptyPath => write!(f, "empty path"),
            Self::PathPrefixNotFound { prefix } => {
                write!(f, "path prefix not found: {prefix}")
            }
            Self::DataTooShort => write!(f, "data too short for header"),
            Self::InvalidVersionHash => write!(f, "invalid version hash"),
            Self::InsufficientForkBytes {
                expected,
                actual,
                byte_index,
            } => write!(
                f,
                "insufficient fork bytes: expected {expected}, got {actual} at byte {byte_index}"
            ),
            Self::RefTooLong { max, actual } => {
                write!(f, "reference too long: max {max}, got {actual}")
            }
            Self::MetadataTooLarge { max, actual } => {
                write!(f, "metadata too large: max {max}, got {actual}")
            }
            Self::InvalidPrefixLength { max, actual } => {
                write!(f, "invalid prefix length: max {max}, got {actual}")
            }
            Self::InvalidMetadata { message } => {
                write!(f, "invalid metadata: {message}")
            }
            Self::NoLoader => write!(f, "no loader provided"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for MantarayError {}

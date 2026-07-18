//! Rejections from the bound-carrying constructors.

use crate::meta::KeyId;

/// Prefix rejected: length exceeds the format's `PLEN_MAX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("prefix length {actual} exceeds the format maximum {max}")]
pub struct PrefixTooLong {
    /// Rejected length in bytes.
    pub actual: usize,
    /// The format's `PLEN_MAX`.
    pub max: usize,
}

/// Metadata length rejected: exceeds the format's `META_MAX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("encoded metadata length {actual} exceeds the format maximum {max}")]
pub struct MetadataTooLong {
    /// Rejected length in bytes.
    pub actual: usize,
    /// The format's `META_MAX`.
    pub max: usize,
}

/// Inline value rejected: length exceeds the format's `VINLINE_MAX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("inline value length {actual} exceeds the format maximum {max}")]
pub struct ValueTooLong {
    /// Rejected length in bytes.
    pub actual: usize,
    /// The format's `VINLINE_MAX`.
    pub max: usize,
}

/// Custom metadata key rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CustomKeyError {
    /// The empty key is not encodable: the wire key length is `1..`.
    #[error("custom metadata key is empty")]
    Empty,
    /// Length exceeds the format's `CKEY_MAX`.
    #[error("custom metadata key length {actual} exceeds the format maximum {max}")]
    TooLong {
        /// Rejected length in bytes.
        actual: usize,
        /// The format's `CKEY_MAX`.
        max: usize,
    },
    /// The key equals a registered name, which must travel as its id.
    #[error("custom metadata key duplicates the registered name {}", .0.name())]
    Registered(KeyId),
}

/// Segment weight rejected: exceeds the format's `BUDGET`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("segment weight {actual} exceeds the format budget {max}")]
pub struct WeightOverBudget {
    /// Rejected weight in bytes.
    pub actual: usize,
    /// The format's `BUDGET`.
    pub max: usize,
}

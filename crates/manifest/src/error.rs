//! Rejections from the bound-carrying constructors.

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

/// Segment weight rejected: exceeds the format's `BUDGET`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("segment weight {actual} exceeds the format budget {max}")]
pub struct WeightOverBudget {
    /// Rejected weight in bytes.
    pub actual: usize,
    /// The format's `BUDGET`.
    pub max: usize,
}

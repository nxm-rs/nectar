use thiserror::Error;

/// Errors from BMT operations.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum BmtError {
    /// A proof was requested for a segment index outside the tree.
    #[error("segment index {index} out of bounds: tree has {branches} segments")]
    SegmentOutOfBounds {
        /// The requested segment index.
        index: usize,
        /// The number of leaf segments in the tree.
        branches: usize,
    },
}

//! Constants used in the Binary Merkle Tree implementation

/// Hash size in bytes (keccak256)
pub(crate) const HASH_SIZE: usize = 32;

/// Size of a segment in the BMT (same as hash size)
pub(crate) const SEGMENT_SIZE: usize = HASH_SIZE;

/// Log2 of segment size for bit shifting
pub(crate) const SEGMENT_SIZE_LOG2: usize = 5; // 32 = 2^5

/// Length of a segment pair (two segments)
pub(crate) const SEGMENT_PAIR_LENGTH: usize = 2 * SEGMENT_SIZE;

/// Number of branches in the Binary Merkle Tree
pub(crate) const BRANCHES: usize = 128;

/// Default body size for chunks (128 branches * 32 byte segments = 4096)
pub const DEFAULT_BODY_SIZE: usize = BRANCHES * SEGMENT_SIZE;

/// Proof length in segments (log2(128) = 7)
pub(crate) const PROOF_LENGTH: usize = 7;

/// Compute number of zero tree levels for a given body size.
#[inline]
pub(crate) const fn zero_tree_levels(body_size: usize) -> usize {
    (body_size / SEGMENT_PAIR_LENGTH).trailing_zeros() as usize + 1
}

/// Compute number of branches for a given body size.
#[inline]
pub(crate) const fn branches_for_body_size(body_size: usize) -> usize {
    body_size / SEGMENT_SIZE
}

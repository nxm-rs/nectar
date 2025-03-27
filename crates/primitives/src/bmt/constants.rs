//! Constants used in the Binary Merkle Tree implementation

/// Default hash size in bytes
pub(crate) const HASH_SIZE: usize = 32;

/// Size of a segment in the BMT (same as hash size)
pub(crate) const SEGMENT_SIZE: usize = HASH_SIZE;

// Precomputed power-of-2 values for bit shifting operations
pub(crate) const SEGMENT_SIZE_LOG2: usize = 5; // SEGMENT_SIZE = 32 = 2^5

/// Number of branches in the Binary Merkle Tree
pub(crate) const BRANCHES: usize = 128;

/// The max data length for the Binary Merkle Tree (number of segments * segment size)
pub const MAX_DATA_LENGTH: usize = BRANCHES * SEGMENT_SIZE;

/// The length of a segment pair (two segments)
pub(crate) const SEGMENT_PAIR_LENGTH: usize = 2 * SEGMENT_SIZE;

/// Length of a BMT proof in segments
pub(crate) const PROOF_LENGTH: usize = 7;

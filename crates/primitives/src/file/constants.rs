//! Constants for file splitting and joining.

use crate::bmt::{BRANCHES, DEFAULT_BODY_SIZE, HASH_SIZE};

/// Derive the maximum tree depth from branching factor and body size.
/// The limit is the number of levels needed to address all storable data:
/// `branches^(limit-1) * body_size` must exceed any practical file size.
const fn compute_level_limit(branches: usize, body_size: usize) -> usize {
    // bits needed = ceil(log2(u64::MAX / body_size)) / log2(branches)
    // For branches=128 (7 bits), body_size=4096 (12 bits): (64-12)/7 + 1 = 8.4 → 9
    let body_bits = body_size.trailing_zeros() as usize;
    let branch_bits = branches.trailing_zeros() as usize;
    (64 - body_bits).div_ceil(branch_bits) + 1
}

/// Maximum tree depth (derived from BRANCHES and DEFAULT_BODY_SIZE).
pub(crate) const LEVEL_LIMIT: usize = compute_level_limit(BRANCHES, DEFAULT_BODY_SIZE);
const _: () = assert!(LEVEL_LIMIT == 9);

/// Size of a chunk reference (hash). Same as bmt::HASH_SIZE.
pub(crate) const REF_SIZE: usize = HASH_SIZE;

/// Number of references per intermediate chunk (plain mode). Same as bmt::BRANCHES.
#[cfg(test)]
const REFS_PER_CHUNK: usize = BRANCHES;

/// Compute span multipliers for a given branching factor.
/// `spans[i] = branches^i`, representing how many level-0 refs each level-i ref covers.
const fn compute_spans(branches: usize) -> [u64; LEVEL_LIMIT] {
    let mut spans = [0u64; LEVEL_LIMIT];
    let mut span = 1u64;
    let mut i = 0;
    while i < LEVEL_LIMIT {
        spans[i] = span;
        span = span.saturating_mul(branches as u64);
        i += 1;
    }
    spans
}

/// Size of an encrypted chunk reference (address + decryption key).
pub(crate) const ENCRYPTED_REF_SIZE: usize = crate::chunk::encryption::EncryptedChunkRef::SIZE;

/// Compute span multipliers for an arbitrary branching factor.
/// Used by `TreeParams` which derives its branching factor from const generics,
/// and by splitters for intermediate span calculation.
pub(crate) const fn compute_spans_inline(branches: usize) -> [u64; LEVEL_LIMIT] {
    compute_spans(branches)
}

/// Assert that BODY_SIZE is a valid chunk body size (power of 2, >= 64).
pub(crate) const fn assert_valid_body_size<const BODY_SIZE: usize>() {
    assert!(BODY_SIZE >= 64, "BODY_SIZE must be at least 64");
    assert!(
        BODY_SIZE.is_power_of_two(),
        "BODY_SIZE must be a power of 2"
    );
}

/// Calculate tree depth for a given file size using integer arithmetic.
pub(crate) const fn tree_depth(length: u64, chunk_size: usize, ref_size: usize) -> usize {
    if length == 0 {
        return 0;
    }

    let branches = (chunk_size / ref_size) as u64;

    // div_ceil(length, chunk_size)
    let data_chunks = length.div_ceil(chunk_size as u64);
    if data_chunks <= 1 {
        return 1;
    }

    let mut depth = 1;
    let mut chunks = data_chunks;
    while chunks > 1 {
        chunks = chunks.div_ceil(branches);
        depth += 1;
    }
    depth
}

/// Calculate subspan size for children of a node with given span, using the
/// provided span multiplier table.
#[inline]
pub(crate) fn subspan_for_spans<const BODY_SIZE: usize>(
    span: u64,
    spans: &[u64; LEVEL_LIMIT],
) -> u64 {
    for i in 0..LEVEL_LIMIT {
        let level_span = spans[i] * BODY_SIZE as u64;
        if span <= level_span {
            return if i == 0 {
                BODY_SIZE as u64
            } else {
                spans[i - 1] * BODY_SIZE as u64
            };
        }
    }
    spans[LEVEL_LIMIT - 2] * BODY_SIZE as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bmt::DEFAULT_BODY_SIZE;
    use crate::file::levels;

    const SPANS: [u64; LEVEL_LIMIT] = compute_spans(REFS_PER_CHUNK);

    /// Calculate span for a chunk at a given level and file position.
    fn span_for_level(level: usize, position: u64, chunk_size: usize) -> u64 {
        let span_size = SPANS[level] * chunk_size as u64;
        (position - 1) % span_size + 1
    }

    #[test]
    fn test_spans_values() {
        assert_eq!(SPANS[0], 1);
        assert_eq!(SPANS[1], 128);
        assert_eq!(SPANS[2], 128 * 128);
        assert_eq!(SPANS[3], 128 * 128 * 128);
    }

    #[test]
    fn test_levels_empty() {
        assert_eq!(levels(0, DEFAULT_BODY_SIZE), 0);
    }

    #[test]
    fn test_levels_single_chunk() {
        // Up to 4096 bytes fits in one chunk
        assert_eq!(levels(1, DEFAULT_BODY_SIZE), 1);
        assert_eq!(levels(4096, DEFAULT_BODY_SIZE), 1);
    }

    #[test]
    fn test_levels_two_chunks() {
        // 4097 bytes needs 2 data chunks + 1 intermediate
        assert_eq!(levels(4097, DEFAULT_BODY_SIZE), 2);
        // Up to 128 chunks (524288 bytes) still fits in level 2
        assert_eq!(levels(524288, DEFAULT_BODY_SIZE), 2);
    }

    #[test]
    fn test_levels_three_levels() {
        // 129 chunks needs level 3
        assert_eq!(levels(524289, DEFAULT_BODY_SIZE), 3);
    }

    #[test]
    fn test_levels_boundary_128_squared() {
        // Exactly 128^2 * 4096 = 67108864 bytes should be depth 3
        let boundary = 128 * 128 * DEFAULT_BODY_SIZE;
        assert_eq!(levels(boundary as u64, DEFAULT_BODY_SIZE), 3);
        // One byte more needs depth 4
        assert_eq!(levels(boundary as u64 + 1, DEFAULT_BODY_SIZE), 4);
    }

    #[test]
    fn test_span_for_level() {
        // At level 0, span equals position for first chunk
        assert_eq!(span_for_level(0, 100, DEFAULT_BODY_SIZE), 100);
        assert_eq!(span_for_level(0, 4096, DEFAULT_BODY_SIZE), 4096);

        // At level 1, span wraps at 4096 * 128 = 524288
        assert_eq!(span_for_level(1, 524288, DEFAULT_BODY_SIZE), 524288);
        assert_eq!(span_for_level(1, 524289, DEFAULT_BODY_SIZE), 1);
    }
}

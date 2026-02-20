//! Constants for file splitting and joining.

use crate::bmt::{BRANCHES, DEFAULT_BODY_SIZE, HASH_SIZE};

/// Maximum tree depth (supports up to 128^8 * 4096 bytes ≈ 295 exabytes).
pub(crate) const LEVEL_LIMIT: usize = 9;

/// Size of a chunk reference (hash). Same as bmt::HASH_SIZE.
pub(crate) const REF_SIZE: usize = HASH_SIZE;

/// Number of references per intermediate chunk. Same as bmt::BRANCHES.
pub(crate) const REFS_PER_CHUNK: usize = BRANCHES;

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

/// Span multipliers per level for plain trees.
/// SPANS[i] = 128^i
pub(crate) static SPANS: [u64; LEVEL_LIMIT] = compute_spans(REFS_PER_CHUNK);

/// Size of an encrypted chunk reference (address + decryption key).
pub(crate) const ENCRYPTED_REF_SIZE: usize = 64;

/// Number of encrypted references per intermediate chunk.
pub(crate) const ENCRYPTED_REFS_PER_CHUNK: usize = DEFAULT_BODY_SIZE / ENCRYPTED_REF_SIZE;

/// Span multipliers per level for encrypted trees.
/// ENCRYPTED_SPANS[i] = 64^i
pub(crate) static ENCRYPTED_SPANS: [u64; LEVEL_LIMIT] = compute_spans(ENCRYPTED_REFS_PER_CHUNK);

/// Compute span multipliers for an arbitrary branching factor at runtime.
/// Used by `TreeParams` which derives its branching factor from const generics.
pub(crate) const fn compute_spans_inline(branches: usize) -> [u64; LEVEL_LIMIT] {
    compute_spans(branches)
}

/// Calculate tree depth for a given file size and reference size.
pub(crate) fn tree_depth(length: u64, chunk_size: usize, ref_size: usize) -> usize {
    if length == 0 {
        return 0;
    }

    let section_size = ref_size as u64;
    let branches = (chunk_size / ref_size) as u64;

    if length <= section_size * branches {
        return 1;
    }

    let chunks = (length - 1) / section_size;
    (chunks as f64).log(branches as f64) as usize + 1
}

/// Calculate subspan size for children of a node with given span, using the
/// provided span multiplier table.
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
    fn test_span_for_level() {
        // At level 0, span equals position for first chunk
        assert_eq!(span_for_level(0, 100, DEFAULT_BODY_SIZE), 100);
        assert_eq!(span_for_level(0, 4096, DEFAULT_BODY_SIZE), 4096);

        // At level 1, span wraps at 4096 * 128 = 524288
        assert_eq!(span_for_level(1, 524288, DEFAULT_BODY_SIZE), 524288);
        assert_eq!(span_for_level(1, 524289, DEFAULT_BODY_SIZE), 1);
    }
}

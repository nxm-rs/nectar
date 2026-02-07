//! Constants for file splitting and joining.

use crate::bmt::DEFAULT_BODY_SIZE;

/// Maximum tree depth (supports up to 128^8 * 4096 bytes ≈ 295 exabytes).
pub const LEVEL_LIMIT: usize = 9;

/// Size of a chunk reference (hash).
pub const REF_SIZE: usize = 32;

/// Span header size in bytes.
pub const SPAN_SIZE: usize = 8;

/// Number of references per intermediate chunk.
pub const REFS_PER_CHUNK: usize = DEFAULT_BODY_SIZE / REF_SIZE; // 128

/// Span multipliers per level.
/// SPANS[i] = 128^i, representing how many level-0 refs each level-i ref covers.
pub static SPANS: [u64; LEVEL_LIMIT] = compute_spans();

const fn compute_spans() -> [u64; LEVEL_LIMIT] {
    let mut spans = [0u64; LEVEL_LIMIT];
    let mut span = 1u64;
    let mut i = 0;
    while i < LEVEL_LIMIT {
        spans[i] = span;
        span = span.saturating_mul(REFS_PER_CHUNK as u64);
        i += 1;
    }
    spans
}

/// Calculate tree depth for a given file size.
/// Returns the level of the root hash (1-indexed, 0 for empty).
pub fn levels(length: u64, chunk_size: usize) -> usize {
    if length == 0 {
        return 0;
    }

    let section_size = REF_SIZE as u64;
    let branches = (chunk_size / REF_SIZE) as u64;

    if length <= section_size * branches {
        return 1;
    }

    let chunks = (length - 1) / section_size;
    (chunks as f64).log(branches as f64) as usize + 1
}

/// Calculate span for a chunk at a given level and file position.
/// This is the amount of original file data the chunk represents.
pub fn span_for_level(level: usize, position: u64, chunk_size: usize) -> u64 {
    let span_size = SPANS[level] * chunk_size as u64;
    (position - 1) % span_size + 1
}

#[cfg(test)]
mod tests {
    use super::*;

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

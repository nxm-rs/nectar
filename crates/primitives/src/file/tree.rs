//! Tree structure calculations for parallel file operations.

use super::constants::{LEVEL_LIMIT, REF_SIZE};
use crate::bmt::DEFAULT_BODY_SIZE;

/// Tree structure for a file of known size.
///
/// Pre-computes chunk counts and spans for efficient parallel operations.
/// The branching factor defaults to `BODY_SIZE / 32` (plain mode, 128 branches).
/// Use [`with_ref_size`](Self::with_ref_size) for encrypted mode (`BODY_SIZE / 64` = 64 branches).
#[derive(Debug, Clone, Copy)]
pub struct TreeParams<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    size: u64,
    depth: usize,
    data_chunks: u64,
    branches: usize,
}

impl<const BODY_SIZE: usize> TreeParams<BODY_SIZE> {
    /// Create tree parameters for a file of given size (plain mode, `REF_SIZE = 32`).
    pub fn new(size: u64) -> Self {
        Self::with_ref_size(size, REF_SIZE)
    }

    /// Create tree parameters with a custom reference size (e.g. 64 for encrypted mode).
    pub fn with_ref_size(size: u64, ref_size: usize) -> Self {
        let branches = BODY_SIZE / ref_size;
        let (depth, data_chunks) = if size == 0 {
            (1, 1) // Empty file still produces one chunk
        } else {
            let data_chunks = size.div_ceil(BODY_SIZE as u64);
            let depth = Self::calculate_depth_with(data_chunks, branches);
            (depth, data_chunks)
        };

        Self {
            size,
            depth,
            data_chunks,
            branches,
        }
    }

    /// File size in bytes.
    pub const fn size(&self) -> u64 {
        self.size
    }

    /// Tree depth (1 = single chunk, 2 = one intermediate level, etc.).
    pub const fn depth(&self) -> usize {
        self.depth
    }

    /// Number of data chunks (level 0).
    pub const fn data_chunks(&self) -> u64 {
        self.data_chunks
    }

    /// Total chunks across all levels.
    pub const fn total_chunks(&self) -> u64 {
        let mut total = self.data_chunks;
        let mut chunks_at_level = self.data_chunks;

        while chunks_at_level > 1 {
            chunks_at_level = chunks_at_level.div_ceil(self.branches as u64);
            total += chunks_at_level;
        }

        total
    }

    /// Chunks at a specific level.
    pub fn chunks_at_level(&self, level: usize) -> u64 {
        if level >= self.depth {
            return 0;
        }
        if level == 0 {
            return self.data_chunks;
        }

        let mut chunks = self.data_chunks;
        for _ in 0..level {
            chunks = chunks.div_ceil(self.branches as u64);
        }
        chunks
    }

    /// Span for a chunk at given level and index.
    pub fn span_at(&self, level: usize, index: u64) -> u64 {
        let spans = super::constants::compute_spans_inline(self.branches);
        let max_span = spans[level] * BODY_SIZE as u64;
        let chunks_at_level = self.chunks_at_level(level);

        if index + 1 == chunks_at_level {
            // Last chunk may have smaller span
            let preceding = index * max_span;
            let level_total = self.level_span(level);
            level_total.saturating_sub(preceding)
        } else {
            max_span
        }
    }

    /// Total bytes covered by chunks at this level.
    const fn level_span(&self, _level: usize) -> u64 {
        self.size
    }

    /// Byte offset for start of chunk at level 0.
    pub const fn chunk_offset(&self, chunk_index: u64) -> u64 {
        chunk_index * BODY_SIZE as u64
    }

    /// Byte range covered by a data chunk.
    pub fn chunk_range(&self, chunk_index: u64) -> (u64, u64) {
        let start = self.chunk_offset(chunk_index);
        let end = (start + BODY_SIZE as u64).min(self.size);
        (start, end)
    }

    /// Calculate required data chunks for a byte range.
    pub fn chunks_for_range(&self, offset: u64, len: u64) -> ChunkRange {
        if len == 0 || offset >= self.size {
            return ChunkRange {
                start: 0,
                end: 0,
            };
        }

        let end_offset = (offset + len).min(self.size);
        let start_chunk = offset / BODY_SIZE as u64;
        let end_chunk = end_offset.div_ceil(BODY_SIZE as u64);

        ChunkRange {
            start: start_chunk,
            end: end_chunk.min(self.data_chunks),
        }
    }

    fn calculate_depth_with(data_chunks: u64, branches: usize) -> usize {
        if data_chunks <= 1 {
            return 1;
        }

        let mut depth = 1;
        let mut chunks = data_chunks;
        while chunks > 1 {
            chunks = chunks.div_ceil(branches as u64);
            depth += 1;
        }
        depth.min(LEVEL_LIMIT)
    }
}

/// Range of chunk indices.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkRange {
    /// First chunk index (inclusive).
    pub start: u64,
    /// Last chunk index (exclusive).
    pub end: u64,
}

impl ChunkRange {
    /// Number of chunks in range.
    pub const fn len(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    /// Whether the range is empty.
    pub const fn is_empty(&self) -> bool {
        self.start >= self.end
    }

    /// Iterate over chunk indices.
    pub fn iter(&self) -> impl Iterator<Item = u64> {
        self.start..self.end
    }
}

/// Assemble decoded chunk bodies into a contiguous output buffer.
///
/// Given a set of chunk bodies corresponding to `chunk_range`, copies the
/// relevant byte slices into a single output buffer accounting for partial
/// reads at the start/end of the range.
pub(crate) fn assemble_range<const BODY_SIZE: usize>(
    tree: &TreeParams<BODY_SIZE>,
    offset: u64,
    actual_len: usize,
    chunk_range: &ChunkRange,
    bodies: &[bytes::Bytes],
) -> Vec<u8> {
    let mut result = vec![0u8; actual_len];
    let mut result_offset = 0;

    for (i, chunk_idx) in chunk_range.iter().enumerate() {
        let (chunk_start, chunk_end) = tree.chunk_range(chunk_idx);
        let chunk_data_len = (chunk_end - chunk_start) as usize;

        let read_start = if chunk_start < offset {
            (offset - chunk_start) as usize
        } else {
            0
        };

        let read_end = if chunk_end > offset + actual_len as u64 {
            chunk_data_len - ((chunk_end - offset - actual_len as u64) as usize)
        } else {
            chunk_data_len
        };

        let bytes_to_copy = read_end - read_start;
        let body = &bodies[i];

        result[result_offset..result_offset + bytes_to_copy]
            .copy_from_slice(&body[read_start..read_end]);
        result_offset += bytes_to_copy;
    }

    result
}

/// Calculate subspan size for children of a node with given span (plain mode).
#[cfg(test)]
fn subspan_size<const BODY_SIZE: usize>(span: u64) -> u64 {
    let spans = super::constants::compute_spans_inline(BODY_SIZE / super::constants::REF_SIZE);
    super::constants::subspan_for_spans::<BODY_SIZE>(span, &spans)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tree_params_empty() {
        let tree = TreeParams::<DEFAULT_BODY_SIZE>::new(0);
        assert_eq!(tree.size(), 0);
        assert_eq!(tree.depth(), 1);
        assert_eq!(tree.data_chunks(), 1);
        assert_eq!(tree.total_chunks(), 1);
    }

    #[test]
    fn test_tree_params_single_chunk() {
        let tree = TreeParams::<DEFAULT_BODY_SIZE>::new(100);
        assert_eq!(tree.depth(), 1);
        assert_eq!(tree.data_chunks(), 1);
        assert_eq!(tree.total_chunks(), 1);
        assert_eq!(tree.chunks_at_level(0), 1);
        assert_eq!(tree.chunks_at_level(1), 0);
    }

    #[test]
    fn test_tree_params_two_chunks() {
        let tree = TreeParams::<DEFAULT_BODY_SIZE>::new(DEFAULT_BODY_SIZE as u64 + 1);
        assert_eq!(tree.depth(), 2);
        assert_eq!(tree.data_chunks(), 2);
        assert_eq!(tree.total_chunks(), 3); // 2 data + 1 intermediate
        assert_eq!(tree.chunks_at_level(0), 2);
        assert_eq!(tree.chunks_at_level(1), 1);
    }

    #[test]
    fn test_tree_params_128_chunks() {
        let size = DEFAULT_BODY_SIZE as u64 * 128;
        let tree = TreeParams::<DEFAULT_BODY_SIZE>::new(size);
        assert_eq!(tree.depth(), 2);
        assert_eq!(tree.data_chunks(), 128);
        assert_eq!(tree.chunks_at_level(0), 128);
        assert_eq!(tree.chunks_at_level(1), 1);
    }

    #[test]
    fn test_tree_params_129_chunks() {
        let size = DEFAULT_BODY_SIZE as u64 * 129;
        let tree = TreeParams::<DEFAULT_BODY_SIZE>::new(size);
        assert_eq!(tree.depth(), 3);
        assert_eq!(tree.data_chunks(), 129);
        assert_eq!(tree.chunks_at_level(0), 129);
        assert_eq!(tree.chunks_at_level(1), 2);
        assert_eq!(tree.chunks_at_level(2), 1);
    }

    #[test]
    fn test_chunk_range() {
        let tree = TreeParams::<DEFAULT_BODY_SIZE>::new(10000);

        // First chunk
        let (start, end) = tree.chunk_range(0);
        assert_eq!(start, 0);
        assert_eq!(end, 4096);

        // Second chunk
        let (start, end) = tree.chunk_range(1);
        assert_eq!(start, 4096);
        assert_eq!(end, 8192);

        // Last partial chunk
        let (start, end) = tree.chunk_range(2);
        assert_eq!(start, 8192);
        assert_eq!(end, 10000);
    }

    #[test]
    fn test_chunks_for_range() {
        let tree = TreeParams::<DEFAULT_BODY_SIZE>::new(10000);

        // Read from start
        let range = tree.chunks_for_range(0, 100);
        assert_eq!(range.start, 0);
        assert_eq!(range.end, 1);
        assert_eq!(range.len(), 1);

        // Read spanning chunks
        let range = tree.chunks_for_range(4000, 200);
        assert_eq!(range.start, 0);
        assert_eq!(range.end, 2);
        assert_eq!(range.len(), 2);

        // Read at end
        let range = tree.chunks_for_range(9000, 1000);
        assert_eq!(range.start, 2);
        assert_eq!(range.end, 3);
    }

    #[test]
    fn test_span_at() {
        let size = DEFAULT_BODY_SIZE as u64 * 3;
        let tree = TreeParams::<DEFAULT_BODY_SIZE>::new(size);

        // Full chunks have full span
        assert_eq!(tree.span_at(0, 0), DEFAULT_BODY_SIZE as u64);
        assert_eq!(tree.span_at(0, 1), DEFAULT_BODY_SIZE as u64);
        assert_eq!(tree.span_at(0, 2), DEFAULT_BODY_SIZE as u64);
    }

    #[test]
    fn test_subspan_size() {
        // Single chunk span -> subspan is chunk size
        assert_eq!(subspan_size::<DEFAULT_BODY_SIZE>(4096), 4096);

        // Two chunk span -> subspan is chunk size
        assert_eq!(subspan_size::<DEFAULT_BODY_SIZE>(8000), 4096);

        // Large span -> subspan is previous level
        let large_span = 128 * DEFAULT_BODY_SIZE as u64 + 1;
        assert_eq!(subspan_size::<DEFAULT_BODY_SIZE>(large_span), 128 * 4096);
    }

    #[test]
    fn test_encrypted_tree_params() {
        use super::super::constants::ENCRYPTED_REF_SIZE;

        // 64 data chunks fills one encrypted intermediate exactly
        let size = DEFAULT_BODY_SIZE as u64 * 64;
        let tree = TreeParams::<DEFAULT_BODY_SIZE>::with_ref_size(size, ENCRYPTED_REF_SIZE);
        assert_eq!(tree.depth(), 2); // 64 chunks / 64 branches = 1 intermediate
        assert_eq!(tree.data_chunks(), 64);
        assert_eq!(tree.chunks_at_level(0), 64);
        assert_eq!(tree.chunks_at_level(1), 1);
        assert_eq!(tree.total_chunks(), 65);

        // 65 data chunks needs a third level
        let size2 = DEFAULT_BODY_SIZE as u64 * 65;
        let tree2 = TreeParams::<DEFAULT_BODY_SIZE>::with_ref_size(size2, ENCRYPTED_REF_SIZE);
        assert_eq!(tree2.depth(), 3);
        assert_eq!(tree2.chunks_at_level(0), 65);
        assert_eq!(tree2.chunks_at_level(1), 2);
        assert_eq!(tree2.chunks_at_level(2), 1);
    }
}
